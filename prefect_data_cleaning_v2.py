import coiled

import logging
import os
from typing import List

# PyPI
import prefect
from prefect import task, Flow, Parameter
# from prefect.engine.executors.dask import DaskExecutor, LocalDaskExecutor
from prefect.executors.dask import DaskExecutor, LocalDaskExecutor
from prefect.utilities.edges import unmapped
import boto3
from botocore.exceptions import ClientError
from tqdm import tqdm
import pandas as pd


def initialize_s3_client(region_name: str) -> boto3.client:
    return boto3.client('s3', region_name=region_name)


@task(log_stdout=True)
def fetch_aws_folders(region_name, bucket_name):
    s3_client = initialize_s3_client(region_name)
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix='', Delimiter='/')
    def yield_folders(response):
        for content in response.get('CommonPrefixes', []):
            yield content.get('Prefix')
    folder_list = yield_folders(response)
    # remove '/' from end of each folder name
    folder_list = [x.split('/')[0] for x in folder_list]
    return sorted(folder_list)[:5]
    # return folder_list


def csv_clean_spatial_check(filename, data):
    records = pd.read_csv(data, #working_dir / file_, 
                          dtype={'FRSHTT': str, 'TEMP': str, 'LATITUDE': str, 'LONGITUDE': str, 'ELEVATION': str, 'DATE': str}
    )
    records.columns = records.columns.str.strip()
    # remove site files with no spatial data
    if (
        not records['STATION'].eq(records['STATION'].iloc[0]).all()
        or not records['LATITUDE'].eq(records['LATITUDE'].iloc[0]).all()
        or not records['LONGITUDE'].eq(records['LONGITUDE'].iloc[0]).all()
        or not records['ELEVATION'].eq(records['ELEVATION'].iloc[0]).all()
    ):
        return filename


def move_s3_file(
    filename: str, 
    bucket_name: str, 
    s3_client: boto3.client,
    note: str
):
    # create bucket object
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(bucket_name)
    # ensure data error folder exists
    s3_client.put_object(Bucket=bucket_name, Body='', Key=f'_data_errors/')
    # ensure year folder exists
    # s3_client.put_object(Bucket=bucket_name, Body='', Key=f'_data_errors/{filename.split("/")[0]}/')
    # Copy object A as object B
    year, file_ = filename.split('/')
    number = file_.split('.')[0]
    copy_source = {'Bucket': bucket_name, 'Key': filename}
    bucket.copy(copy_source, f'_data_errors/{year}-{number}-{note}.csv')
    # Delete object A
    s3_resource.Object(bucket_name, filename).delete()


def s3_upload_file(s3_client: boto3.client, file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    Args:
        s3_client: initated boto3 s3_client object
        file_name (str): File to upload
        bucket (str): target AWS bucket
        object_name (str): S3 object name. If not specified then file_name is used [Optional]
    
    Return (bool): True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    try:
        s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


@task(log_stdout=True)
def aws_all_year_files(year: list, bucket_name: str, region_name: str):
    if year == '':
        return []
    s3_client = initialize_s3_client(region_name)
    aws_file_set = set()
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=year)
    for page in pages:
        list_all_keys = page['Contents']
        # item arrives in format of 'year/filename'; this extracts that
        file_l = [x['Key'] for x in list_all_keys]
        for f in file_l:
            aws_file_set.add(f)
    return list(sorted(aws_file_set))


@task(log_stdout=True)
def aws_lists_prep_for_map(file_l: list, list_size: int) -> List[list]:
    def chunks(file_l, list_size):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(file_l), list_size):
            yield file_l[i:i + list_size]
    file_l_consolidated = [i for l in file_l for i in l]
    return list(chunks(file_l_consolidated, list_size))


@task(log_stdout=True)
def process_year_files(files_l: list, region_name: str, bucket_name: str):
    s3_client = initialize_s3_client(region_name)
    for filename in tqdm(files_l):
        if len(filename) <= 5:
            continue
        else:
            obj = s3_client.get_object(Bucket=bucket_name, Key=filename) 
            missing_spatial = csv_clean_spatial_check(
                filename=filename,
                data=obj['Body']
                # db_connection=db_connection
            )
            if missing_spatial:
                move_s3_file(missing_spatial, bucket_name, s3_client, note='missing_spatial')
    print('TASK')


if os.environ.get('EXECUTOR') == 'coiled':
    print("Coiled")
    coiled.create_software_environment(
        name="NOAA-temperature-data-clean1",
        pip="requirements.txt"
    )
    executor = DaskExecutor(
        debug=True,
        cluster_class=coiled.Cluster,
        cluster_kwargs={
            "shutdown_on_close": True,
            "name": "NOAA-temperature-data-clean1",
            "software": "darrida/noaa-temperature-data-clean1",
            "worker_cpu": 4,
            "n_workers": 8,
            "worker_memory":"16 GiB",
            "scheduler_memory": "16 GiB",
        },
    )
else:
    executor=LocalDaskExecutor(scheduler="threads", num_workers=14)
        

with Flow(name="NOAA-files-upload-to-AWS") as flow:#, executor=executor) as flow:
    # working_dir = Parameter('WORKING_LOCAL_DIR', default=Path('/mnt/c/Users/benha/data_downloads/noaa_global_temps'))
    region_name = Parameter('REGION_NAME', default='us-east-1')
    bucket_name = Parameter('BUCKET_NAME', default='noaa-temperature-data')
    map_list_size = Parameter('MAP_LIST_SIZE', default=1000)
    t1_aws_years = fetch_aws_folders(region_name, bucket_name)
    t2_all_files = aws_all_year_files.map(t1_aws_years, unmapped(bucket_name), unmapped(region_name))
    t3_map_prep_l = aws_lists_prep_for_map(t2_all_files, map_list_size)
    t4_result = process_year_files.map(t3_map_prep_l, unmapped(region_name), unmapped(bucket_name))


if __name__ == '__main__':
    state = flow.run(executor=executor)
    # flow.visualize(flow_state=state)
    print(state.is_successful())
    # assert state.is_successful()