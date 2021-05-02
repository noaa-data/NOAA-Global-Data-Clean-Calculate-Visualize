import coiled

coiled.create_software_environment(
    name="NOAA-temperature-data-cleaning",
    conda="environment.yml",
)

from datetime import datetime
import logging
import os
from pathlib import Path
import io
from pprint import pprint

# PyPI
from prefect import task, Flow, Parameter
from prefect.schedules import IntervalSchedule
from prefect.tasks.secrets import PrefectSecret
from prefect.executors.dask import LocalDaskExecutor, DaskExecutor
import boto3
from botocore.exceptions import ClientError
from prefect.utilities.edges import unmapped
from tqdm import tqdm



import csv, os
from pathlib import Path
import pandas as pd
from tqdm import tqdm

data_dir = Path('/mnt/c/Users/benha/data_downloads/noaa_global_temps')
working_dir = data_dir / '1931'
save_dir = Path('csvs')

region_name = 'us-east-2'#Parameter('REGION_NAME', default='us-east-2')
bucket_name = 'noaa-temperature-data'#Parameter('BUCKET_NAME', default='noaa-temperature-data')

# files = os.listdir(str(working_dir))

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
    return [x.split('/')[0] for x in folder_list]


def csv_clean_mismatched(filename, data, exclude_file):
    records = pd.read_csv(data, #working_dir / file_, 
                          dtype={'FRSHTT': str, 'TEMP': str, 'LATITUDE': str, 'LONGITUDE': str, 'ELEVATION': str, 'DATE': str}
    )
#     with pd.option_context("display.min_rows", 50, "display.max_rows", 100, "display.max_columns", 40):
#         display(records)
    records.columns = records.columns.str.strip()
    # remove site files with no spatial data
    if (
        not records['STATION'].eq(records['STATION'].iloc[0]).all()
#         or not records['DATE'].eq(records['DATE'].iloc[0]).all()
        or not records['LATITUDE'].eq(records['LATITUDE'].iloc[0]).all()
        or not records['LONGITUDE'].eq(records['LONGITUDE'].iloc[0]).all()
        or not records['ELEVATION'].eq(records['ELEVATION'].iloc[0]).all()
    ):
        with open(exclude_file, 'a') as f:
            f.write(filename)
            f.write('\n')
        return filename

# missing_data_l = []

with open(save_dir / '_exclude.csv', 'w') as f:
    pass


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
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def aws_year_files(bucket_name: str, region_name: str, year: str):
    s3_client = initialize_s3_client(region_name)
    # If not exists - creates year folder in aws
    s3_client.put_object(Bucket=bucket_name, Body='', Key=f'{year}/')
    # File difference between local and aws for indidivual folder/year
    aws_file_set = set()
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=year)
    for page in tqdm(pages):
        list_all_keys = page['Contents']
        # item arrives in format of 'year/filename'; this removes 'year/'
        file_l = [x['Key'].split('/')[1] for x in list_all_keys]
        for f in file_l:
            aws_file_set.add(f)
    # print(aws_file_set)
    return list(sorted(aws_file_set))


# for year in aws_folders:
#     print(year)
#     aws_files = aws_year_files(bucket_name, year)
    
@task(log_stdout=True)
def process_year_files(year, region_name, bucket_name):
    print(year)
    s3_client = initialize_s3_client(region_name)
    aws_files = aws_year_files(bucket_name, region_name, year)
    for file_ in tqdm(aws_files, desc=year):
        if file_ == '':
            continue
        obj = s3_client.get_object(Bucket=bucket_name, Key=f'{year}/{file_}') 
        missing = csv_clean_mismatched(
            filename=f'{year}/{file_}',
            data=obj['Body'],
            exclude_file=save_dir / '_exclude.csv'
        )
        # if missing:
        #     missing_data_l.append(missing)


# print(len(missing_data_l))
# print(missing_data_l)


executor=LocalDaskExecutor(scheduler="threads", num_workers=14)

# executor = DaskExecutor(
#     debug=True,
#     cluster_class=coiled.Cluster,
#     cluster_kwargs={
#         #"software": "jrbourbeau/prefect",
#         "shutdown_on_close": False,
#         "name": "NOAA-temperature-data-cleaning",
#     },
# )

region_name = 'us-east-2'
bucket_name = 'noaa-temperature-data'

with Flow(name="NOAA-files-upload-to-AWS", executor=executor) as flow:
    # working_dir = Parameter('WORKING_LOCAL_DIR', default=Path('/mnt/c/Users/benha/data_downloads/noaa_global_temps'))
    region_name = Parameter('REGION_NAME', default='us-east-2')
    bucket_name = Parameter('BUCKET_NAME', default='noaa-temperature-data')
    t1_aws_years = fetch_aws_folders(region_name, bucket_name)
    t2_process_files = process_year_files.map(t1_aws_years, unmapped(region_name), unmapped(bucket_name))


if __name__ == '__main__':
    state = flow.run()
    assert state.is_successful()