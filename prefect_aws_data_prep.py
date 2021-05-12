##############################################################################
# Author: Ben Hammond
# Last Changed: 5/7/21
#
# REQUIREMENTS
# - Detailed dependencies in requirements.txt
# - Directly referenced:
#   - prefect, boto3, tqdm, pandas, icecream, coiled
#
# - Infrastructure:
#   - Prefect: Script is registered as a Prefect flow with api.prefect.io
#     - Source: https://prefect.io 
#   - Coiled: Prefect executor calls a Dask cluster hosted on Coiled (which is on AWS)
#     - Source: https://coiled.io
#     - Credentials: Stored localled in default user folder created by Coiled CLI
#   - AWS S3: Script retrieves and creates files stored in a S3 bucket
#     - Credentials: Stored localled in default user folder created by AWS CLI
#
# DESCRIPTION
# - Reads files containing raw NOAA temperature information in S3 bucket
# - Creates a set of lists (list of lists) to run as a map against data functions
# - Map: Removes files that are missing spatial data
# - Map: Removes files that have inconsistent spatial data (i.e., latitude changes part of 
#        the way through the year in such a way that seems like a mistake)
# - Reduce: Takes the site data files from each year and creates a single file for
#           the year that contains the yearly averages for each temperature site
#   - The data in these new files will be inserted into a PostgreSQL database
##############################################################################

import coiled

import logging
import os, io
from typing import List
from datetime import timedelta

# PyPI
import prefect
from prefect import task, Flow, Parameter
# from prefect.engine.executors.dask import DaskExecutor, LocalDaskExecutor
from prefect.executors.dask import DaskExecutor, LocalDaskExecutor
from prefect.utilities.edges import unmapped, mapped
from prefect.run_configs.local import LocalRun
from prefect.tasks.aws.s3 import S3Download, S3Upload, S3List
from prefect.tasks.secrets.base import PrefectSecret
import boto3
from botocore.exceptions import ClientError
from tqdm import tqdm
import pandas as pd
from pandas.errors import EmptyDataError
from icecream import ic

# s3_client = boto3.client('s3', 'us-east-1')
# s3_resource = boto3.resource('s3')


########################
# SUPPORTING FUNCTIONS #
########################
def initialize_s3_client(region_name: str) -> boto3.client:
    return boto3.client('s3', region_name=region_name)


def csv_clean_spatial_check(filename, data):
    records = pd.read_csv(io.BytesIO(data), #working_dir / file_, 
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


def unique_values_spatial_check(filename, data):
    """ Ensure spatial fields are consistent for a site
    - The spatial fields (latitude, lontitude, elevation) should be the same
      for a site over the course of the year. There are NOAA temp files
      where one of the fields will change part of the way through the year.
      - This *could* be because the site moved, but sometimes it's only
        one field that changes, which suggests a mistake
      - There are some circumstances where a float with 1 decimal changes to 2,
        or an integer becomes a float. These should be identified and corrected.
    - Also checks to ensure the station ID number doesn't change in the file.
    """
    try:
        records = pd.read_csv(io.BytesIO(data), #working_dir / file_, 
                              dtype={'FRSHTT': str, 'TEMP': str, 'LATITUDE': str, 'LONGITUDE': str, 'ELEVATION': str, 'DATE': str}
        )
        # records = pd.DataFrame(
        #     data[1:], columns=data[0],
        #     dtype={'FRSHTT': str, 'TEMP': str, 'LATITUDE': str, 'LONGITUDE': str, 'ELEVATION': str, 'DATE': str}
        # )
        records.columns = records.columns.str.strip()    
        site_number = column_unique_values_check(records['STATION'])
        latitude = column_unique_values_check(records['LATITUDE'])
        longitude = column_unique_values_check(records['LONGITUDE'])
        elevation = column_unique_values_check(records['ELEVATION'])
        if site_number == 'X':
            return filename
        if latitude == 'X':
            return filename
        if longitude == 'X':
            return filename
        if elevation == 'X':
            return filename
    except EmptyDataError as e:
        return 'X'


def column_unique_values_check(column) -> str:
    value_l = column.unique()
    if len(value_l) > 1:
        return 'X'
    return value_l[0]


def aws_year_files(year: str, bucket_name: str, region_name: str):
    print(region_name)
    # if year == '':
    #     return []
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


def move_s3_file(
    filename: str, 
    bucket_name: str, 
    # s3_client: boto3.client,
    note: str
):
    try:
        # create bucket object
        s3_resource = boto3.resource('s3')
        s3_client = initialize_s3_client(region_name)
        bucket = s3_resource.Bucket(bucket_name)
        # ensure data error folder exists
        s3_client.put_object(Bucket=bucket_name, Body='', Key=f'_data_error/')
        # ensure year folder exists
        # s3_client.put_object(Bucket=bucket_name, Body='', Key=f'_data_errors/{filename.split("/")[0]}/')
        # Copy object A as object B
        year, file_ = filename.split('/')
        number = file_.split('.')[0]
        copy_source = {'Bucket': bucket_name, 'Key': filename}
        bucket.copy(copy_source, f'_data_error/{year}-{number}-{note}.csv')
        # Delete object A
    except ValueError as e:
        if 'not enough values to unpack' not in str(e):
            raise ValueError(e)
    s3_resource.Object(bucket_name, filename).delete()


# def s3_upload_file(s3_client: boto3.client, file_name, bucket, object_name=None):
#     """Upload a file to an S3 bucket

#     Args:
#         s3_client: initated boto3 s3_client object
#         file_name (str): File to upload
#         bucket (str): target AWS bucket
#         object_name (str): S3 object name. If not specified then file_name is used [Optional]
    
#     Return (bool): True if file was uploaded, else False
#     """
#     # If S3 object_name was not specified, use file_name
#     if object_name is None:
#         object_name = file_name

#     # Upload the file
#     try:
#         s3_client.upload_file(file_name, bucket, object_name)
#     except ClientError as e:
#         logging.error(e)
#         return False
#     return True


####################
# PREFECT WORKFLOW #
####################
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
    # ic(folder_list)
    folder_list = [x for x in folder_list if x != '']
    return sorted(folder_list)
    # return ['1929', '1930','1931']


@task(log_stdout=True, max_retries=5, retry_delay=timedelta(seconds=5))
def aws_all_year_files(year: list, bucket_name: str, region_name: str, wait_for=None):
    # if year == '':
    #     return []
    # s3_client = initialize_s3_client(region_name)
    # aws_file_set = set()
    aws_file_set = S3List(bucket=bucket_name).run(
        credentials={
            "ACCESS_KEY": PrefectSecret('AWS_ACCESS_KEY_ID').run(),
            "SECRET_ACCESS_KEY": PrefectSecret('AWS_SECRET_ACCESS_KEY').run()
        },
        prefix=year,
    )
    # ic(len(aws_file_set))
    # aws_file_set = [x for x in aws_file_set if len(x) > 5]
    # ic(aws_file_set[:100])
    # paginator = s3_client.get_paginator('list_objects_v2')
    # pages = paginator.paginate(Bucket=bucket_name, Prefix=year)
    # for page in pages:
    #     list_all_keys = page['Contents']
    #     # item arrives in format of 'year/filename'; this extracts that
    #     file_l = [x['Key'] for x in list_all_keys]
    #     for f in file_l:
    #         aws_file_set.add(f)
        # break
    return list(sorted(aws_file_set))


@task(log_stdout=True)
def aws_lists_prep_for_map(file_l: list, list_size: int, wait_for=None) -> List[list]:
    def chunks(file_l, list_size):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(file_l), list_size):
            yield file_l[i:i + list_size]
    file_l_consolidated = [i for l in file_l for i in l]
    file_l_consolidated = list(chunks(file_l_consolidated, list_size))
    ic(len(file_l_consolidated))

    return file_l_consolidated


@task(log_stdout=True, max_retries=5, retry_delay=timedelta(seconds=5))
def process_year_files(files_l: list, region_name: str, bucket_name: str):
    # ic(files_l)
    # s3_client = initialize_s3_client(region_name)
    s3_resource = boto3.resource('s3')
    for filename in tqdm(files_l):
        if len(filename) <= 5:
            continue
        else:
            try:
                # obj = s3_client.get_object(Bucket=bucket_name, Key=filename) 
                # data = obj['Body']
                data = S3Download(bucket=bucket_name,).run(
                    key=filename,
                    credentials={
                        "ACCESS_KEY": PrefectSecret('AWS_ACCESS_KEY_ID').run(), 
                        "SECRET_ACCESS_KEY": PrefectSecret('AWS_SECRET_ACCESS_KEY').run()
                    },
                    as_bytes=True
                )
                non_unique_spatial = unique_values_spatial_check(
                    filename=filename,
                    data=data
                )
                if non_unique_spatial:
                    move_s3_file(non_unique_spatial, bucket_name, note='non_unique_spatial')
                    print('uploaded')
                    continue
                # obj = s3_client.get_object(Bucket=bucket_name, Key=filename)
                # data = obj['Body']
                data = S3Download(bucket=bucket_name,).run(
                    key=filename,
                    credentials={
                        "ACCESS_KEY": PrefectSecret('AWS_ACCESS_KEY_ID').run(),
                        "SECRET_ACCESS_KEY": PrefectSecret('AWS_SECRET_ACCESS_KEY').run()
                    },
                    as_bytes=True
                )
                spatial_errors = csv_clean_spatial_check(
                    filename=filename,
                    data=data
                )
                if spatial_errors:
                    move_s3_file(spatial_errors, bucket_name, note='missing_spatial')
                    continue
            except EmptyDataError as e:
                move_s3_file(spatial_errors, bucket_name, note='empty_data_error')
            except s3_resource.meta.client.exceptions.NoSuchKey as e:
                move_s3_file(spatial_errors, bucket_name, note='no_such_key_error')
    print('TASK')


@task(log_stdout=True, max_retries=5, retry_delay=timedelta(seconds=5))
def calculate_year_csv(year_folder, bucket_name, region_name, wait_for: str):
    # s3_client = initialize_s3_client(region_name)
    files_l = aws_year_files(year_folder, bucket_name, region_name)
    files_l = [x for x in files_l if len(x) > 6]
    columns = 'SITE_NUMBER,LATITUDE,LONGITUDE,ELEVATION,AVERAGE_TEMP,DEWP,STP,MIN,MAX,PRCP\n'
    content = columns
    try:
        for site in tqdm(files_l, desc=year_folder):
            # obj = s3_client.get_object(Bucket=bucket_name, Key=site) 
            # data = obj['Body']
            data = S3Download(bucket=bucket_name,).run(
                    key=site,
                    credentials={
                        "ACCESS_KEY": PrefectSecret('AWS_ACCESS_KEY_ID').run(),
                        "SECRET_ACCESS_KEY": PrefectSecret('AWS_SECRET_ACCESS_KEY').run()
                    },
                    as_bytes=True
                )
            df1 = pd.read_csv(io.BytesIO(data))
            average_temp = df1['TEMP'].mean()
            average_dewp = df1['DEWP'].mean()
            average_stp = df1['STP'].mean()
            average_min = df1['MIN'].mean()
            average_max = df1['MAX'].mean()
            average_prcp = df1['PRCP'].mean()
            site_number = df1['STATION'].unique()
            latitude = df1['LATITUDE'].unique()
            longitude = df1['LONGITUDE'].unique()
            elevation = df1['ELEVATION'].unique()
            row = f'{site_number},{latitude},{longitude},{elevation},{average_temp},{average_dewp},{average_stp},{average_min},{average_max},{average_prcp}\n'
            content += row
        # s3_client.put_object(Body=content, Bucket=bucket_name, Key=f'year_average/avg_{year_folder}.csv')
        S3Upload(bucket=bucket_name,).run(
            data=content,
            key=f'year_average/avg_{year_folder}.csv',
            credentials={
                "ACCESS_KEY": PrefectSecret('AWS_ACCESS_KEY_ID').run(),
                "SECRET_ACCESS_KEY": PrefectSecret('AWS_SECRET_ACCESS_KEY').run()
            },
        )
    except EmptyDataError as e:
        pass



# IF REGISTERING FOR THE CLOUD, CREATE A LOCAL ENVIRONMENT VARIALBE FOR 'EXECTOR' BEFORE REGISTERING
coiled_ex = True
if coiled_ex == True:
    print("Coiled")
    coiled.create_software_environment(
        name="NOAA-temperature-data-clean",
        pip="requirements.txt"
    )
    executor = DaskExecutor(
        debug=True,
        cluster_class=coiled.Cluster,
        cluster_kwargs={
            "shutdown_on_close": True,
            "name": "NOAA-temperature-data-clean",
            "software": "darrida/noaa-temperature-data-clean",
            "worker_cpu": 4,
            "n_workers": 8,
            "worker_memory":"8 GiB",
            "scheduler_memory": "8 GiB",
        },
    )
else:
    executor=LocalDaskExecutor(scheduler="threads", num_workers=8)
        

with Flow(name="NOAA files: clean and calc averages", executor=executor) as flow:
    region_name = Parameter('REGION_NAME', default='us-east-1')
    bucket_name = Parameter('BUCKET_NAME', default='noaa-temperature-data')
    map_list_size = Parameter('MAP_LIST_SIZE', default=1000)
    t1_aws_years = fetch_aws_folders(region_name, bucket_name)
    t2_all_files = aws_all_year_files.map(mapped(t1_aws_years), unmapped(bucket_name), unmapped(region_name))
    t3_map_prep_l = aws_lists_prep_for_map(t2_all_files, map_list_size)
    t4_clean_complete = process_year_files.map(mapped(t3_map_prep_l), unmapped(region_name), unmapped(bucket_name))
    t5_calc_complete = calculate_year_csv.map(
        t1_aws_years, unmapped(bucket_name), unmapped(region_name), wait_for=unmapped(t4_clean_complete)
    )

flow.run_config = LocalRun(working_dir="/home/share/github/1-NOAA-Data-Download-Cleaning-Verification")

if __name__ == '__main__':
    state = flow.run(executor=executor)
