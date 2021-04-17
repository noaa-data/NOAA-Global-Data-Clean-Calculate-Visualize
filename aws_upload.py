from datetime import datetime
import logging
import os
from pathlib import Path
import sys
import threading
import datetime
from multiprocessing import Pool

import boto3
from botocore.exceptions import ClientError
from botocore.retries import bucket
from tqdm import tqdm


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
    # s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def s3_list_folders(s3_client: boto3.client, bucket_name: str): # -> yield object
    """ List folder as root of AWS bucket

    Args:
        s3_client: initated boto3 s3_client object
        bucket_name (str): target AWS bucket

    Return (yield): Iterable yield object with AWS bucket root folder names
    """
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix='', Delimiter='/')
    for content in response.get('CommonPrefixes', []):
        yield content.get('Prefix')


def aws_local_year_find_difference(s3_client: boto3.client, bucket: str, year: str, local_dir: str) -> tuple:
    """ Takes individual year and finds file difference between AWS and Local
    
    Args:
        s3_client: initated boto3 s3_client object
        bucket (str): target AWS bucket
        year (str): year to check difference for
        local_dir (str): local directory with year folders

    Return (set): Diference between AWS and Local
    """
    # If not exists - creates year folder in aws
    s3_client.put_object(Bucket=bucket, Body='', Key=f'{year}/')

    # File difference between local and aws for indidivual folder/year
    aws_file_set = set()
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=year)
    for page in tqdm(pages):
        list_all_keys = page['Contents']
        # item arrives in format of 'year/filename'; this removes 'year/'
        file_l = [x['Key'].split('/')[1] for x in list_all_keys]
        for f in file_l:
            aws_file_set.add(f)

    # List local files for year
    local_file_set = set(os.listdir(str(local_dir / year)))

    # List local files not yet in aws bucket/year
    file_difference_set = local_file_set - aws_file_set
    return list(file_difference_set)


def aws_load_files_year(s3_client: boto3.client, bucket: str, year: str, local_dir: str, files_l: list) -> int:
    """ Loads set of csv files into aws
    
    Args:
        s3_client: initated boto3 s3_client object
        bucket (str): target AWS bucket
        year (str): year to check difference for
        local_dir (str): local directory with year folders
        files (list): filenames to upload

    Return (tuple): Number of upload success (index 0) and failures (index 1)
    """
    upload_count, failed_count = 0, 0
    for csv_file in tqdm(files_l):
        result = s3_upload_file(
            s3_client=s3_client,
            file_name=str(local_dir / year / csv_file), 
            bucket=bucket, 
            object_name=f'{year}/{csv_file}'
        )
        if not result:
            with open('failed.txt', 'a') as f:
                f.write(f'{year} | {csv_file} | {datetime.datetime.now()}')
                f.write('\n')
            failed_count += 1
            continue
        upload_count += 1
    return upload_count, failed_count


########################################################
# AWS Upload Script
########################################################

# General variable initialization
bucket_name = 'mssi-noaa-temperature-data'
s3_client = boto3.client('s3', region_name='us-east-2')

# LIST LOCAL YEAR FOLDERS
working_dir = Path('C:/Users/Ben/Documents/working_datasets/noaa_global_temps')
year_folders = os.listdir(str(working_dir))

# LIST AWS BUCKET YEAR FOLDERS
# get list of folders in bucket root
folder_list = s3_list_folders(s3_client, bucket_name)
# remove '/' from end of each folder name
folder_list = [x.split('/')[0] for x in folder_list]

# FIND LOCAL FOLDERS NOT IN AWS BUCKET
difference_set = sorted(set(year_folders) - set(folder_list))

# add one folder below lowest to check for missing files before moving to new folders
# - new folder = subtract 1 from first item in sorted list
#   - first item converted to int for math operation
#   - result converted back to string before appending to list
difference_set.append(str(int(difference_set[0]) - 1))
folder_difference_set = sorted(difference_set)
print('\nFOLDERS TO CHECK AND UPLOAD')
print(folder_difference_set)

for year in tqdm(folder_difference_set):
    # If not exists - creates year folder in aws
    s3_client.put_object(Bucket=bucket_name, Body='', Key=f'{year}/')
    file_diff_l = aws_local_year_find_difference(
        s3_client=s3_client,
        bucket=bucket_name,
        year=year,
        local_dir=working_dir
    )
    print(year, 'difference:', len(file_diff_l))
    if file_diff_l:
        success, failed = aws_load_files_year(
            s3_client=s3_client,
            bucket=bucket_name,
            year=year,
            local_dir=working_dir,
            files_l=file_diff_l
        )
        print(year, 'success:', success, 'failed:', failed)
