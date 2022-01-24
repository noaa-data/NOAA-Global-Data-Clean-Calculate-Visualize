##############################################################################
# Author: Ben Hammond
# Last Changed: 5/7/21
#
# REQUIREMENTS
# - Detailed dependencies in requirements.txt
# - Directly referenced:
#   - prefect, boto3, tqdm
#
# - Infrastructure:
#   - Prefect: Script is registered as a Prefect flow with api.prefect.io
#     - Source: https://prefect.io
#   - AWS S3: Script retrieves and creates files stored in a S3 bucket
#     - Credentials: Stored localled in default user folder created by AWS CLI
#
# DESCRIPTION
# - Uploads local NOAA temperature csv files to AWS S3 storage
# - Includes the following features (to assist with handling the download of 538,000 [34gb] csv files):
#   - Continue Downloading: If the download is interrupted, the script can pick up where it left off
#   - Find Gaps: If an indidivual file is added to the source for any year, or removed from the server
#     for any year, the script can quickly scan all data in both locations, find the differences
#     and download the missing file(s)
# - Map: Uses map over a list of folders to upload files from each folder in a distributed/parallel fashion
##############################################################################

from datetime import datetime, timedelta
import logging
import os
from pathlib import Path

# PyPI
from prefect import task, Flow, Parameter
from prefect.executors.dask import LocalDaskExecutor
from prefect.run_configs.local import LocalRun
import boto3
from botocore.exceptions import ClientError
from prefect.utilities.edges import unmapped
from tqdm import tqdm

#####################
# SUPPORT FUNCTIONS #
#####################
def aws_local_year_find_difference(s3_client: boto3, bucket: str, year: str, local_dir: str) -> dict:
    """Takes individual year and finds file difference between AWS and Local

    Args:
        s3_client: initated boto3 s3_client object
        bucket (str): target AWS bucket
        year (str): year to check difference for
        local_dir (str): local directory with year folders

    Return (set): Diference between AWS and Local
    """
    # If not exists - creates year folder in aws
    s3_client.put_object(Bucket=bucket, Body="", Key=f"{year}/")

    # File difference between local and aws for indidivual folder/year
    aws_file_set = set()
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=year)
    for page in tqdm(pages):
        list_all_keys = page["Contents"]
        # item arrives in format of 'year/filename'; this removes 'year/'
        file_l = [x["Key"].split("/")[1] for x in list_all_keys]
        for f in file_l:
            aws_file_set.add(f)

    # List local files for year
    local_file_set = set(os.listdir(str(Path(local_dir) / year)))

    # if local files exist for year, but no AWS files, simply pass on set of local files to upload
    if len(local_file_set) > 1 and len(aws_file_set) == 0:
        return list(local_file_set)

    # List local files not yet in aws bucket/year
    file_difference_set = local_file_set - aws_file_set

    # Subtrack 1 from aws_file_ser, because the folder results from AWS include an empty string as one of the set items
    # print(f'{year} - DIFFERENCE: {len(file_difference_set)} (LOCAL: {len(local_file_set)} | CLOUD: {len(aws_file_set) - 1})')
    return {
        "file_diff_l": list(file_difference_set),
        "local_count": len(local_file_set),
        "cloud_count": len(aws_file_set) - 1,
    }


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


def aws_load_files_year(
    s3_client: boto3.client, bucket: str, year: str, local_dir: str, files_l: list, local_count: int, cloud_count: int
) -> int:
    """Loads set of csv files into aws

    Args:
        s3_client: initated boto3 s3_client object
        bucket (str): target AWS bucket
        year (str): year to check difference for
        local_dir (str): local directory with year folders
        files (list): filenames to upload

    Return (tuple): Number of upload success (index 0) and failures (index 1)
    """
    upload_count, failed_count = 0, 0
    for csv_file in tqdm(files_l, desc=f"{year} | local: {local_count} | cloud: {cloud_count}"):
        result = s3_upload_file(
            s3_client=s3_client,
            file_name=str(Path(local_dir) / year / csv_file),
            bucket=bucket,
            object_name=f"{year}/{csv_file}",
        )
        if not result:
            with open("failed.txt", "a") as f:
                f.write(f"{year} | {csv_file} | {datetime.now()}")
                f.write("\n")
            failed_count += 1
            continue
        upload_count += 1
    return upload_count, failed_count


def initialize_s3_client(region_name: str) -> boto3.client:
    return boto3.client("s3", region_name=region_name)


####################
# PREFECT WORKFLOW #
####################
@task(log_stdout=True)
def local_list_folders(working_dir: str) -> list:
    return os.listdir(str(working_dir))


@task(log_stdout=True, max_retries=5, retry_delay=timedelta(seconds=5))
def s3_list_folders(region_name, bucket_name: str) -> list:
    """List folder as root of AWS bucket

    Args:
        s3_client: initated boto3 s3_client object
        bucket_name (str): target AWS bucket

    Return (list): list with AWS bucket root folder names
    """
    s3_client = initialize_s3_client(region_name)
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="", Delimiter="/")

    def yield_folders(response):
        for content in response.get("CommonPrefixes", []):
            yield content.get("Prefix")

    folder_list = yield_folders(response)
    # remove '/' from end of each folder name
    return [x.split("/")[0] for x in folder_list]


@task(log_stdout=True, max_retries=5, retry_delay=timedelta(seconds=5))
def aws_local_folder_difference(aws_year_folders: list, local_year_folders: list, all: bool = False) -> set:
    """Finds year folders not yet in AWS

    - Finds folders not yet in AWS
    - Sorts and finds the lowest folder number, then adds a year folder below that number
      - This is to ensure that highest year in AWS is double check if it uploaded all files for it

    Args:
        aws_year_folders (list): year folders present in AWS bucket
        local_year_folders (list): year folders present in local dir

    Return (set): set of folders not in AWS + the year below lowest year
    """
    if all:
        return sorted(set(local_year_folders))
    difference_set = sorted(set(aws_year_folders) - set(local_year_folders))
    difference_set.append(str(int(difference_set[0]) - 1))
    return sorted(difference_set)


@task(log_stdout=True)
def load_year_files(year: str, region_name: str, bucket_name: str, working_dir: str):
    s3_client = initialize_s3_client(region_name)

    # If not exists - creates year folder in aws
    s3_client.put_object(Bucket=bucket_name, Body="", Key=f"{year}/")

    file_diffs = aws_local_year_find_difference(
        s3_client=s3_client, bucket=bucket_name, year=year, local_dir=working_dir
    )
    if file_diffs["file_diff_l"]:
        success, failed = aws_load_files_year(
            s3_client=s3_client,
            bucket=bucket_name,
            year=year,
            local_dir=working_dir,
            files_l=file_diffs["file_diff_l"],
            local_count=file_diffs["local_count"],
            cloud_count=file_diffs["cloud_count"],
        )
        print(f"{year} success: {success}, failed: {failed}")
    return True


executor = LocalDaskExecutor(scheduler="threads", num_workers=4)
with Flow(name="NOAA files: AWS Upload", executor=executor) as flow:
    #    working_dir = Parameter('WORKING_LOCAL_DIR', default=Path('/mnt/c/Users/benha/data_downloads/noaa_global_temps'))
    working_dir = Parameter("WORKING_LOCAL_DIR", default=str(Path("./local_data/noaa_temp_downloads")))
    region_name = Parameter("REGION_NAME", default="us-east-1")
    bucket_name = Parameter("BUCKET_NAME", default="noaa-temperature-data")
    all_folders = Parameter("ALL_FOLDERS", default=True)
    t1_local_folders = local_list_folders(working_dir)
    t2_aws_folders = s3_list_folders(region_name, bucket_name)
    t3_years = aws_local_folder_difference(t2_aws_folders, t1_local_folders, all_folders)
    load_year_files.map(t3_years, unmapped(region_name), unmapped(bucket_name), unmapped(working_dir))

flow.run_config = LocalRun(working_dir="/home/share/github/1-NOAA-Data-Download-Cleaning-Verification/")


if __name__ == "__main__":
    state = flow.run()
    assert state.is_successful()
