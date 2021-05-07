# Standard
import sys
sys.settrace
from csv import reader
from pathlib import Path
from pprint import pprint
from datetime import timedelta, datetime
import os

# PyPI
import prefect
from prefect import task, Flow, Parameter
from prefect.tasks.postgres import PostgresExecute, PostgresFetch, PostgresExecuteMany
from prefect.schedules import IntervalSchedule
from prefect.tasks.secrets import PrefectSecret
from prefect.engine.signals import LOOP
from prefect.utilities.edges import unmapped
from prefect.run_configs.local import LocalRun
import boto3
#from prefect.engine.executors import LocalDaskExecutor
#from prefect.executors import LocalDaskExecutor
from prefect.executors.dask import LocalDaskExecutor
import psycopg2 as pg
from psycopg2.errors import UniqueViolation, InvalidTextRepresentation # pylint: disable=no-name-in-module

# url = 'https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/'
# export PREFECT__CONTEXT__SECRETS__MY_KEY="MY_VALUE"
# export PREFECT__ENGINE__EXECUTOR__DEFAULT_CLASS="prefect.engine.executors.LocalDaskExecutor"
# prefect agent start --name dask_test
# prefect register flow --file psql_sample.py --name psql_test_v2 --project 
# ? add_default_labels=False

# local testing: export NOAA_TEMP_CSV_DIR=$PWD/test/data_downloads/noaa_daily_avg_temps


########################
# SUPPORTING FUNCTIONS #
########################
def initialize_s3_client(region_name: str) -> boto3.client:
    return boto3.client('s3', region_name=region_name)


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
    #return ['2016', '2017', '2018', '2019', '2020', '2021']


@task(log_stdout=True, max_retries=5, retry_delay=timedelta(seconds=5))
def aws_all_year_files(year: list, bucket_name: str, region_name: str, wait_for=None):
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
        break
    return list(sorted(aws_file_set))


# @task(log_stdout=True)
# def list_csvs(work_dir: str):
#     csv_list = []
#     data_dir = Path(work_dir)
#     for year in os.listdir(path=data_dir):
#         csv_folder = (data_dir / str(year)).rglob('*.csv')
#         csv_list = csv_list + [str(x) for x in csv_folder]
#     print(csv_list[0])
#     return csv_list


# @task(log_stdout=True)
# def list_db_years(db_name: str, user: str, host: str, port: str, waiting_for: str) -> list:
#     db_years = PostgresFetch(
#         db_name=db_name, user=user, host=host, port=port,
#         fetch="all",
#         query="""
#         select distinct year, date_update from climate.csv_checker
#         order by date_update
#         """
#     ).run(password=PrefectSecret('HEROKU_DB_PW').run())
#     db_years.insert(0, db_years.pop())   # Move last item in the list to the first
#                                          # - We want to check the most recent year first, since csvs in that dir
#                                          #   may not be complete (we are not doing the full number of csvs for some dirs
#                                          #   with each run)
#                                          # - Then we move to the oldest checked folder in the list to move forward
#     return db_years


@task(log_stdout=True)
def select_session_csvs(
    aws_csvs: list, job_size: int, db_name: str, user: str, host: str,
    port: str
) -> list:
    return_list = []

    aws_files = sum(aws_csvs, [])
    print(aws_files[:3])
    aws_files = [x for x in aws_files if x != '']

    # AWS SET
    csv_set = set()
    for csv in aws_files:
        csv_list = csv.split('/') if '/' in csv else csv.split('\\')
        csv_str = f'{csv_list[-2]}-{csv_list[-1]}'
        csv_set.add(csv_str)
    print(f'csvs from folder: {len(csv_set)}')
    print(aws_files[:3])

    year_db_csvs = PostgresFetch(
        db_name=db_name, user=user, host=host, port=port,
        fetch="all",
        query=f"""
        select year, station from climate.csv_checker
        order by date_update
        """
    ).run(password='37b339a09ef5d442dfe0ba71065ebdc9520a00f71599d4e51fb243af86e79b3e')#PrefectSecret('HEROKU_DB_PW').run())

    # DB SET
    year_db_set = set()
    for year_db in year_db_csvs:
        year_db_str = f'{year_db[0]}-{year_db[1]}'
        year_db_set.add(year_db_str)
    print(f'csv_checker set: {len(year_db_set)}')

    # SET DIFF, SORT
    new_set = csv_set.difference(year_db_set)
    new_set = sorted(new_set)
    print(f'new_set: {len(new_set)}')

    # CONVERT TO LIST, SELECT SHORT SUBSET
    new_list = []
    set_empty = False
    while len(new_list) < job_size and not set_empty:
        if len(new_set)>0:
            new_list.append(new_set.pop())
        else:
            set_empty = True
    new_list = [x.split('-') for x in new_list]
    new_list = new_list[:job_size]

    # REBUILD LIST OF FILE PATH LOCATIONS
    # data_dir = Path(data_dir)
    # return_list = [f'{data_dir}/{x[0]}/{x[1]}' for x in new_list]
    # print(f'retun_list: {len(return_list)}')
    # return return_list
                

# @task(log_stdout=True) # pylint: disable=no-value-for-parameter
# def open_csv(filename: str):
#     print(filename)
#     with open(filename) as read_obj:
#         csv_reader = reader(read_obj)
#         # Get all rows of csv from csv_reader object as list of tuples
#         return list(map(tuple, csv_reader))
#     raise LOOP(message)

# @task(log_stdout=True) # pylint: disable=no-value-for-parameter
# def insert_stations(list_of_tuples: list):#, password: str):
#     insert = 0
#     unique_key_violation = 0

#     #print(len(list_of_tuples))
#     insert = 0
#     unique_key_violation = 0
#     for row in list_of_tuples[1:2]:
#         station = row[0]
#         latitude = row[2] if row[2] != '' else None
#         longitude = row[3] if row[3] != '' else None
#         elevation = row[4] if row[4] != '' else None
#         name = row[5]
#         try:
#             PostgresExecute(
#                 db_name=local_config.DB_NAME, #'climatedb', 
#                 user=local_config.DB_USER, #'postgres', 
#                 host=local_config.DB_HOST, #'192.168.86.32', 
#                 port=local_config.DB_PORT, #5432, 
#                 query="""
#                 insert into climate.noaa_global_temp_sites 
#                     (station, latitude, longitude, elevation, name)
#                 values (%s, %s, %s, %s, %s)
#                 """, 
#                 data=(station, latitude, longitude, elevation, name), 
#                 commit=True,
#             ).run(password=PrefectSecret('HEROKU_DB_PW').run())
#             insert += 1
#         except UniqueViolation:
#             unique_key_violation += 1
#         except InvalidTextRepresentation as e:
#             print(e)
#     print(f'STATION INSERT RESULT: inserted {insert} records | {unique_key_violation} duplicates')

@task(log_stdout=True) # pylint: disable=no-value-for-parameter
def insert_records(filename, db_name: str, user: str, host: str, port: str):
    with open(filename) as read_obj:
        csv_reader = reader(read_obj)
        # Get all rows of csv from csv_reader object as list of tuples
        list_of_tuples = list(map(tuple, csv_reader))
    
    #insert = 0
    if not list_of_tuples:
        return
    unique_key_violation = 0
    new_list = []
    for row in list_of_tuples[1:]:
        # SITE_NUMBER,LATITUDE,LONGITUDE,ELEVATION,AVERAGE_TEMP,DEWP,STP,MIN,MAX,PRCP
        year=row[1][:4]
        station=row[0]
        latitude=row[2]
        longitude=row[3]
        elevation=row[4]
        temp=row[6]
        dewp=row[8]
        stp=row[12]
        max_v=row[20]
        min_v=row[22]
        prcp=row[24] 
        new_tuple = (year, station, latitude, longitude, elevation, temp, dewp, stp, max_v, min_v, prcp)
        new_list.append(new_tuple)
        insert = 0
    try:
        PostgresExecuteMany(
            db_name=db_name, user=user, host=host, port=port, 
            query="""
            insert into climate.noaa_global_daily_temps 
                (year, station, latitude, longitude, elevation, temp, dewp, stp, max, min, prcp)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, 
            data=new_list,
            commit=True,
        ).run(password=PrefectSecret('HEROKU_DB_PW').run())
        insert = len(new_list)
    except UniqueViolation:
        unique_key_violation += 1
    try:
        csv_filename = station + '.csv'
        PostgresExecute(
            db_name=db_name, user=user, host=host, port=port,  
            query="""
            insert into climate.csv_checker 
                (station, date_create, date_update, year)
            values (%s, CURRENT_DATE, CURRENT_DATE, %s)
            """, 
            data=(csv_filename, year),
            commit=True,
        ).run(password=PrefectSecret('HEROKU_DB_PW').run())
    except UniqueViolation:
        pass
    print(f'RECORD INSERT RESULT: inserted {insert} records | {unique_key_violation} duplicates')


schedule = IntervalSchedule(
    start_date=datetime.utcnow() + timedelta(seconds=1),
    interval=timedelta(seconds=10),
)

executor=LocalDaskExecutor(scheduler="processes", num_workers=8)

with Flow(name="NOAA Temps: Process CSVs", executor=executor) as flow:
    region_name = Parameter('REGION_NAME', default='us-east-1')
    bucket_name = Parameter('BUCKET_NAME', default='noaa-temperature-data')
    db_name = Parameter('DB_NAME', default='d5kg55pc96p21p')
    user = Parameter('USER', default='ziuixeipnmbrjm')
    host = Parameter('HOST', default='ec2-3-231-241-17.compute-1.amazonaws.com')
    port = Parameter('PORT', default='5432')
    job_size = Parameter('JOB_SIZE', default=200)
    # t1_csvs = list_csvs()
    t2_aws_years = fetch_aws_folders(region_name, bucket_name)
    t3_aws_files = aws_all_year_files.map(t2_aws_years, unmapped(bucket_name), unmapped(region_name))
    t4_session = select_session_csvs(t3_aws_files, job_size, db_name, user, host, port)
    t5_records = insert_records.map(t4_session, db_name, user, host, port)


flow.run_config = LocalRun(working_dir="/home/share/github/1-NOAA-Data-Download-Cleaning-Verification")

if __name__ == '__main__':
    state = flow.run()
    assert state.is_successful()