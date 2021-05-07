##############################################################################
# Author: Ben Hammond
# Last Changed: 5/7/21
#
# REQUIREMENTS
# - Detailed dependencies in requirements.txt
# - Directly referenced:
#   - prefect, boto3, tqdm, pandas, icecream, coiled, psycopg2
#   - psycopg2-binary==2.8.6 ('binary' version apparently intended to be used for dev/testing,
#                             but requires fewer additional system dependencies to be installed)
# - Infrastructure:
#   - Prefect: Script is registered as a Prefect flow with api.prefect.io
#     - Source: https://prefect.io 
#   - Coiled: Prefect executor calls a Dask cluster hosted on Coiled (which is on AWS)
#     - Source: https://coiled.io
#     - Credentials: Stored localled in default user folder created by Coiled CLI
#   - AWS S3: Script retrieves stored in a S3 bucket
#     - Credentials: Stored localled in default user folder created by AWS CLI
#   - Heroku: Records are inserted into a PostgreSQL database on Heroku
#     - Source: https://www.heroku.com/
#     - Notes: PostGIS is installed in the PostgreSQL instance
#     - DB Credentials: Basic creds are hardcoded in script. Password is retrieved
#       from the Prefect Secrets manager
#
# DESCRIPTION
# - Reads files containing NOAA temp calculated averages per year per site
# - Inserts data from files into PostgreSQL (PostGIS) hosted in Heroku
#   - Table: climate.noaa_year_averages
#   - Uses latitude and longitude to create PostGIS geometry columns
# - Additional light data cleaning to catch what previous processes missed
##############################################################################

# Standard
import sys
sys.settrace
from datetime import timedelta
import os
import traceback

# PyPI
import coiled
import prefect
from prefect import task, Flow, Parameter
from prefect.tasks.postgres import PostgresExecute, PostgresFetch
from prefect.tasks.secrets import PrefectSecret
from prefect.utilities.edges import unmapped
from prefect.run_configs.local import LocalRun
import boto3
import pandas as pd
from prefect.executors.dask import DaskExecutor, LocalDaskExecutor
import psycopg2
from psycopg2.errors import UniqueViolation, InFailedSqlTransaction
from psycopg2.errors import SyntaxError, InFailedSqlTransaction
from icecream import ic
from tqdm import tqdm


########################
# SUPPORTING FUNCTIONS #
########################
def initialize_s3_client(region_name: str) -> boto3.client:
    return boto3.client('s3', region_name=region_name)


def df_if_two_one(value):
    """ Final Data Cleaning Function
    - This is run against station, latitude, longitude, and elevation for indidividual records
      - Many of these records have usable data, so don't want to just throw them out.
      - Example issues:
        - Instead of a station of '000248532' a value may contain '000248532 000248532'
          - Both are equal - function returns the first one
        - Instead of a latitude of '29.583' a value may contain '29.583 29.58333333'
          - This is from raw csv data files where they changed the number of decimal points userd 
            part of the way through a year.
          - Function converts both to integers, which rounds up to the nearest whole number. If both
            whole numbers are equal, then the function returns the first value from the original pair.

    Args:
        value (str): value to check and clean if needed
    
    Returns: str
    """
    split = value.split(' ')
    if len(split) > 1:
        if '.' in split[0]:
            if int(float(split[0])) == int(float(split[1])):
                return split[0]
        elif split[0] == split[1]:
            return split[0]
    return value


class database:
    def __init__(self, user, password, port, dbname, host):
        try:
            self.__db_connection = psycopg2.connect(
                user=user,
                password=password,
                port=port,
                database=dbname,
                host=host,
                sslmode='require'
            )
            self.cursor = self.__db_connection.cursor
            self.commit = self.__db_connection.commit
            self.rollback = self.__db_connection.rollback
        except psycopg2.OperationalError as e:
            if str(e).startswith("FATAL:"):
                sys.exit()
            if str(e).startswith("could not connect to server: Connection refused"):
                sys.exit()
            if str(e).startswith("could not translate host name"):
                sys.exit()
            raise psycopg2.OperationalError(e)
        except psycopg2.Error as e:
            exit()

    def __enter__(self):
        return self

    def __del__(self):
        try:
            self.__db_connection.close()
        except AttributeError as e:
            if (
                str(e)
                != "'database' object has no attribute '_database__db_connection'"
            ):
                raise AttributeError(e)

    def __exit__(self, ext_type, exc_value, traceback):
        if isinstance(exc_value, Exception) or ext_type is not None:
            self.rollback()
        else:
            self.commit()
        self.__db_connection.close()

    def execute_insert(self, sql: str, params=None):
        try:
            cursor = self.cursor()
            if params:
                cursor.execute(sql, params)
                return True
            cursor.execute(sql)
            return True
        except SyntaxError as e:
            self.rollback()
            traceback.print_exc()
            sys.exit()
        except InFailedSqlTransaction as e:
            self.rollback()
            if not str(e).startswith("current transaction is aborted"):
                raise InFailedSqlTransaction(e)
            traceback.print_exc()
            sys.exit()

    def execute_query(self, sql: str, params=None):
        try:
            cursor = self.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            return cursor.fetchall()
        except SyntaxError as e:
            self.rollback()   
            traceback.print_exc()
            sys.exit()
        except InFailedSqlTransaction as e:
            self.rollback()
            if not str(e).startswith("current transaction is aborted"):
                raise InFailedSqlTransaction(e)
            traceback.print_exc()
            sys.exit()


####################
# PREFECT WORKFLOW #
####################
@task(log_stdout=True, max_retries=5, retry_delay=timedelta(seconds=5))
def aws_all_year_files(bucket_name: str, region_name: str, wait_for=None):
    s3_client = initialize_s3_client(region_name)
    aws_file_set = set()
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix='year_average/')
    for page in pages:
        list_all_keys = page['Contents']
        # item arrives in format of 'year/filename'; this extracts that
        file_l = [x['Key'] for x in list_all_keys]
        for f in file_l:
            aws_file_set.add(f)
        break
    return list(sorted(aws_file_set))


@task(log_stdout=True)
def select_session_csvs(
    aws_files: list, db_name: str, user: str, host: str,
    port: str
) -> list:
    aws_files = [x for x in aws_files if x != '']
    aws_files = [x for x in aws_files if x.split('/')[1]]

    db_years = PostgresFetch(
        db_name=db_name, user=user, host=host, port=port,
        fetch="all",
        query=f"""
        select year from climate.csv_checker
        order by date_update
        """
    ).run(password=PrefectSecret('HEROKU_DB_PW').run())

    # SET DIFF, SORT
    diff_list = [x for x in aws_files if x.split('_')[1] not in db_years]
    # return (sorted(diff_list))
    return ['year_average/avg_2021.csv']


@task(log_stdout=True)
def insert_records(filename, db_name: str, user: str, host: str, port: str, bucket_name, region_name):
    year = filename.strip('year_average/avg_')
    year = year.strip('.csv')
    # Retrieve file data from AWS S3
    s3_client = initialize_s3_client(region_name)
    obj = s3_client.get_object(Bucket=bucket_name, Key=filename) 
    data = obj['Body']
    csv_df = pd.read_csv(data)
    csv_df['SITE_NUMBER'] = csv_df['SITE_NUMBER'].str.strip(']')
    csv_df['SITE_NUMBER'] = csv_df['SITE_NUMBER'].str.strip('[')
    csv_df['LATITUDE'] = csv_df['LATITUDE'].str.strip(']')
    csv_df['LATITUDE'] = csv_df['LATITUDE'].str.strip('[')
    csv_df['LONGITUDE'] = csv_df['LONGITUDE'].str.strip(']')
    csv_df['LONGITUDE'] = csv_df['LONGITUDE'].str.strip('[')
    csv_df['ELEVATION'] = csv_df['ELEVATION'].str.strip(']')
    csv_df['ELEVATION'] = csv_df['ELEVATION'].str.strip('[')

    conn_info = {
        "user": user,
        "password": PrefectSecret('HEROKU_DB_PW').run(),
        "host": host,
        "dbname": db_name,
        "port": port,
    }

    with database(**conn_info) as conn:
        commit_count = 0
        for i in tqdm(csv_df.index):
            vals  = [csv_df.at[i,col] for col in list(csv_df.columns)]
            station = vals[0]
            # df_if_two_one cleans a few issues left over from the data cleaning and calc steps
            station = df_if_two_one(station)
            latitude = vals[1]
            latitude = df_if_two_one(latitude)
            longitude = vals[2]
            longitude = df_if_two_one(longitude)
            if latitude == 'nan' and longitude == 'nan':
                continue
            try:
                cursor = conn.cursor()
                val = cursor.callproc('ST_GeomFromText', ((f'POINT({latitude} {longitude})'), 4326))
                geom = cursor.fetchone()[0]
                insert_str="""
                    insert into climate.noaa_year_averages 
                        (year, station, latitude, longitude, elevation, temp, dewp, stp, max, min, prcp, geom)
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                conn.execute_insert(insert_str, (
                    year, vals[0], vals[1], vals[2], vals[3], vals[4], vals[5], vals[6], vals[7], vals[8], vals[9], geom,)
                )
                commit_count += 1
            except UniqueViolation as e:
                # Record already exists
                pass
            except InFailedSqlTransaction as e:
                # Record exists, so transaction with "geom" is removed
                pass
            except Exception as e:
                if 'parse error - invalid geometry' in str(e):
                    # Error in spatial data
                    ic(latitude, longitude)
                print(e)
                ic(vals[0], year)
                raise Exception(e)
            if commit_count >= 100:
                conn.commit()
                commit_count = 0
    try:
        PostgresExecute(
            db_name=db_name, user=user, host=host, port=port,  
        ).run(
            query="""
            insert into climate.csv_checker 
                (year, date_create, date_update)
            values (%s, CURRENT_DATE, CURRENT_DATE)
            """, 
            data=(year,),
            commit=True,
            password=PrefectSecret('HEROKU_DB_PW').run()
        )
    except UniqueViolation:
        pass
    except TypeError as e:
        ic(vals[0], year)
        ic(e)


@task(log_stdout=True)
def vacuum_indexes(db_name: str, user: str, host: str, port: str):
    PostgresExecute(
            db_name=db_name, user=user, host=host, port=port,  
        ).run(
            query="""ANALYZE climate.noaa_year_averages""", 
            commit=True,
            password=PrefectSecret('HEROKU_DB_PW').run())
    PostgresExecute(
            db_name=db_name, user=user, host=host, port=port,  
        ).run(
            query="""VACUUM ANALYZE climate.noaa_year_averages""", 
            commit=True,
            password=PrefectSecret('HEROKU_DB_PW').run())
           

# IF REGISTERING FOR THE CLOUD, CREATE A LOCAL ENVIRONMENT VARIALBE FOR 'EXECTOR' BEFORE REGISTERING
if os.environ.get('EXECUTOR') == 'coiled':
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
            "n_workers": 3,
            "worker_memory":"16 GiB",
            "scheduler_memory": "16 GiB",
        },
    )
else:
    executor=LocalDaskExecutor(scheduler="threads", num_workers=15)


with Flow(name="NOAA Temps: Process CSVs", executor=executor) as flow:
    region_name = Parameter('REGION_NAME', default='us-east-1')
    bucket_name = Parameter('BUCKET_NAME', default='noaa-temperature-data')
    db_name = Parameter('DB_NAME', default='d5kg55pc96p21p')
    user = Parameter('USER', default='ziuixeipnmbrjm')
    host = Parameter('HOST', default='ec2-3-231-241-17.compute-1.amazonaws.com')
    port = Parameter('PORT', default='5432')
    t3_aws_files = aws_all_year_files(bucket_name, region_name)
    t4_csv_list = select_session_csvs(t3_aws_files, db_name, user, host, port)
    t5_task = insert_records.map(t4_csv_list, 
        unmapped(db_name), unmapped(user), unmapped(host), unmapped(port), unmapped(bucket_name), unmapped(region_name)
    )
    t6_task = vacuum_indexes(db_name, user, host, port, wait_for=t5_task)

flow.run_config = LocalRun(working_dir="/home/share/github/1-NOAA-Data-Download-Cleaning-Verification")

if __name__ == '__main__':
    state = flow.run()