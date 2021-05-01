
# exit()

# list_all_keys = s3_year_folders['Contents']
# next = s3_year_folders['Contents']['NextContinuationToken']

# folder_l = [x['Key'].split('/')[0] for x in list_all_keys]
# folder_set = set(folder_l)
# print(folder_set)

# exit()

# for year in tqdm(year_folders):
#     s3_client.put_object(Bucket=bucket_name,Body='', Key=f'{year}/')
#     csv_files = os.listdir(str(working_dir / year))
#     for csv_file in tqdm(csv_files):
#         # with open(year_dir / csv_file, "rb") as f:
#             #s3.upload_fileobj(f, "mssi-noaa-temperature-data", csv_file)
#         result = upload_file(str(working_dir / year / csv_file), bucket_name, object_name=f'{year}/{csv_file}')
#         if not result:
#             with open('failed.txt', 'a') as f:
#                 f.write(f'{year} | {csv_file} | {datetime.datetime.now()}')
#                 f.write('\n')
#             print('failed')



# # To get the top level directories:
# # list(bucket.list("", "/"))
# #s3_year_folders = s3_client.list_objects_v2(Bucket=bucket_name)#, Prefix='/')
# # To get the subdirectories of files:
# # list(bucket.list("files/", "/")




# bucket_name = 'mssi-noaa-temperature-data'
# s3_client = boto3.client('s3', region_name='us-east-2')

# # LIST LOCAL YEAR FOLDERS
# working_dir = Path('C:/Users/Ben/Documents/working_datasets/noaa_global_temps')
# year_folders = os.listdir(str(working_dir))

# # LIST AWS BUCKET YEAR FOLDERS
# # get list of folders in bucket root
# folder_list = s3_list_folders(s3_client, bucket_name)
# # remove '/' from end of each folder name
# folder_list = [x.split('/')[0] for x in folder_list]

# # FIND LOCAL FOLDERS NOT IN AWS BUCKET
# difference_set = sorted(set(year_folders) - set(folder_list))

# # add one folder below lowest to check for missing files before moving to new folders
# # - new folder = subtract 1 from first item in sorted list
# #   - first item converted to int for math operation
# #   - result converted back to string before appending to list
# difference_set.append(str(int(difference_set[0]) - 1))
# folder_difference_set = sorted(difference_set)
# print(folder_difference_set)


# folder_set = set()
# paginator = s3_client.get_paginator('list_objects_v2')
# pages = paginator.paginate(Bucket=bucket_name, Prefix='')
# for page in tqdm(pages):
#     list_all_keys = page['Contents']
#     folder_l = [x['Key'].split('/')[0] for x in list_all_keys]
#     folder_set.update(folder_l)
# print((set(year_folders) - folder_set).sorted())



# for year in tqdm(folder_difference_set):
#     # If not exists - creates year folder in aws
#     s3_client.put_object(Bucket=bucket_name,Body='', Key=f'{year}/')
#     # FIND FILE DIFFERENCE FOR INDIVIDUAL YEAR/FOLDER
#     aws_file_set = set()
#     total = 0
#     paginator = s3_client.get_paginator('list_objects_v2')
#     pages = paginator.paginate(Bucket=bucket_name, Prefix=year)
#     for page in tqdm(pages):
#         list_all_keys = page['Contents']
#         # item arrives in format of 'year/filename'; this removes 'year/'
#         print(len(list_all_keys))
#         file_l = [x['Key'].split('/')[1] for x in list_all_keys]
#         for f in file_l:
#             aws_file_set.add(f)
#     print(total)
#     local_file_set = set(os.listdir(str(working_dir / year)))
#     file_difference_set = local_file_set - aws_file_set
#     print(year, len(list(file_difference_set)))
#     # LOAD DIFFERENCE FILES IN AWS FROM LOCAL
#     # s3_client.put_object(Bucket=bucket_name,Body='', Key=f'{year}/')
#     # csv_files = os.listdir(str(working_dir / year))
#     for csv_file in tqdm(file_difference_set):
#         # with open(year_dir / csv_file, "rb") as f:
#             #s3.upload_fileobj(f, "mssi-noaa-temperature-data", csv_file)
#         result = upload_file(str(working_dir / year / csv_file), bucket_name, object_name=f'{year}/{csv_file}')
#         if not result:
#             with open('failed.txt', 'a') as f:
#                 f.write(f'{year} | {csv_file} | {datetime.datetime.now()}')
#                 f.write('\n')
#             print('failed')
#     exit()

    
# class ProgressPercentage(object):
#     def __init__(self, filename):
#         self._filename = filename
#         self._size = float(os.path.getsize(filename))
#         self._seen_so_far = 0
#         self._lock = threading.Lock()

#     def __call__(self, bytes_amount):
#         # To simplify, assume this is hooked up to a single filename
#         with self._lock:
#             self._seen_so_far += bytes_amount
#             percentage = (self._seen_so_far / self._size) * 100
#             sys.stdout.write(
#                 "\r%s  %s / %s  (%.2f%%)" % (
#                     self._filename, self._seen_so_far, self._size,
#                     percentage))
#             sys.stdout.flush()