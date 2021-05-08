# NOAA-Spatial-Data-Processing
- Project features:
  - Cloud data management via AWS
  - "Big Data" map/reduce concepts through Prefect and Coiled (utilizing Dask and AWS)
  - Spatial Database programming using Python and PostGIS
  - Spatial Database indexes using PostGIS

## Overview of Entire Process
- (1) (scripted) Downloads ~538,000 CSV files from NOAA containing temperature data
- (2) (scripted) Uploads the ~538,000 CSV files to AWS
- (3) (scripted) Use map/reduce concepts to perform simple data cleaning and summary calculations in a distributed computing culster 
- (4) (scripted) Insert resulting data into a cloud hosted PostgreSQL database, using PostGIS database functions to create geometry column entries
- (5) (manual) Use OGR2OGR to create an additional table with polygons representing countries
- (5) (manual) Create standard and spatial indexes for both tables
- (6) (manual) Run spatial queries against the database using "st_contains"

## Files description:
- prefect_files_download_all.py
  - Uses Python and Prefect to download 538,000 csv files from https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/
  - Trigger through Prefect cloud orchestrator
- prefect_aws_upload.py
  - Uses Python, Prefect, and AWS to upload downloaded CSVs to AWS S3 storage.
  - Trigger through Prefect cloud orchestrator
- prefect_aws_data_prep.py
  - Uses Python, Prefect, AWS, and Coiled to do some simply data cleaning, and simple calculations (averages individual days for each sensor station to get annual data)
  - Uses map/reduce to clean data, and consolidate separate CSVs for each year into a single CSV containing a single record with the annual data for each year
  - Trigger through Prefect cloud orchestrator
- prefect_db_csv_insert.py
  - Uses Python, Prefect, AWS, Coiled, and Heroku to take files with calculated data from previous step and insert records into a PostgreSQL database
  - Creates geometry column for "point" data from latitude and longitude
  - Trigger through Prefect cloud orchestrator
- climate.countries.json
  - Generated GEOJSON file from https://geojson-maps.ash.ms/
  - Used to serve the purpose of another spatial layer that the NOAA data can be queried against
  - Use OGR2OGR to create a database table from the GEOJSON file
- noaa_data_tables.sql
  - Contains SQL used for:
    - Table for the NOAA related data
    - Standard B-Tree indexes for the NOAA data table, and the Countries table
    - Spatial R-Tree indexes for the NOAA data table, and the Countries table
    - 2 spatial queries of the two tables using "st_contains"

### Detailed Information For Each Script
- At the top of each script is a block of commented out text that describes the library requirements, infrustructure used, and description of what each script does.

## Countries Overlay
- Source: https://geojson-maps.ash.ms/
- Renamed file to: climate.countries.json
  - climate = schema
  - countries = table name
- Converted and inserted into PostgreSQL using PostGIS and ogr2ogr:
```shell
ogr2ogr -f "PostgreSQL" PG:"host=ec2-3-231-241-17.compute-1.amazonaws.com user=ziuixeipnmbrjm dbname=d5kg55pc96p21p password=<PASSWORD>" "climate.countries.json" -t_srs "EPSG:4326"
```

### Reference Material for Countries Overlay
- OGR2OGR Cheat Sheet: http://www.bostongis.com/PrinterFriendly.aspx?content_name=ogr_cheatsheet
- Install GDAL (used Conda): https://ljvmiranda921.github.io/notebook/2019/04/13/install-gdal/#using-conda

## Issues Encountered/Additional Work Needed
- Conceptually the functionality of this project serves it's purpose, but this is quite a broad project, done outside of an enterprise production context. As such, it's not perfect.
- Things to improve:
  - The data cleaning step has room for improvement
    - The majority of the time at this point my process is only encountering 9 or 10 files that have mistakes/omissions (the kind that don't crash the script) in them that my code is not accounting for (pretty decent considering it's processing through over 500,000 files). That said, some of my error handling for bad data is fairly broad or generalized. Depending on what the data would be used for it might be too brought. It does try to correct easily idenfiable minor mistakes causing what would otherwise be bad data though.
  - Spatial Reference Issues
    - Between my two tables there are some spatial inconsitencies. I created the geometry colomns in the NOAA temp table using 4326, and converted the GEOJSON file using 4326, but possible there are properties of the GEOJSON file unknown to me causing the issues. I'm looking forwad to learning more about this in the GIS class in the Fall. I can tell there are issues because the queries between the two tables are providing strange results (i.e., my uneducated guess would be that there are not more NOAA sensing sites in Yemon than in the US or UK...). This specific concept is a little outside of the scope of this.
    - Outside of this (not minor - if this were to be used in a production context) issue, the spatial indexes work well, as well as the queries using spatial joins to identify NOAA sensing site points in the Countries table polygons.