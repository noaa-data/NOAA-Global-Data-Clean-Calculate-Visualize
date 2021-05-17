# NOAA-Spatial-Data-Processing
- Project features:
  - Cloud data management via AWS
  - "Big Data" map/reduce concepts through Prefect and Coiled (utilizing Dask and AWS)
  - Spatial Database programming using Python and PostGIS
  - Spatial database indexes using PostGIS
  - Spatial queries using a SQL IDE and QGIS
- Google Sheets Slides: https://docs.google.com/presentation/d/1IbAh9S_fQA_ZzCy-E5UYQETqLkWdqNlPzbcCE3s8jAU/edit?usp=sharing

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

## Urban Areas Overlay
- Source: https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/10m/cultural/ne_10m_urban_areas.zip
- Converted and inserted into PostgreSQL using GDAL
  - climate = schema
  - ne_10m_urban_areas = table name
```shell

shp2pgsql -s 4326 ne_10m_urban_areas > urban_areas.sql

```

### Reference Material for Countries Overlay
- OGR2OGR Cheat Sheet: http://www.bostongis.com/PrinterFriendly.aspx?content_name=ogr_cheatsheet
- Install GDAL (used Conda): https://ljvmiranda921.github.io/notebook/2019/04/13/install-gdal/#using-conda
