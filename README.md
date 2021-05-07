# NOAA-Data-Download-Cleaning-Verification

## AWS Upload
- `prefect_aws_upload.py`
- Where: Home server
- How: Prefect universal agent, scheduled by cloud orchestrator

## AWS Data Cleaning
- `prefect_data_cleaning.py`
- Where: Coiled
  - Script sits on home server, but it utilizes a Coiled Dask cluster
- How: Prefect universal agent, schedule by cloud orchestrator







## Countries Overlay
- Source: https://geojson-maps.ash.ms/
- Renamed file to: climate.countries.json
  - climate = schema
  - countries = table name
- Converted and inserted into PostgreSQL using PostGIS and ogr2ogr:
```shell
ogr2ogr -f "PostgreSQL" PG:"host=ec2-3-231-241-17.compute-1.amazonaws.com user=ziuixeipnmbrjm dbname=d5kg55pc96p21p password=<PASSWORD>" "climate.countries.json"
```

### Reference Material
- OGR2OGR Cheat Sheet: http://www.bostongis.com/PrinterFriendly.aspx?content_name=ogr_cheatsheet
- Install GDAL (used Conda): https://ljvmiranda921.github.io/notebook/2019/04/13/install-gdal/#using-conda