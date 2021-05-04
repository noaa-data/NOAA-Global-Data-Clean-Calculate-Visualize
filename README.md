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