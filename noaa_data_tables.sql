CREATE TABLE IF NOT EXISTS climate.csv_checker (
  year CHARACTER VARYING NOT NULL,
  date_create DATE NOT NULL,
  date_update DATE NOT NULL,
  CONSTRAINT csv_checker_pkey PRIMARY KEY (year)
) USING HEAP;

CREATE SEQUENCE IF NOT EXISTS climate.noaa_year_averages_uid_seq;

CREATE TABLE IF NOT EXISTS climate.noaa_year_averages (
  uid BIGINT NOT NULL DEFAULT nextval('climate.noaa_year_averages_uid_seq' :: REGCLASS),
  year INTEGER NOT NULL,
  station CHARACTER VARYING NOT NULL,
  latitude DOUBLE PRECISION NOT NULL,
  longitude DOUBLE PRECISION NOT NULL,
  elevation DOUBLE PRECISION NOT NULL,
  temp DOUBLE PRECISION NOT NULL,
  dewp DOUBLE PRECISION,
  stp DOUBLE PRECISION,
  max DOUBLE PRECISION,
  min DOUBLE PRECISION,
  prcp DOUBLE PRECISION,
  CONSTRAINT noaa_year_averages_pkey PRIMARY KEY (year, station)
) USING HEAP;

SELECT AddGeometryColumn('','climate.noaa_year_averages','geom','4326','POINT', 2);