drop database if exists s3_to_snowflake;

use role accountadmin;

--Database Creation 
create database if not exists s3_to_snowflake;

-- --Specify the active/current database for the session.
use s3_to_snowflake;

-- Create storage integration (run this once, outside of the main script)
CREATE OR REPLACE STORAGE INTEGRATION S3_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = GCS
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://gcp_pipelines/output_files/');

-- Describe the integration to get the service account email
DESC INTEGRATION S3_INTEGRATION;

-- GRANT USAGE ON INTEGRATION S3_INTEGRATION TO ROLE accountadmin;

--File Format Creation
create or replace file format my_parquet_format
type = parquet;

-- Create stage
CREATE OR REPLACE STAGE s3_to_snowflake.PUBLIC.superstore_sfdataset 
  URL = 'gcs://gcp_pipelines/output_files/'
  STORAGE_INTEGRATION = S3_INTEGRATION;  

DESC STAGE s3_to_snowflake.PUBLIC.superstore_sfdataset;

list @s3_to_snowflake.PUBLIC.superstore_sfdataset;

--Table Creation
CREATE OR REPLACE EXTERNAL TABLE s3_to_snowflake.PUBLIC.Airflow_Test01
WITH LOCATION = @s3_to_snowflake.PUBLIC.superstore_sfdataset
auto_refresh = false
FILE_FORMAT = (format_name = my_parquet_format);

select * from s3_to_snowflake.PUBLIC.Airflow_Test01;

Create Snowflake Connection