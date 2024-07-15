from airflow import models
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

CLUSTER_NAME = 'dataproc-airflow-cluster'
REGION = 'us-central1' #region
PROJECT_ID = 'airflow-workings'
PYSPARK_URI = 'gs://gcp_pipelines/code/transform.py'
# PATH =  'gs://gcp_pipelines/dataproc-spark-configs/pip_install.sh'


CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
        # "init_actions_uri": [PATH],
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
        # "init_actions_uri": [PATH],
    },
    "gce_cluster_config": {
        "zone_uri": "us-central1-a",  # Changed from asia-southeast1-c to asia-southeast1-a
    },
}


PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}

with models.DAG(
    "example_dataproc_airflow_gcp_to_snowflake",
    schedule_interval=None,  
    start_date=days_ago(1),
    catchup=False,
    tags=["dataproc_airflow"],
) as dag:
    
    create_schema = SnowflakeOperator(
        task_id='create_schema',
        sql="CREATE SCHEMA IF NOT EXISTS Airflow_Test01",
        snowflake_conn_id='snowflake_conn'
    )
    
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    submit_job = DataprocSubmitJobOperator(
        task_id="pyspark_task", 
        job=PYSPARK_JOB, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    
    gcs_to_snowflake = SnowflakeOperator(
        task_id="transfer_data_to_snowflake",
        sql="""
        CREATE OR REPLACE EXTERNAL TABLE s3_to_snowflake.PUBLIC.Airflow_Test01
        WITH LOCATION = @s3_to_snowflake.PUBLIC.superstore_sfdataset
        auto_refresh = false
        FILE_FORMAT = (format_name = my_parquet_format);
        """,
        snowflake_conn_id="snowflake_conn"
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", 
        project_id=PROJECT_ID, 
        cluster_name=CLUSTER_NAME, 
        region=REGION
    )

    create_schema >> create_cluster >> submit_job >> [delete_cluster, gcs_to_snowflake]