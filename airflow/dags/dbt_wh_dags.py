import os
import logging
import glob
import boto3
import snowflake.connector
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


# Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = os.getenv("MINIO_BUCKET")
LOCAL_TEMP_DIR = os.getenv("MINIO_LOCAL_DIR")

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

TABLES = ["customers", "accounts", "customers_accounts", "transactions"]

DBT_PROJECT_DIR = "/opt/airflow/realtime_banking_dbt"

# Helpers
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

def archive_files(s3_client, bucket, key):
    """
    Function to archive files in MinIO by moving them to an 'archive/' prefix
    """
    new_key = f"processed/{key}"
    try:
        s3_client.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': key},
            Key=new_key
        )
        s3_client.delete_object(Bucket=bucket, Key=key)
        logger.info(f"Moved {key} to {new_key} in bucket {bucket}.")
    except Exception as e:
        logger.error(f"Error archiving file {key} in bucket {bucket}: {e}")

# dag functions
def download_from_minio(**kwargs):
    """
    Function to download Parquet files from MinIO
    """
    logger.info("Starting download from MinIO...")
    s3 = get_s3_client()
    downloaded_files = {}
    for table in TABLES:
        # Create local directory for the table
        local_table_dir = os.path.join(LOCAL_TEMP_DIR, table)
        os.makedirs(local_table_dir, exist_ok=True)

        # List objects in the MinIO bucket for the table
        prefix = f"{table}/"
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)

        if 'Contents' not in response:
            logger.warning(f"No files found for table {table} in MinIO.")
            continue
        
        file_list = []
        for obj in response['Contents']:
            key = obj['Key']
            if key.startswith("processed/") or not key.endswith('.parquet'):
                continue
            file_name = os.path.basename(key)
            path = os.path.join(local_table_dir, file_name)
            s3.download_file(BUCKET_NAME, key, path)
            file_list.append({
                "local_path": path,
                "s3_key": key,
                "file_name": file_name
            })
        logger.info(f"Downloaded {len(file_list)} new files for table {table}.")
        if file_list:
            downloaded_files[table] = file_list
    return downloaded_files

def load_to_snowflake(**kwargs):
    """
    Function to load Parquet files into Snowflake
    """    
    ti = kwargs['ti'] # task instance
    downloaded_tables = ti.xcom_pull(task_ids='download_minio')

    if not downloaded_tables:
        logger.warning("No new files to process. Skipping...")
        return
    logger.info("Starting load to Snowflake...")
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    cur = conn.cursor()
    s3 = get_s3_client()
    try:
        for table, files in downloaded_tables.items():
            logger.info(f"Processing table {table} with {len(files)} files.")
            for file_info in files:
                local_path = file_info['local_path']
                s3_key = file_info['s3_key']
                file_name = file_info['file_name']

                put_sql = f"PUT file://{local_path} @%{table} AUTO_COMPRESS=TRUE"
                cur.execute(put_sql)
                copy_sql = f"""
                    COPY INTO {table} (v)
                    FROM (
                        SELECT $1 FROM @%{table}
                    )
                    FILE_FORMAT = (TYPE = 'PARQUET')
                    PATTERN = '.*{file_name}.*' 
                    ON_ERROR = 'CONTINUE'
                """
                # we use pattern to ensure only the specific file is loaded, because there are old files from previous loads in the stage
                cur.execute(copy_sql)
                logger.info(f"Loaded file {file_name} into Snowflake table {table}.")

                # Archive file in MinIO after successful load
                archive_files(s3, BUCKET_NAME, s3_key)

                # Remove file from Snowflake stage
                remove_sql = f"REMOVE @%{table}/{file_name}"
                cur.execute(remove_sql)
                logger.info(f"Removed file {file_name} from Snowflake stage for table {table}.")
    except Exception as e:
        logger.error(f"Error loading data into Snowflake: {e}")
    finally:
        cur.close()
        conn.close()
        logger.info("Snowflake connection closed.")

def cleanup_local_files(**kwargs):
    """
    Function to clean up local temporary files 
    """
    import shutil
    if os.path.exists(LOCAL_TEMP_DIR):
        shutil.rmtree(LOCAL_TEMP_DIR)
        logger.info("Cleaned up local temporary files.")
    else:
        logger.info("No local temporary files to clean.")

# DAG
default_args = {
    "owner": "airflow",
    "retries": 1,
    "depends_on_past": True, # current task can only run if previous run succeeded
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="banking_realtime_wh",
    default_args=default_args,
    description="DAG to load data from MinIO to Snowflake and run dbt models/tests",
    schedule="*/15 * * * *",
    start_date=datetime(2023, 11, 29),
    max_active_runs=1,
    catchup=False # do not perform backfill runs
# True when your start date is in the past and you want to run all the missed intervals
) as dag:
    t1_download = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )


    t2_upload = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake
    )

    t3_dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt clean && dbt deps --profiles-dir ."
    )

    # 2. Run Staging (To create view stg_accounts for snapshot)
    t4_dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select staging --profiles-dir ."
    )

    # 3. Snapshot
    t5_dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt snapshot --profiles-dir ."
    )

    # 4. Run Marts
    t6_dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        # exclude staging vì đã chạy ở trên rồi
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --exclude staging --profiles-dir ."
    )
    
    # 5. Test 
    t7_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir ."
    )

    # 6. Cleanup
    t8_cleanup = PythonOperator(
        task_id="cleanup_temp_local",
        python_callable=cleanup_local_files,
        trigger_rule="all_done"
    )
    t1_download >> t2_upload >> t3_dbt_deps >> t4_dbt_run_staging >> t5_dbt_snapshot >> t6_dbt_run_marts >> t7_dbt_test >> t8_cleanup