from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.docker.operators.docker import DockerOperator
import csv
import boto3
import os
from io import StringIO

# === CONFIG ===
BUCKET_NAME = os.environ['BUCKET_NAME']
PREFIX = os.environ['PREFIX']
ARCHIVE_PREFIX = os.environ['ARCHIVE_PREFIX']
STAGE_NAME = os.environ['STAGE_NAME']
SUPPORTED_EXTENSIONS = os.environ['SUPPORTED_EXTENSIONS']
DELIMITER=os.environ['DELIMITER']
SNOWFLAKE_SCHEMA= os.environ['SNOWFLAKE_SCHEMA']
SNOWFLAKE_DATABASE= os.environ['SNOWFLAKE_DATABASE']
# === CLIENTS ===

def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        region_name=os.environ.get('AWS_REGION', '')
    )

def get_snowflake_hook():
    return SnowflakeHook(
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
        database=os.environ['SNOWFLAKE_DATABASE'],
        schema=os.environ['SNOWFLAKE_SCHEMA']
    )

# === TASK LOGIC ===

def list_files(**context):
    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)
    files = [
        obj['Key']
        for obj in response.get('Contents', [])
        if os.path.splitext(obj['Key'])[1].lower() in SUPPORTED_EXTENSIONS
    ]
    print(f"Discovered files: {files}")
    return files

# Create Storage Integration
def generate_storage_integration_sql():
    aws_role = os.getenv("AWS_ROLE_ARN")
    external_id = os.getenv("AWS_EXTERNAL_ID")
    allowed_locations = os.getenv("AWS_ALLOWED_LOCATIONS")

    if not all([aws_role, external_id, allowed_locations]):
        raise ValueError("Missing required environment variables.")

    return f"""
    CREATE OR REPLACE STORAGE INTEGRATION my_s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = '{aws_role}'
    STORAGE_ALLOWED_LOCATIONS = {allowed_locations}
    STORAGE_AWS_EXTERNAL_ID = '{external_id}'
    """

def run_storage_integration_sql(**context):
    sql = context['ti'].xcom_pull(task_ids='prepare_storage_sql', key='integration_sql')
    sf = get_snowflake_hook()
    conn = sf.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    cursor.close()
    conn.close()

def create_database_schema_if_not_exists():
    sf = get_snowflake_hook()
    conn = sf.get_conn()
    cursor = conn.cursor()
    try:
        # Make sure database is set
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE}")
        cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")

        # Create schema inside it
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_SCHEMA}")
        print(f"✅ Created/Ensured SCHEMA `{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}`")
    finally:
        cursor.close()
        conn.close()

def create_stage_if_not_exists():
    # Load required values (make sure these are available at runtime)
    stage_name = STAGE_NAME
    bucket_name = BUCKET_NAME
    prefix = PREFIX
    if not bucket_name:
        raise ValueError("Missing required env var BUCKET_NAME")

    sql = f"""
        CREATE STAGE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{stage_name}
        URL='s3://{bucket_name}/{prefix}'
        STORAGE_INTEGRATION = my_s3_integration;
    """

    sf = get_snowflake_hook()
    conn = sf.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        print(f"✅ Created/Ensured stage `{stage_name}`")
    finally:
        cursor.close()
        conn.close()

def get_csv_headers_from_s3(bucket, key):
    """Reads the header row from a CSV file in S3."""
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    first_line = obj['Body'].readline().decode('utf-8')
    reader = csv.reader(StringIO(first_line), delimiter=DELIMITER)
    return next(reader)

def create_file_format():
    sf = get_snowflake_hook()
    conn = sf.get_conn()
    cursor = conn.cursor()

    sql = f"""
    CREATE OR REPLACE FILE FORMAT my_csv_format
      TYPE = 'CSV'
      FIELD_DELIMITER = '{DELIMITER}'
      FIELD_OPTIONALLY_ENCLOSED_BY = '"'
      SKIP_HEADER = 1;
    """

    cursor.execute(sql)
    cursor.close()
    conn.close()

def create_tables(**context):
    sf = get_snowflake_hook()
    conn = sf.get_conn()
    cursor = conn.cursor()

    STAGE_NAME = 'my_s3_stage'  # Replace if not global
    TABLE_PREFIX = 'raw_'

    files = context['ti'].xcom_pull(task_ids='list_files')

    for file_key in files:
        filename = os.path.basename(file_key)
        table_name = TABLE_PREFIX + os.path.splitext(filename)[0].lower()
        stage_path = f"@{STAGE_NAME}/{filename}"

        infer_sql = f"""
        SELECT *
        FROM TABLE(
            INFER_SCHEMA(
                LOCATION => '{stage_path}',
                FILE_FORMAT => 'my_csv_format'
            )
        );
        """

        try:
            headers = get_csv_headers_from_s3(BUCKET_NAME, file_key)
            if not headers:
                print(f"⚠️ No headers found in `{file_key}`, skipping.")
                continue

            cursor.execute(infer_sql)
            inferred_types  = cursor.fetchall()

            if len(headers) != len(inferred_types):
                print(f"❌ Column count mismatch in `{file_key}` (headers vs inferred types).")
                print(f" headers {headers}")
                print(f" inferred_types {inferred_types}")
                continue

            cols_sql = ",\n  ".join([
                f'"{col_name.strip()}" {col_type} {"NULL" if nullable == "YES" else "NOT NULL"}'
                for col_name, (col_id, col_type, nullable, *_) in zip(headers, inferred_types)
            ])

            create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n  {cols_sql}\n);'

            cursor.execute(create_sql)
            print(f"✅ Created/Ensured table `{table_name}` from `{file_key}`")

        except Exception as e:
            print(f"❌ Error processing `{file_key}`: {e}")

    cursor.close()
    conn.close()

def load_data(**context):
    files = context['ti'].xcom_pull(task_ids='list_files')

    if not files:
        raise ValueError("No files found in XCom from task `list_files`. Got: None")

    sf = get_snowflake_hook()
    conn = sf.get_conn()
    cursor = conn.cursor()

    STAGE_NAME = 'my_s3_stage'     # or your actual stage name
    TABLE_PREFIX = 'raw_'          # to match created table names

    try:
        for key in files:
            filename = os.path.basename(key)
            table_name = TABLE_PREFIX + os.path.splitext(filename)[0].lower()
            pattern = f".*{filename}"

            sql = f"""
                COPY INTO "{table_name}"
                FROM @{STAGE_NAME}
                PATTERN = '{pattern}'
                FILE_FORMAT = my_csv_format
                ON_ERROR = 'CONTINUE';
            """

            cursor.execute(sql)
            print(f"⬇️ Loaded data into `{table_name}`")
    finally:
        cursor.close()
        conn.close()


def archive_files(**context):
    s3 = get_s3_client()
    files = context['ti'].xcom_pull(task_ids='list_files')
    for key in files:
        archive_key = key.replace(PREFIX, ARCHIVE_PREFIX)
        s3.copy_object(Bucket=BUCKET_NAME, CopySource={'Bucket': BUCKET_NAME, 'Key': key}, Key=archive_key)
        s3.delete_object(Bucket=BUCKET_NAME, Key=key)
        print(f"📦 Archived {key} → {archive_key}")

# === DAG ===

with DAG(
        dag_id='s3_to_snowflake_modular',
        schedule_interval="49 18 * * *",
        start_date=days_ago(1),
        catchup=False,
        description='Modular S3 to Snowflake pipeline with stage + schema inference',
) as dag:


    list_files = PythonOperator(
        task_id='list_files',
        python_callable=list_files
    )


    create_storage_integration = PythonOperator(
        task_id='create_storage_integration',
        python_callable= run_storage_integration_sql
    )

    create_database_and_schema = PythonOperator(
        task_id='create_database_and_schema',
        python_callable= create_database_schema_if_not_exists
    )

    create_stage_if_not_exists = PythonOperator(
        task_id='create_stage_if_not_exists',
        python_callable=create_stage_if_not_exists
    )

    create_tables_in_snowflake = PythonOperator(
        task_id='create_tables_in_snowflake',
        python_callable=create_tables
    )

    load_data_from_stage = PythonOperator(
        task_id='load_data_from_stage',
        python_callable=load_data
    )

    archive_processed_files = PythonOperator(
        task_id='archive_processed_files',
        python_callable=archive_files
    )

    create_format_task = PythonOperator(
        task_id="create_file_format",
        python_callable=create_file_format
    )

    dbt_run = DockerOperator(
        task_id='dbt_run',
        image='dbt-project-image:latest',
        api_version='auto',
        auto_remove=True,
        command='dbt run',
        docker_url='unix://var/run/docker.sock',
        network_mode='etl_angular',
        working_dir='/dbt',
        mount_tmp_dir=False
    )

    # DAG flow
    list_files >> create_storage_integration >> create_database_and_schema >> create_stage_if_not_exists >> create_format_task >> create_tables_in_snowflake >> load_data_from_stage >> dbt_run >> archive_processed_files
