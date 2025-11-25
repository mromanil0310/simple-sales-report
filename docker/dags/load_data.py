import os
import glob
import shutil
from datetime import datetime, timedelta

import snowflake.connector
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python import PythonOperator

# Load environment variables
load_dotenv()

TABLES = ["users", "orders"]

# Use your local Mac paths (these must be mounted into the container via docker-compose)
LOCAL_DIR = "/Users/marloromanillos/Desktop/Folders/mrom/Snowflake_Project/simple-sales-report/source"
SUCCESS_DIR = "/Users/marloromanillos/Desktop/Folders/mrom/Snowflake_Project/simple-sales-report/success"
FAIL_DIR = "/Users/marloromanillos/Desktop/Folders/mrom/Snowflake_Project/simple-sales-report/fail"


# -------- Scan for files --------
def collect_local_files():
    """Scan local folder for CSV files per table."""
    if not os.path.isdir(LOCAL_DIR):
        raise FileNotFoundError(f"Source folder not found: {LOCAL_DIR}")

    local_files = {}
    for table in TABLES:
        pattern = os.path.join(LOCAL_DIR, f"{table}*.csv")
        files = glob.glob(pattern)
        print(f"Found {len(files)} CSV files for table '{table}'")
        local_files[table] = files

    return local_files


# -------- Archive files --------
def archive_file(src_file, table, status):
    """Move files to success or fail folder with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    base_dir = SUCCESS_DIR if status == "success" else FAIL_DIR
    target_dir = os.path.join(base_dir, table, timestamp)

    os.makedirs(target_dir, exist_ok=True)

    filename = os.path.basename(src_file)
    new_name = f"{table}_{timestamp}_{filename}"
    target_file = os.path.join(target_dir, new_name)

    shutil.move(src_file, target_file)
    print(f"{status.upper()}: Archived to {target_file}")


# -------- Snowflake Process --------
def load_to_snowflake(**kwargs):
    """Upload CSV files for each table: PUT + COPY command."""
    ti = kwargs["ti"]
    local_files = ti.xcom_pull(task_ids="collect_local_files")

    if not local_files:
        print("No files found.")
        return

    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )
    cur = conn.cursor()

    try:
        for table, files in local_files.items():
            if not files:
                print(f"No CSV files for table '{table}', skipping.")
                continue

            for file_path in files:
                try:
                    put_sql = f"PUT file://{file_path} @%{table} AUTO_COMPRESS=TRUE"
                    print(f"Running: {put_sql}")
                    cur.execute(put_sql)
                    print(f"Uploaded {file_path} to @%{table} stage")

                    copy_sql = f"""
                    COPY INTO {table}
                    FROM @%{table}
                    FILE_FORMAT = (
                        TYPE = CSV,
                        FIELD_DELIMITER = ',',
                        SKIP_HEADER = 1,
                        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                        DATE_FORMAT = 'YYYY-MM-DD',
                        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
                    )
                    ON_ERROR = 'CONTINUE'
                    """
                    print(f"Running COPY INTO for {file_path}")
                    cur.execute(copy_sql)
                    print(f"Snowflake COPY successful for {file_path}")

                    archive_file(file_path, table, "success")

                except Exception as e:
                    print(f"FAILED: {file_path} -> {str(e)}")
                    archive_file(file_path, table, "fail")

    finally:
        cur.close()
        conn.close()
        print("Snowflake connection closed.")


# -------- Airflow DAG --------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 11, 24),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "local_csv_to_snowflake",
    default_args=default_args,
    schedule_interval="0 6 * * *",  # every day at 6 AM
    catchup=False,
) as dag:

    collect_local_files_task = PythonOperator(
        task_id="collect_local_files",
        python_callable=collect_local_files,
    )

    load_snowflake_task = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
        provide_context=True,
    )

    collect_local_files_task >> load_snowflake_task
