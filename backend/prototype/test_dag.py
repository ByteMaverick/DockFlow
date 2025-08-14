from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import json
from google.cloud import storage
from dotenv import load_dotenv

# ==============================
# Your existing functions
# ==============================
def live_station_data():
    return [{
        'station_id': '46b4ef45-b06b-40eb-9fdf-9bc8ff104a4f',
        'last_reported': 1754985922,
        'num_ebikes_available': 2,
        'legacy_id': '299',
        'num_docks_disabled': 0,
        'num_scooters_unavailable': 0,
        'eightd_has_available_keys': False,
        'num_bikes_disabled': 0,
        'is_renting': 1,
        'num_docks_available': 7,
        'num_bikes_available': 8,
        'num_scooters_available': 0,
        'is_installed': 1,
        'is_returning': 1
    }]

def upload_gbfs_data(bucket_name: str):
    """Uploads live GBFS data to GCS every 2 min in a partitioned structure."""
    load_dotenv()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    now = datetime.utcnow()
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    hour = now.strftime("%H")
    minute = now.strftime("%M")
    timestamp_str = now.strftime("%Y%m%dT%H%M%SZ")

    data = live_station_data()
    json_data = json.dumps(data, indent=2)

    blob_path = f"raw/gbfs/year={year}/month={month}/day={day}/hour={hour}/minute={minute}/gbfs_{timestamp_str}.json"
    blob = bucket.blob(blob_path)
    blob.upload_from_string(json_data, content_type="application/json")

    print(f"Uploaded GBFS data to gs://{bucket_name}/{blob_path}")

# ==============================
# Airflow DAG
# ==============================
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "gbfs_live_data_to_gcs",
    default_args=default_args,
    description="Collect GBFS live data every 2 minutes and store in GCS",
    schedule_interval="*/2 * * * *",  # every 2 minutes
    start_date=datetime(2025, 8, 12),
    catchup=False,
    max_active_runs=1,
    tags=["gbfs", "cityride"],
) as dag:

    upload_task = PythonOperator(
        task_id="upload_gbfs_data",
        python_callable=upload_gbfs_data,
        op_args=["your-gcs-bucket-name"],  # Replace with your bucket
    )

    upload_task
