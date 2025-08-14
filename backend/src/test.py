from __future__ import annotations
import os, json
from datetime import datetime, timezone
from airflow import DAG
from airflow.decorators import task
import pendulum

# --- your two functions, unchanged (minor: use UTC tz-aware time) ---
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
    # If you're using a key file via .env, uncomment the two lines below
    # from dotenv import load_dotenv
    # load_dotenv(); os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    from google.cloud import storage  # import inside so Airflow can serialize DAG
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    now = datetime.now(timezone.utc)  # tz-aware UTC
    year   = now.strftime("%Y")
    month  = now.strftime("%m")
    day    = now.strftime("%d")
    hour   = now.strftime("%H")
    minute = now.strftime("%M")
    ts     = now.strftime("%Y%m%dT%H%M%SZ")

    data = live_station_data()
    json_data = json.dumps(data, indent=2)

    blob_path = f"raw/gbfs/year={year}/month={month}/day={day}/hour={hour}/minute={minute}/gbfs_{ts}.json"
    bucket.blob(blob_path).upload_from_string(json_data, content_type="application/json")
    print(f"Uploaded GBFS data to gs://{bucket_name}/{blob_path}")

# --- Airflow DAG ---
with DAG(
    dag_id="cityride_gbfs_to_gcs",
    description="Every 2 minutes: pull GBFS stub and upload JSON to GCS partitioned path",
    schedule="*/2 * * * *",
    start_date=pendulum.datetime(2025, 8, 12, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["cityride", "gbfs", "gcs"],
) as dag:

    @task()
    def run_upload(bucket_name: str):
        upload_gbfs_data(bucket_name)

    # Option A: hardcode your bucket here
    run_upload(bucket_name="YOUR_GCS_BUCKET_NAME")
