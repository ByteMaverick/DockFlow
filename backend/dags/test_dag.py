import os
import json
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

from dotenv import load_dotenv
from google.cloud import storage

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

def _upload_gbfs_data():
    load_dotenv(dotenv_path=os.path.expanduser("~/airflow/.env"), override=True)

    creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds or not os.path.exists(creds):
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS missing or file not found")

    bucket_name = os.getenv("CITYRIDE_BUCKET")
    if not bucket_name:
        raise RuntimeError("CITYRIDE_BUCKET is not set")

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    now = datetime.utcnow()
    y, m, d = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
    H, M = now.strftime("%H"), now.strftime("%M")
    ts = now.strftime("%Y%m%dT%H%M%SZ")

    blob_path = f"raw/gbfs/year={y}/month={m}/day={d}/hour={H}/minute={M}/gbfs_{ts}.json"
    data = live_station_data()
    bucket.blob(blob_path).upload_from_string(json.dumps(data, indent=2),
                                              content_type="application/json")
    print(f"Uploaded gbfs to gs://{bucket_name}/{blob_path}")

local_tz = pendulum.timezone("UTC")
with DAG(
    dag_id="cityride_gbfs_every_2min",
    description="Upload GBFS live data to GCS every 2 minutes",
    start_date=pendulum.datetime(2025, 8, 13, tz=local_tz),
    schedule="*/2 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "cityride", "retries": 2},
    tags=["cityride", "gbfs", "gcs"],
) as dag:
    upload_task = PythonOperator(
        task_id="upload_gbfs_to_gcs",
        python_callable=_upload_gbfs_data,
    )
