import requests
import os
import json
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

from dotenv import load_dotenv
from google.cloud import storage


load_dotenv(dotenv_path=os.path.expanduser("~/airflow/.env"), override=True)

def get_weather_data_san_jose():
    ninja_api = os.getenv("NINJAAPI")
    # Latitude & Longitude values
    lat=37.3382
    lon=-121.8863
    api_url = f'https://api.api-ninjas.com/v1/weather?lat={lat}&lon={lon}'
    # Make the request
    response = requests.get(api_url, headers={'X-Api-Key': ninja_api})

    if response.status_code == requests.codes.ok:
        return response.json()

    else:
        return "error"

def _upload_gbfs_data():

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

    blob_path = f"raw/weather/year={y}/month={m}/day={d}/hour={H}/minute={M}/weather_{ts}.json"
    data = get_weather_data_san_jose()
    bucket.blob(blob_path).upload_from_string(json.dumps(data, indent=2),
                                              content_type="application/json")
    print(f"Uploaded gbfs to gs://{bucket_name}/{blob_path}")

local_tz = pendulum.timezone("UTC")
with DAG(
    dag_id="dockflow_weather_data_15minutes",
    description="Upload GBFS live data to GCS every 2 minutes",
    start_date=pendulum.datetime(2025, 8, 13, tz=local_tz),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "cityride", "retries": 2},
    tags=["cityride", "gbfs", "gcs"],
) as dag:
    upload_task = PythonOperator(
        task_id="upload_gbfs_to_gcs",
        python_callable=_upload_gbfs_data,
    )
