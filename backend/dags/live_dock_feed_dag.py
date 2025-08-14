import os
import json
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

from dotenv import load_dotenv
from google.cloud import storage



#Import Dependencies
import requests, json

def live_station_data() -> list:
  #get station Ids for San Jose stations
  region5_station_ids = {}
  ## hardcode region, San Jose = 5
  san_jose_region_id="5"

  url = "https://gbfs.lyft.com/gbfs/1.1/bay/en/station_information.json"
  resp = requests.get(url, timeout=15)
  resp.raise_for_status()
  payload = resp.json()

  # load latest station_information
  stations = payload["data"]["stations"]

  for station in stations:

        if station.get("region_id") == san_jose_region_id:
            region5_station_ids[station.get("station_id")] = "5"

  url = "https://gbfs.lyft.com/gbfs/1.1/bay/en/station_status.json"
  resp = requests.get(url, timeout=15)
  resp.raise_for_status()
  payload = resp.json()

  stations_live_status_sj = []
  # load latest station_information
  stations_live_status = payload["data"]["stations"]
  for station in stations_live_status:
      if station.get("station_id") in region5_station_ids:
            stations_live_status_sj.append(station)

  return stations_live_status_sj


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
    dag_id="dockflow_gbfs_every_2min",
    description="Upload GBFS live data to GCS every 2 minutes",
    start_date=pendulum.datetime(2025, 8, 14, tz=local_tz),
    schedule="*/2 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "cityride", "retries": 2},
    tags=["dockflow", "gbfs", "gcs"],
) as dag:
    upload_task = PythonOperator(
        task_id="upload_gbfs_to_gcs",
        python_callable=_upload_gbfs_data,
    )
