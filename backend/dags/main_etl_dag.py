import json
import os
from datetime import datetime
from google.cloud import storage
from dotenv import load_dotenv
import pandas as pd

load_dotenv()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

def get_dock_feed():

    bucket_name,start_date =  "dockflow", "2025-08-10"
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Broad prefix to get all GBFS data
    prefix = "raw/gbfs/"
    data =[]

    for blob in bucket.list_blobs(prefix=prefix):
        #  parsing date from blob name: raw/gbfs/year=YYYY/month=MM/day=DD/...
        try:
            parts = blob.name.split("/")
            year = int(parts[2].split("=")[1])
            month = int(parts[3].split("=")[1])
            day = int(parts[4].split("=")[1])
            blob_date = datetime(year, month, day).date()
        except Exception:
            continue  # Skip if format doesn't match

        if blob_date >= start_date:
            data_json = json.loads(blob.download_as_text())
            data.append(data_json)


    flattened_data = []
    for sublist in data:
        flattened_data.extend(sublist)

    df1 = pd.DataFrame(flattened_data)

    return df1

def get_dock_feed_static():

    bucket_name,start_date =  "dockflow", "2025-08-10"
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Broad prefix to get all GBFS data
    prefix = "raw/static/"
    data =[]

    for blob in bucket.list_blobs(prefix=prefix):
        try:
            parts = blob.name.split("/")
            # Case 1: folder-style year/month/day
            filename = parts[-1]
            # Extract 20250814 from "stations_20250814T105313Z.json"
            date_str = filename.split("_")[1][:8]
            blob_date = datetime.strptime(date_str, "%Y%m%d").date()

        except Exception as e:
            continue

    if blob_date >= start_date:
        data_json = json.loads(blob.download_as_text())
        data.append(data_json)



    flattened_data = []
    for sublist in data:
        flattened_data.extend(sublist)

    df1 = pd.DataFrame(flattened_data)

    return df1


def get_event_metadata_table():
    bucket_name, start_date = "dockflow", "2025-08-10"
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    prefix = "raw/events/"

    rows = []

    for blob in bucket.list_blobs(prefix=prefix):
        try:
            parts = blob.name.split("/")
            year = int(parts[2].split("=")[1])
            month = int(parts[3].split("=")[1])
            day = int(parts[4].split("=")[1])
            blob_date = datetime(year, month, day).date()
        except Exception:
            continue

        if blob_date >= start_date:
            data_json = json.loads(blob.download_as_text())
            meta = data_json.get("metadata", {})

            row = {
                "date_stamp": str(blob_date),
                "radius_events": meta.get("radius_events"),
                "number_events": meta.get("number_events")
            }
            rows.append(row)

    df = pd.DataFrame(rows)
    return df

def get_weather_feed():
    bucket_name, start_date = "dockflow", "2025-08-10"
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    prefix = "raw/weather/"

    rows = []

    for blob in bucket.list_blobs(prefix=prefix):
        try:
            parts = blob.name.split("/")
            year = int(parts[2].split("=")[1])
            month = int(parts[3].split("=")[1])
            day = int(parts[4].split("=")[1])
            blob_date = datetime(year, month, day).date()
        except Exception:
            continue

        if blob_date >= start_date:
            data_json = json.loads(blob.download_as_text())

            # Add blob timestamp
            row = data_json.copy()
            row["collected_at"] = blob.time_created.isoformat()
            rows.append(row)

    df1 = pd.DataFrame(rows)
    return df1

