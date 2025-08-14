import os, time, requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import os
import json
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

from dotenv import load_dotenv
from google.cloud import storage

load_dotenv(dotenv_path=os.path.expanduser("~/airflow/.env"), override=True)

def fetch_events_ticketmaster_miles(radius_miles=30, page_size=100, max_pages=10, sleep_sec=0.2, apikey=None):


    lat=37.3382
    lon=-121.8863
    start_utc = datetime.now(timezone.utc)
    end_utc = start_utc + timedelta(days=1)

    def iso_utc(dt):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    apikey = apikey or os.getenv("TICKETMASTER")
    if not apikey:
        raise RuntimeError("Set TICKETMASTER env var or pass apikey=...")

    base_url = "https://app.ticketmaster.com/discovery/v2/events.json"
    params = {
        "apikey": apikey,
        "latlong": f"{lat},{lon}",
        "radius": int(radius_miles),
        "unit": "miles",
        "startDateTime": iso_utc(start_utc),
        "endDateTime": iso_utc(end_utc),
        "countryCode": "US",
        "locale": "en-us",
        "size": int(page_size),
        "sort": "date,asc",
        "page": 0,
    }

    events = []
    for page in range(int(max_pages)):
        params["page"] = page
        r = requests.get(base_url, params=params, timeout=20)
        if r.status_code == 429:
            time.sleep(1.0)
            r = requests.get(base_url, params=params, timeout=20)
        if not r.ok:
            try:
                print("Error body:", r.json())
            except Exception:
                print("Error body (text):", r.text)
            r.raise_for_status()

        data = r.json()
        page_events = (data.get("_embedded") or {}).get("events") or []
        if not page_events:
            break

        fetched_at = iso_utc(datetime.now(timezone.utc))
        for ev in page_events:
            classes = ev.get("classifications") or []
            seg = (classes[0].get("segment") or {}).get("name") if classes else None
            genr = (classes[0].get("genre") or {}).get("name") if classes else None

            venues = (ev.get("_embedded") or {}).get("venues") or []
            v = venues[0] if venues else {}
            loc = v.get("location") or {}
            try:
                lat_v = float(loc.get("latitude")) if loc.get("latitude") is not None else None
                lon_v = float(loc.get("longitude")) if loc.get("longitude") is not None else None
            except (TypeError, ValueError):
                lat_v = lon_v = None

            public_sales = (ev.get("sales") or {}).get("public") or {}
            public_start = public_sales.get("startDateTime")
            public_end = public_sales.get("endDateTime")

            events.append({
                "event_id": ev.get("id"),
                "name": ev.get("name"),
                "segment": seg,
                "genre": genr,
                "start_datetime": (ev.get("dates") or {}).get("start", {}).get("dateTime"),
                "timezone": (ev.get("dates") or {}).get("timezone"),
                "venue_id": v.get("id"),
                "venue_name": v.get("name"),
                "venue_city": (v.get("city") or {}).get("name"),
                "venue_state": (v.get("state") or {}).get("stateCode"),
                "venue_lat": lat_v,
                "venue_lon": lon_v,
                "public_sale_start": public_start,
                "public_sale_end": public_end,
                "fetched_at": fetched_at,
                "source": "ticketmaster",
                "query_radius_miles": radius_miles,
                "query_lat": lat,
                "query_lon": lon,
            })

        page_info = data.get("page", {})
        if page >= page_info.get("totalPages", 1) - 1:
            break
        time.sleep(float(sleep_sec))

    return {"events": events, "metadata": {"number_events": len(events), "radius_events": radius_miles}}

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

    blob_path = f"raw/events/year={y}/month={m}/day={d}/events_{ts}.json"
    data = fetch_events_ticketmaster_miles()
    bucket.blob(blob_path).upload_from_string(json.dumps(data, indent=2),
                                              content_type="application/json")
    print(f"Uploaded gbfs to gs://{bucket_name}/{blob_path}")

local_tz = pendulum.timezone("UTC")
with DAG(
    dag_id="dockflow_event_feed_24hours",
    description="Upload GBFS live data to GCS every 2 minutes",
    start_date=pendulum.datetime(2025, 8, 13, tz=local_tz),
    schedule="0 1 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "cityride", "retries": 2},
    tags=["cityride", "gbfs", "gcs"],
) as dag:
    upload_task = PythonOperator(
        task_id="upload_gbfs_to_gcs",
        python_callable=_upload_gbfs_data,
    )

