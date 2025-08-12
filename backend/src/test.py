# How to store all the raw data a gcp bucket
import os


def static_data():
    """ Collects the dock static info every one month"""

    # Will add the logic later
    static_station_data = [{'eightd_station_services': [],
      'electric_bike_surcharge_waiver': False,
      'eightd_has_key_dispenser': False,
      'name': 'Saint James Park',
      'region_id': '5',
      'rental_methods': ['KEY', 'CREDITCARD'],
      'short_name': 'SJ-L10',
      'capacity': 15,
      'station_id': 'bae9be55-04d4-4641-9781-3d1c4b6950f1',
      'lon': -121.889937,
      'has_kiosk': True,
      'external_id': 'bae9be55-04d4-4641-9781-3d1c4b6950f1',
      'rental_uris': {'ios': 'https://sfo.lft.to/lastmile_qr_scan',
       'android': 'https://sfo.lft.to/lastmile_qr_scan'},
      'lat': 37.339301,
      'station_type': 'classic'},
     {'eightd_station_services': [],
      'electric_bike_surcharge_waiver': False,
      'eightd_has_key_dispenser': False,
      'name': 'San Jose Diridon Station',
      'region_id': '5',
      'rental_methods': ['KEY', 'CREDITCARD'],
      'short_name': 'SJ-M7-1',
      'capacity': 35,
      'station_id': 'dc2f7685-c3e3-4536-b78b-740479cbb207',
      'lon': -121.901782,
      'has_kiosk': True,
      'external_id': 'dc2f7685-c3e3-4536-b78b-740479cbb207',
      'rental_uris': {'ios': 'https://sfo.lft.to/lastmile_qr_scan',
       'android': 'https://sfo.lft.to/lastmile_qr_scan'},
      'lat': 37.329732,
      'station_type': 'classic'}]

def live_station_data():
    """ Collects and return the live data(Need to update evey 2 minutes)"""

    # Will add the logic later

    live_station_data = [{'station_id': '46b4ef45-b06b-40eb-9fdf-9bc8ff104a4f',
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
     'is_returning': 1}]

    return live_station_data



def get_weather_data():
    """Collects and returns the weather data every 30 minutes"""

    # Will add the logic later
    weather_data ={'cloud_pct': 100, 'temp': 17, 'feels_like': 17, 'humidity': 84, 'min_temp': 16, 'max_temp': 18, 'wind_speed': 1.54, 'wind_degrees': 360, 'sunrise': 1754918447, 'sunset': 1754967886}

    return  weather_data


# updated every 24 hours

def event_data():
    """ Collect return the event data every 24 hours """

    # Will add the logic later
    event_data ={'events': [{'start_datetime': '2025-08-14T03:00:00Z',
       'timezone': 'America/Los_Angeles',
       'venue_lat': 37.335634,
       'venue_lon': -121.887954,
       'source': 'ticketmaster',
       'query_radius_miles': 2,
       'query_lat': 37.323345,
       'query_lon': -121.913497}],
     'metadata': {'number_events': 1, 'radius_events': 2}}

    return event_data




import json
from datetime import datetime
from google.cloud import storage


from dotenv import load_dotenv
load_dotenv()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")



def upload_cityride_data(bucket_name: str):
    """Uploads static, live, weather, and event data to GCS in a partitioned structure."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    now = datetime.utcnow()
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    hour = now.strftime("%H")
    minute = now.strftime("%M")
    timestamp_str = now.strftime("%Y%m%dT%H%M%SZ")  # e.g. 20250812T1402Z

    # Define sources and their data
    sources = {
        "static": static_data(),  # monthly
        "gbfs": live_station_data(),  # every 2 min
        "weather": get_weather_data(),  # every 30 min
        "events": event_data(),  # daily
    }

    for source_name, data in sources.items():
        # Build path
        if source_name == "static":
            blob_path = f"raw/static/year={year}/month={month}/stations_{timestamp_str}.json"
        elif source_name == "gbfs":
            blob_path = f"raw/gbfs/year={year}/month={month}/day={day}/hour={hour}/minute={minute}/gbfs_{timestamp_str}.json"
        elif source_name == "weather":
            blob_path = f"raw/weather/year={year}/month={month}/day={day}/hour={hour}/weather_{timestamp_str}.json"
        elif source_name == "events":
            blob_path = f"raw/events/year={year}/month={month}/day={day}/events_{timestamp_str}.json"

        # Convert to JSON string
        json_data = json.dumps(data, indent=2)

        print(json_data)
        # Upload
        blob = bucket.blob(blob_path)
        blob.upload_from_string(json_data, content_type="application/json")
        print(f"Uploaded {source_name} to gs://{bucket_name}/{blob_path}")


if __name__ == "__main__":
    upload_cityride_data("dockflow")