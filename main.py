# main.py
import os
import csv
import io
import time
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dateutil import parser as dateparser
from google.cloud import storage

OPENAQ_API_KEY = os.environ.get("OPENAQ_API_KEY")
GCS_BUCKET = os.environ.get("GCS_BUCKET")
DEFAULT_CITIES = os.environ.get("CITIES", "Warsaw,London").split(",")

PARAMETERS = ["pm25", "pm10", "o3", "no2"]


def requests_session_with_retries(total_retries=3, backoff_factor=0.5) -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


SESSION = requests_session_with_retries()

BASE_URL = "https://api.openaq.org/v3"


def _get(path: str, params: Dict[str, Any]):
    headers = {}
    if OPENAQ_API_KEY:
        headers["Authorization"] = f"Bearer {OPENAQ_API_KEY}"
    url = f"{BASE_URL}{path}"
    logging.debug("GET %s params=%s", url, params)
    resp = SESSION.get(url, params=params, headers=headers, timeout=15)
    resp.raise_for_status()
    return resp.json()


# Get up to N distinct locations (station names) for a city and parameter
def get_top_locations_for_city(city: str, parameter: str, limit=10, take_n=3) -> List[Dict[str, Any]]:
    """
    Returns a list of location dicts (as returned by /locations) up to take_n unique locations.
    """
    params = {"city": city, "parameter": parameter, "limit": limit, "sort": "desc", "order_by": "lastUpdated"}
    data = _get("/locations", params)
    locations = data.get("results", [])
    # Deduplicate by location name and return up to take_n
    seen = set()
    out = []
    for loc in locations:
        name = loc.get("name") or loc.get("location")
        if not name:
            continue
        if name in seen:
            continue
        seen.add(name)
        out.append(loc)
        if len(out) >= take_n:
            break
    return out


# Get latest measurement for a given location id and parameter
def get_latest_measurement_for_location(location_id: int, parameter: str) -> Optional[Dict[str, Any]]:
    params = {
        "location_id": location_id,
        "parameter": parameter,
        "limit": 1,
        "sort": "desc",
        "order_by": "date"
    }
    try:
        data = _get("/measurements", params)
    except requests.HTTPError as e:
        logging.warning("HTTPError when fetching measurements: %s", e)
        return None
    results = data.get("results", [])
    if not results:
        return None
    return results[0]


# Validate and normalize a measurement result
def validate_measurement(meas: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Ensure value present, numeric, unit present, and datetime parseable.
    Returns normalized dict with keys: value (float), unit, date_utc (ISO str)
    """
    try:
        value = meas.get("value")
        if value is None:
            return None
        # numeric?
        value = float(value)
        unit = meas.get("unit") or meas.get("parameter_unit")
        if not unit:
            return None
        date_info = meas.get("date") or {}
        # date.utc expected
        date_utc = date_info.get("utc") if isinstance(date_info, dict) else date_info
        if not date_utc:
            # try local
            date_utc = date_info.get("local") if isinstance(date_info, dict) else None
        # parse
        dt = dateparser.parse(date_utc)
        # normalize to ISO-8601 UTC
        dt_utc = dt.astimezone(timezone.utc).isoformat()
        return {"value": value, "unit": unit, "date_utc": dt_utc}
    except Exception as e:
        logging.debug("Measurement validation failed: %s  -- %s", meas, e)
        return None


def build_csv_rows_for_city(city: str, max_locations_per_city=3) -> List[List[Any]]:
    """
    For each requested parameter, fetch top locations and get latest measurement.
    Return rows: [city, location_name, parameter, value, unit, date_utc]
    """
    rows = []
    # We'll collect up to max_locations_per_city unique locations per city (across parameters)
    chosen_locations = {}  # location_id -> location_name
    for param in PARAMETERS:
        try:
            locs = get_top_locations_for_city(city, param, limit=10, take_n=max_locations_per_city)
        except Exception as e:
            logging.warning("Failed to get locations for city=%s param=%s: %s", city, param, e)
            continue
        for loc in locs:
            loc_id = loc.get("id") or loc.get("locationId") or loc.get("location_id")
            loc_name = loc.get("name") or loc.get("location") or loc.get("city")
            if not loc_id:
                continue
            if len(chosen_locations) < max_locations_per_city and loc_id not in chosen_locations:
                chosen_locations[loc_id] = loc_name
    # For each chosen location and each parameter, attempt to fetch latest measurement
    for loc_id, loc_name in chosen_locations.items():
        for param in PARAMETERS:
            try:
                meas = get_latest_measurement_for_location(loc_id, param)
            except Exception as e:
                logging.warning("Failed to fetch measurement loc=%s param=%s: %s", loc_id, param, e)
                meas = None
            if not meas:
                continue
            valid = validate_measurement(meas)
            if not valid:
                logging.debug("Invalid measurement for loc=%s param=%s: %s", loc_id, param, meas)
                continue
            rows.append([
                city.strip(),
                loc_name,
                param,
                valid["value"],
                valid["unit"],
                valid["date_utc"]
            ])
    return rows


def upload_text_to_gcs(bucket_name: str, destination_path: str, text: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_path)
    blob.upload_from_string(text, content_type="text/csv")
    logging.info("Uploaded to gs://%s/%s", bucket_name, destination_path)
    return f"gs://{bucket_name}/{destination_path}"


# Cloud Function entrypoint
def fetch_openaq_measurements(request):
    """
    HTTP Cloud Function.
    Optional JSON body to override:
    {
      "cities": ["Warsaw","London"],
      "max_locations_per_city": 3
    }
    """
    try:
        req_json = request.get_json(silent=True) or {}
    except Exception:
        req_json = {}
    cities = req_json.get("cities") or DEFAULT_CITIES
    try:
        max_locations = int(req_json.get("max_locations_per_city", 3))
    except Exception:
        max_locations = 3

    if not GCS_BUCKET:
        return ("Environment variable GCS_BUCKET not set", 500)

    # Collect rows
    all_rows = []
    header = ["city", "location", "parameter", "value", "unit", "date_utc"]
    for city in cities:
        try:
            rows = build_csv_rows_for_city(city, max_locations_per_city=max_locations)
            logging.info("City %s -> %d rows", city, len(rows))
            all_rows.extend(rows)
            # polite pause to respect rate limits
            time.sleep(0.2)
        except Exception as e:
            logging.exception("Error processing city %s: %s", city, e)

    if not all_rows:
        return ("No measurements collected", 204)

    # Build CSV in-memory
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(header)
    for r in all_rows:
        writer.writerow(r)
    csv_text = output.getvalue()

    # Filename with timestamp
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = f"/openaq_measurements_{ts}.csv"

    # Upload to GCS
    try:
        gspath = upload_text_to_gcs(GCS_BUCKET, filename, csv_text)
    except Exception as e:
        logging.exception("Failed to upload CSV to GCS: %s", e)
        return (f"Upload failed: {e}", 500)

    return (f"OK. Uploaded {len(all_rows)} rows to {gspath}", 200)
