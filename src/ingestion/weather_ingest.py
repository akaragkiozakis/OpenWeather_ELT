import json
import os
import sys

import requests

# Add project root to path for imports
sys.path.append(r"C:\courses\OpenWeather_elt")

from config.api_params import api_params

# Parameters
BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
START_DATE = "2020-02-15"
END_DATE = "2020-12-31"
DAILY = "temperature_2m_max,temperature_2m_min,precipitation_sum,snowfall_sum,wind_speed_10m_max"
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw")
os.makedirs(OUTPUT_DIR, exist_ok=True)

for r in api_params:
    url = f"{BASE_URL}?latitude={r['lat']}&longitude={r['lon']}&start_date={START_DATE}&end_date={END_DATE}&daily={DAILY}&timezone=auto"
    resp = requests.get(url)
    if resp.status_code == 200:
        out_path = os.path.join(OUTPUT_DIR, f"{r['region'].replace(' ', '_')}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(resp.json(), f, indent=2)
        print(f"Saved {r['region']}")
    else:
        print(f"Failed for {r['region']}: {resp.status_code}")
