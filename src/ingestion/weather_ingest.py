import requests 
import json 
from datetime import datetime
import sys
import os
from transformation.clean_weather_data import clean_weather_data


filename = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'raw', 'athens-2025-present.json')
url = "https://archive-api.open-meteo.com/v1/archive?latitude=37.9838&longitude=23.7278&start_date=2025-09-30&end_date=2025-10-14&hourly=temperature_2m,relative_humidity_2m,precipitation,rain,snowfall,wind_speed_10m,wind_gusts_10m"


def log_message(message):
    # print log message with timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def weather_ingestion(url):
    try:
        response = requests.get(url)
        
        ## Check status code category
        if 200 <= response.status_code < 300:
            try:
                weather_data = response.json()
                if weather_data is None:
                    print("JSON file is None")
                elif not weather_data:
                    print("JSON file is empty")
                else:
                    print("JSON has data:", weather_data["hourly"].keys())
            except ValueError:
                print("Response is not valid JSON file")
            log_message(f"Success! Status code: {response.status_code}")
            return weather_data
        
        elif 300 <= response.status_code < 400:
            log_message(f"Redirection! Status code: {response.status_code}")
            ## For redirection, you might want to follow the redirect
            return None
        
        elif 400 <= response.status_code < 500:
            log_message(f"Client error! Status code: {response.status_code}")
            ## Handle client errors
            return None
        
        elif 500 <= response.status_code < 600:
            log_message(f"Server error! Status code: {response.status_code}")
            ## Handle server errors
            return None
        
    except requests.exceptions.RequestException as e:
        log_message(f"Request failed: {e}")
        return None

# Import config parameters
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from config.api_params import api_params 

print("Config loaded:", api_params)
weather_data = weather_ingestion(url)

os.makedirs(os.path.dirname(filename), exist_ok=True)

# checking if the json file exists and if it has data in it.
if os.path.exists(filename) and os.path.getsize(filename) > 0:
    with open(filename, 'r') as f:
        existing_data = json.load(f)
else:
    existing_data = []

if not weather_data:
    log_message("No data to append. Skipping save.")
else:
    cleaned_data = clean_weather_data(weather_data, city_name="Athens")
    existing_data.append(cleaned_data)
    with open(filename, 'w') as f:
        json.dump(existing_data, f, indent=4)
    log_message(f"File saved successfully: {filename}")

