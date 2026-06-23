"""
Apache Airflow DAG: weather_acquisition_dag (dag1)
Author: Data Engineering Pipeline
Description: Fetches real-time weather information for key Indian cities from Open-Meteo REST API,
             cleans and shapes the responses, and appends them to a shared CSV filesystem volume.
Schedule: Runs every 5 minutes (*/5 * * * *).
"""

import os
import json
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments for Airflow Tasks
default_args = {
    'owner': 'weather_admin',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}

# CSV location shared across containers via Docker Volumizing
SHARED_CSV_PATH = os.environ.get("WEATHER_CSV_PATH", "/app/data/weather_data.csv")

# Coordinate mapping for major Indian cities
CITIES = {
    "New Delhi": {"lat": 28.6139, "lon": 77.2090},
    "Mumbai": {"lat": 19.0760, "lon": 72.8777},
    "Bengaluru": {"lat": 12.9716, "lon": 77.5946},
    "Kolkata": {"lat": 22.5726, "lon": 88.3639},
    "Chennai": {"lat": 13.0827, "lon": 80.2707},
    "Hyderabad": {"lat": 17.3850, "lon": 78.4867},
    "Srinagar": {"lat": 34.0837, "lon": 74.7973},
    "Jaipur": {"lat": 26.9124, "lon": 75.7873},
    "Guwahati": {"lat": 26.1445, "lon": 91.7362},
    "Pune": {"lat": 18.5204, "lon": 73.8567}
}

# Weather code definition mapper matching Open-Meteo codes
def translate_weather_code(code):
    weather_codes = {
        0: "Clear Sky",
        1: "Mainly Clear", 2: "Partly Cloudy", 3: "Overcast",
        45: "Foggy", 48: "Depositing Rime Fog",
        51: "Light Drizzle", 53: "Moderate Drizzle", 55: "Dense Drizzle",
        61: "Slight Rain", 63: "Moderate Rain", 65: "Heavy Rain",
        71: "Slight Snow Fall", 73: "Moderate Snow Fall", 75: "Heavy Snow Fall",
        80: "Slight Rain Showers", 81: "Moderate Rain Showers", 82: "Violent Rain Showers",
        95: "Slight/Moderate Thunderstorm", 96: "Thunderstorm with Hail"
    }
    return weather_codes.get(code, "Unspecified Conditions")

def fetch_and_write_weather_data(**context):
    """
    Task executor that requests real-time weather stats from Open-Meteo,
    structures the payload, and outputs to the shared weather CSV database.
    """
    logging.info("Initiating weather data ingestion task...")
    
    # Initialize list to compile city records
    extracted_records = []
    
    for city, coords in CITIES.items():
        try:
            # Build API request parameters (Open-Meteo is a clean free public API without API keys)
            url = f"https://api.open-meteo.com/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}&current_weather=true&relative_humidity_2m=true"
            response = requests.get(url, timeout=12)
            
            if response.status_code == 200:
                data = response.json()
                current = data.get("current_weather", {})
                
                # Fetch humidity if provided or interpolate
                humidity = data.get("current", {}).get("relative_humidity_2m", 65)  # fallback
                
                record = {
                    "city": city,
                    "latitude": coords['lat'],
                    "longitude": coords['lon'],
                    "temp_c": current.get("temperature", 25.0),
                    "humidity": humidity,
                    "wind_kph": current.get("windspeed", 10.0),
                    "condition": translate_weather_code(current.get("weathercode", 0)),
                    "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                
                extracted_records.append(record)
                logging.info(f"Retrieved weather for {city}: {record['temp_c']}°C, {record['condition']}")
            else:
                logging.warning(f"Could not retrieve weather for {city}. HTTP Status Code: {response.status_code}")
                
        except Exception as e:
            logging.error(f"Error extracting data for {city}: {str(e)}")
            
    if not extracted_records:
        raise ValueError("Critical: Extracted weather dataset is empty. Pipeline failed!")

    # Load compiled records into Pandas DataFrame
    df_new = pd.DataFrame(extracted_records)
    
    # Ensure directory folder exists
    dir_name = os.path.dirname(SHARED_CSV_PATH)
    if dir_name and not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)
        logging.info(f"Created shared volume directory path: {dir_name}")
        
    # Append or write fresh records to weather_data.csv
    if os.path.exists(SHARED_CSV_PATH):
        # Read historical records to keep a rolling 24-hour log
        try:
            df_existing = pd.read_csv(SHARED_CSV_PATH)
            # Combine
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            # Remove records older than 48 hours to preserve file size in running contexts
            df_combined['updated_at_dt'] = pd.to_datetime(df_combined['updated_at'])
            cutoff_date = datetime.now() - timedelta(hours=48)
            df_combined = df_combined[df_combined['updated_at_dt'] > cutoff_date]
            df_combined = df_combined.drop(columns=['updated_at_dt'])
            
            df_combined.to_csv(SHARED_CSV_PATH, index=False)
            logging.info(f"Appended records. Total rows in weather storage: {len(df_combined)}")
        except Exception as csv_err:
            logging.error(f"Failed to merge existing CSV file, writing fresh instead. Error: {csv_err}")
            df_new.to_csv(SHARED_CSV_PATH, index=False)
    else:
        df_new.to_csv(SHARED_CSV_PATH, index=False)
        logging.info("Created new weather data CSV and stored baseline records!")
        
    logging.info("Weather pipeline orchestration cycle completed successfully!")


# Declare the DAG Context
with DAG(
    'weather_acquisition_dag',
    default_args=default_args,
    description='Automated 5-minute pipeline fetching and storing Indian city weather datasets',
    schedule_interval='*/5 * * * *', # Execute every 5 minutes
    catchup=False,
    max_active_runs=1,
) as dag:
    
    # Extract Weather Task
    fetch_weather_metrics = PythonOperator(
        task_id='fetch_and_write_weather_data',
        python_callable=fetch_and_write_weather_data,
    )

    fetch_weather_metrics
