"""
Apache Airflow DAG: weather_analytics_dag (dag2)
Author: Data Engineering Pipeline
Description: Hourly/Daily analytical rollup aggregation. Reads raw high-frequency weather records,
             computes daily minimum, maximum, and average temperates/humidity per city, 
             and writes summaries to 'city_analytics.csv' for long-term trends display.
Schedule: Runs daily at midnight (@daily).
"""

import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments for Airflow Tasks
default_args = {
    'owner': 'analytics_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=60),
}

SHARED_CSV_PATH = os.environ.get("WEATHER_CSV_PATH", "/app/data/weather_data.csv")
ANALYTICS_CSV_PATH = os.environ.get("ANALYTICS_CSV_PATH", "/app/data/city_analytics.csv")

def aggregate_daily_weather(**context):
    """
    Task executor that aggregates weather logs by city, computes summary metrics,
    and updates the persistence CSV database path.
    """
    logging.info("Initiating weather analytical rollup task...")
    
    if not os.path.exists(SHARED_CSV_PATH):
        logging.warning("Primary weather_data.csv doesn't exist yet. Aggregation bypassed until records are extracted.")
        return
        
    try:
        # Load raw data records
        df = pd.read_csv(SHARED_CSV_PATH)
        if df.empty:
            logging.warning("Weather logs are empty. Aggregation task bypassed.")
            return
            
        logging.info(f"Loaded {len(df)} raw data records to aggregate.")

        # Compute aggregates per city
        grouped = df.groupby('city')
        
        analytics_records = []
        for city, group in grouped:
            avg_temp = group['temp_c'].mean()
            max_temp = group['temp_c'].max()
            min_temp = group['temp_c'].min()
            avg_humidity = group['humidity'].mean()
            max_wind = group['wind_kph'].max()
            
            # Find the most frequent weather condition in this timeframe
            most_freq_condition = group['condition'].mode().iloc[0] if not group['condition'].empty else "Clear"
            
            analytics_records.append({
                "city": city,
                "avg_temperature_c": round(avg_temp, 1),
                "peak_temperature_c": round(max_temp, 1),
                "floor_temperature_c": round(min_temp, 1),
                "mean_humidity": round(avg_humidity, 1),
                "peak_windspeed_kph": round(max_wind, 1),
                "dominant_condition": most_freq_condition,
                "aggregated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })

        df_analytics = pd.DataFrame(analytics_records)
        
        # Write analytics results
        dir_name = os.path.dirname(ANALYTICS_CSV_PATH)
        if dir_name and not os.path.exists(dir_name):
            os.makedirs(dir_name, exist_ok=True)
            
        df_analytics.to_csv(ANALYTICS_CSV_PATH, index=False)
        logging.info(f"Aggregated trends written to: {ANALYTICS_CSV_PATH}. Rows updated: {len(df_analytics)}")
        
    except Exception as e:
        logging.error(f"Critical error aggregating analytical dataset: {str(e)}")
        raise


with DAG(
    'weather_analytics_dag',
    default_args=default_args,
    description='Cron daily rollup executing average, min, max computations on raw high-frequency weather assets',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:

    compute_aggregates = PythonOperator(
        task_id='aggregate_daily_weather',
        python_callable=aggregate_daily_weather,
    )

    compute_aggregates
