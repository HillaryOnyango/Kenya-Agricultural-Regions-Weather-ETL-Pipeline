from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import sqlalchemy
import logging
import os
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("weather_etl_logger")

# Load environment variables
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

# Environment variables
API_KEY = os.getenv("API_KEY")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB")
BASE_URL = os.getenv("WEATHER_API_BASE_URL", "https://api.openweathermap.org/data/2.5/weather")

# Construct database URL
POSTGRES_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Validate environment variables
def validate_env_vars():
    required_vars = ["API_KEY", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Agricultural Regions
REGIONS = [
    {"name": "Eldoret", "lat": 0.5204, "lon": 35.2698},
    {"name": "Nakuru", "lat": -0.3031, "lon": 36.0800},
    {"name": "Kitale", "lat": 1.0159, "lon": 35.0064},
    {"name": "Embu", "lat": -0.5311, "lon": 37.4500},
    {"name": "Kericho", "lat": -0.3683, "lon": 35.2833},
    {"name": "Thika", "lat": -1.0333, "lon": 37.0693},
    {"name": "Kisii", "lat": -0.6817, "lon": 34.7667},
    {"name": "Nyeri", "lat": -0.4167, "lon": 36.9500},
    {"name": "Meru", "lat": 0.0463, "lon": 37.6559},
    {"name": "Machakos", "lat": -1.5167, "lon": 37.2667},
    {"name": "Kakamega", "lat": 0.2827, "lon": 34.7519},
    {"name": "Makueni", "lat": -1.8046, "lon": 37.6284},
    {"name": "Garissa", "lat": -0.4532, "lon": 39.6460},
    {"name": "Elgeyo Marakwet", "lat": 1.0490, "lon": 35.4784},
    {"name": "Kajiado", "lat": -1.8500, "lon": 36.7833},
    {"name": "Nyamira", "lat": -0.5632, "lon": 34.9390},
    {"name": "Narok", "lat": -1.0800, "lon": 35.8667},
]

def extract_weather_data():
    """Extract weather data from API for all regions."""
    validate_env_vars()
    weather_data = []
    session = requests.Session()

    for region in REGIONS:
        params = {
            "lat": region["lat"],
            "lon": region["lon"],
            "appid": API_KEY,
            "units": "metric"
        }

        try:
            response = session.get(BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            weather_data.append({
                "region": region["name"],
                "timestamp": datetime.utcnow(),
                "temperature": data.get("main", {}).get("temp"),
                "humidity": data.get("main", {}).get("humidity"),
                "pressure": data.get("main", {}).get("pressure"),
                "wind_speed": data.get("wind", {}).get("speed"),
                "rain": data.get("rain", {}).get("1h", 0) if "rain" in data else 0,
                "weather_description": data.get("weather", [{}])[0].get("description", "")
            })

            logger.info(f"✅ Success: {region['name']} data fetched.")

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to fetch data for {region['name']}: {e}")
            continue

    session.close()
    return weather_data

def transform_and_load(**kwargs):
    """Transform and load weather data into PostgreSQL database."""
    weather_data = kwargs['ti'].xcom_pull(task_ids='extract_weather')
    
    if not weather_data:
        logger.warning("⚠️ No weather data found. Skipping load.")
        return

    df = pd.DataFrame(weather_data)
    engine = sqlalchemy.create_engine(POSTGRES_URL)

    create_table_query = """
    CREATE TABLE IF NOT EXISTS weather_data (
        id SERIAL PRIMARY KEY,
        region TEXT,
        timestamp TIMESTAMP,
        temperature REAL,
        humidity REAL,
        pressure REAL,
        wind_speed REAL,
        rain REAL,
        weather_description TEXT
    );
    """

    try:
        with engine.begin() as conn:
            conn.execute(sqlalchemy.text(create_table_query))
            logger.info("✅ Weather table checked/created successfully.")
            
            df.to_sql(
                "weather_data",
                con=conn,
                if_exists="append",
                index=False,
                method='multi'
            )
            logger.info(f"✅ Inserted {len(df)} rows into weather_data table.")

    except Exception as e:
        logger.error(f"❌ Failed to insert data into database: {e}")
        raise

    finally:
        engine.dispose()

# DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
}

with DAG(
    dag_id="daily_weather_etl_kenya",
    default_args=default_args,
    description="Daily ETL for weather data in Kenyan agricultural regions",
    start_date=datetime(2025, 7, 22),
    schedule_interval="@daily",
    catchup=False,
    tags=["weather", "kenya", "etl"]
) as dag:

    extract_weather = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather_data
    )

    transform_load = PythonOperator(
        task_id="transform_and_load",
        python_callable=transform_and_load,
        provide_context=True
    )

    extract_weather >> transform_load