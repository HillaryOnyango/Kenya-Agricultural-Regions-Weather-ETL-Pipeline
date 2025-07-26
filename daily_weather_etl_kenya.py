from datetime import datetime, timedelta
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import json
import os

# # Configuration
# API_KEY = "a29106e328f2added1ff60240ccbe153"
# WEATHER_API_BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

# # Database Configuration
# POSTGRES_CONFIG = {
#     'host': 'localhost',
#     'port': 5432,
#     'database': 'weather',
#     'user': 'postgres',
#     'password': '1900'
# }

# 15 Major Agricultural Regions in Kenya with their coordinates
AGRICULTURAL_REGIONS = {
    "Nakuru": {"lat": -0.3031, "lon": 36.0800},
    "Meru": {"lat": 0.0467, "lon": 37.6500},
    "Eldoret": {"lat": 0.5143, "lon": 35.2698},
    "Kitale": {"lat": 1.0154, "lon": 35.0062},
    "Kericho": {"lat": -0.3676, "lon": 35.2866},
    "Nyeri": {"lat": -0.4167, "lon": 36.9500},
    "Embu": {"lat": -0.5308, "lon": 37.4500},
    "Machakos": {"lat": -1.5177, "lon": 37.2634},
    "Thika": {"lat": -1.0332, "lon": 37.0692},
    "Mombasa": {"lat": -4.0435, "lon": 39.6682},
    "Kisumu": {"lat": -0.1022, "lon": 34.7617},
    "Narok": {"lat": -1.0833, "lon": 35.8667},
    "Bungoma": {"lat": 0.5692, "lon": 34.5606},
    "Kakamega": {"lat": 0.2827, "lon": 34.7519},
    "Bomet": {"lat": -0.7833, "lon": 35.3333}
}

# Default arguments for the DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Create the DAG
dag = DAG(
    'kenya_agricultural_weather_etl',
    default_args=default_args,
    description='Daily ETL pipeline for weather data in Kenya agricultural regions',
    schedule_interval='0 6 * * *',  # Daily at 6 AM
    max_active_runs=1,
    tags=['weather', 'agriculture', 'kenya', 'etl']
)

def get_postgres_connection():
    """Get PostgreSQL database connection"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        return conn
    except psycopg2.Error as e:
        logging.error(f"Error connecting to PostgreSQL: {e}")
        raise

def create_weather_table():
    """Create weather_data table if it doesn't exist"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS weather_data (
        id SERIAL PRIMARY KEY,
        region VARCHAR(50) NOT NULL,
        latitude DECIMAL(10, 6) NOT NULL,
        longitude DECIMAL(10, 6) NOT NULL,
        temperature DECIMAL(5, 2),
        feels_like DECIMAL(5, 2),
        temp_min DECIMAL(5, 2),
        temp_max DECIMAL(5, 2),
        pressure INTEGER,
        humidity INTEGER,
        visibility INTEGER,
        wind_speed DECIMAL(5, 2),
        wind_direction INTEGER,
        cloudiness INTEGER,
        weather_main VARCHAR(50),
        weather_description VARCHAR(100),
        rainfall_1h DECIMAL(8, 2) DEFAULT 0,
        rainfall_3h DECIMAL(8, 2) DEFAULT 0,
        sunrise TIMESTAMP,
        sunset TIMESTAMP,
        data_timestamp TIMESTAMP NOT NULL,
        extraction_timestamp TIMESTAMP NOT NULL,
        heat_index DECIMAL(5, 2),
        dew_point DECIMAL(5, 2),
        is_favorable_temp BOOLEAN,
        is_high_humidity BOOLEAN,
        rainfall_category VARCHAR(20),
        date DATE,
        hour INTEGER,
        month INTEGER,
        year INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(region, data_timestamp)
    );

    -- Create indexes for better query performance
    CREATE INDEX IF NOT EXISTS idx_weather_region ON weather_data(region);
    CREATE INDEX IF NOT EXISTS idx_weather_date ON weather_data(date);
    CREATE INDEX IF NOT EXISTS idx_weather_timestamp ON weather_data(data_timestamp);
    CREATE INDEX IF NOT EXISTS idx_weather_region_date ON weather_data(region, date);
    """
    
    conn = get_postgres_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        conn.commit()
        logging.info("Weather table created successfully")
        cursor.close()
    except psycopg2.Error as e:
        logging.error(f"Error creating table: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

def extract_weather_data(**context):
    """
    Extract weather data from OpenWeatherMap API for all agricultural regions
    """
    weather_data = []
    failed_regions = []
    
    for region, coords in AGRICULTURAL_REGIONS.items():
        try:
            # API request parameters
            params = {
                'lat': coords['lat'],
                'lon': coords['lon'],
                'appid': API_KEY,
                'units': 'metric'  # Celsius temperature
            }
            
            # Make API request
            response = requests.get(WEATHER_API_BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Extract relevant weather information
            weather_record = {
                'region': region,
                'latitude': coords['lat'],
                'longitude': coords['lon'],
                'temperature': data['main']['temp'],
                'feels_like': data['main']['feels_like'],
                'temp_min': data['main']['temp_min'],
                'temp_max': data['main']['temp_max'],
                'pressure': data['main']['pressure'],
                'humidity': data['main']['humidity'],
                'visibility': data.get('visibility', None),
                'wind_speed': data['wind'].get('speed', None),
                'wind_direction': data['wind'].get('deg', None),
                'cloudiness': data['clouds']['all'],
                'weather_main': data['weather'][0]['main'],
                'weather_description': data['weather'][0]['description'],
                'sunrise': datetime.fromtimestamp(data['sys']['sunrise']),
                'sunset': datetime.fromtimestamp(data['sys']['sunset']),
                'data_timestamp': datetime.fromtimestamp(data['dt']),
                'extraction_timestamp': datetime.now()
            }
            
            # Add rainfall data if available
            if 'rain' in data:
                weather_record['rainfall_1h'] = data['rain'].get('1h', 0)
                weather_record['rainfall_3h'] = data['rain'].get('3h', 0)
            else:
                weather_record['rainfall_1h'] = 0
                weather_record['rainfall_3h'] = 0
            
            weather_data.append(weather_record)
            logging.info(f"Successfully extracted weather data for {region}")
            
        except requests.exceptions.RequestException as e:
            logging.error(f"API request failed for {region}: {str(e)}")
            failed_regions.append(region)
        except KeyError as e:
            logging.error(f"Missing expected data field for {region}: {str(e)}")
            failed_regions.append(region)
        except Exception as e:
            logging.error(f"Unexpected error for {region}: {str(e)}")
            failed_regions.append(region)
    
    if failed_regions:
        logging.warning(f"Failed to extract data for regions: {failed_regions}")
    
    if not weather_data:
        raise Exception("No weather data was successfully extracted")
    
    # Store data in XCom for next task
    return weather_data
    """
    Extract weather data from OpenWeatherMap API for all agricultural regions
    """
    weather_data = []
    failed_regions = []
    
    for region, coords in AGRICULTURAL_REGIONS.items():
        try:
            # API request parameters
            params = {
                'lat': coords['lat'],
                'lon': coords['lon'],
                'appid': API_KEY,
                'units': 'metric'  # Celsius temperature
            }
            
            # Make API request
            response = requests.get(WEATHER_API_BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Extract relevant weather information
            weather_record = {
                'region': region,
                'latitude': coords['lat'],
                'longitude': coords['lon'],
                'temperature': data['main']['temp'],
                'feels_like': data['main']['feels_like'],
                'temp_min': data['main']['temp_min'],
                'temp_max': data['main']['temp_max'],
                'pressure': data['main']['pressure'],
                'humidity': data['main']['humidity'],
                'visibility': data.get('visibility', None),
                'wind_speed': data['wind'].get('speed', None),
                'wind_direction': data['wind'].get('deg', None),
                'cloudiness': data['clouds']['all'],
                'weather_main': data['weather'][0]['main'],
                'weather_description': data['weather'][0]['description'],
                'sunrise': datetime.fromtimestamp(data['sys']['sunrise']),
                'sunset': datetime.fromtimestamp(data['sys']['sunset']),
                'data_timestamp': datetime.fromtimestamp(data['dt']),
                'extraction_timestamp': datetime.now()
            }
            
            # Add rainfall data if available
            if 'rain' in data:
                weather_record['rainfall_1h'] = data['rain'].get('1h', 0)
                weather_record['rainfall_3h'] = data['rain'].get('3h', 0)
            else:
                weather_record['rainfall_1h'] = 0
                weather_record['rainfall_3h'] = 0
            
            weather_data.append(weather_record)
            logging.info(f"Successfully extracted weather data for {region}")
            
        except requests.exceptions.RequestException as e:
            logging.error(f"API request failed for {region}: {str(e)}")
            failed_regions.append(region)
        except KeyError as e:
            logging.error(f"Missing expected data field for {region}: {str(e)}")
            failed_regions.append(region)
        except Exception as e:
            logging.error(f"Unexpected error for {region}: {str(e)}")
            failed_regions.append(region)
    
    if failed_regions:
        logging.warning(f"Failed to extract data for regions: {failed_regions}")
    
    if not weather_data:
        raise Exception("No weather data was successfully extracted")
    
    # Store data in XCom for next task
    return weather_data

def transform_weather_data(**context):
    """
    Transform and validate the extracted weather data
    """
    # Get data from previous task
    weather_data = context['task_instance'].xcom_pull(task_ids='extract_weather_data')
    
    if not weather_data:
        raise Exception("No data received from extraction task")
    
    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(weather_data)
    
    # Data validation and cleaning
    logging.info(f"Processing {len(df)} weather records")
    
    # Remove any duplicate records
    initial_count = len(df)
    df = df.drop_duplicates(subset=['region', 'data_timestamp'])
    if len(df) < initial_count:
        logging.info(f"Removed {initial_count - len(df)} duplicate records")
    
    # Validate temperature ranges (reasonable for Kenya: -5°C to 50°C)
    df = df[(df['temperature'] >= -5) & (df['temperature'] <= 50)]
    
    # Validate humidity (0-100%)
    df = df[(df['humidity'] >= 0) & (df['humidity'] <= 100)]
    
    # Calculate additional agricultural metrics
    df['heat_index'] = df.apply(calculate_heat_index, axis=1)
    df['dew_point'] = df.apply(calculate_dew_point, axis=1)
    df['is_favorable_temp'] = (df['temperature'] >= 18) & (df['temperature'] <= 28)
    df['is_high_humidity'] = df['humidity'] > 70
    df['rainfall_category'] = df['rainfall_1h'].apply(categorize_rainfall)
    
    # Add date components for easier querying
    df['date'] = df['data_timestamp'].dt.date
    df['hour'] = df['data_timestamp'].dt.hour
    df['month'] = df['data_timestamp'].dt.month
    df['year'] = df['data_timestamp'].dt.year
    
    logging.info(f"Transformation completed. Final dataset has {len(df)} records")
    
    # Convert to dictionary and handle timestamp serialization
    records = df.to_dict('records')
    
    # Convert pandas Timestamps to Python datetime objects for JSON serialization
    for record in records:
        for key, value in record.items():
            if hasattr(value, 'to_pydatetime'):  # Pandas Timestamp
                record[key] = value.to_pydatetime()
            elif hasattr(value, 'item'):  # Numpy types
                record[key] = value.item()
    
    return records

def calculate_heat_index(row):
    """Calculate heat index using temperature and humidity"""
    temp = row['temperature']
    humidity = row['humidity']
    
    # Simplified heat index calculation
    if temp < 27:
        return temp
    
    heat_index = (
        -8.78469475556 +
        1.61139411 * temp +
        2.33854883889 * humidity +
        -0.14611605 * temp * humidity +
        -0.012308094 * temp**2 +
        -0.0164248277778 * humidity**2 +
        0.002211732 * temp**2 * humidity +
        0.00072546 * temp * humidity**2 +
        -0.000003582 * temp**2 * humidity**2
    )
    
    return round(heat_index, 2)

def calculate_dew_point(row):
    """Calculate dew point using temperature and humidity"""
    temp = row['temperature']
    humidity = row['humidity']
    
    # Magnus formula approximation
    a = 17.27
    b = 237.7
    
    alpha = ((a * temp) / (b + temp)) + (humidity / 100.0)
    dew_point = (b * alpha) / (a - alpha)
    
    return round(dew_point, 2)

def categorize_rainfall(rainfall):
    """Categorize rainfall intensity"""
    if rainfall == 0:
        return 'No Rain'
    elif rainfall < 2.5:
        return 'Light Rain'
    elif rainfall < 10:
        return 'Moderate Rain'
    elif rainfall < 50:
        return 'Heavy Rain'
    else:
        return 'Very Heavy Rain'

def load_weather_data(**context):
    """
    Load transformed data into PostgreSQL database
    """
    # Get transformed data
    weather_data = context['task_instance'].xcom_pull(task_ids='transform_weather_data')
    
    if not weather_data:
        raise Exception("No data received from transformation task")
    
    # Get PostgreSQL connection
    conn = get_postgres_connection()
    
    # Prepare data for insertion
    columns = [
        'region', 'latitude', 'longitude', 'temperature', 'feels_like',
        'temp_min', 'temp_max', 'pressure', 'humidity', 'visibility',
        'wind_speed', 'wind_direction', 'cloudiness', 'weather_main',
        'weather_description', 'rainfall_1h', 'rainfall_3h', 'sunrise',
        'sunset', 'data_timestamp', 'extraction_timestamp', 'heat_index',
        'dew_point', 'is_favorable_temp', 'is_high_humidity', 'rainfall_category',
        'date', 'hour', 'month', 'year'
    ]
    
    # Convert data to list of tuples
    values = []
    for record in weather_data:
        values.append(tuple(record[col] for col in columns))
    
    # Insert data using batch insert for better performance
    insert_sql = f"""
        INSERT INTO weather_data ({', '.join(columns)})
        VALUES %s
        ON CONFLICT (region, data_timestamp) DO UPDATE SET
            temperature = EXCLUDED.temperature,
            feels_like = EXCLUDED.feels_like,
            temp_min = EXCLUDED.temp_min,
            temp_max = EXCLUDED.temp_max,
            pressure = EXCLUDED.pressure,
            humidity = EXCLUDED.humidity,
            visibility = EXCLUDED.visibility,
            wind_speed = EXCLUDED.wind_speed,
            wind_direction = EXCLUDED.wind_direction,
            cloudiness = EXCLUDED.cloudiness,
            weather_main = EXCLUDED.weather_main,
            weather_description = EXCLUDED.weather_description,
            rainfall_1h = EXCLUDED.rainfall_1h,
            rainfall_3h = EXCLUDED.rainfall_3h,
            sunrise = EXCLUDED.sunrise,
            sunset = EXCLUDED.sunset,
            extraction_timestamp = EXCLUDED.extraction_timestamp,
            heat_index = EXCLUDED.heat_index,
            dew_point = EXCLUDED.dew_point,
            is_favorable_temp = EXCLUDED.is_favorable_temp,
            is_high_humidity = EXCLUDED.is_high_humidity,
            rainfall_category = EXCLUDED.rainfall_category,
            date = EXCLUDED.date,
            hour = EXCLUDED.hour,
            month = EXCLUDED.month,
            year = EXCLUDED.year;
    """
    
    try:
        cursor = conn.cursor()
        execute_values(cursor, insert_sql, values, template=None, page_size=100)
        conn.commit()
        
        logging.info(f"Successfully loaded {len(values)} weather records to database")
        
        cursor.close()
        
    except Exception as e:
        logging.error(f"Failed to load data to database: {str(e)}")
        conn.rollback()
        raise
    finally:
        conn.close()

def data_quality_check(**context):
    """
    Perform data quality checks on loaded data
    """
    conn = get_postgres_connection()
    
    try:
        cursor = conn.cursor()
        
        # Get today's date
        today = datetime.now().date()
        
        # Check 1: Verify all regions have data for today
        region_count_sql = """
            SELECT COUNT(DISTINCT region) as region_count
            FROM weather_data 
            WHERE date = %s;
        """
        
        cursor.execute(region_count_sql, [today])
        result = cursor.fetchone()
        region_count = result[0] if result else 0
        
        expected_regions = len(AGRICULTURAL_REGIONS)
        
        if region_count < expected_regions:
            logging.warning(f"Data quality issue: Only {region_count} regions found, expected {expected_regions}")
        else:
            logging.info(f"Data quality check passed: All {expected_regions} regions have data")
        
        # Check 2: Verify no null values in critical fields
        null_check_sql = """
            SELECT 
                SUM(CASE WHEN temperature IS NULL THEN 1 ELSE 0 END) as null_temp,
                SUM(CASE WHEN humidity IS NULL THEN 1 ELSE 0 END) as null_humidity,
                SUM(CASE WHEN pressure IS NULL THEN 1 ELSE 0 END) as null_pressure
            FROM weather_data 
            WHERE date = %s;
        """
        
        cursor.execute(null_check_sql, [today])
        result = cursor.fetchone()
        if result and any(result):
            logging.warning(f"Data quality issue: Found null values - Temperature: {result[0]}, Humidity: {result[1]}, Pressure: {result[2]}")
        else:
            logging.info("Data quality check passed: No null values in critical fields")
        
        # Check 3: Verify reasonable temperature ranges
        temp_range_sql = """
            SELECT MIN(temperature) as min_temp, MAX(temperature) as max_temp
            FROM weather_data 
            WHERE date = %s;
        """
        
        cursor.execute(temp_range_sql, [today])
        result = cursor.fetchone()
        if result:
            min_temp, max_temp = result
            if min_temp < -10 or max_temp > 60:
                logging.warning(f"Data quality issue: Extreme temperatures detected - Min: {min_temp}°C, Max: {max_temp}°C")
            else:
                logging.info(f"Data quality check passed: Temperature range is reasonable - Min: {min_temp}°C, Max: {max_temp}°C")
        
        cursor.close()
        
    except psycopg2.Error as e:
        logging.error(f"Error during data quality check: {e}")
        raise
    finally:
        conn.close()

# Define tasks
create_table_task = PythonOperator(
    task_id='create_weather_table',
    python_callable=create_weather_table,
    dag=dag
)

extract_task = PythonOperator(
    task_id='extract_weather_data',
    python_callable=extract_weather_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_weather_data',
    python_callable=transform_weather_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_weather_data',
    python_callable=load_weather_data,
    dag=dag
)

quality_check_task = PythonOperator(
    task_id='data_quality_check',
    python_callable=data_quality_check,
    dag=dag
)

# Set task dependencies
create_table_task >> extract_task >> transform_task >> load_task >> quality_check_task