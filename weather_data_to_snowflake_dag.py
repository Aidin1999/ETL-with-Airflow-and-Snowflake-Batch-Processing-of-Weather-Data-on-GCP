from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
import requests_cache
from retry_requests import retry
import openmeteo_requests

# Function to retrieve and process weather data for yesterday
def fetch_weather_data(ti):
    yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Setup cache and retry logic
    session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)
    
    # API request parameters
    url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 51.4552,
        "longitude": -2.5966,
        "start_date": yesterday,
        "end_date": yesterday,
        "daily": ["weather_code", "temperature_2m_max", "temperature_2m_min", 
                  "apparent_temperature_max", "apparent_temperature_min", 
                  "sunrise", "sunset", "daylight_duration", "sunshine_duration", 
                  "uv_index_max"],
        "timezone": "GMT"
    }

    # Fetching and processing the data
    response = openmeteo.weather_api(url, params=params)[0].Daily()

    # Helper function to handle potential single values vs array-like objects
    def safe_get(variable, index, default=None):
        try:
            value = variable.ValuesAsNumpy()[0]
            return value.item() if hasattr(value, 'item') else value
        except (TypeError, IndexError):
            return default

    # Converting all values to JSON-serializable types with defaults for None
    daily_data = {
        "date": yesterday,
        "weather_code": int(safe_get(response.Variables(0), 0, default=0)),
        "temperature_2m_max": float(safe_get(response.Variables(1), 1, default=0.0)),
        "temperature_2m_min": float(safe_get(response.Variables(2), 2, default=0.0)),
        "apparent_temperature_max": float(safe_get(response.Variables(3), 3, default=0.0)),
        "apparent_temperature_min": float(safe_get(response.Variables(4), 4, default=0.0)),
        "sunrise": safe_get(response.Variables(5), 5, default='00:00:00'),
        "sunset": safe_get(response.Variables(6), 6, default='00:00:00'),
        "daylight_duration": float(safe_get(response.Variables(7), 7, default=0.0)),
        "sunshine_duration": float(safe_get(response.Variables(8), 8, default=0.0)),
        "uv_index_max": int(safe_get(response.Variables(9), 9, default=0)),
    }

    # Push to XCom for downstream tasks
    ti.xcom_push(key="weather_data", value=daily_data)

# Default arguments for the DAG
default_args = {
    'start_date': datetime.now() - timedelta(days=1),
    'catchup': False
}

# Define the DAG
with DAG('weather_data_to_snowflake_dag', default_args=default_args, schedule_interval='@daily') as dag:

    # Task to fetch weather data
    fetch_data_task = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data
    )

    # Task to load the data into Snowflake without the ID field
    load_to_snowflake_task = SnowflakeOperator(
        task_id='load_data_to_snowflake',
        sql="""
        INSERT INTO WEATHERFORCASTING.BRISTOL.WEATHER_DATA 
        (DATE, WEATHER_CODE, TEMPERATURE_2M_MAX, TEMPERATURE_2M_MIN, 
        APPARENT_TEMPERATURE_MAX, APPARENT_TEMPERATURE_MIN, SUNRISE, SUNSET, 
        DAYLIGHT_DURATION, SUNSHINE_DURATION, UV_INDEX_MAX)
        VALUES (
            '{{ ti.xcom_pull(task_ids="fetch_weather_data", key="weather_data")["date"] }}',
            '{{ ti.xcom_pull(task_ids="fetch_weather_data", key="weather_data")["weather_code"] }}',
            '{{ ti.xcom_pull(task_ids="fetch_weather_data", key="weather_data")["temperature_2m_max"] }}',
            '{{ ti.xcom_pull(task_ids="fetch_weather_data", key="weather_data")["temperature_2m_min"] }}',
            '{{ ti.xcom_pull(task_ids="fetch_weather_data", key="weather_data")["apparent_temperature_max"] }}',
            '{{ ti.xcom_pull(task_ids="fetch_weather_data", key="weather_data")["apparent_temperature_min"] }}',
            '{{ ti.xcom_pull(task_ids="fetch_weather_data", key="weather_data")["sunrise"] }}',
            '{{ ti.xcom_pull(task_ids="fetch_weather_data", key="weather_data")["sunset"] }}',
            '{{ ti.xcom_pull(task_ids="fetch_weather_data", key="weather_data")["daylight_duration"] }}',
            '{{ ti.xcom_pull(task_ids="fetch_weather_data", key="weather_data")["sunshine_duration"] }}',
            '{{ ti.xcom_pull(task_ids="fetch_weather_data", key="weather_data")["uv_index_max"] }}'
        );
        """,
        snowflake_conn_id='snowflake_default'
    )

    # Set task dependencies1
    fetch_data_task >> load_to_snowflake_task
