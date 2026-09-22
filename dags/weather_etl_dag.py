from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import logging
from datetime import datetime

import pandas as pd
import requests


logger = logging.getLogger(__name__)

# This DAG pulls hourly weather data for a configured location, normalizes it,
# and stores the full batch in Snowflake for downstream dbt transformations.


def return_snowflake_conn():
    """Create a Snowflake cursor for the DAG's warehouse connection."""
    # Reuse the Airflow Snowflake connection configured in the environment so
    # each task can open a fresh cursor without hardcoding credentials here.
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    return conn.cursor()


@task
def extract(latitude, longitude):
    """Fetch hourly weather history/forecast for a location."""
    # Query the Open-Meteo API for both historical and short-term forecast data.
    # The DAG intentionally pulls a rolling window so the downstream dbt models
    # can compare recent conditions with a consistent daily baseline.
    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": [
            "temperature_2m",
            "relative_humidity_2m",
            "apparent_temperature",
            "precipitation",
            "rain",
            "showers",
            "snowfall",
            "weather_code",
            "wind_speed_10m",
            "wind_direction_10m",
        ],
        "past_days": 60,
        "forecast_days": 7,

        "timezone": "America/Los_Angeles"
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    payload = response.json()

    # Validate the payload before handing off to the transform step. If the API
    # response is incomplete, fail early instead of producing a partially loaded batch.
    if "hourly" not in payload or not payload["hourly"].get("time"):
        raise ValueError("Open-Meteo response is missing hourly data.")

    logger.info("Weather API call succeeded for latitude=%s, longitude=%s", latitude, longitude)
    return payload


@task
def transform(data, latitude, longitude):
    """Convert API payload into rows ready for Snowflake insertion."""
    # Normalize the API response from a nested JSON payload into a tabular format.
    # Each row represents one hourly observation for the selected latitude/longitude pair.
    hourly = data.get("hourly", {})

    # Use a single DataFrame for easier validation and row shaping.
    df = pd.DataFrame(
        {
            "Date": hourly.get("time", []),
            "Temperature": hourly.get("temperature_2m", []),
            "Humidity": hourly.get("relative_humidity_2m", []),
            "Apparent_temperature": hourly.get("apparent_temperature", []),
            "Precipitation": hourly.get("precipitation", []),
            "Rain": hourly.get("rain", []),
            "Showers": hourly.get("showers", []),
            "Snowfall": hourly.get("snowfall", []),
            "Weather_code": hourly.get("weather_code", []),
            "Wind_speed": hourly.get("wind_speed_10m", []),
            "Wind_direction": hourly.get("wind_direction_10m", []),
        }
    )

    if df.empty:
        raise ValueError("No weather rows were returned from the API payload.")

    df["Date"] = pd.to_datetime(df["Date"])

    # Convert the DataFrame to a list of tuples so the subsequent Snowflake insert
    # can use executemany efficiently and keep the load step fast.
    records = [
        (
            latitude,
            longitude,
            str(row.Date),
            row.Temperature,
            row.Humidity,
            row.Apparent_temperature,
            row.Precipitation,
            row.Rain,
            row.Showers,
            row.Snowfall,
            row.Weather_code,
            row.Wind_speed,
            row.Wind_direction,
        )
        for row in df.itertuples(index=False)
    ]

    logger.info("Transformed %s weather records for location (%s, %s)", len(records), latitude, longitude)
    return records


@task
def load(records, target_table):
    """Full-refresh load: clear the table and insert the latest weather batch."""
    # Keep the raw table as a single fresh snapshot for the configured location.
    # A full refresh is simplest here because the downstream dbt models expect a
    # clean, current set of hourly observations for each DAG run.
    if not records:
        raise ValueError("No records available to load into Snowflake.")

    cur = return_snowflake_conn()

    try:
        cur.execute("BEGIN;")

        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                latitude                FLOAT,
                longitude               FLOAT,
                weather_time            TIMESTAMP_NTZ,
                temperature             FLOAT,
                humidity                FLOAT,
                apparent_temperature    FLOAT,
                precipitation           FLOAT,
                rain                    FLOAT,
                showers                 FLOAT,
                snowfall                FLOAT,
                weather_code            INTEGER,
                wind_speed              FLOAT,
                wind_direction          FLOAT,
                ingested_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY KEY (latitude, longitude, weather_time)
            );
            """
        )

        # Full refresh: remove all prior rows before inserting the new batch so the
        # table reflects only the latest API pull for this location.
        cur.execute(f"DELETE FROM {target_table};")

        insert_sql = f"""
            INSERT INTO {target_table} (
                latitude,
                longitude,
                weather_time,
                temperature,
                humidity,
                apparent_temperature,
                precipitation,
                rain,
                showers,
                snowfall,
                weather_code,
                wind_speed,
                wind_direction
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s
            )
        """

        cur.executemany(insert_sql, records)
        cur.execute("COMMIT;")

        logger.info("Full refresh complete: %s records loaded into %s", len(records), target_table)

    except Exception as exc:
        cur.execute("ROLLBACK;")
        logger.exception("Error full-refreshing %s: %s", target_table, exc)
        raise

    finally:
        cur.close()


with DAG(
    dag_id="WeatherETL",
    description="Extract weather data from Open-Meteo, transform it, and load it to Snowflake.",
    start_date=datetime(2026, 9, 20),
    catchup=False,
    schedule="0 0 * * *",
    tags=["ETL", "Weather"],
    default_args={
        "owner": "data226",
        "retries": 1,
    },
) as dag:
    # Pull the location settings from Airflow Variables so the DAG can be reused
    # for different coordinates without editing the code.
    target_table = "raw.weather_hourly"
    latitude = Variable.get("LATITUDE")
    longitude = Variable.get("LONGITUDE")

    # Task chain: fetch -> normalize -> load into Snowflake.
    data = extract(latitude, longitude)
    records = transform(data, latitude, longitude)
    load_task = load(records, target_table)

    trigger_dbt = TriggerDagRunOperator(
    task_id="trigger_weather_dbt_dag",
    trigger_dag_id="dbt_weather_pipeline",
    reset_dag_run=True,
)

    load_task >> trigger_dbt