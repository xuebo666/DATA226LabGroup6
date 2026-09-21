from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

import logging
from datetime import datetime

import pandas as pd
import requests


logger = logging.getLogger(__name__)


def return_snowflake_conn():
    """Create a Snowflake cursor for the DAG's warehouse connection."""
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    return conn.cursor()


@task
def extract(latitude, longitude):
    """Fetch hourly weather history/forecast for a location."""
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

    # Validate the payload before handing off to the transform step.
    if "hourly" not in payload or not payload["hourly"].get("time"):
        raise ValueError("Open-Meteo response is missing hourly data.")

    logger.info("Weather API call succeeded for latitude=%s, longitude=%s", latitude, longitude)
    return payload


@task
def transform(data, latitude, longitude):
    """Convert API payload into rows ready for Snowflake insertion."""
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

    # Use itertuples for faster row conversion than iterrows.
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

        # Full refresh: remove all prior rows before inserting the new batch.
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
    target_table = "raw.weather_hourly"
    latitude = Variable.get("LATITUDE")
    longitude = Variable.get("LONGITUDE")
    data = extract(latitude, longitude)
    records = transform(data, latitude, longitude)
    load(records,target_table)