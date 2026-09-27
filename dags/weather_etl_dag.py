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

        "timezone": "Asia/Shanghai"
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    payload = response.json()

    # Validate the payload before handing off to the transform step. If the API
    # response is incomplete, fail early instead of producing a partially loaded batch.
    if "hourly" not in payload or not payload["hourly"].get("time"):
        raise ValueError(
            f"Open-Meteo response is missing hourly data "
            f"for latitude={latitude}, longitude={longitude}."
        )

    logger.info("Weather API call succeeded for latitude=%s, longitude=%s", 
                latitude, longitude)
    
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
        raise ValueError(
            f"No weather rows were returned from the API payload "
            f"for latitude={latitude}, longitude={longitude}."
        )

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

    logger.info("Transformed %s weather records for location (%s, %s)",
                 len(records), latitude, longitude)
    
    return records


@task
def combine_records(beijing_records, shanghai_records):
    """Combine both locations into one batch for Snowflake."""

    records = beijing_records + shanghai_records

    if not records:
        raise ValueError("No weather records available for either location.")

    logger.info(
        "Combined weather batch contains %s records",
        len(records),
    )

    return records

@task
def load(records, target_table):
    """Full-refresh load: clear the table and insert the latest weather batch."""
    # flow:
    # 1. Create staging table
    # 2. Load staging table
    # 3. Validate primary-key uniqueness
    # 4. Create final table if needed
    # 5. Swap staging and final table

    if not records:
        raise ValueError("No records available to load into Snowflake.")

    stage_table = f"{target_table}_stage"
    
    cur = return_snowflake_conn()

    try:
        # Start transaction for the transactional portions of the load.
        cur.execute("BEGIN;")

        # Create a fresh staging table for this batch.
        cur.execute(
            f"""
            CREATE OR REPLACE TABLE {stage_table} (
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

        insert_sql = f"""
            INSERT INTO {stage_table} (
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
        
        # Validate primary-key uniqueness.
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT
                    latitude,
                    longitude,
                    weather_time
                FROM {stage_table}
                GROUP BY
                    latitude,
                    longitude,
                    weather_time
                HAVING COUNT(*) > 1
            );
            """
        )

        duplicate_count = cur.fetchone()[0]

        if duplicate_count > 0:
            raise ValueError(
                f"{stage_table} contains "
                f"{duplicate_count} duplicate primary keys. "
                "Aborting load."
            )

        # Validate that the staging table is not empty.
        cur.execute(f"SELECT COUNT(*) FROM {stage_table};")
        staged_count = cur.fetchone()[0]

        if staged_count == 0:
            raise ValueError(
                f"{stage_table} is empty. Aborting load."
            )

        logger.info(
            "Validation passed. %s records ready for swap.",
            staged_count,
        )

        # Create the final table if it does not exist.
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

        # Swap two tables.
        cur.execute(
            f"ALTER TABLE {target_table} SWAP WITH {stage_table};"
        )

        logger.info(
            "Successfully swapped %s records into %s",
            staged_count,
            target_table,
        )

        cur.execute("COMMIT;")

    except Exception as exc:
        try:
            cur.execute("ROLLBACK;")
        except Exception:
            logger.warning("Rollback could not be completed.")

        logger.exception(
            "Error loading weather data into %s: %s",
            target_table,
            exc,
        )

        raise

    finally:
        try:
            cur.execute(
                f"DROP TABLE IF EXISTS {stage_table};"
            )
        except Exception:
            logger.warning(
                "Could not drop staging table %s. "
                "It will be replaced on the next run.",
                stage_table,
            )

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

    # LOCATION 1: BEIJING
    beijing_latitude = Variable.get("LOCATION_1_LATITUDE")
    beijing_longitude = Variable.get("LOCATION_1_LONGITUDE")

    beijing_data = extract(
        beijing_latitude,
        beijing_longitude,
    )

    beijing_records = transform(
        beijing_data,
        beijing_latitude,
        beijing_longitude,
    )

    # LOCATION 2: SHANGHAI
    shanghai_latitude = Variable.get("LOCATION_2_LATITUDE")
    shanghai_longitude = Variable.get("LOCATION_2_LONGITUDE")

    shanghai_data = extract(
        shanghai_latitude,
        shanghai_longitude,
    )

    shanghai_records = transform(
        shanghai_data,
        shanghai_latitude,
        shanghai_longitude,
    )

    # COMBINE BOTH LOCATIONS
    combined_records = combine_records(
        beijing_records,
        shanghai_records,
    )

    # LOAD TO SNOWFLAKE
    target_table = "raw.weather_hourly"

    load_task = load(
        combined_records,
        target_table,
    )

    # TRIGGER DBT AFTER SUCCESSFUL LOAD
   
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_weather_dbt_dag",
        trigger_dag_id="dbt_weather_pipeline",
        reset_dag_run=True,
    )

    load_task >> trigger_dbt