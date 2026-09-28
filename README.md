# Weather Prediction Analytics Using Snowflake, Airflow, dbt & Preset

---

# Authors

**DATA 226 — Weather Prediction Analytics Project**

Team Members:

* Xuebo Zhou
* Yifan Huang

## Project Overview

This project implements an end-to-end weather data analytics pipeline for **Beijing and Shanghai, China**.

Weather data is collected from the **Open-Meteo API**, orchestrated and loaded into **Snowflake** using **Apache Airflow**, transformed and analyzed using **dbt**, and visualized through **Preset BI dashboards**.

The project demonstrates an automated data engineering and analytics workflow that converts raw hourly weather observations and forecasts into meaningful daily weather metrics.

### Technology Stack

* **Open-Meteo API** — Weather data source
* **Apache Airflow** — ETL orchestration and scheduling
* **Snowflake** — Cloud data warehouse
* **dbt** — Data transformation, testing, and snapshots
* **Preset** — Business intelligence and visualization
* **Python / Pandas** — Data extraction and transformation
* **Docker / Docker Compose** — Development and execution environment

---

# System Architecture

```text
                    ┌─────────────────────┐
                    │    Open-Meteo API   │
                    │  Weather Data Source│
                    └──────────┬──────────┘
                               │
                               │ Hourly Weather Data
                               ▼
                    ┌─────────────────────┐
                    │      Airflow        │
                    │     ETL Pipeline    │
                    │                     │
                    │ Extract → Transform │
                    │        → Load       │
                    └──────────┬──────────┘
                               │
                               │
                               ▼
                    ┌─────────────────────┐
                    │      Snowflake      │
                    │                     │
                    │ RAW.WEATHER_HOURLY  │
                    └──────────┬──────────┘
                               │
                               │ dbt
                               ▼
             ┌─────────────────────────────────┐
             │              dbt                │
             │                                 │
             │  Transform                      │
             │  └── weather_daily              │
             │                                 │
             │  Analytics                      │
             │  └── weather_metrics            │
             │                                 │
             │  Snapshot                       │
             │  └── weather_hourly_snapshot    │
             └────────────────┬────────────────┘
                              │
                              │ Analytics Data
                              ▼
                    ┌─────────────────────┐
                    │       Preset        │
                    │    BI Dashboard     │
                    │                     │
                    │ Beijing & Shanghai  │
                    │ Weather Analytics   │
                    └─────────────────────┘
```

---

# Locations

The pipeline collects weather data for two locations:

| Location | Latitude | Longitude |
| -------- | -------: | --------: |
| Beijing  |  39.9042 |  116.4074 |
| Shanghai |  31.2304 |  121.4737 |

Both locations use the `Asia/Shanghai` timezone.

---

# Project Objectives

The primary objectives of this project are to:

1. Collect hourly historical and forecast weather data from Open-Meteo.
2. Automate data extraction and loading using Apache Airflow.
3. Store raw weather data in Snowflake.
4. Transform hourly weather data into daily-level analytics using dbt.
5. Calculate useful weather metrics such as:

   * 7-day temperature moving average
   * Temperature anomaly
   * 7-day rolling rainfall
   * Daily precipitation
   * Dry day indicator
   * Dry spell length
6. Maintain historical changes using dbt snapshots.
7. Validate transformed data using dbt tests.
8. Build an interactive BI dashboard using Preset.
9. Compare weather patterns between Beijing and Shanghai.

---

# Data Source

Weather data is collected from the **Open-Meteo Forecast API**.

The pipeline requests hourly weather variables including:

* Temperature
* Relative humidity
* Apparent temperature
* Precipitation
* Rain
* Showers
* Snowfall
* Weather code
* Wind speed
* Wind direction

The API request retrieves approximately **60 days of historical data and 7 days of forecast data**.

---

# Data Pipeline

## 1. Extract

Apache Airflow calls the Open-Meteo API for each location.

The Airflow pipeline reads the following location configuration from Airflow Variables:

```text
LOCATION_1_LATITUDE
LOCATION_1_LONGITUDE
LOCATION_2_LATITUDE
LOCATION_2_LONGITUDE
```

The API response is validated before continuing to the transformation stage.

---

## 2. Transform

The raw API response is converted into structured records using Python and Pandas.

Each weather record contains:

```text
latitude
longitude
weather_time
temperature
humidity
apparent_temperature
precipitation
rain
showers
snowfall
weather_code
wind_speed
wind_direction
```

A location is identified using its latitude and longitude.

---

## 3. Load

The transformed records are loaded into:

```text
DATA226LAB.RAW.WEATHER_HOURLY
```

The ETL pipeline uses transaction handling and validation to make the load process safer and repeatable.

The loading process includes:

1. Begin transaction.
2. Create the required table if it does not exist.
3. Load data into a staging table.
4. Validate that records exist.
5. Check for duplicate weather records.
6. Swap the staging table with the target table.
7. Roll back when an error occurs.
8. Raise the exception so that Airflow marks the task as failed.

---

# Snowflake Data Warehouse

The project uses the following Snowflake environment:

```text
Database:  DATA226LAB
Warehouse: DATA226
Schema:    RAW / ANALYTICS
```

## Raw Layer

### `RAW.WEATHER_HOURLY`

This table stores the hourly weather data collected from Open-Meteo.

| Column                 | Description                            |
| ---------------------- | -------------------------------------- |
| `LATITUDE`             | Location latitude                      |
| `LONGITUDE`            | Location longitude                     |
| `WEATHER_TIME`         | Weather observation/forecast timestamp |
| `TEMPERATURE`          | Temperature                            |
| `HUMIDITY`             | Relative humidity                      |
| `APPARENT_TEMPERATURE` | Feels-like temperature                 |
| `PRECIPITATION`        | Precipitation amount                   |
| `RAIN`                 | Rain amount                            |
| `SHOWERS`              | Shower amount                          |
| `SNOWFALL`             | Snowfall amount                        |
| `WEATHER_CODE`         | Open-Meteo weather code                |
| `WIND_SPEED`           | Wind speed                             |
| `WIND_DIRECTION`       | Wind direction                         |
| `INGESTED_AT`          | Data ingestion timestamp               |

The logical uniqueness of an hourly weather record is:

```text
LATITUDE + LONGITUDE + WEATHER_TIME
```

---

# dbt Transformation Layer

The dbt project is organized according to the provided project template.

```text
dbt/
├── dbt_project.yml
├── models/
│   ├── transform/
│   │   └── weather_daily.sql
│   │
│   └── analytics/
│       ├── weather_metrics.sql
│       └── schema.yml
│
└── snapshots/
    └── weather_hourly_snapshot.sql
```

---

# dbt Models

## `transform/weather_daily.sql`

The `weather_daily` model converts hourly weather data into daily weather summaries.

Daily metrics include:

* Average temperature
* Maximum temperature
* Minimum temperature
* Average humidity
* Average apparent temperature
* Daily precipitation
* Daily rain
* Daily showers
* Daily snowfall
* Average wind speed
* Maximum wind speed
* Number of rainy hours
* Dominant weather code
* Historical / forecast classification

The model groups data by:

```text
latitude
longitude
weather_date
```

This prevents data from Beijing and Shanghai from being combined incorrectly.

---

## `analytics/weather_metrics.sql`

The `weather_metrics` model creates the main analytics dataset used by the BI dashboard.

The model calculates:

### 7-Day Temperature Moving Average

A seven-day moving average is calculated independently for each location.

```text
AVG(avg_temperature)
OVER (
    PARTITION BY latitude, longitude
    ORDER BY weather_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
)
```

### Historical Average Temperature

The historical average temperature is calculated separately for each location.

### Temperature Anomaly

Temperature anomaly is calculated as:

```text
Temperature Anomaly =
Daily Average Temperature - Historical Average Temperature
```

Positive values indicate temperatures above the historical average, while negative values indicate temperatures below the historical average.

### 7-Day Rolling Rainfall

The total precipitation over the current day and previous six days is calculated separately for each location.

### Dry Day

A day is classified as dry when:

```text
Daily precipitation < 1
```

### Dry Spell Length

The pipeline calculates the consecutive number of dry days for each location.

All location-based calculations use:

```text
PARTITION BY latitude, longitude
```

so that Beijing and Shanghai are analyzed independently.

---

# dbt Snapshot

The project uses:

```text
snapshots/weather_hourly_snapshot.sql
```

The snapshot preserves historical versions of hourly weather records.

The snapshot uses a composite weather key:

```text
latitude + longitude + weather_time
```

This allows records from both Beijing and Shanghai to be tracked independently.

The snapshot uses the dbt `check` strategy to detect changes in weather attributes.

---

# dbt Tests

Data quality tests are defined in:

```text
models/analytics/schema.yml
```

Tests include:

* `not_null`
* `accepted_values`

Examples include validating:

```text
weather_date
latitude
longitude
location
data_type
avg_temperature
temperature_7day_moving_avg
rainfall_7day_rolling
temperature_anomaly
is_dry_day
```

The `data_type` field is expected to contain:

```text
HISTORICAL
FORECAST
```

The `is_dry_day` field is expected to contain:

```text
0
1
```

---

# Airflow DAGs

The Airflow project contains two main DAGs.

## 1. Weather ETL DAG

The ETL DAG:

```text
WeatherETL
```

performs:

```text
Open-Meteo
     ↓
Extract Beijing
     ↓
Extract Shanghai
     ↓
Transform
     ↓
Combine Records
     ↓
Load Snowflake
     ↓
Trigger dbt DAG
```

The DAG is scheduled to run daily.

Airflow Variables are used for location configuration rather than hard-coding coordinates in the DAG.

---

## 2. dbt Weather Pipeline

The dbt DAG:

```text
dbt_weather_pipeline
```

is triggered after the ETL DAG completes successfully.

The execution order is:

```text
dbt run
   ↓
dbt test
   ↓
dbt snapshot
```

This ensures that the transformed models are created and validated before the snapshot is updated.

---

# Airflow Configuration

The project uses the following Airflow Variables:

```text
LOCATION_1_LATITUDE
LOCATION_1_LONGITUDE
LOCATION_2_LATITUDE
LOCATION_2_LONGITUDE
```

The Snowflake connection is configured through:

```text
snowflake_default
```

Credentials and private keys should be configured through Airflow or environment configuration and should **not** be committed to GitHub.

---

# Preset BI Dashboard

The main BI dataset is:

```text
DATA226LAB.ANALYTICS.WEATHER_METRICS
DATA226LAB.ANALYTICS.WEATHER_DAILY
```

## Dashboard Title

**Weather Prediction Analytics — Beijing & Shanghai**

## Dashboard Purpose

The dashboard provides an interactive overview of historical and forecast weather conditions for Beijing and Shanghai.

It allows users to examine temperature behavior, rainfall patterns, humidity, dry spells, and temperature anomalies over time.

---

# Dashboard Visualizations

The dashboard contains the following KPIs and charts.

## 1. Longest Dry Spell — KPI

This KPI displays the longest observed consecutive dry spell for the selected location and date range.

It helps identify periods with extended periods of low precipitation.

---

## 2. Average Temperature — KPI

This KPI displays the average temperature for the selected location and analysis period.

It provides a quick summary of the overall temperature conditions.

---

## 3. 7-Day Forecast — Line Chart

The 7-day forecast visualization displays forecast temperature values over the upcoming seven days.

This chart provides a forward-looking view of expected temperature conditions.

---

## 4. Humidity vs. Temperature — Scatter Plot

The scatter plot compares:

```text
X-axis: Temperature
Y-axis: Humidity
```

Each point represents a weather observation.

This visualization helps examine the relationship between temperature and relative humidity.

---

## 5. Temperature Distribution — Box Plot

The box plot shows the distribution of temperature values.

It provides information about:

* Median temperature
* Lower and upper quartiles
* Temperature spread
* Potential outliers

The visualization can be used to compare the temperature distributions of Beijing and Shanghai.

---

## 6. Dry Spell Length — Bar Chart

The bar chart displays dry spell length over time.

It highlights periods when consecutive dry days occurred and allows users to identify longer dry periods.

---

## 7. Rolling Rainfall — Line Chart

The rolling rainfall chart displays the seven-day rolling precipitation total.

It helps identify periods with increasing or decreasing rainfall accumulation.

---

## 8. Temperature Trend — Line Chart

The temperature trend chart displays daily temperature over time.

It can include:

* Daily average temperature
* 7-day moving average

The chart allows users to observe short-term temperature patterns and smoother longer-term trends.

---

## 9. Temperature Anomaly — Line Chart

The temperature anomaly chart displays the difference between daily average temperature and the historical average temperature.

```text
Temperature Anomaly =
Daily Temperature - Historical Average Temperature
```

Values above zero represent temperatures above the historical average, while values below zero represent temperatures below the historical average.

---

# Dashboard Filters

The dashboard provides interactive filtering capabilities, including:

### Location

Users can select:

```text
Beijing
Shanghai
```

### Date Range

Users can select different analysis periods to examine historical weather conditions.

Changing the filters updates the dashboard visualizations based on the selected location and date range.

---

# Running the Project

## Start Airflow

From the project directory:

```bash
docker compose up -d
```

Check the running containers:

```bash
docker compose ps
```

Open the Airflow web interface and verify that the required DAGs are available.

---

# Run dbt Manually

Enter the Airflow container:

```bash
docker compose exec airflow bash
```

Navigate to the dbt project:

```bash
cd /opt/airflow/dbt
```

Run dbt:

```bash
dbt run --profiles-dir . --project-dir .
```

Run tests:

```bash
dbt test --profiles-dir . --project-dir .
```

Run the snapshot:

```bash
dbt snapshot --profiles-dir . --project-dir .
```

Run all three in sequence:

```bash
dbt run --profiles-dir . --project-dir .
dbt test --profiles-dir . --project-dir .
dbt snapshot --profiles-dir . --project-dir .
```

---

# Verify dbt Connection

The dbt connection can be verified with:

```bash
dbt debug --profiles-dir . --project-dir .
```

---

# Verify Snowflake Data

After the ETL and dbt pipelines complete successfully, the analytics table can be checked in Snowflake:

```sql
SELECT *
FROM DATA226LAB.ANALYTICS.WEATHER_METRICS
LIMIT 20;
```

To verify that both locations are present:

```sql
SELECT
    location,
    COUNT(*) AS row_count
FROM DATA226LAB.ANALYTICS.WEATHER_METRICS
GROUP BY location;
```

---

# Useful Snowflake Tables

The main project tables/models are:

```text
DATA226LAB.RAW.WEATHER_HOURLY
DATA226LAB.ANALYTICS.WEATHER_METRICS
DATA226LAB.ANALYTICS.WEATHER_HOURLY_SNAPSHOT
```

The `weather_daily` model is configured as an intermediate dbt transformation and may not appear as a physical table depending on its materialization configuration.

---

# Project Directory Structure

```text
.
├── dags/
│   ├── weather_etl.py
│   └── dbt_weather_pipeline.py
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   │
│   ├── models/
│   │   ├── transform/
│   │   │   └── weather_daily.sql
│   │   │
│   │   └── analytics/
│   │       ├── weather_metrics.sql
│   │       └── schema.yml
│   │
│   └── snapshots/
│       └── weather_hourly_snapshot.sql
│
├── keys/
│   └── rsa_key.p8
│
├── docker-compose.yaml
├── README.md
└── .gitignore
```

---

# Error Handling and Idempotency

The Airflow ETL pipeline includes transaction handling to reduce the risk of partial loads.

The load process follows the general pattern:

```text
BEGIN
   ↓
Create / Prepare Staging Table
   ↓
Load Data
   ↓
Validate Data
   ↓
Swap Staging and Target
   ↓
COMMIT / Complete
```

If an exception occurs:

```text
ROLLBACK
   ↓
Log Exception
   ↓
Raise Exception
   ↓
Airflow Task = FAILED
```

The pipeline also validates duplicate weather keys using:

```text
latitude
longitude
weather_time
```

This helps prevent duplicate records for the same location and timestamp.

---

# Future Improvements

Possible future improvements include:

1. Add more cities and geographic regions.
2. Increase the historical weather dataset.
3. Add additional weather prediction metrics.
4. Add extreme weather detection.
5. Add automated data-quality alerts.
6. Add email or Slack notifications when Airflow tasks fail.
7. Implement more advanced forecasting models using machine learning.
8. Add weather-condition classification.
9. Add automated dashboard refresh and monitoring.
10. Deploy the pipeline to a cloud production environment.

---

# Conclusion

This project demonstrates a complete modern data engineering pipeline for weather analytics.

Open-Meteo provides the source weather data, Apache Airflow manages the extraction and loading workflow, Snowflake provides scalable cloud storage, dbt transforms the raw data into analytical metrics, and Preset provides interactive visualization.

The resulting system supports separate analysis of **Beijing and Shanghai** and provides metrics such as temperature moving averages, temperature anomalies, rolling rainfall, and dry spell length.

The project also demonstrates important data engineering practices including:

* Workflow orchestration
* Cloud data warehousing
* SQL-based transformation
* Data quality testing
* Historical snapshots
* Transaction handling
* Error handling
* Pipeline automation
* Business intelligence visualization

---

# References

* Open-Meteo — Weather API and documentation
* Apache Airflow — Workflow orchestration
* Snowflake — Cloud data warehouse
* dbt — Data transformation and testing
* Preset — Business intelligence and dashboard visualization