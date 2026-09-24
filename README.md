# Weather Prediction Analytics — Snowflake · Airflow · dbt · Preset

An end-to-end ELT pipeline that pulls hourly weather (past 60 days + 7-day forecast) from the [Open-Meteo API](https://api.open-meteo.com/v1/forecast), loads it into **Snowflake**, transforms it with **dbt** into daily analytics (moving averages, temperature anomaly, rolling rainfall, dry-spell length), and visualizes the results in **Preset**. **Airflow** orchestrates the whole thing: the dbt DAG is triggered automatically right after the ETL DAG succeeds.

## Tech stack

| Layer | Tool |
|---|---|
| Source | Open-Meteo Forecast API (`/v1/forecast`, hourly, no API key) |
| Orchestration | Apache Airflow 2.x (Docker) — TaskFlow API, Connections, Variables |
| Warehouse | Snowflake — database `DATA226LAB`, schemas `RAW` and `ANALYTICS` |
| Transformation | dbt-snowflake 1.8 (models, tests, snapshot), key-pair authentication |
| BI | Preset|

## Repository layout

```
.
├── dags/
│   ├── weather_etl_dag.py          # WeatherETL: extract -> transform -> load -> trigger dbt DAG
│   └── dbt_weather_dag.py          # dbt_weather_pipeline: dbt run >> dbt test >> dbt snapshot
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml                # no secrets — reads env vars for the Snowflake key
│   ├── macros/
│   │   └── generate_schema_name.sql
│   ├── models/
│   │   ├── transform/
│   │   │   └── weather_daily.sql   # ephemeral: hourly -> daily aggregation
│   │   └── analytics/
│   │       ├── weather_metrics.sql # table: moving averages, anomaly, rolling rainfall, dry spell
│   │       └── schema.yml          # dbt tests
│   └── snapshots/
│       └── weather_hourly_snapshot.sql
├── docs/
│   └── screenshots/                # Airflow UI, dbt run/test/snapshot, Preset dashboard
└── README.md
```

## How it works

1. **`WeatherETL`** (`schedule="0 0 * * *"`) calls the Open-Meteo API for a configured latitude/longitude, normalizes the hourly JSON into rows, and loads them into `RAW.weather_hourly` using a full-refresh transaction (`BEGIN` → `DELETE` → `INSERT` → `COMMIT`, with `ROLLBACK` + `raise` on failure).
2. The last ETL task triggers **`dbt_weather_pipeline`** (`schedule=None`) via `TriggerDagRunOperator`, so dbt always runs after a successful load — never on a fixed clock time.
3. **dbt** builds:
   - `weather_daily` (ephemeral) — aggregates hourly rows to one row per location per day, and labels each day
     `HISTORICAL` or `FORECAST`.
   - `weather_metrics` (table, schema `ANALYTICS`) — 7-day moving average temperature, temperature anomaly vs.
     the historical baseline, 7-day rolling rainfall, and dry-spell length.
   - `weather_hourly_snapshot` (schema `ANALYTICS`) — SCD Type 2 history of the raw hourly data, so forecast
     revisions over time are preserved (`check` strategy).
4. **Preset** reads `ANALYTICS.weather_metrics` and renders the dashboard.

## Snowflake objects

| Schema | Object | Created by |
|---|---|---|
| `RAW` | `weather_hourly` | Airflow (`load` task) |
| `ANALYTICS` | `weather_metrics` | dbt model (table) |
| `ANALYTICS` | `weather_hourly_snapshot` | dbt snapshot |
| — | `weather_daily` | dbt model, **ephemeral** — compiled inline into `weather_metrics`, not a queryable table |

`macros/generate_schema_name.sql` overrides dbt's default schema-naming behavior so custom schemas (`+schema: analytics`) resolve to exactly `ANALYTICS`, instead of dbt's default `RAW_ANALYTICS` concatenation.

## Setup

### 1. Snowflake

- Database: `DATA226LAB`
- Schemas: `RAW` (created by the ETL if missing) and `ANALYTICS` (created by dbt)
- Warehouse: `DATA226HOMEWORK`
- Role: `ACCOUNTADMIN`
- User: `DATA226STUDY`, authenticated with an RSA key pair (`private_key_path`, `private_key_passphrase`)

### 2. Airflow — Connections and Variables

| Kind | Name | Purpose |
|---|---|---|
| Connection | `snowflake_default` | Used by `SnowflakeHook` in `WeatherETL` |
| Variable | `LATITUDE`, `LONGITUDE` | Location to fetch |
| Variable | `DBT_PROJECT_DIR` | Path to the dbt project inside the container (e.g. `/opt/airflow/dbt`) |

The dbt DAG's Snowflake credentials are **not** stored in `profiles.yml`. They are supplied as environment variables at task run time:

| Env var | Used for |
|---|---|
| `DBT_KEY_PATH` | Path to the mounted `.p8` private key inside the container |
| `DBT_KEY_PASSPHRASE` | Passphrase for the private key |

Mount the key file into the container (do **not** commit it to git):

```yaml
# docker-compose.yaml
volumes:
  - ${AIRFLOW_PROJ_DIR:-.}/dbt:/opt/airflow/dbt
  - ${AIRFLOW_PROJ_DIR:-.}/keys:/opt/airflow/keys:ro
```

and pass the env vars to each dbt `BashOperator` (see `dags/dbt_weather_dag.py`):

```python
DBT_ENV = {
    "DBT_KEY_PATH": "/opt/airflow/keys/rsa_key.p8",
    "DBT_KEY_PASSPHRASE": "<passphrase>",   # source from your own .env, not hardcoded in git
}
```

### 3. Run it

1. Start Airflow (`docker compose up -d`), set the connection and variables above.
2. Un-pause `WeatherETL` and `dbt_weather_pipeline` in the Airflow UI.
3. Trigger `WeatherETL`. When `load` succeeds, it triggers `dbt_weather_pipeline` automatically
   (`dbt run >> dbt test >> dbt snapshot`).


### 4. Preset

Connect Superset to Snowflake and register `ANALYTICS.weather_metrics` as a dataset. The dashboard has six charts:

| Chart | Type | Shows |
|---|---|---|
| Raw daily values | Table | Underlying daily rows behind the other charts, for reference/debugging |
| Temperature with 7-day moving average | Line | `avg_temperature` vs. `temperature_7day_moving_avg` over time |
| Temperature anomaly | Bar | `temperature_anomaly` per day (deviation from the historical baseline) |
| Temperature distribution | Box plot | Spread of daily temperature (min/max/avg) across the selected range |
| Humidity vs. temperature | Scatter plot | Relationship between `avg_humidity` and `avg_temperature` |
| Weather condition distribution | Pie chart | Share of days by `dominant_weather_code` |

Add a date-range filter on `weather_date` so all six charts can be filtered to a selected window.

## Idempotency

The ETL load runs as a single transaction — `BEGIN` → `DELETE` → `INSERT` → `COMMIT`, with `ROLLBACK` and `raise` in the `except` block — so a failed run leaves `RAW.weather_hourly` untouched, and DDL (`CREATE TABLE`) runs *before* `BEGIN`, since Snowflake implicitly commits an open transaction on DDL.

The `weather_hourly_snapshot` snapshot re-checks the tracked columns on every run: unchanged rows produce no new versions, and a genuinely revised forecast value opens a new row and closes the old one (`dbt_valid_to`).

## Known limitations / notes

- `weather_daily`'s `data_type` split (`HISTORICAL` vs. `FORECAST`) uses `CURRENT_TIMESTAMP()` in the Snowflake session timezone, not the location's local timezone — see `REPORT.md` for details.
- The temperature anomaly baseline is the mean of all historical rows currently in the table (a rolling 60-day-ish window, given `past_days=60` in the ETL), not a long-term climatological normal.

## Team 6
Xuebo Zhou
Yifan Huang

## Repository

_<https://github.com/xuebo666/DATA226LabGroup6>_
