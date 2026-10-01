from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt"

# BashOperator renders `env` as a template field, so these resolve at execution
# time rather than when the scheduler parses this file.
DBT_ENV = {
    "DBT_KEY_PATH": "/opt/airflow/keys/rsa_key.p8",
    "SNOWFLAKE_ACCOUNT": "{{ var.value.SNOWFLAKE_ACCOUNT }}",
    "SNOWFLAKE_USER": "{{ var.value.SNOWFLAKE_USER }}",
    "SNOWFLAKE_ROLE": "{{ var.value.SNOWFLAKE_ROLE }}",
    "SNOWFLAKE_DATABASE": "{{ var.value.SNOWFLAKE_DATABASE }}",
    "SNOWFLAKE_WAREHOUSE": "{{ var.value.SNOWFLAKE_WAREHOUSE }}",
    "SNOWFLAKE_KEY_PASSPHRASE": "{{ var.value.SNOWFLAKE_KEY_PASSPHRASE }}",
}

with DAG(
    dag_id="dbt_weather_pipeline",
    description="Run dbt models, tests, and snapshot after the weather ETL finishes.",
    start_date=datetime(2026, 9, 20),
    catchup=False,
    schedule=None,
    tags=["dbt", "weather", "etl"],
    default_args={
        "owner": "data226",
        "retries": 0,
    },
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd '{DBT_PROJECT_DIR}' && dbt run --profiles-dir . --project-dir .",
        env=DBT_ENV,
        append_env=True,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd '{DBT_PROJECT_DIR}' && dbt test --profiles-dir . --project-dir .",
        env=DBT_ENV,
        append_env=True,
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd '{DBT_PROJECT_DIR}' && dbt snapshot --profiles-dir . --project-dir .",
        env=DBT_ENV,
        append_env=True,
    )

    dbt_run >> dbt_test >> dbt_snapshot