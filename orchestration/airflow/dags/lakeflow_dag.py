"""Nightly convergence DAG: rebuild silver from immutable bronze, gate on data quality, refresh gold marts."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

REPO = "{{ var.value.get('LAKEFLOW_REPO', '/opt/lakeflow-cdc-pipeline') }}"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["data-alerts@example.com"],
}

with DAG(
    dag_id="lakeflow_nightly_rebuild",
    start_date=datetime(2025, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    default_args=default_args,
    tags=["cdc", "lakehouse", "spark"],
) as dag:
    rebuild_silver = BashOperator(task_id="rebuild_silver_from_bronze",
                                  bash_command=f"cd {REPO} && python -m batch.backfill")
    dq_gate = BashOperator(task_id="data_quality_gate",
                           bash_command=f"cd {REPO} && python -m batch.dq_gate --max-quarantine-pct 5")
    build_gold = BashOperator(task_id="build_gold_marts",
                              bash_command=f"cd {REPO} && python -m batch.build_gold")

    rebuild_silver >> dq_gate >> build_gold
