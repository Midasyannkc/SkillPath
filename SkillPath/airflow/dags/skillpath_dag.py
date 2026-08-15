"""
SkillPath orchestration DAG.

Bronze (extract) -> Silver (PySpark clean/join) -> Gold (PySpark aggregate)
-> dbt build (marts + tests) -> ML scoring.

Design decisions:
  - dbt build runs AFTER the Spark gold job, not instead of it. The Spark
    jobs own the heavy join/aggregation work against the full completions
    history; dbt owns the governed, tested SQL layer on top of what lands
    in the warehouse. Keeping the heavy lifting in Spark and the contract
    layer in dbt avoids re-implementing PySpark aggregation logic in SQL
    twice.
  - retries=2 with exponential backoff on every task, since the most common
    real-world failure mode here is an upstream LMS export landing late or
    the connection timing out mid-extract, not a logic bug.
  - on_failure_callback would route to Slack/PagerDuty in production; left
    as a named stub here since this repo has no live alerting endpoint.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}


def run_bronze_ingest():
    import subprocess
    subprocess.run(["python3", "data_generation/generate_lms_hris_data.py"], check=True)


def run_silver_transform():
    from pipeline.silver_transform import run
    run()


def run_gold_aggregate():
    from pipeline.gold_aggregate import run
    run()


def run_ml_scoring():
    from ml.skill_gap_model import train
    train()


def alert_on_failure(context):
    """Stub for Slack/PagerDuty integration in a real deployment."""
    task_id = context["task_instance"].task_id
    print(f"[ALERT STUB] Task failed: {task_id}. Would notify #data-eng-alerts here.")


with DAG(
    dag_id="skillpath_pipeline",
    description="LMS/HRIS bronze->silver->gold->dbt->ML skill-gap pipeline",
    default_args=default_args,
    schedule="0 5 * * *",  # daily at 5am, after overnight LMS export lands
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["skillpath", "l_and_d", "data-platform"],
    on_failure_callback=alert_on_failure,
) as dag:

    bronze_ingest = PythonOperator(
        task_id="bronze_ingest",
        python_callable=run_bronze_ingest,
    )

    silver_transform = PythonOperator(
        task_id="silver_transform",
        python_callable=run_silver_transform,
    )

    gold_aggregate = PythonOperator(
        task_id="gold_aggregate",
        python_callable=run_gold_aggregate,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="cd {{ var.value.skillpath_repo_path }}/dbt_skillpath && dbt build --target prod",
    )

    ml_scoring = PythonOperator(
        task_id="ml_skill_gap_scoring",
        python_callable=run_ml_scoring,
    )

    bronze_ingest >> silver_transform >> gold_aggregate >> dbt_build >> ml_scoring
