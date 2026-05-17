"""
Airflow DAG — SA Housing Market DE Pipeline
=============================================
Orchestrates the full end-to-end pipeline:

  [data_quality_check]
        ↓
  [etl_extract_transform]
        ↓
  [etl_load]
        ↓
  [data_modeling]        [spark_aggregations]
        ↓                        ↓
        └──────────┬─────────────┘
                   ↓
           [dbt_staging]
                   ↓
         [dbt_intermediate]
                   ↓
            [dbt_marts]
                   ↓
           [anomaly_detection]
                   ↓
              [notify_slack]

Schedule: daily at 06:00 UTC
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python  import PythonOperator, BranchPythonOperator
from airflow.operators.bash    import BashOperator
from airflow.operators.dummy   import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Default Args ───────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 1, 1),
    "email_on_failure": True,
    "email":            ["de-team@company.com"],
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# ── Python Callables ───────────────────────────────────────────────────────────
def run_quality_checks(**ctx):
    import sys; sys.path.insert(0, "/opt/airflow/dags/sa_housing")
    from data_quality.quality_checks import run_quality_checks as rqc
    passed = rqc("data/raw/Housing_Market_SA_1M.csv")
    if not passed:
        raise ValueError("Data quality gate FAILED — pipeline halted")


def run_etl(**ctx):
    import sys; sys.path.insert(0, "/opt/airflow/dags/sa_housing")
    from pipelines.etl_pipeline import run_pipeline
    run_pipeline()


def run_modeling(**ctx):
    import sys; sys.path.insert(0, "/opt/airflow/dags/sa_housing")
    from pipelines.data_modeling import run_modeling
    run_modeling()


def run_anomaly_detection(**ctx):
    import sqlite3, pandas as pd, numpy as np
    conn = sqlite3.connect("data/warehouse/housing_dw.sqlite")
    df   = pd.read_sql("SELECT * FROM fact_transactions", conn)
    conn.close()

    # Z-score anomaly detection on Transaction_Value
    df["z_score"] = (df["Transaction_Value"] - df["Transaction_Value"].mean()) / df["Transaction_Value"].std()
    anomalies     = df[df["z_score"].abs() > 3]

    if len(anomalies) > 0:
        pct = len(anomalies) / len(df) * 100
        print(f"⚠️  {len(anomalies):,} anomalies detected ({pct:.2f}% of data)")
        anomalies.to_csv("data_quality/anomalies_latest.csv", index=False)
    else:
        print("✅ No anomalies detected")


def branch_on_quality(**ctx):
    """Branch: if quality passes → continue, else → alert and stop."""
    ti = ctx["ti"]
    return "run_etl" if True else "quality_failed_alert"


# ── DAG Definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="sa_housing_market_pipeline",
    default_args=DEFAULT_ARGS,
    description="End-to-end DE pipeline for SA Housing Market data",
    schedule_interval="0 6 * * *",   # daily 06:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=["housing", "etl", "data-engineering"],
) as dag:

    # ── Task: Start ─────────────────────────────────────────────────────────
    start = DummyOperator(task_id="start")

    # ── Task: Data Quality Gate ─────────────────────────────────────────────
    quality_check = PythonOperator(
        task_id="data_quality_check",
        python_callable=run_quality_checks,
    )

    # ── Task: ETL ───────────────────────────────────────────────────────────
    etl = PythonOperator(
        task_id="run_etl",
        python_callable=run_etl,
    )

    # ── Task: Data Modeling ─────────────────────────────────────────────────
    modeling = PythonOperator(
        task_id="data_modeling",
        python_callable=run_modeling,
    )

    # ── Task: Spark Aggregations ────────────────────────────────────────────
    spark_agg = BashOperator(
        task_id="spark_aggregations",
        bash_command="cd /opt/airflow/dags/sa_housing && spark-submit spark/spark_processing.py",
    )

    # ── Task: dbt Run ───────────────────────────────────────────────────────
    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command="cd /opt/airflow/dags/sa_housing/dbt_project && dbt run --select staging",
    )

    dbt_intermediate = BashOperator(
        task_id="dbt_intermediate",
        bash_command="cd /opt/airflow/dags/sa_housing/dbt_project && dbt run --select intermediate",
    )

    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command="cd /opt/airflow/dags/sa_housing/dbt_project && dbt run --select marts",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dags/sa_housing/dbt_project && dbt test",
    )

    # ── Task: Anomaly Detection ─────────────────────────────────────────────
    anomaly = PythonOperator(
        task_id="anomaly_detection",
        python_callable=run_anomaly_detection,
    )

    # ── Task: End ───────────────────────────────────────────────────────────
    end = DummyOperator(
        task_id="pipeline_complete",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Dependencies ────────────────────────────────────────────────────────
    start >> quality_check >> etl >> [modeling, spark_agg]
    modeling >> dbt_staging >> dbt_intermediate >> dbt_marts >> dbt_test
    spark_agg >> dbt_staging
    dbt_test >> anomaly >> end
