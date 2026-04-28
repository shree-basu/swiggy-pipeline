"""
dags/swiggy_pipeline_dag.py

Airflow DAG — orchestrates the full Swiggy data pipeline.

Production schedule: Daily at 11pm (cron: 0 23 * * *)
Cloud Composer manages this DAG on GCP.

Pipeline flow:
    generate_data → ingest → transform → load → generate_report

Each task is independent and monitored separately.
If transform fails, load and report do NOT run.
"""

import sys
import os

# Allow imports from project root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import glob


# ─────────────────────────────────────────────
# TASK FUNCTIONS
# ─────────────────────────────────────────────

def generate_mock_data():
    """
    Task 1: Generate raw data.
    In production: Pub/Sub events stream continuously — this step doesn't exist.
    Here: Generates fresh CSV for each pipeline run.
    """
    import subprocess
    subprocess.run(["python", "data/generate_mock_data.py"], check=True)
    print("[DAG] ✅ Mock data generated")


def run_ingestion():
    """
    Task 2: Ingest raw data.
    Validates schema, routes dirty records to quarantine, lands clean data in GCS.
    """
    from ingestion.ingest import run_ingestion as ingest
    ingest("data/swiggy_orders_raw.csv")
    print("[DAG] ✅ Ingestion complete")


def run_transformation(**context):
    """
    Task 3: Transform clean data.
    Deduplicates, masks PII, adds derived columns.
    Reads latest file from GCS raw landing zone.
    """
    from transformation.transform import run_transformation as transform
    files = sorted(glob.glob("gcs_simulation/raw_landing/*.csv"))
    if not files:
        raise FileNotFoundError("No files in raw landing zone — ingestion may have failed")
    transform(files[-1])
    print("[DAG] ✅ Transformation complete")


def run_loading(**context):
    """
    Task 4: Load to BigQuery.
    Loads transformed data into partitioned + clustered BigQuery table.
    """
    from loading.load_to_bigquery import run_loading as load
    files = sorted(glob.glob("gcs_simulation/transformed/*.csv"))
    if not files:
        raise FileNotFoundError("No transformed files found — transformation may have failed")
    load(files[-1])
    print("[DAG] ✅ Loading complete")


def run_report():
    """
    Task 5: Generate daily restaurant performance report.
    Runs SQL analysis queries against BigQuery.
    Scheduled at 11pm — after the day's orders are fully loaded.
    """
    import sqlite3
    from analysis.restaurant_analysis import run_restaurant_report
    conn = sqlite3.connect("gcs_simulation/bigquery_simulation.db")
    run_restaurant_report(conn)
    conn.close()
    print("[DAG] ✅ Daily report generated")


# ─────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────

with DAG(
    dag_id="swiggy_restaurant_pipeline",
    description="End-to-end Swiggy orders pipeline: ingest → transform → load → report",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 23 * * *",   # Daily at 11pm
    catchup=False,
    tags=["swiggy", "data-engineering", "gcp"]
) as dag:

    t1_generate = PythonOperator(
        task_id="generate_mock_data",
        python_callable=generate_mock_data
    )

    t2_ingest = PythonOperator(
        task_id="run_ingestion",
        python_callable=run_ingestion
    )

    t3_transform = PythonOperator(
        task_id="run_transformation",
        python_callable=run_transformation,
        provide_context=True
    )

    t4_load = PythonOperator(
        task_id="run_loading",
        python_callable=run_loading,
        provide_context=True
    )

    t5_report = PythonOperator(
        task_id="generate_daily_report",
        python_callable=run_report
    )

    # Pipeline dependency chain — linear flow
    t1_generate >> t2_ingest >> t3_transform >> t4_load >> t5_report
