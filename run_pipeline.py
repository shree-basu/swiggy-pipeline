"""
run_pipeline.py

Main entry point — runs the full Swiggy pipeline end to end.
Simulates what Cloud Composer (Airflow) would orchestrate on GCP.

Usage:
    python run_pipeline.py

Production equivalent:
    Cloud Composer triggers this DAG at 11pm daily.
    Each step runs as a separate Airflow task with monitoring + alerting.
"""

import sys
import os
import glob
from datetime import datetime

# Ensure all modules are importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def main():
    start_time = datetime.now()

    print("\n" + "=" * 60)
    print("  SWIGGY RESTAURANT ANALYTICS PIPELINE")
    print("  Simulating GCP: Pub/Sub → GCS → Dataflow → BigQuery")
    print("  Orchestrated by: Cloud Composer (Apache Airflow)")
    print("=" * 60)
    print(f"  Pipeline started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")

    # ── Step 1: Generate mock data ────────────────────────────────
    print("\n[STEP 1/5] Generating mock Swiggy orders data...")
    from data.generate_mock_data import main as generate
    generate()

    # ── Step 2: Ingestion ─────────────────────────────────────────
    print("\n[STEP 2/5] Running ingestion layer...")
    from ingestion.ingest import run_ingestion
    clean_path = run_ingestion("data/swiggy_orders_raw.csv")

    # ── Step 3: Transformation ────────────────────────────────────
    print("\n[STEP 3/5] Running transformation layer...")
    from transformation.transform import run_transformation
    files = sorted(glob.glob("gcs_simulation/raw_landing/*.csv"))
    transformed_path = run_transformation(files[-1])

    # ── Step 4: Load to BigQuery ──────────────────────────────────
    print("\n[STEP 4/5] Loading to BigQuery...")
    from loading.load_to_bigquery import run_loading
    files = sorted(glob.glob("gcs_simulation/transformed/*.csv"))
    run_loading(files[-1])

    # ── Step 5: Generate report ───────────────────────────────────
    print("\n[STEP 5/5] Generating daily restaurant performance report...")
    import sqlite3
    from analysis.restaurant_analysis import run_restaurant_report
    conn = sqlite3.connect("gcs_simulation/bigquery_simulation.db")
    run_restaurant_report(conn)
    conn.close()

    # ── Summary ───────────────────────────────────────────────────
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    print("\n" + "=" * 60)
    print("  ✅ PIPELINE COMPLETE")
    print(f"  Total duration : {duration:.2f} seconds")
    print(f"  Finished at    : {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
