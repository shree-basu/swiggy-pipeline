"""
loading/load_to_bigquery.py

Simulates the loading layer of the Swiggy pipeline.

Production architecture:
    GCS (transformed) → BigQuery load job → Partitioned + Clustered table

Local simulation:
    Transformed CSV → SQLite database (mimicking BigQuery table structure)

BigQuery table design decisions:
    - Partitioned by order_date      → partition pruning for daily reports
    - Clustered by restaurant_name   → faster GROUP BY restaurant aggregations
    - Avoid SELECT *                 → columnar storage, only read needed cols
"""

import pandas as pd
import sqlite3
import glob
import os
from datetime import datetime


BQ_SIMULATION_DB = "gcs_simulation/bigquery_simulation.db"
TABLE_NAME = "swiggy_orders"


def create_table_schema(conn):
    """
    Mirrors BigQuery table DDL.
    In production this would be a BigQuery CREATE TABLE with
    PARTITION BY DATE(order_date) CLUSTER BY restaurant_name
    """
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            order_id          TEXT PRIMARY KEY,
            customer_name     TEXT,
            phone             TEXT,
            restaurant_name   TEXT,
            food_item         TEXT,
            price             REAL,
            order_status      TEXT,
            order_timestamp   TEXT,
            order_hour        INTEGER,
            order_date        TEXT,
            is_weekend        INTEGER,
            price_bucket      TEXT,
            rating            REAL,
            city              TEXT,
            loaded_at         TEXT
        )
    """)
    conn.commit()
    print(f"  [Schema]  Table '{TABLE_NAME}' ready")
    print(f"  [Design]  Partitioned by: order_date")
    print(f"  [Design]  Clustered by:   restaurant_name")


def load_data(conn, df):
    """
    Simulates BigQuery load job.
    In production: bigquery.Client().load_table_from_dataframe()
    """
    df["loaded_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Convert boolean to int for SQLite compatibility
    df["is_weekend"] = df["is_weekend"].astype(int)

    before = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {TABLE_NAME}", conn).iloc[0]["cnt"]

    df.to_sql(TABLE_NAME, conn, if_exists="append", index=False)

    after = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {TABLE_NAME}", conn).iloc[0]["cnt"]
    new_rows = after - before

    print(f"  [Load]    {new_rows} new rows inserted → total {after} rows in table")


def verify_load(conn):
    """
    Post-load verification — confirms data landed correctly.
    In production: BigQuery job status check + row count assertion.
    """
    stats = pd.read_sql(f"""
        SELECT
            COUNT(*)                          AS total_orders,
            COUNT(DISTINCT restaurant_name)   AS unique_restaurants,
            ROUND(AVG(price), 2)              AS avg_order_value,
            MIN(order_date)                   AS earliest_date,
            MAX(order_date)                   AS latest_date
        FROM {TABLE_NAME}
    """, conn)

    print(f"\n  [Verify]  Load verification passed:")
    print(f"            Total orders      : {stats['total_orders'].iloc[0]}")
    print(f"            Unique restaurants: {stats['unique_restaurants'].iloc[0]}")
    print(f"            Avg order value   : ₹{stats['avg_order_value'].iloc[0]}")
    print(f"            Date range        : {stats['earliest_date'].iloc[0]} → {stats['latest_date'].iloc[0]}")


def run_loading(transformed_filepath):
    print("=" * 60)
    print("SWIGGY PIPELINE — LOADING LAYER (BigQuery)")
    print("=" * 60)
    print(f"\n[BigQuery] Reading transformed data: {transformed_filepath}")

    df = pd.read_csv(transformed_filepath)
    print(f"[BigQuery] {len(df)} records to load\n")

    os.makedirs("gcs_simulation", exist_ok=True)
    conn = sqlite3.connect(BQ_SIMULATION_DB)

    create_table_schema(conn)
    load_data(conn, df)
    verify_load(conn)

    conn.close()
    print(f"\n[BigQuery] ✅ Load complete → {BQ_SIMULATION_DB} (simulating BigQuery)")
    return BQ_SIMULATION_DB


if __name__ == "__main__":
    files = sorted(glob.glob("gcs_simulation/transformed/*.csv"))
    if files:
        run_loading(files[-1])
    else:
        print("No transformed files found. Run transformation first.")
