"""
ingestion/ingest.py

Simulates the ingestion layer of the Swiggy pipeline.

Production architecture:
    Swiggy App → Pub/Sub (event streaming) → GCS (raw landing zone)

Local simulation:
    Raw CSV → validated schema check → "GCS landing zone" (local folder)

Why GCS as landing zone?
- Acts as a safety net — raw data preserved before any transformation
- Decouples ingestion from transformation (Dataflow reads from GCS)
- Enables reprocessing if Dataflow fails
- Object versioning tracks every batch
"""

import pandas as pd
import os
import shutil
from datetime import datetime


# Simulated GCS bucket paths (local folders acting as GCS)
RAW_BUCKET = "gcs_simulation/raw_landing"
QUARANTINE_BUCKET = "gcs_simulation/quarantine"

REQUIRED_COLUMNS = [
    "order_id", "customer_name", "phone", "address",
    "restaurant_name", "food_item", "price",
    "order_status", "order_timestamp", "rating"
]

VALID_STATUSES = {"completed", "cancelled", "refunded"}


def simulate_pubsub_receive(filepath):
    """
    In production: Pub/Sub subscriber pulls messages from topic.
    Here: We read the CSV as a batch (simulating a micro-batch from Pub/Sub).
    """
    print(f"[Pub/Sub] Receiving batch from: {filepath}")
    df = pd.read_csv(filepath)
    print(f"[Pub/Sub] Batch received — {len(df)} messages")
    return df


def validate_schema(df):
    """
    Lightweight pre-Dataflow validation.
    Fail fast principle — catch obvious issues before spinning up transformation.
    """
    print("\n[Ingestion] Running schema validation...")

    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"❌ Schema mismatch — missing columns: {missing_cols}")

    print(f"  ✅ Schema valid — all {len(REQUIRED_COLUMNS)} required columns present")
    return True


def route_records(df):
    """
    Dead letter queue pattern:
    - Clean records → raw landing zone (GCS)
    - Dirty records → quarantine bucket for investigation

    Rules:
    - Negative price → quarantine
    - Invalid order_status → quarantine
    - Rating out of range (< 1 or > 5) → quarantine
    - Nulls in critical fields → quarantine (customer_name, phone, address)
    """
    print("\n[Ingestion] Routing records...")

    dirty_mask = (
        (df["price"] < 0) |
        (~df["order_status"].isin(VALID_STATUSES)) |
        (df["rating"] < 1.0) | (df["rating"] > 5.0) |
        df["customer_name"].isna() |
        df["phone"].isna() |
        df["address"].isna()
    )

    clean_df = df[~dirty_mask].copy()
    quarantine_df = df[dirty_mask].copy()

    pct_dirty = (len(quarantine_df) / len(df)) * 100
    print(f"  Total records    : {len(df)}")
    print(f"  Clean records    : {len(clean_df)} → raw landing zone")
    print(f"  Quarantine records: {len(quarantine_df)} ({pct_dirty:.1f}%) → quarantine bucket")

    # Business SLA check — if > 30% dirty, raise alert
    if pct_dirty > 30:
        print(f"\n  🚨 ALERT: {pct_dirty:.1f}% dirty records exceeds 30% SLA threshold!")
        print(f"     Pipeline continues with clean records — quarantine sent for investigation.")

    return clean_df, quarantine_df


def land_to_gcs(clean_df, quarantine_df):
    """
    Simulates writing to GCS buckets.
    In production: google-cloud-storage client uploads to gs://bucket/path
    """
    os.makedirs(RAW_BUCKET, exist_ok=True)
    os.makedirs(QUARANTINE_BUCKET, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    clean_path = f"{RAW_BUCKET}/swiggy_orders_{timestamp}.csv"
    quarantine_path = f"{QUARANTINE_BUCKET}/quarantine_{timestamp}.csv"

    clean_df.to_csv(clean_path, index=False)
    quarantine_df.to_csv(quarantine_path, index=False)

    print(f"\n[GCS] ✅ Clean data landed    → {clean_path}")
    print(f"[GCS] ⚠️  Quarantine landed   → {quarantine_path}")

    return clean_path


def run_ingestion(source_filepath):
    print("=" * 60)
    print("SWIGGY PIPELINE — INGESTION LAYER")
    print("=" * 60)

    df = simulate_pubsub_receive(source_filepath)
    validate_schema(df)
    clean_df, quarantine_df = route_records(df)
    clean_path = land_to_gcs(clean_df, quarantine_df)

    print("\n[Ingestion] ✅ Ingestion complete — ready for Dataflow transformation")
    return clean_path


if __name__ == "__main__":
    run_ingestion("data/swiggy_orders_raw.csv")
