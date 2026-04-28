"""
transformation/transform.py

Simulates the transformation layer of the Swiggy pipeline.

Production architecture:
    GCS (raw) → Apache Beam pipeline on Dataflow → GCS (transformed)

Local simulation:
    CSV from GCS landing zone → Pandas transformations → transformed CSV

Transformations applied:
    1. Deduplication
    2. Standardisation (casing, whitespace)
    3. PII masking (simulating Cloud DLP)
    4. Derived columns (order_hour, is_weekend, price_bucket)
    5. Null handling for non-critical fields
"""

import pandas as pd
import hashlib
import os
from datetime import datetime


TRANSFORMED_BUCKET = "gcs_simulation/transformed"


# ─────────────────────────────────────────────
# TRANSFORM FUNCTIONS (each = one Beam ParDo)
# ─────────────────────────────────────────────

def deduplicate(df):
    """
    ParDo equivalent: Remove duplicate order_ids.
    Duplicates can occur when Pub/Sub delivers at-least-once.
    """
    before = len(df)
    df = df.drop_duplicates(subset=["order_id"])
    after = len(df)
    print(f"  [Dedup]      {before - after} duplicates removed → {after} records remaining")
    return df


def standardise(df):
    """
    ParDo equivalent: Normalise string fields.
    Prevents 'Mumbai' vs 'mumbai' causing GROUP BY splits.
    """
    df["restaurant_name"] = df["restaurant_name"].str.strip().str.title()
    df["food_item"] = df["food_item"].str.strip().str.title()
    df["order_status"] = df["order_status"].str.strip().str.lower()
    print(f"  [Standardise] Casing and whitespace normalised")
    return df


def mask_pii(df):
    """
    Simulates Cloud DLP masking.
    In production: DLP API tokenizes PII before BigQuery load.

    customer_name → masked (first name only + ***)
    phone         → hashed (SHA-256 one-way hash for analytics use)
    address       → city extracted, full address dropped
    """
    df["customer_name"] = df["customer_name"].apply(
        lambda x: x.split()[0] + " ***" if pd.notna(x) else x
    )
    df["phone"] = df["phone"].apply(
        lambda x: hashlib.sha256(str(x).encode()).hexdigest()[:16] if pd.notna(x) else x
    )
    # Extract city from address (last word), drop full address
    df["city"] = df["address"].apply(
        lambda x: x.split(",")[-1].strip() if pd.notna(x) else "Unknown"
    )
    df = df.drop(columns=["address"])
    print(f"  [PII Mask]   customer_name masked, phone hashed, address → city extracted")
    return df


def add_derived_columns(df):
    """
    ParDo equivalent: Enrich records with derived business fields.
    Makes downstream SQL analysis much more powerful.
    """
    df["order_timestamp"] = pd.to_datetime(df["order_timestamp"])
    df["order_hour"] = df["order_timestamp"].dt.hour
    df["order_date"] = df["order_timestamp"].dt.date
    df["is_weekend"] = df["order_timestamp"].dt.dayofweek >= 5

    df["price_bucket"] = pd.cut(
        df["price"],
        bins=[0, 200, 500, 800, float("inf")],
        labels=["budget", "mid", "premium", "luxury"]
    )

    print(f"  [Derived]    Added: order_hour, order_date, is_weekend, price_bucket")
    return df


def handle_nulls(df):
    """
    Fill nulls in non-critical fields.
    Critical fields (phone, customer_name) were already quarantined in ingestion.
    """
    df["rating"] = df["rating"].fillna(df["rating"].median())
    print(f"  [Nulls]      rating nulls filled with median")
    return df


# ─────────────────────────────────────────────
# PIPELINE RUNNER
# ─────────────────────────────────────────────

def run_transformation(clean_filepath):
    print("=" * 60)
    print("SWIGGY PIPELINE — TRANSFORMATION LAYER")
    print("=" * 60)
    print(f"\n[Dataflow] Reading from GCS: {clean_filepath}")

    df = pd.read_csv(clean_filepath)
    print(f"[Dataflow] {len(df)} records loaded\n")

    print("[Dataflow] Applying transforms (ParDo operations):")
    df = deduplicate(df)
    df = standardise(df)
    df = mask_pii(df)
    df = add_derived_columns(df)
    df = handle_nulls(df)

    os.makedirs(TRANSFORMED_BUCKET, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"{TRANSFORMED_BUCKET}/swiggy_transformed_{timestamp}.csv"
    df.to_csv(output_path, index=False)

    print(f"\n[Dataflow] ✅ Transformation complete → {output_path}")
    print(f"[Dataflow] Final schema: {list(df.columns)}")
    return output_path


if __name__ == "__main__":
    # For standalone testing — finds latest file in raw landing
    import glob
    files = sorted(glob.glob("gcs_simulation/raw_landing/*.csv"))
    if files:
        run_transformation(files[-1])
    else:
        print("No files found in raw landing zone. Run ingestion first.")
