# Swiggy Restaurant Analytics Pipeline 🍕

An end-to-end data engineering pipeline simulating a production-grade GCP architecture for Swiggy restaurant order analytics.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     SWIGGY ORDERS PIPELINE                          │
│              Orchestrated by Cloud Composer (Airflow)               │
└─────────────────────────────────────────────────────────────────────┘

  Swiggy App
      │
      ▼
┌──────────┐     Events stream       ┌─────────────────────┐
│  Pub/Sub │ ──── in real-time ────▶ │  GCS: Raw Landing   │
│  (Topic) │                         │  gs://raw-bucket/   │
└──────────┘                         └─────────┬───────────┘
                                               │
                                    ┌──────────▼───────────┐
                                    │   Ingestion Layer     │
                                    │  • Schema validation  │
                                    │  • Dead letter queue  │
                                    │  • Quarantine routing │
                                    └──────────┬───────────┘
                                               │
                               ┌───────────────┴──────────────┐
                               │                              │
                    ┌──────────▼──────────┐     ┌────────────▼────────────┐
                    │  GCS: Clean Data    │     │  GCS: Quarantine        │
                    │  gs://raw-bucket/   │     │  Dirty / invalid records│
                    │  clean/             │     │  for investigation      │
                    └──────────┬──────────┘     └─────────────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │  Dataflow (Beam)    │
                    │  • Deduplication   │
                    │  • Standardisation │
                    │  • PII masking     │  ← Cloud DLP
                    │  • Derived columns │
                    │  • Null handling   │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │  GCS: Transformed  │
                    │  gs://clean-bucket/│
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │     BigQuery        │
                    │  PARTITION BY date  │
                    │  CLUSTER BY         │
                    │  restaurant_name    │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │  Daily Report       │
                    │  (11pm schedule)    │
                    │  • Revenue analysis │
                    │  • Peak hours       │
                    │  • Cancellations    │
                    └─────────────────────┘
```

---

## Tech Stack

| Layer | Local Simulation | Production GCP Service |
|---|---|---|
| Event streaming | CSV batch read | **Pub/Sub** |
| Raw storage | Local folder | **Google Cloud Storage** |
| Transformation | Pandas | **Dataflow (Apache Beam)** |
| PII masking | SHA-256 hash | **Cloud DLP** |
| Data warehouse | SQLite | **BigQuery** |
| Orchestration | Python script | **Cloud Composer (Airflow)** |

---

## Project Structure

```
swiggy-pipeline/
│
├── data/
│   ├── generate_mock_data.py    # Generates 2000 orders with dirty records
│   └── swiggy_orders_raw.csv   # Generated raw data (gitignored in prod)
│
├── ingestion/
│   └── ingest.py               # Schema validation, dead letter queue, GCS landing
│
├── transformation/
│   └── transform.py            # Dedup, PII masking, enrichment (simulates Dataflow)
│
├── loading/
│   └── load_to_bigquery.py     # Partitioned + clustered BigQuery load
│
├── analysis/
│   └── restaurant_analysis.py  # 7 SQL queries powering the daily report
│
├── dags/
│   └── swiggy_pipeline_dag.py  # Airflow DAG — full pipeline orchestration
│
├── run_pipeline.py             # Main entry point — runs all 5 steps
├── requirements.txt
└── README.md
```

---

## Key Design Decisions

### 1. Dead Letter Queue Pattern
Dirty records (invalid status, negative prices, out-of-range ratings, null PII) are routed to a quarantine bucket instead of stopping the pipeline. This is the production-standard approach — the clean majority of orders continue processing while bad records are isolated for investigation.

If dirty record percentage exceeds **30%**, a business SLA alert is triggered.

### 2. PII Masking (Cloud DLP Simulation)
Before data reaches BigQuery:
- `customer_name` → masked (first name only)
- `phone` → SHA-256 hashed (one-way, analytics-safe)
- `address` → city extracted, full address dropped

In production, **Cloud DLP** handles this automatically inside Dataflow.

### 3. BigQuery Table Design
```sql
-- Production DDL
CREATE TABLE swiggy_orders
PARTITION BY DATE(order_date)
CLUSTER BY restaurant_name;
```
- **Partitioning** by `order_date` → daily reports only scan today's partition (cost + speed)
- **Clustering** by `restaurant_name` → GROUP BY restaurant queries scan co-located data
- No `SELECT *` anywhere in analysis queries — columnar billing model

### 4. Separation of Concerns
Each pipeline stage is an independent module. In production, each maps directly to a separate GCP service. Failures in one stage don't cascade — Airflow retries the failed task independently.

---

## Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Run the full pipeline
python run_pipeline.py
```

---

## Sample Output

```
[STEP 2/5] Ingestion layer
  Total records    : 2000
  Clean records    : 1800 → raw landing zone
  Quarantine       : 200 (10.0%) → quarantine bucket

[STEP 3/5] Transformation (Dataflow)
  [Dedup]      0 duplicates removed
  [PII Mask]   customer_name masked, phone hashed
  [Derived]    Added: order_hour, order_date, is_weekend, price_bucket

📊 Top 5 Restaurants by Revenue
  Box8            ₹117,146  (163 orders)
  Domino's        ₹94,883   (139 orders)
  Burger King     ₹88,733   (136 orders)
```

---

## What This Would Look Like on GCP

In a production deployment:
1. **Pub/Sub** receives order events from the Swiggy app in real time
2. **Dataflow** runs as a streaming pipeline (not batch) using Apache Beam
3. **Cloud DLP** is called inside the Dataflow pipeline for PII tokenization
4. **BigQuery** receives data via streaming inserts or scheduled load jobs
5. **Cloud Composer** orchestrates the DAG and sends alerts on failure
6. **Cloud Monitoring** tracks pipeline health, latency, and error rates

The local simulation mirrors this architecture exactly — same stages, same data contracts, same design decisions. Only the runtime differs.

---

## Author

Built as a portfolio project to demonstrate GCP Data Engineering concepts:
Pub/Sub · GCS · Dataflow · BigQuery · Cloud Composer · Cloud DLP · IAM

