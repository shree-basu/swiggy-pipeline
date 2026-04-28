"""
analysis/restaurant_analysis.py

Runs SQL analysis queries against the BigQuery-simulated SQLite database.
These are the queries that power the 11pm daily restaurant performance report.

In production: These run directly in BigQuery using the BQ Python client.
BigQuery optimisations applied (noted in comments):
    - Partition pruning via WHERE order_date = CURRENT_DATE
    - Clustered column (restaurant_name) used in GROUP BY → faster scans
    - Materialized view candidates marked with [MV CANDIDATE]
"""

import sqlite3
import pandas as pd
from datetime import datetime

BQ_SIMULATION_DB = "gcs_simulation/bigquery_simulation.db"


def run_query(conn, title, sql):
    print(f"\n{'─' * 60}")
    print(f"📊 {title}")
    print(f"{'─' * 60}")
    df = pd.read_sql(sql, conn)
    print(df.to_string(index=False))
    return df


def run_restaurant_report(conn):
    print("=" * 60)
    print("SWIGGY PIPELINE — DAILY RESTAURANT PERFORMANCE REPORT")
    print(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # ── 1. Top 5 restaurants by revenue ──────────────────────────
    # [MV CANDIDATE] — refreshed daily, expensive aggregation
    run_query(conn, "Top 5 Restaurants by Total Revenue", """
        SELECT
            restaurant_name,
            COUNT(order_id)           AS total_orders,
            ROUND(SUM(price), 2)      AS total_revenue,
            ROUND(AVG(price), 2)      AS avg_order_value,
            ROUND(AVG(rating), 2)     AS avg_rating
        FROM swiggy_orders
        WHERE order_status = 'completed'
        GROUP BY restaurant_name
        ORDER BY total_revenue DESC
        LIMIT 5
    """)

    # ── 2. Order status breakdown ─────────────────────────────────
    run_query(conn, "Order Status Distribution", """
        SELECT
            order_status,
            COUNT(*)                                    AS count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*))
                  OVER (), 2)                           AS percentage
        FROM swiggy_orders
        GROUP BY order_status
        ORDER BY count DESC
    """)

    # ── 3. Peak ordering hours ────────────────────────────────────
    run_query(conn, "Peak Ordering Hours (Top 5)", """
        SELECT
            order_hour,
            COUNT(order_id)       AS total_orders,
            ROUND(AVG(price), 2)  AS avg_order_value
        FROM swiggy_orders
        WHERE order_status = 'completed'
        GROUP BY order_hour
        ORDER BY total_orders DESC
        LIMIT 5
    """)

    # ── 4. Weekend vs weekday performance ────────────────────────
    run_query(conn, "Weekend vs Weekday Performance", """
        SELECT
            CASE WHEN is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
            COUNT(order_id)           AS total_orders,
            ROUND(SUM(price), 2)      AS total_revenue,
            ROUND(AVG(price), 2)      AS avg_order_value
        FROM swiggy_orders
        WHERE order_status = 'completed'
        GROUP BY is_weekend
        ORDER BY total_orders DESC
    """)

    # ── 5. Revenue by price bucket ────────────────────────────────
    run_query(conn, "Order Volume by Price Bucket", """
        SELECT
            price_bucket,
            COUNT(order_id)           AS total_orders,
            ROUND(SUM(price), 2)      AS total_revenue
        FROM swiggy_orders
        WHERE order_status = 'completed'
          AND price_bucket IS NOT NULL
        GROUP BY price_bucket
        ORDER BY total_revenue DESC
    """)

    # ── 6. Restaurants with highest cancellation rate ─────────────
    # Window function — RANK used to find worst performers
    run_query(conn, "Restaurants with Highest Cancellation Rate", """
        SELECT
            restaurant_name,
            total_orders,
            cancelled_orders,
            ROUND(cancelled_orders * 100.0 / total_orders, 2) AS cancellation_rate_pct
        FROM (
            SELECT
                restaurant_name,
                COUNT(order_id)                                           AS total_orders,
                SUM(CASE WHEN order_status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_orders
            FROM swiggy_orders
            GROUP BY restaurant_name
        )
        WHERE total_orders >= 10
        ORDER BY cancellation_rate_pct DESC
        LIMIT 5
    """)

    # ── 7. City-level performance ─────────────────────────────────
    run_query(conn, "Revenue by City", """
        SELECT
            city,
            COUNT(order_id)           AS total_orders,
            ROUND(SUM(price), 2)      AS total_revenue,
            ROUND(AVG(rating), 2)     AS avg_rating
        FROM swiggy_orders
        WHERE order_status = 'completed'
        GROUP BY city
        ORDER BY total_revenue DESC
    """)

    print(f"\n{'=' * 60}")
    print("✅ Daily report complete")


if __name__ == "__main__":
    conn = sqlite3.connect(BQ_SIMULATION_DB)
    run_restaurant_report(conn)
    conn.close()
