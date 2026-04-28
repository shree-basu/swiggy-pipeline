"""
generate_mock_data.py
Generates mock Swiggy orders CSV with realistic dirty records injected.
In production, this data would stream in via Pub/Sub.
"""

import pandas as pd
import random
from datetime import datetime, timedelta

random.seed(42)

RESTAURANTS = [
    "Behrouz Biryani", "Faasos", "Box8", "Freshmenu",
    "Oven Story", "Wow Momo", "Burger King", "McDonald's",
    "Pizza Hut", "Domino's"
]

FOOD_ITEMS = [
    "Chicken Biryani", "Paneer Butter Masala", "Veg Burger",
    "Chicken Wings", "Pasta Arrabbiata", "Masala Dosa",
    "Pepperoni Pizza", "Hakka Noodles", "Dal Makhani", "Momos"
]

ORDER_STATUSES = ["completed", "cancelled", "refunded"]
STATUS_WEIGHTS = [0.75, 0.15, 0.10]  # realistic distribution

FIRST_NAMES = ["Rahul", "Priya", "Amit", "Sneha", "Karan",
               "Neha", "Rohit", "Pooja", "Vijay", "Anjali"]
LAST_NAMES = ["Sharma", "Verma", "Patel", "Singh", "Kumar",
              "Gupta", "Joshi", "Mehta", "Nair", "Reddy"]

CITIES = ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Pune"]


def random_phone():
    return f"9{random.randint(100000000, 999999999)}"


def random_address(city):
    return f"{random.randint(1, 999)}, {random.choice(['MG Road', 'Park Street', 'Link Road', 'Main Street'])}, {city}"


def random_timestamp():
    base = datetime(2024, 1, 1)
    delta = timedelta(days=random.randint(0, 364),
                      hours=random.randint(0, 23),
                      minutes=random.randint(0, 59))
    return (base + delta).strftime("%Y-%m-%d %H:%M:%S")


def generate_clean_record(order_id):
    city = random.choice(CITIES)
    return {
        "order_id": f"ORD{order_id:05d}",
        "customer_name": f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
        "phone": random_phone(),
        "address": random_address(city),
        "restaurant_name": random.choice(RESTAURANTS),
        "food_item": random.choice(FOOD_ITEMS),
        "price": round(random.uniform(150, 1200), 2),
        "order_status": random.choices(ORDER_STATUSES, STATUS_WEIGHTS)[0],
        "order_timestamp": random_timestamp(),
        "rating": round(random.uniform(1.0, 5.0), 1)
    }


def inject_dirty_records(records):
    """
    Deliberately inject dirty data to simulate real-world pipeline issues.
    Tests our validation and dead letter queue logic.
    """
    dirty_indices = random.sample(range(len(records)), 200)  # ~10% dirty

    for i, idx in enumerate(dirty_indices):
        dirty_type = i % 6

        if dirty_type == 0:
            # Null customer name — PII field missing
            records[idx]["customer_name"] = None

        elif dirty_type == 1:
            # Null phone — PII field missing
            records[idx]["phone"] = None

        elif dirty_type == 2:
            # Negative price — impossible business value
            records[idx]["price"] = -abs(records[idx]["price"])

        elif dirty_type == 3:
            # Invalid order status — not in allowed enum
            records[idx]["order_status"] = "pending_forever"

        elif dirty_type == 4:
            # Rating out of range — should be 1.0 to 5.0
            records[idx]["rating"] = random.choice([0.0, 6.5, 99.0])

        elif dirty_type == 5:
            # Null address — incomplete record
            records[idx]["address"] = None

    return records


def main():
    print("Generating 2000 mock Swiggy orders...")
    records = [generate_clean_record(i) for i in range(1, 2001)]
    records = inject_dirty_records(records)

    df = pd.DataFrame(records)
    output_path = "data/swiggy_orders_raw.csv"
    df.to_csv(output_path, index=False)

    print(f"✅ Generated {len(df)} records → {output_path}")
    print(f"   Dirty records injected: ~200 (~10%)")
    print(f"   Columns: {list(df.columns)}")
    print(f"\nSample (first 3 rows):\n{df.head(3).to_string()}")


if __name__ == "__main__":
    main()
