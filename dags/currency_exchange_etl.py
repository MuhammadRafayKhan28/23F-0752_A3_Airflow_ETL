"""
Currency Exchange ETL Pipeline
------------------------------
Extract  : daily exchange rates from the Frankfurter API (v2)
Transform: reshape each rate into the required document format
Load     : upsert the documents into MongoDB

Data moves between tasks with NAMED XComs (explicit keys via
ti.xcom_push / ti.xcom_pull), as the assignment requires.
"""

import os
from datetime import datetime, timedelta, timezone

import requests
from pymongo import MongoClient, UpdateOne

# Works on Astro Runtime 3 (Airflow 3) and falls back to Airflow 2 imports
try:
    from airflow.sdk import DAG
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow 2.x
    from airflow import DAG
    from airflow.operators.python import PythonOperator


# ---------------- Configuration ----------------
API_URL = "https://api.frankfurter.dev/v2/rates"
BASE_CURRENCY = "EUR"
TARGET_CURRENCIES = ["USD", "GBP", "JPY", "PKR", "CHF"]

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin123@mongodb:27017/")
MONGO_DB = os.getenv("MONGO_DB", "currency_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "exchange_rates")

# Named XCom keys
XCOM_RAW_KEY = "raw_exchange_rates"
XCOM_TRANSFORMED_KEY = "transformed_exchange_rates"


# ---------------- Task 1: Extract ----------------
def extract_exchange_rates(ti, **_):
    params = {"base": BASE_CURRENCY, "quotes": ",".join(TARGET_CURRENCIES)}
    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()
    raw_data = response.json()  # flat list: [{"date","base","quote","rate"}, ...]

    if not raw_data:
        raise ValueError("Frankfurter API returned no rates")

    print(f"Extracted {len(raw_data)} rates: {raw_data}")
    ti.xcom_push(key=XCOM_RAW_KEY, value=raw_data)


# ---------------- Task 2: Transform ----------------
def transform_exchange_rates(ti, **_):
    raw_data = ti.xcom_pull(task_ids="extract_exchange_rates", key=XCOM_RAW_KEY)
    retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    # v2 returns a flat list of rows. If the API ever returns the older
    # object shape {"base", "date", "rates": {...}}, convert it to rows.
    if isinstance(raw_data, dict):
        raw_data = [
            {"date": raw_data["date"], "base": raw_data["base"],
             "quote": quote, "rate": rate}
            for quote, rate in raw_data["rates"].items()
        ]

    transformed = []
    for row in raw_data:
        transformed.append(
            {
                "base_currency": row["base"].upper(),
                "target_currency": row["quote"].upper(),
                "exchange_rate": round(float(row["rate"]), 4),
                "rate_date": row["date"],
                "source": "Frankfurter API",
                "retrieved_at": retrieved_at,
            }
        )

    print(f"Transformed {len(transformed)} records:")
    for doc in transformed:
        print(doc)
    ti.xcom_push(key=XCOM_TRANSFORMED_KEY, value=transformed)


# ---------------- Task 3: Load ----------------
def load_into_mongodb(ti, **_):
    records = ti.xcom_pull(
        task_ids="transform_exchange_rates", key=XCOM_TRANSFORMED_KEY
    )

    client = MongoClient(MONGO_URI)
    try:
        collection = client[MONGO_DB][MONGO_COLLECTION]

        # Upsert on (base, target, date) so re-runs don't create duplicates
        operations = [
            UpdateOne(
                {
                    "base_currency": r["base_currency"],
                    "target_currency": r["target_currency"],
                    "rate_date": r["rate_date"],
                },
                {"$set": r},
                upsert=True,
            )
            for r in records
        ]
        result = collection.bulk_write(operations)
        print(
            f"MongoDB load complete -> inserted: {result.upserted_count}, "
            f"updated: {result.modified_count}, "
            f"total in collection: {collection.count_documents({})}"
        )
    finally:
        client.close()


# ---------------- DAG definition ----------------
default_args = {
    "owner": "rafay",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="currency_exchange_etl",
    description="Daily EUR exchange rates: Frankfurter API -> MongoDB",
    default_args=default_args,
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "currency", "mongodb"],
) as dag:

    extract = PythonOperator(
        task_id="extract_exchange_rates",
        python_callable=extract_exchange_rates,
    )

    transform = PythonOperator(
        task_id="transform_exchange_rates",
        python_callable=transform_exchange_rates,
    )

    load = PythonOperator(
        task_id="load_into_mongodb",
        python_callable=load_into_mongodb,
    )

    extract >> transform >> load
