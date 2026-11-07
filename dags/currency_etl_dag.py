"""
ETL pipeline: fetch daily EUR exchange rates from the Frankfurter API,
transform into a normalized schema, and load into MongoDB.

Tasks communicate exclusively through named XComs:
  extract_rates   -> pushes "raw_rates" (key="raw_rates")
  transform_rates -> pulls "raw_rates", pushes "transformed_rates"
"""

from __future__ import annotations

from datetime import datetime

import requests
from airflow.sdk import dag, task

BASE_CURRENCY = "EUR"
TARGET_CURRENCIES = ["USD", "GBP", "JPY"]
FRANKFURTER_URL = "https://api.frankfurter.dev/v1/latest"

RAW_RATES_XCOM_KEY = "raw_rates"
TRANSFORMED_RATES_XCOM_KEY = "transformed_rates"


@dag(
    dag_id="currency_exchange_etl",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "currency", "mongodb"],
)
def currency_exchange_etl():

    @task(task_id="extract_rates")
    def extract_rates(**context) -> None:
        """Call the Frankfurter API and push the raw JSON response via named XCom."""
        params = {"base": BASE_CURRENCY, "symbols": ",".join(TARGET_CURRENCIES)}
        response = requests.get(FRANKFURTER_URL, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()

        context["ti"].xcom_push(key=RAW_RATES_XCOM_KEY, value=payload)

    @task(task_id="transform_rates")
    def transform_rates(**context) -> None:
        """Pull the raw payload, reshape each quote into the target schema, push the result."""
        ti = context["ti"]
        raw_payload = ti.xcom_pull(task_ids="extract_rates", key=RAW_RATES_XCOM_KEY)

        rate_date = raw_payload["date"]
        retrieved_at = datetime.utcnow().isoformat()

        transformed_records = []
        for target_currency, exchange_rate in raw_payload["rates"].items():
            transformed_records.append(
                {
                    "base_currency": raw_payload["base"],
                    "target_currency": target_currency,
                    "exchange_rate": exchange_rate,
                    "rate_date": rate_date,
                    "source": "Frankfurter API",
                    "retrieved_at": retrieved_at,
                }
            )

        ti.xcom_push(key=TRANSFORMED_RATES_XCOM_KEY, value=transformed_records)

    extract_rates() >> transform_rates()


currency_exchange_etl()
