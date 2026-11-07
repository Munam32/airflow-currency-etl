"""
ETL pipeline: fetch daily EUR exchange rates from the Frankfurter API,
transform into a normalized schema, and load into MongoDB.

Tasks communicate exclusively through named XComs:
  extract_rates   -> pushes "raw_rates" (key="raw_rates")
"""

from __future__ import annotations

from datetime import datetime

import requests
from airflow.sdk import dag, task

BASE_CURRENCY = "EUR"
TARGET_CURRENCIES = ["USD", "GBP", "JPY"]
FRANKFURTER_URL = "https://api.frankfurter.dev/v1/latest"

RAW_RATES_XCOM_KEY = "raw_rates"


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

    extract_rates()


currency_exchange_etl()
