# Airflow Currency Exchange ETL

Astro CLI (Apache Airflow) pipeline that fetches daily EUR exchange rates from the
[Frankfurter API](https://frankfurter.dev/), transforms them, and loads them into MongoDB.

## Architecture

- **Extract** (`extract_rates`): calls `https://api.frankfurter.dev/v1/latest` for EUR -> USD, GBP, JPY.
- **Transform** (`transform_rates`): reshapes the raw payload into one document per currency pair.
- **Load** (`load_to_mongo`): inserts the transformed documents into MongoDB.

Tasks pass data exclusively through **named XComs** (`raw_rates`, `transformed_rates`) — no task re-fetches
or recomputes data already produced by a previous task.

## Services (docker-compose.override.yml)

| Service | Purpose | Port |
|---|---|---|
| `mongodb` | Primary database | 27017 |
| `mongo-express` | Web-based MongoDB admin UI | 8081 |

Both services join the same Compose network Astro creates for Airflow, so the DAG reaches Mongo at
host `mongodb`. MongoDB data persists in the named volume `mongodb_data`.

## Running locally

```bash
astro dev start
```

- Airflow UI: http://localhost:8080 (admin/admin)
- Mongo Express UI: http://localhost:8081 (admin/admin_password)

Unpause and trigger the `currency_exchange_etl` DAG from the Airflow UI, then verify documents in
Mongo Express under database `currency_db`, collection `exchange_rates`.

```bash
astro dev stop
```

## Transformed document schema

```json
{
  "base_currency": "EUR",
  "target_currency": "USD",
  "exchange_rate": 1.15,
  "rate_date": "2026-11-04",
  "source": "Frankfurter API",
  "retrieved_at": "2026-11-05T07:15:00"
}
```
