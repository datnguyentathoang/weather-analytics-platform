# Weather Analytics Platform

A local weather data platform for batch ingestion, data quality processing, PostgreSQL analytics, anomaly detection, and short-horizon temperature forecasting. The repository also contains a Kafka and Spark Structured Streaming prototype for realtime alerting.

The project is currently optimized for local Windows development. Docker Compose provisions supporting services, while the batch scripts still use local Windows data-lake paths.

## Architecture

The implemented batch path is:

```text
Historical JSON -> Raw CSV -> Curated Parquet -> PostgreSQL staging
	-> dbt staging -> fact_observation / dim_station
	-> Prophet forecast and Metabase analytics
```

The separate realtime prototype is:

```text
Kafka producer -> weather_realtime topic -> streaming alert simulation -> console alert
```

Key boundaries in the current implementation:

- `crawl_data.py` generates an Open-Meteo-compatible offline dataset. It does not make an API request.
- Batch processing writes to `D:\weather_datalake`, not to MinIO.
- `streaming_alert.py` simulates a streaming consumer and does not currently read Kafka messages or persist alerts.
- The Airflow DAG runs crawl, batch ingestion, and batch cleaning only. It does not trigger PostgreSQL loading, dbt, forecasting, or Metabase refreshes.
- MinIO, Kafka, PostgreSQL, and Metabase are provisioned by Docker Compose, but the Python integration is only partial.

The repository includes the detailed Mermaid architecture in [workfolw.mmd](workfolw.mmd).

## Repository Layout

```text
airflow/dags/weather_dag.py          Airflow batch orchestration
analytics_ai/forecast_temperature.py Prophet forecast and fallback model
dbt_project/models/                  Active dbt staging and mart models
deployments/docker-compose.yml       Local MinIO, Kafka, PostgreSQL, Metabase
scripts/crawl_data.py                Offline historical JSON generator
scripts/ingest_batch.py              JSON to raw CSV ingestion
scripts/kafka_producer.py             Kafka realtime event producer
scripts/load_parquet_to_postgres.py   Curated Parquet to PostgreSQL loader
spark/jobs/batch_clean.py             Batch cleaning and Parquet writer
spark/jobs/streaming_alert.py         Realtime alert prototype
```

`dbt_project/model/` is a legacy duplicate tree. The configured dbt project uses `dbt_project/models/`.

## Prerequisites

- Windows with Python 3.10 or newer
- Docker Desktop with Compose support
- Java compatible with the installed PySpark version, when running the batch Spark job
- Python dependencies from [requirement.txt](requirement.txt)
- A PostgreSQL client or dbt CLI available in the active Python environment

The repository's current dependency file covers the warehouse and Parquet stack. The following components are additionally required when their scripts are used: `requests`, `python-dotenv`, `kafka-python`, `pyspark`, `matplotlib`, `prophet`, and `scikit-learn`.

## Quick Start

### 1. Create the Python environment

```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirement.txt
```

Install the optional script dependencies for the complete prototype:

```powershell
pip install requests python-dotenv kafka-python pyspark matplotlib prophet scikit-learn
```

### 2. Start local infrastructure

```powershell
docker compose -f deployments/docker-compose.yml up -d
```

Provisioned endpoints:

| Service | Address | Purpose |
| --- | --- | --- |
| MinIO API | `http://localhost:9000` | Object storage API |
| MinIO Console | `http://localhost:9001` | Object storage administration |
| Kafka | `localhost:9092` | Realtime event broker |
| Kafka UI | `http://localhost:8085` | Topic inspection |
| PostgreSQL | `localhost:5432` | Warehouse database |
| Metabase | `http://localhost:3000` | BI dashboard |

The Compose file initializes the `weather-raw` MinIO bucket. Current batch scripts do not upload to that bucket.

### 3. Configure environment variables

Create `deployments/.env`. Do not commit this file.

```dotenv
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=change-this-password
POSTGRES_DB=weather_db
PARQUET_DIR=D:\weather_datalake\weather-processed\curated_weather
KAFKA_BROKER=localhost:9092
KAFKA_TOPIC_NAME=weather_realtime
WEATHER_API_KEY=
```

The loader requires all PostgreSQL variables and `PARQUET_DIR`. The dbt profile has development defaults, but explicit environment variables are recommended.

## Run the Batch Pipeline

Run these commands from the repository root:

```powershell
python scripts/crawl_data.py
python scripts/ingest_batch.py
python spark/jobs/batch_clean.py
python scripts/load_parquet_to_postgres.py
dbt run --project-dir dbt_project
```

The resulting flow is:

1. Six city datasets are generated under `data_sample/`.
2. CSV files are written under `D:\weather_datalake\weather-raw\batch`.
3. Cleaned Snappy Parquet is written under `D:\weather_datalake\weather-processed\curated_weather`, partitioned by `obs_date` and `region`.
4. Parquet files replace the PostgreSQL table `public.staging_weather`.
5. dbt builds `stg_weather`, `fact_observation`, and `dim_station` as tables in the configured schema.

### Data model

`stg_weather` normalizes the source fields into a consistent weather observation contract:

| Field | Meaning |
| --- | --- |
| `station_key` | City or station identifier |
| `station_name` | Human-readable Vietnamese station name |
| `observation_timestamp` | Observation time |
| `temperature` | Air temperature |
| `humidity` | Relative humidity |
| `apparent_temperature` | Feels-like temperature |
| `precipitation` | Precipitation amount |
| `wind_speed` | Wind speed |
| `pressure` | Normalized pressure value |

`fact_observation` adds an `anomaly_flag` when `precipitation > 90.0` or `temperature > 38.0`. `dim_station` contains one row per station key and name.

## Forecasting

After dbt has built `fact_observation`, run:

```powershell
python analytics_ai/forecast_temperature.py
```

The script queries Hanoi observations, trains Prophet with daily and weekly seasonality, and forecasts the next 168 hours. It writes:

- `analytics_ai/forecast_results.csv`
- `analytics_ai/temperature_forecast.png`

If Prophet fails, the script falls back to a `RandomForestRegressor`. The fallback currently reports completion but does not write a forecast artifact.

## Realtime Prototype

Start the producer in one terminal:

```powershell
python scripts/kafka_producer.py
```

Start the alert prototype separately:

```powershell
python spark/jobs/streaming_alert.py
```

The producer publishes JSON events to `weather_realtime`. The alert script currently generates its own simulated micro-batches and prints alerts; it is not yet a Kafka-backed Spark consumer.

## Airflow

The DAG is defined in [airflow/dags/weather_dag.py](airflow/dags/weather_dag.py). Its current task chain is:

```text
crawl_api_data -> ingest_batch_to_datalake -> pyspark_batch_cleaning
```

It is configured as a manual, non-catchup DAG. The Bash commands contain fixed `D:/weather-analytics-platform` paths, so update them before running Airflow from another checkout or environment.

## Validation and Operations

Useful checks after a batch run:

```powershell
Get-ChildItem D:\weather_datalake\weather-raw\batch
Get-ChildItem D:\weather_datalake\weather-processed\curated_weather -Recurse -Filter *.parquet
dbt debug --project-dir dbt_project
dbt run --project-dir dbt_project
```

Stop local services when finished:

```powershell
docker compose -f deployments/docker-compose.yml down
```

Use `down -v` only when you intentionally want to remove the PostgreSQL and MinIO data volumes.

## Production Roadmap

- Replace local Windows paths with an object-storage abstraction and upload batch data to MinIO or S3.
- Add a real Spark Structured Streaming Kafka source and a durable alert sink.
- Move credentials and service endpoints fully into environment-based configuration.
- Extend the Airflow DAG through Parquet loading, dbt, and forecast publication.
- Add dbt schema tests and data-quality checks for timestamps, station keys, ranges, and duplicate observations.
- Make forecasting idempotent and persist model metadata, evaluation metrics, and fallback output.
- Add automated tests and CI for Python scripts, SQL models, and Mermaid syntax.

## Security Notes

- Never commit `.env` files or production credentials.
- Replace the development passwords in `docker-compose.yml` before exposing any service beyond localhost.
- Restrict MinIO, Kafka UI, PostgreSQL, and Metabase ports in shared or production environments.
- Validate and parameterize database connections before accepting untrusted configuration.
# weather-analytics-platform