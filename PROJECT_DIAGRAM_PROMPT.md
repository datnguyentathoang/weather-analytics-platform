# Prompt for Another AI

Create a professional Mermaid architecture flowchart for the weather analytics platform described below. Return valid Mermaid syntax only in a `.mmd` file, using `flowchart TD`, clear subgraphs, concise labels, and distinct styles for sources, processing, storage, warehouse, analytics, orchestration, and provisioned infrastructure.

## Project behavior from source code

- `scripts/crawl_data.py` generates Open-Meteo-compatible hourly historical JSON for six Vietnamese cities and writes `data_sample/CITY_*_raw.json`. It is an offline data generator, even though the project concept refers to Open-Meteo.
- `scripts/ingest_batch.py` reads those JSON files, flattens them into Vietnamese weather columns, and writes CSV files to `D:\weather_datalake\weather-raw\batch`.
- `spark/jobs/batch_clean.py` reads the raw CSV files, fills missing temperature, humidity, and precipitation values, derives `obs_date` and `region`, and writes Snappy Parquet to `D:\weather_datalake\weather-processed\curated_weather`.
- `scripts/load_parquet_to_postgres.py` reads the curated Parquet files and replaces PostgreSQL table `public.staging_weather`.
- `dbt_project/models/staging/stg_weather.sql` normalizes both the current Vietnamese columns and legacy English column names. The active dbt models are under `dbt_project/models`, not the duplicate `dbt_project/model` tree.
- `fact_observation` creates an observation id and `anomaly_flag = 1` when precipitation is greater than 90 mm or temperature is greater than 38 C. `dim_station` contains distinct station keys and names.
- `analytics_ai/forecast_temperature.py` queries Hanoi rows from `fact_observation`, trains Prophet with daily and weekly seasonality, forecasts 168 hours, and writes `analytics_ai/forecast_results.csv` plus a forecast PNG. It has a Random Forest fallback if Prophet fails.
- `scripts/kafka_producer.py` publishes events to Kafka topic `weather_realtime`, with a local fallback stream and synthetic extreme-rain events.
- `spark/jobs/streaming_alert.py` is a simulation that emits micro-batch-style alerts to the console. It does not currently implement a real Kafka consumer or write streaming results to PostgreSQL.
- `airflow/dags/weather_dag.py` defines a manual, non-catchup DAG with the sequential tasks `crawl_api_data -> ingest_batch_to_datalake -> pyspark_batch_cleaning`. It does not currently trigger Parquet loading, dbt, forecasting, or Metabase.
- `deployments/docker-compose.yml` provisions MinIO, Kafka, Zookeeper, PostgreSQL, and Metabase. MinIO is provisioned with a `weather-raw` bucket, but the current Python batch scripts use local Windows paths instead of the MinIO API.

## Diagram requirements

1. Show six numbered layers: Sources, Batch ingestion and processing, Realtime path, Orchestration, Warehouse and transformation, and Analytics and BI.
2. Show the implemented batch edges in this order: JSON -> CSV -> cleaned Parquet -> PostgreSQL staging -> dbt staging -> fact and dimension marts -> Prophet and Metabase.
3. Show the separate realtime edges: producer -> Kafka topic -> streaming alert -> console alert. Use dashed edges where the code is simulated or only provisioned.
4. Show Airflow task links to the three tasks it actually runs.
5. Explicitly distinguish provisioned Docker infrastructure from components currently used by the scripts, especially MinIO.
6. Do not claim that Kafka streaming feeds PostgreSQL, that Airflow runs dbt or forecasting, or that MinIO stores the current batch files.
7. Keep labels readable and avoid unsupported Mermaid syntax, unescaped brackets, or overly long node text.

Use the attached reference image as a visual style reference for a left-to-right data-platform architecture, but treat the source code above as authoritative when the image and implementation differ.