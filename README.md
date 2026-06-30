# NOAA CSV Stream Processing Pipeline

This project runs a small stream pipeline for local NOAA Global Hourly CSV files:

```text
data/<year>/<station>.csv -> producer -> noaa.raw -> PyFlink normalizer -> weather.readings
```

PostgreSQL and Grafana are included for later analytical jobs. No result tables, schemas, dashboards, or use-case Flink jobs are created yet.

## Start

```bash
docker compose up -d
```

UIs:

```text
Kafka UI: http://localhost:8080
Flink UI: http://localhost:8081
Grafana: http://localhost:3000
```

Grafana login is `admin` / `admin`. PostgreSQL is available on `localhost:5432` with database/user/password `weather`.

## Data

Put NOAA Global Hourly CSV files under:

```text
data/
  2023/
    <station>.csv
  2024/
    <station>.csv
```

The current sample data uses the same layout with older years.

## Run Producer

Install the local producer dependency:

```bash
python -m pip install -r requirements.txt
```

Publish raw CSV line events to Kafka:

```bash
python src/producer.py --data-dir data --delay-ms 50 --max-records 10000
```

Optional filters:

```bash
python src/producer.py --data-dir data --year 1901 --station 02907099999.csv --max-records 100
```

The producer skips CSV headers and publishes minimal metadata plus `raw_line` to `noaa.raw`.

## Run Normalizer

Submit the PyFlink job:

```bash
docker compose exec flink-jobmanager flink run -py /opt/project/src/normalizer_job.py
```

Inspect `noaa.raw`, `weather.readings`, offsets, and consumer groups in Kafka UI.
