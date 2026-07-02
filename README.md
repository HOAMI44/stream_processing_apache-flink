**Amir, Samir, Fabian**

# NOAA CSV Stream Processing Pipeline

This project runs a small stream pipeline for local NOAA Global Hourly CSV files:

```text
data/<year>/<station>.csv -> producer -> noaa.raw -> PyFlink normalizer -> weather.readings
```

PostgreSQL result tables for the use-case jobs are initialized from `init.sql`; Grafana can read those tables directly. The implemented stream patterns and jobs are described in [use_cases.md](use_cases.md).

## Start

Start the full demo pipeline and Grafana dashboard:

```bash
./run_all.sh
```

`run_all.sh` installs the Python producer requirements, starts Docker services, submits the normalizer and all use-case jobs, then publishes the showcase dataset from `data/1901/showcase.csv`.

The showcase producer sends one row per second by default so the Grafana panels fill progressively during the demo. Override it with `DELAY_MS=0 ./run_all.sh` for an instant publish.

Or just the compose stack:

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

The Compose setup uses Docker's `host.docker.internal:host-gateway` alias for service-to-service demo traffic. This avoids host-specific bridge subnet collisions while still exposing the UIs and producer endpoints on `localhost`.

## Data

Put NOAA Global Hourly CSV files under:

```text
data/
  1901/
    <station>.csv
  1902/
    <station>.csv
```

For the Grafana demo, use `data/1901/showcase.csv`; it is tailored to close the event-time windows and populate every use-case panel.

## Run Producer

Install the local producer dependency when running the producer manually:

```bash
python -m pip install -r requirements.txt
```

Publish raw CSV line events to Kafka:

```bash
python src/producer.py --data-dir data --year 1901 --station showcase.csv --delay-ms 50
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

## Test Use Cases

```bash
docker run --rm -v "$PWD":/opt/project -w /opt/project pyflink-kafka:1.20.1 python3 -m unittest src/tests.py
```

## Run Use-Case Jobs

Each use case from `use_cases.md` has one PyFlink job in `src/use_cases/` and writes a Grafana-ready table in PostgreSQL:

```bash
PY_FILES=/opt/project/src/use_cases/common.py,/opt/project/src/config.py
docker compose exec flink-jobmanager flink run --pyFiles "$PY_FILES" -py /opt/project/src/use_cases/uc01_temperature_stats.py
docker compose exec flink-jobmanager flink run --pyFiles "$PY_FILES" -py /opt/project/src/use_cases/uc02_data_quality.py
docker compose exec flink-jobmanager flink run --pyFiles "$PY_FILES" -py /opt/project/src/use_cases/uc03_temperature_change.py
docker compose exec flink-jobmanager flink run --pyFiles "$PY_FILES" -py /opt/project/src/use_cases/uc04_data_gaps.py
docker compose exec flink-jobmanager flink run --pyFiles "$PY_FILES" -py /opt/project/src/use_cases/uc05_temperature_deviation.py
docker compose exec flink-jobmanager flink run --pyFiles "$PY_FILES" -py /opt/project/src/use_cases/uc06_temperature_rankings.py
docker compose exec flink-jobmanager flink run --pyFiles "$PY_FILES" -py /opt/project/src/use_cases/uc07_climate_trend.py
docker compose exec flink-jobmanager flink run --pyFiles "$PY_FILES" -py /opt/project/src/use_cases/uc08_user_notifications.py
docker compose exec flink-jobmanager flink run --pyFiles "$PY_FILES" -py /opt/project/src/use_cases/uc09_low_visibility.py
docker compose exec flink-jobmanager flink run --pyFiles "$PY_FILES" -py /opt/project/src/use_cases/uc10_storm_warning.py
```

If the `postgres` volume already existed before these tables were added, recreate it once:

```bash
docker compose down -v
docker compose up -d --build
```
