#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "$script_dir/compose.yaml" ]]; then
  cd "$script_dir"
else
  cd "$script_dir/.."
fi

DATA_DIR="${DATA_DIR:-data}"
DELAY_MS="${DELAY_MS:-0}"
MAX_RECORDS="${MAX_RECORDS:-}"
GRAFANA_URL="${GRAFANA_URL:-http://localhost:3000}"

jobs=(
  normalizer_job.py
  use_cases/uc01_temperature_stats.py
  use_cases/uc02_data_quality.py
  use_cases/uc03_temperature_change.py
  use_cases/uc04_data_gaps.py
  use_cases/uc05_temperature_deviation.py
  use_cases/uc06_temperature_histogram.py
  use_cases/uc07_climate_trend.py
  use_cases/uc08_resort_recommendations.py
  use_cases/uc09_low_visibility.py
  use_cases/uc10_fire_risk.py
)

wait_for() {
  local name="$1"
  shift
  printf 'Waiting for %s' "$name"
  until "$@" >/dev/null 2>&1; do
    printf '.'
    sleep 2
  done
  printf '\n'
}

flink_run() {
  docker compose exec -T flink-jobmanager flink run -d -py "/opt/project/src/$1"
}

docker compose up -d --build
docker compose restart flink-jobmanager flink-taskmanager >/dev/null

wait_for kafka docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list
wait_for postgres docker compose exec -T postgres pg_isready -U weather -d weather
wait_for flink curl -fsS http://localhost:8081/overview
wait_for grafana curl -fsS "$GRAFANA_URL/api/health"

docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic noaa.raw >/dev/null
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic weather.readings >/dev/null

for job in "${jobs[@]}"; do
  echo "Submitting $job"
  flink_run "$job" >/dev/null
done

producer_args=(--data-dir "$DATA_DIR" --bootstrap-servers localhost:9092 --delay-ms "$DELAY_MS")
if [[ -n "$MAX_RECORDS" ]]; then
  producer_args+=(--max-records "$MAX_RECORDS")
fi

echo "Publishing data from $DATA_DIR"
python src/producer.py "${producer_args[@]}"

cat <<EOF

Done.
Grafana:  $GRAFANA_URL/d/weather-use-cases/weather-use-cases
Flink:    http://localhost:8081
Kafka UI: http://localhost:8080

Optional: MAX_RECORDS=1000 DELAY_MS=10 ./run_all.sh
EOF
