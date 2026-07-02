#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "$script_dir/compose.yaml" ]]; then
  cd "$script_dir"
else
  cd "$script_dir/.."
fi

DATA_DIR="${DATA_DIR:-data}"
YEAR="${YEAR:-1901}"
STATION="${STATION:-showcase.csv}"
DELAY_MS="${DELAY_MS:-1000}"
MAX_RECORDS="${MAX_RECORDS:-}"
GRAFANA_URL="${GRAFANA_URL:-http://localhost:3000}"
RUN_ID="${RUN_ID:-$(date +%s)}"

jobs=(
  normalizer_job.py
  use_cases/uc01_temperature_stats.py
  use_cases/uc02_data_quality.py
  use_cases/uc03_temperature_change.py
  use_cases/uc04_data_gaps.py
  use_cases/uc05_temperature_deviation.py
  use_cases/uc06_temperature_rankings.py
  use_cases/uc07_climate_trend.py
  use_cases/uc08_user_notifications.py
  use_cases/uc09_low_visibility.py
  use_cases/uc10_storm_warning.py
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

wait_for_flink_slots() {
  printf 'Waiting for flink task slots'
  until python - <<'PY' >/dev/null 2>&1
import json
import urllib.request

overview = json.load(urllib.request.urlopen("http://localhost:8081/overview", timeout=2))
if overview.get("slots-total", 0) < 1:
    raise SystemExit(1)
PY
  do
    printf '.'
    sleep 2
  done
  printf '\n'
}

wait_for_showcase_outputs() {
  if [[ "$DATA_DIR/$YEAR/$STATION" != "data/1901/showcase.csv" || -n "$MAX_RECORDS" ]]; then
    return
  fi

  printf 'Waiting for showcase outputs'
  until docker compose exec -T postgres psql -U weather -d weather -At -c "
WITH checks AS (
  SELECT COUNT(*) >= 350 AS ok FROM uc01_temperature_stats
  UNION ALL SELECT COALESCE(MAX(bad_count), 0) >= 5 FROM uc02_data_quality
  UNION ALL SELECT COUNT(*) >= 30 FROM uc07_climate_trend
  UNION ALL SELECT COUNT(*) >= 3 FROM uc09_low_visibility
  UNION ALL SELECT COUNT(*) >= 12 FROM uc10_storm_warning
)
SELECT bool_and(ok) FROM checks;
" | grep -qx t; do
    printf '.'
    sleep 2
  done
  printf '\n'
}

kafka_topic() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 "$@"
}

flink_run() {
  docker compose exec -T -e CONSUMER_GROUP_SUFFIX="$RUN_ID" flink-jobmanager flink run -d \
    --pyFiles /opt/project/src/use_cases/common.py,/opt/project/src/config.py \
    -py "/opt/project/src/$1"
}

docker compose up -d --build
docker compose restart flink-jobmanager flink-taskmanager >/dev/null

wait_for kafka kafka_topic --list
wait_for postgres docker compose exec -T postgres pg_isready -U weather -d weather
wait_for flink curl -fsS http://localhost:8081/overview
wait_for grafana curl -fsS "$GRAFANA_URL/api/health"
wait_for_flink_slots

kafka_topic --delete --if-exists --topic noaa.raw >/dev/null
kafka_topic --delete --if-exists --topic weather.readings >/dev/null
sleep 2
kafka_topic --create --if-not-exists --topic noaa.raw >/dev/null
kafka_topic --create --if-not-exists --topic weather.readings >/dev/null
wait_for noaa.raw kafka_topic --describe --topic noaa.raw
wait_for weather.readings kafka_topic --describe --topic weather.readings

docker compose exec -T postgres psql -U weather -d weather -c "
TRUNCATE
  uc01_temperature_stats,
  uc02_data_quality,
  uc03_temperature_change,
  uc04_data_gaps,
  uc05_temperature_deviation,
  uc06_temperature_rankings,
  uc07_climate_trend,
  uc08_user_notifications,
  uc09_low_visibility,
  uc10_storm_warning;
" >/dev/null

python -m pip install -r requirements.txt

for job in "${jobs[@]}"; do
  echo "Submitting $job"
  flink_run "$job" >/dev/null
done

producer_args=(--data-dir "$DATA_DIR" --year "$YEAR" --station "$STATION" --bootstrap-servers localhost:9092 --delay-ms "$DELAY_MS")
if [[ -n "$MAX_RECORDS" ]]; then
  producer_args+=(--max-records "$MAX_RECORDS")
fi

echo "Publishing data from $DATA_DIR/$YEAR/$STATION"
python src/producer.py "${producer_args[@]}"
wait_for_showcase_outputs

cat <<EOF

Done.
Grafana:  $GRAFANA_URL/d/weather-use-cases/weather-use-cases
Flink:    http://localhost:8081
Kafka UI: http://localhost:8080

Optional: YEAR=1902 STATION=02907099999.csv MAX_RECORDS=1000 DELAY_MS=10 ./run_all.sh
EOF
