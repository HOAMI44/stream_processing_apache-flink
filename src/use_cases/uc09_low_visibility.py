from common import jdbc_options, run

VISIBILITY_THRESHOLD_M = 200

SINK = """
CREATE TABLE sink (
  station STRING,
  period_start TIMESTAMP(3),
  period_end TIMESTAMP(3),
  duration_hours BIGINT,
  reading_count BIGINT,
  PRIMARY KEY (station, period_start) NOT ENFORCED
""" + jdbc_options("uc09_low_visibility")

QUERY = f"""
WITH base AS (
  SELECT
    station,
    event_time,
    visibility.distance_meters < {VISIBILITY_THRESHOLD_M} AS is_low
  FROM weather_readings
  WHERE visibility.distance_meters IS NOT NULL
),
edges AS (
  SELECT
    station,
    event_time,
    is_low,
    LAG(is_low) OVER (PARTITION BY station ORDER BY event_time) AS was_low
  FROM base
),
grouped AS (
  SELECT
    station,
    event_time,
    is_low,
    SUM(CASE WHEN is_low AND COALESCE(was_low, FALSE) = FALSE THEN 1 ELSE 0 END)
      OVER (PARTITION BY station ORDER BY event_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS low_group
  FROM edges
)
SELECT
  station,
  MIN(CASE WHEN is_low THEN event_time END),
  COALESCE(MIN(CASE WHEN is_low = FALSE THEN event_time END), MAX(CASE WHEN is_low THEN event_time END)),
  TIMESTAMPDIFF(HOUR, MIN(CASE WHEN is_low THEN event_time END), COALESCE(MIN(CASE WHEN is_low = FALSE THEN event_time END), MAX(CASE WHEN is_low THEN event_time END))),
  SUM(CASE WHEN is_low THEN 1 ELSE 0 END)
FROM grouped
WHERE low_group > 0
GROUP BY station, low_group
HAVING SUM(CASE WHEN is_low THEN 1 ELSE 0 END) > 0
"""
INSERT = "INSERT INTO sink\n" + QUERY


if __name__ == "__main__":
    run("uc09-low-visibility", SINK, INSERT)
