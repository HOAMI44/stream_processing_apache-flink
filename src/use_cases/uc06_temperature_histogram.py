from common import jdbc_options, run

SINK = """
CREATE TABLE sink (
  region STRING,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  bucket STRING,
  reading_count BIGINT,
  PRIMARY KEY (region, window_start, bucket) NOT ENFORCED
""" + jdbc_options("uc06_temperature_histogram")

QUERY = """
WITH base AS (
  SELECT
    COALESCE(name, station) AS region,
    event_time,
    CASE WHEN temperature.value_celsius < 0 THEN 'freezing'
         WHEN temperature.value_celsius < 10 THEN 'cold'
         WHEN temperature.value_celsius < 20 THEN 'mild'
         WHEN temperature.value_celsius < 30 THEN 'warm'
         ELSE 'hot'
    END AS bucket
  FROM weather_readings
  WHERE temperature.value_celsius IS NOT NULL
)
SELECT region, window_start, window_end, bucket, COUNT(*)
FROM TABLE(TUMBLE(TABLE base, DESCRIPTOR(event_time), INTERVAL '24' HOUR))
GROUP BY region, window_start, window_end, bucket
"""
INSERT = "INSERT INTO sink\n" + QUERY


if __name__ == "__main__":
    run("uc06-temperature-histogram", SINK, INSERT)
