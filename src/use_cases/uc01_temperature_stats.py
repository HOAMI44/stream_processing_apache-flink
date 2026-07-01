from common import jdbc_options, run

SINK = """
CREATE TABLE sink (
  station STRING,
  window_size STRING,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  reading_count BIGINT,
  min_temp_c DOUBLE,
  max_temp_c DOUBLE,
  avg_temp_c DOUBLE,
  PRIMARY KEY (station, window_size, window_start) NOT ENFORCED
""" + jdbc_options("uc01_temperature_stats")

QUERY = """
WITH base AS (
  SELECT station, event_time, temperature.value_celsius AS temp_c
  FROM weather_readings
  WHERE temperature.value_celsius IS NOT NULL
)
SELECT station, '1h', window_start, window_end, COUNT(*), MIN(temp_c), MAX(temp_c), AVG(temp_c)
FROM TABLE(TUMBLE(TABLE base, DESCRIPTOR(event_time), INTERVAL '1' HOUR))
GROUP BY station, window_start, window_end
UNION ALL
SELECT station, '24h', window_start, window_end, COUNT(*), MIN(temp_c), MAX(temp_c), AVG(temp_c)
FROM TABLE(TUMBLE(TABLE base, DESCRIPTOR(event_time), INTERVAL '24' HOUR))
GROUP BY station, window_start, window_end
"""
INSERT = "INSERT INTO sink\n" + QUERY


if __name__ == "__main__":
    run("uc01-temperature-stats", SINK, INSERT)
