from common import jdbc_options, run

SINK = """
CREATE TABLE sink (
  station STRING,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  total_count BIGINT,
  bad_count BIGINT,
  bad_ratio DOUBLE,
  PRIMARY KEY (station, window_start) NOT ENFORCED
""" + jdbc_options("uc02_data_quality")

BAD = """
temperature.value_celsius IS NULL
OR temperature.is_valid = FALSE
OR temperature.value_celsius < -80
OR temperature.value_celsius > 60
OR wind.speed_rate IS NULL
OR wind.speed_rate < 0
OR wind.speed_rate > 75
OR visibility.distance_meters IS NULL
OR visibility.distance_meters < 0
OR latitude IS NULL
OR longitude IS NULL
"""

QUERY = f"""
SELECT
  station,
  window_start,
  window_end,
  COUNT(*) AS total_count,
  SUM(CASE WHEN {BAD} THEN 1 ELSE 0 END) AS bad_count,
  CAST(SUM(CASE WHEN {BAD} THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS bad_ratio
FROM TABLE(HOP(TABLE weather_readings, DESCRIPTOR(event_time), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
GROUP BY station, window_start, window_end
"""
INSERT = "INSERT INTO sink\n" + QUERY


if __name__ == "__main__":
    run("uc02-data-quality", SINK, INSERT)
