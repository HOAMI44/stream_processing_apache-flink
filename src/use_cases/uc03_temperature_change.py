from common import jdbc_options, run

SINK = """
CREATE TABLE sink (
  station STRING,
  event_time TIMESTAMP(3),
  previous_time TIMESTAMP(3),
  temp_c DOUBLE,
  previous_temp_c DOUBLE,
  change_c_per_hour DOUBLE,
  PRIMARY KEY (station, event_time) NOT ENFORCED
""" + jdbc_options("uc03_temperature_change")

QUERY = """
SELECT
  station,
  CAST(event_time AS TIMESTAMP(3)) AS event_time,
  CAST(previous_time AS TIMESTAMP(3)) AS previous_time,
  temp_c,
  previous_temp_c,
  (temp_c - previous_temp_c) * 3600.0 / TIMESTAMPDIFF(SECOND, previous_time, event_time)
FROM (
  SELECT
    station,
    event_time,
    temperature.value_celsius AS temp_c,
    LAG(event_time) OVER (PARTITION BY station ORDER BY event_time) AS previous_time,
    LAG(temperature.value_celsius) OVER (PARTITION BY station ORDER BY event_time) AS previous_temp_c
  FROM weather_readings
  WHERE temperature.value_celsius IS NOT NULL
)
WHERE previous_time IS NOT NULL AND TIMESTAMPDIFF(SECOND, previous_time, event_time) > 0
"""
INSERT = "INSERT INTO sink\n" + QUERY


if __name__ == "__main__":
    run("uc03-temperature-change", SINK, INSERT)
