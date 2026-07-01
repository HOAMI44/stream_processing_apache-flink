from common import jdbc_options, run

SINK = """
CREATE TABLE sink (
  station STRING,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  avg_temp_c DOUBLE,
  reading_count BIGINT,
  PRIMARY KEY (station, window_start) NOT ENFORCED
""" + jdbc_options("uc07_climate_trend")

QUERY = """
SELECT station, window_start, window_end, AVG(temperature.value_celsius), COUNT(*)
FROM TABLE(HOP(TABLE weather_readings, DESCRIPTOR(event_time), INTERVAL '1' DAY, INTERVAL '30' DAY))
WHERE temperature.value_celsius IS NOT NULL
GROUP BY station, window_start, window_end
"""
INSERT = "INSERT INTO sink\n" + QUERY


if __name__ == "__main__":
    run("uc07-climate-trend", SINK, INSERT)
