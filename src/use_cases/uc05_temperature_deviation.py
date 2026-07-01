from common import jdbc_options, run

SINK = """
CREATE TABLE sink (
  station STRING,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  rolling_avg_temp_c DOUBLE,
  historical_avg_temp_c DOUBLE,
  deviation_c DOUBLE,
  PRIMARY KEY (station, window_start) NOT ENFORCED
""" + jdbc_options("uc05_temperature_deviation")

QUERY = """
SELECT
  station,
  window_start,
  window_end,
  rolling_avg_temp_c,
  historical_avg_temp_c,
  rolling_avg_temp_c - historical_avg_temp_c
FROM (
  SELECT
    station,
    window_start,
    window_end,
    AVG(temperature.value_celsius) AS rolling_avg_temp_c,
    CASE station
      WHEN '02907099999' THEN 3.0
      WHEN '02950099999' THEN 4.0
      WHEN '02960099999' THEN 4.5
      WHEN '02972099999' THEN 5.0
      WHEN '02981099999' THEN 5.0
      WHEN '22707099999' THEN 2.5
      ELSE 4.0
    END AS historical_avg_temp_c
  FROM TABLE(HOP(TABLE weather_readings, DESCRIPTOR(event_time), INTERVAL '1' HOUR, INTERVAL '24' HOUR))
  WHERE temperature.value_celsius IS NOT NULL
  GROUP BY station, window_start, window_end
)
"""
INSERT = "INSERT INTO sink\n" + QUERY


if __name__ == "__main__":
    run("uc05-temperature-deviation", SINK, INSERT)
