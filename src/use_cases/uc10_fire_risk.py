from common import jdbc_options, run

SINK = """
CREATE TABLE sink (
  region STRING,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  fire_index DOUBLE,
  risk_level STRING,
  avg_temp_c DOUBLE,
  avg_wind_mps DOUBLE,
  avg_humidity_pct DOUBLE,
  avg_dryness_c DOUBLE,
  PRIMARY KEY (region, window_start) NOT ENFORCED
""" + jdbc_options("uc10_fire_risk")

QUERY = """
WITH base AS (
  SELECT name, station, event_time, temperature.value_celsius AS temp_c, dew_point.value_celsius AS dew_c, wind.speed_rate AS wind_mps
  FROM weather_readings
  WHERE temperature.value_celsius IS NOT NULL AND dew_point.value_celsius IS NOT NULL AND wind.speed_rate IS NOT NULL
)
SELECT
  region,
  window_start,
  window_end,
  fire_index,
  CASE WHEN fire_index >= 80 THEN 'extreme'
       WHEN fire_index >= 55 THEN 'high'
       WHEN fire_index >= 30 THEN 'moderate'
       ELSE 'low'
  END,
  avg_temp_c,
  avg_wind_mps,
  avg_humidity_pct,
  avg_dryness_c
FROM (
  SELECT
    COALESCE(name, station) AS region,
    window_start,
    window_end,
    AVG(temp_c) AS avg_temp_c,
    AVG(wind_mps) AS avg_wind_mps,
    AVG(CASE
      WHEN 100.0 - ((temp_c - dew_c) * 5.0) < 0.0 THEN 0.0
      WHEN 100.0 - ((temp_c - dew_c) * 5.0) > 100.0 THEN 100.0
      ELSE 100.0 - ((temp_c - dew_c) * 5.0)
    END) AS avg_humidity_pct,
    AVG(temp_c - dew_c) AS avg_dryness_c,
    AVG(
      (temp_c * 1.5)
      + (wind_mps * 3.0)
      + ((temp_c - dew_c) * 4.0)
      + ((100.0 - CASE
        WHEN 100.0 - ((temp_c - dew_c) * 5.0) < 0.0 THEN 0.0
        WHEN 100.0 - ((temp_c - dew_c) * 5.0) > 100.0 THEN 100.0
        ELSE 100.0 - ((temp_c - dew_c) * 5.0)
      END) * 0.4)
    ) AS fire_index
  FROM TABLE(TUMBLE(TABLE base, DESCRIPTOR(event_time), INTERVAL '1' HOUR))
  GROUP BY COALESCE(name, station), window_start, window_end
)
"""
INSERT = "INSERT INTO sink\n" + QUERY


if __name__ == "__main__":
    run("uc10-fire-risk", SINK, INSERT)
