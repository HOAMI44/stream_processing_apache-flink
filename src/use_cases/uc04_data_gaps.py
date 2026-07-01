from common import jdbc_options, run

SINK = """
CREATE TABLE sink (
  station STRING,
  interval_start TIMESTAMP(3),
  interval_end TIMESTAMP(3),
  gap_hours BIGINT,
  status STRING,
  PRIMARY KEY (station, interval_end) NOT ENFORCED
""" + jdbc_options("uc04_data_gaps")

QUERY = """
SELECT
  station,
  previous_time,
  event_time,
  TIMESTAMPDIFF(HOUR, previous_time, event_time) AS gap_hours,
  CASE WHEN TIMESTAMPDIFF(HOUR, previous_time, event_time) <= 1 THEN 'complete' ELSE 'incomplete' END AS status
FROM (
  SELECT
    station,
    event_time,
    LAG(event_time) OVER (PARTITION BY station ORDER BY event_time) AS previous_time
  FROM weather_readings
)
WHERE previous_time IS NOT NULL
"""
INSERT = "INSERT INTO sink\n" + QUERY


if __name__ == "__main__":
    run("uc04-data-gaps", SINK, INSERT)
