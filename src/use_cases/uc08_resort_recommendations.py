from common import jdbc_options, run

SINK = """
CREATE TABLE sink (
  resort_id STRING,
  event_time TIMESTAMP(3),
  resort_name STRING,
  booking_count INT,
  recommendation STRING,
  temp_c DOUBLE,
  wind_mps DOUBLE,
  visibility_m INT,
  PRIMARY KEY (resort_id, event_time) NOT ENFORCED
""" + jdbc_options("uc08_resort_recommendations")

QUERY = """
WITH bookings(resort_id, resort_name, valid_from, valid_to, booking_count) AS (
  VALUES
    ('02907099999', 'Ulkokalla Outdoor Center', TIMESTAMP '1901-01-01 00:00:00', TIMESTAMP '1906-01-01 00:00:00', 42),
    ('02950099999', 'Raahe Ski Club', TIMESTAMP '1901-01-01 00:00:00', TIMESTAMP '1906-01-01 00:00:00', 31),
    ('22707099999', 'Northern Forest Trails', TIMESTAMP '1901-01-01 00:00:00', TIMESTAMP '1906-01-01 00:00:00', 18)
)
SELECT
  b.resort_id,
  w.event_time,
  b.resort_name,
  b.booking_count,
  CASE WHEN w.temp_c BETWEEN -15 AND 5 AND w.wind_mps <= 12 AND w.visibility_m >= 1000 THEN 'go'
       WHEN w.wind_mps > 18 OR w.visibility_m < 500 THEN 'warn'
       ELSE 'hold'
  END,
  w.temp_c,
  w.wind_mps,
  w.visibility_m
FROM (
  SELECT station, event_time, temperature.value_celsius AS temp_c, wind.speed_rate AS wind_mps, visibility.distance_meters AS visibility_m
  FROM weather_readings
  WHERE temperature.value_celsius IS NOT NULL AND wind.speed_rate IS NOT NULL AND visibility.distance_meters IS NOT NULL
) w
JOIN bookings b
  ON w.station = b.resort_id
 AND w.event_time BETWEEN b.valid_from AND b.valid_to
"""
INSERT = "INSERT INTO sink\n" + QUERY


if __name__ == "__main__":
    run("uc08-resort-recommendations", SINK, INSERT)
