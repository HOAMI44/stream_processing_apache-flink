CREATE TABLE IF NOT EXISTS uc01_temperature_stats (
  station text NOT NULL,
  window_size text NOT NULL,
  window_start timestamp NOT NULL,
  window_end timestamp NOT NULL,
  reading_count bigint NOT NULL,
  min_temp_c double precision,
  max_temp_c double precision,
  avg_temp_c double precision,
  PRIMARY KEY (station, window_size, window_start)
);

CREATE TABLE IF NOT EXISTS uc02_data_quality (
  station text NOT NULL,
  window_start timestamp NOT NULL,
  window_end timestamp NOT NULL,
  total_count bigint NOT NULL,
  bad_count bigint NOT NULL,
  bad_ratio double precision NOT NULL,
  PRIMARY KEY (station, window_start)
);

CREATE TABLE IF NOT EXISTS uc03_temperature_change (
  station text NOT NULL,
  event_time timestamp NOT NULL,
  previous_time timestamp NOT NULL,
  temp_c double precision NOT NULL,
  previous_temp_c double precision NOT NULL,
  change_c_per_hour double precision NOT NULL,
  PRIMARY KEY (station, event_time)
);

CREATE TABLE IF NOT EXISTS uc04_data_gaps (
  station text NOT NULL,
  interval_start timestamp NOT NULL,
  interval_end timestamp NOT NULL,
  gap_hours bigint NOT NULL,
  status text NOT NULL,
  PRIMARY KEY (station, interval_end)
);

CREATE TABLE IF NOT EXISTS uc05_temperature_deviation (
  station text NOT NULL,
  window_start timestamp NOT NULL,
  window_end timestamp NOT NULL,
  rolling_avg_temp_c double precision NOT NULL,
  historical_avg_temp_c double precision NOT NULL,
  deviation_c double precision NOT NULL,
  PRIMARY KEY (station, window_start)
);

CREATE TABLE IF NOT EXISTS uc06_temperature_histogram (
  region text NOT NULL,
  window_start timestamp NOT NULL,
  window_end timestamp NOT NULL,
  bucket text NOT NULL,
  reading_count bigint NOT NULL,
  PRIMARY KEY (region, window_start, bucket)
);

CREATE TABLE IF NOT EXISTS uc07_climate_trend (
  station text NOT NULL,
  window_start timestamp NOT NULL,
  window_end timestamp NOT NULL,
  avg_temp_c double precision NOT NULL,
  reading_count bigint NOT NULL,
  PRIMARY KEY (station, window_start)
);

CREATE TABLE IF NOT EXISTS uc08_resort_recommendations (
  resort_id text NOT NULL,
  event_time timestamp NOT NULL,
  resort_name text NOT NULL,
  booking_count int NOT NULL,
  recommendation text NOT NULL,
  temp_c double precision,
  wind_mps double precision,
  visibility_m int,
  PRIMARY KEY (resort_id, event_time)
);

CREATE TABLE IF NOT EXISTS uc09_low_visibility (
  station text NOT NULL,
  period_start timestamp NOT NULL,
  period_end timestamp NOT NULL,
  duration_hours bigint NOT NULL,
  reading_count bigint NOT NULL,
  PRIMARY KEY (station, period_start)
);

CREATE TABLE IF NOT EXISTS uc10_fire_risk (
  region text NOT NULL,
  window_start timestamp NOT NULL,
  window_end timestamp NOT NULL,
  fire_index double precision NOT NULL,
  risk_level text NOT NULL,
  avg_temp_c double precision,
  avg_wind_mps double precision,
  avg_humidity_pct double precision,
  avg_dryness_c double precision,
  PRIMARY KEY (region, window_start)
);
