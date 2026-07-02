CREATE TABLE IF NOT EXISTS uc01_temperature_stats (
  station text NOT NULL,
  window_size text NOT NULL,
  window_start timestamp NOT NULL,
  window_end timestamp NOT NULL,
  min_temp_c double precision,
  max_temp_c double precision,
  avg_temp_c double precision,
  PRIMARY KEY (station, window_size, window_start)
);

CREATE TABLE IF NOT EXISTS uc02_data_quality (
  station text NOT NULL,
  window_start timestamp NOT NULL,
  window_end timestamp NOT NULL,
  bad_count bigint NOT NULL,
  PRIMARY KEY (station, window_start)
);

CREATE TABLE IF NOT EXISTS uc03_temperature_change (
  station text NOT NULL,
  event_time timestamp NOT NULL,
  change_c_per_hour double precision NOT NULL,
  PRIMARY KEY (station, event_time)
);

CREATE TABLE IF NOT EXISTS uc04_data_gaps (
  station text NOT NULL,
  interval_start timestamp NOT NULL,
  interval_end timestamp NOT NULL,
  status text NOT NULL,
  PRIMARY KEY (station, interval_end)
);

CREATE TABLE IF NOT EXISTS uc05_temperature_deviation (
  station text NOT NULL,
  window_start timestamp NOT NULL,
  window_end timestamp NOT NULL,
  deviation_c double precision NOT NULL,
  PRIMARY KEY (station, window_start)
);

CREATE TABLE IF NOT EXISTS uc06_temperature_rankings (
  station text NOT NULL,
  window_start timestamp NOT NULL,
  window_end timestamp NOT NULL,
  list_name text NOT NULL,
  rank_position bigint NOT NULL,
  temp_c double precision NOT NULL,
  PRIMARY KEY (window_start, list_name, rank_position)
);

CREATE TABLE IF NOT EXISTS uc07_climate_trend (
  station text NOT NULL,
  window_start timestamp NOT NULL,
  window_end timestamp NOT NULL,
  avg_temp_c double precision NOT NULL,
  trend_direction text NOT NULL,
  PRIMARY KEY (station, window_start)
);

CREATE TABLE IF NOT EXISTS uc08_user_notifications (
  user_id text NOT NULL,
  booking_id text NOT NULL,
  resort_id text NOT NULL,
  event_time timestamp NOT NULL,
  alert text NOT NULL,
  wind_mps double precision,
  visibility_m int,
  PRIMARY KEY (user_id, booking_id, event_time)
);

CREATE TABLE IF NOT EXISTS uc09_low_visibility (
  station text NOT NULL,
  period_start timestamp NOT NULL,
  period_end timestamp NOT NULL,
  duration_hours bigint NOT NULL,
  PRIMARY KEY (station, period_start)
);

CREATE TABLE IF NOT EXISTS uc10_storm_warning (
  station text NOT NULL,
  window_start timestamp NOT NULL,
  window_end timestamp NOT NULL,
  pressure_drop_hpa_per_hour double precision NOT NULL,
  max_wind_kmh double precision NOT NULL,
  alert text NOT NULL,
  PRIMARY KEY (station, window_start)
);
