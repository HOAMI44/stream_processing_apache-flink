from pyflink.table import EnvironmentSettings, TableEnvironment


settings = EnvironmentSettings.in_batch_mode()
t_env = TableEnvironment.create(settings)

t_env.execute_sql("""
CREATE TEMPORARY TABLE weather (
    station_id STRING,
    event_time STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    temperature DOUBLE,
    wind_speed DOUBLE
)
WITH (
    'connector' = 'filesystem',
    'path' = 'data/output/weather_clean.csv',
    'format' = 'csv',
    'csv.ignore-parse-errors' = 'true'
)
""")
#--Filter Parameter
TEMP_MIN = -20
TEMP_MAX = 40

WIND_THRESHOLD = 5

MIN_LAT = -90
MAX_LAT = 90

MIN_LON = -180
MAX_LON = 180

query = f"""
SELECT
    station_id,
    COUNT(*) AS cnt,
    MIN(temperature) AS min_temp,
    MAX(temperature) AS max_temp,
    AVG(temperature) AS avg_temp,
    MAX(wind_speed) AS max_wind
FROM weather
WHERE
    temperature BETWEEN {TEMP_MIN} AND {TEMP_MAX}
    AND wind_speed >= {WIND_THRESHOLD}
    AND latitude BETWEEN {MIN_LAT} AND {MAX_LAT}
    AND longitude BETWEEN {MIN_LON} AND {MAX_LON}
GROUP BY station_id
"""

result = t_env.sql_query(query)

result.execute().print()