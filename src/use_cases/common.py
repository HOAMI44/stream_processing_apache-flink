import os
import sys

from pyflink.table import EnvironmentSettings, TableEnvironment

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import DOCKER_BOOTSTRAP_SERVERS, READINGS_TOPIC

POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/weather")
POSTGRES_USER = os.getenv("POSTGRES_USER", "weather")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "weather")


def readings_ddl(group_id):
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", DOCKER_BOOTSTRAP_SERVERS)
    return f"""
CREATE TABLE weather_readings (
  station STRING,
  `date` STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  name STRING,
  quality_control STRING,
  wind ROW<speed_rate DOUBLE>,
  visibility ROW<distance_meters INT>,
  temperature ROW<value_celsius DOUBLE, is_valid BOOLEAN>,
  dew_point ROW<value_celsius DOUBLE, is_valid BOOLEAN>,
  sea_level_pressure ROW<value_hpa DOUBLE>,
  event_time AS TO_TIMESTAMP(REPLACE(`date`, 'T', ' ')),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' MINUTE
) WITH (
  'connector' = 'kafka',
  'topic' = '{READINGS_TOPIC}',
  'properties.bootstrap.servers' = '{bootstrap}',
  'properties.group.id' = '{group_id}',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
)
"""


def jdbc_options(table):
    return f"""
) WITH (
  'connector' = 'jdbc',
  'url' = '{POSTGRES_URL}',
  'table-name' = '{table}',
  'driver' = 'org.postgresql.Driver',
  'username' = '{POSTGRES_USER}',
  'password' = '{POSTGRES_PASSWORD}'
)
"""


def run(job_name, sink_ddl, insert_sql):
    env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    env.execute_sql(readings_ddl(job_name))
    env.execute_sql(sink_ddl)
    env.execute_sql(insert_sql)
