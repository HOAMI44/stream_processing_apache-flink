import os
import sys
from datetime import datetime, timezone

from pyflink.table import (
    DataTypes,
    EnvironmentSettings,
    Schema,
    StreamTableEnvironment,
    TableDescriptor,
    TableEnvironment,
)
from pyflink.datastream import StreamExecutionEnvironment

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import DOCKER_BOOTSTRAP_SERVERS, READINGS_TOPIC

POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/weather")
POSTGRES_USER = os.getenv("POSTGRES_USER", "weather")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "weather")
SINK_NAME = "sink"


def utc_millis_to_text(timestamp_ms):
    return (
        datetime.fromtimestamp(timestamp_ms / 1000, timezone.utc)
        .replace(tzinfo=None)
        .strftime("%Y-%m-%d %H:%M:%S")
    )


def datetime_to_utc_millis(value):
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.astimezone(timezone.utc).timestamp() * 1000)


def iso_to_utc_millis(value):
    return datetime_to_utc_millis(datetime.fromisoformat(value))


def readings_schema():
    return (
        Schema.new_builder()
        .column("station", DataTypes.STRING())
        .column("date", DataTypes.STRING())
        .column("source", DataTypes.STRING())
        .column("latitude", DataTypes.DOUBLE())
        .column("longitude", DataTypes.DOUBLE())
        .column("elevation", DataTypes.DOUBLE())
        .column("name", DataTypes.STRING())
        .column("report_type", DataTypes.STRING())
        .column("call_sign", DataTypes.STRING())
        .column("quality_control", DataTypes.STRING())
        .column(
            "wind",
            DataTypes.ROW(
                [
                    DataTypes.FIELD("direction_angle", DataTypes.INT()),
                    DataTypes.FIELD("direction_quality", DataTypes.STRING()),
                    DataTypes.FIELD("type", DataTypes.STRING()),
                    DataTypes.FIELD("speed_rate", DataTypes.DOUBLE()),
                    DataTypes.FIELD("speed_quality", DataTypes.STRING()),
                    DataTypes.FIELD("speed_is_valid", DataTypes.BOOLEAN()),
                ]
            ),
        )
        .column(
            "ceiling",
            DataTypes.ROW(
                [
                    DataTypes.FIELD("height_meters", DataTypes.INT()),
                    DataTypes.FIELD("quality", DataTypes.STRING()),
                    DataTypes.FIELD("determination", DataTypes.STRING()),
                    DataTypes.FIELD("cavok", DataTypes.STRING()),
                    DataTypes.FIELD("is_valid", DataTypes.BOOLEAN()),
                ]
            ),
        )
        .column(
            "visibility",
            DataTypes.ROW(
                [
                    DataTypes.FIELD("distance_meters", DataTypes.INT()),
                    DataTypes.FIELD("quality", DataTypes.STRING()),
                    DataTypes.FIELD("variability", DataTypes.STRING()),
                    DataTypes.FIELD("variability_quality", DataTypes.STRING()),
                    DataTypes.FIELD("is_valid", DataTypes.BOOLEAN()),
                ]
            ),
        )
        .column(
            "temperature",
            DataTypes.ROW(
                [
                    DataTypes.FIELD("value_celsius", DataTypes.DOUBLE()),
                    DataTypes.FIELD("quality", DataTypes.STRING()),
                    DataTypes.FIELD("is_valid", DataTypes.BOOLEAN()),
                ]
            ),
        )
        .column(
            "dew_point",
            DataTypes.ROW(
                [
                    DataTypes.FIELD("value_celsius", DataTypes.DOUBLE()),
                    DataTypes.FIELD("quality", DataTypes.STRING()),
                    DataTypes.FIELD("is_valid", DataTypes.BOOLEAN()),
                ]
            ),
        )
        .column(
            "sea_level_pressure",
            DataTypes.ROW(
                [
                    DataTypes.FIELD("value_hpa", DataTypes.DOUBLE()),
                    DataTypes.FIELD("quality", DataTypes.STRING()),
                    DataTypes.FIELD("is_valid", DataTypes.BOOLEAN()),
                ]
            ),
        )
        .column_by_expression("event_time", "TO_TIMESTAMP(REPLACE(`date`, 'T', ' '))")
        .watermark("event_time", "event_time - INTERVAL '5' MINUTE")
        .build()
    )


def register_readings(env, group_id):
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", DOCKER_BOOTSTRAP_SERVERS)
    group_suffix = os.getenv("CONSUMER_GROUP_SUFFIX", "")
    group_id = f"{group_id}-{group_suffix}" if group_suffix else group_id
    env.create_temporary_table(
        "weather_readings",
        TableDescriptor.for_connector("kafka")
        .schema(readings_schema())
        .option("topic", READINGS_TOPIC)
        .option("properties.bootstrap.servers", bootstrap)
        .option("properties.group.id", group_id)
        .option("scan.startup.mode", "earliest-offset")
        .option("format", "json")
        .option("json.ignore-parse-errors", "true")
        .build(),
    )


def jdbc_sink(table, schema):
    return (
        TableDescriptor.for_connector("jdbc")
        .schema(schema)
        .option("url", POSTGRES_URL)
        .option("table-name", table)
        .option("driver", "org.postgresql.Driver")
        .option("username", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .build()
    )


def schema(columns, primary_key=None):
    builder = Schema.new_builder()
    primary_key = primary_key or []
    for name, data_type in columns:
        if name in primary_key:
            data_type = data_type.not_null()
        builder.column(name, data_type)
    if primary_key:
        builder.primary_key(*primary_key)
    return builder.build()


def run(job_name, sink_descriptor, query):
    env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    register_readings(env, job_name)
    env.create_temporary_table(SINK_NAME, sink_descriptor)
    query(env).execute_insert(SINK_NAME)


def run_stream(job_name, sink_descriptor, query):
    stream_env = StreamExecutionEnvironment.get_execution_environment()
    env = StreamTableEnvironment.create(stream_execution_environment=stream_env)
    register_readings(env, job_name)
    env.create_temporary_table(SINK_NAME, sink_descriptor)
    query(env).execute_insert(SINK_NAME)
