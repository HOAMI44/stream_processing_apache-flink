import json
import os
import sys

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)

sys.path.append(os.path.dirname(__file__))
from config import DOCKER_BOOTSTRAP_SERVERS, RAW_TOPIC, READINGS_TOPIC
from noaa_csv_normalizer import normalize_raw_event


def normalize_json(value):
    try:
        return json.dumps(normalize_raw_event(json.loads(value)), separators=(",", ":"))
    except Exception as exc:
        return json.dumps(
            {"normalization_error": str(exc), "raw_message": value},
            separators=(",", ":"),
        )


def main():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", DOCKER_BOOTSTRAP_SERVERS)
    group_suffix = os.getenv("CONSUMER_GROUP_SUFFIX", "")
    group_id = "noaa-normalizer"
    if group_suffix:
        group_id = f"{group_id}-{group_suffix}"
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_python_file(
        os.path.join(os.path.dirname(__file__), "noaa_csv_normalizer.py")
    )

    source = (
        KafkaSource.builder()
        .set_topics(RAW_TOPIC)
        .set_group_id(group_id)
        .set_bootstrap_servers(bootstrap)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(bootstrap)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(READINGS_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "noaa.raw")
        .map(normalize_json, output_type=Types.STRING())
        .sink_to(sink)
    )
    env.execute("noaa-normalizer")


if __name__ == "__main__":
    main()
