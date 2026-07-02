from pyflink.common import Row
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.table import DataTypes
from pyflink.table.expressions import call_sql, col

from common import jdbc_sink, run_stream, schema

SINK = jdbc_sink(
    "uc03_temperature_change",
    schema(
        [
            ("station", DataTypes.STRING()),
            ("event_time", DataTypes.TIMESTAMP(3)),
            ("change_c_per_hour", DataTypes.DOUBLE()),
        ],
        ["station", "event_time"],
    ),
)


class TemperatureChange(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        self.previous = runtime_context.get_state(
            ValueStateDescriptor("previous_temperature", Types.PICKLED_BYTE_ARRAY())
        )

    def process_element(self, value, _ctx):
        station, event_time, temp_c = value
        previous = self.previous.value()
        self.previous.update((event_time, temp_c))

        if previous is None:
            return

        previous_time, previous_temp_c = previous
        seconds = (event_time - previous_time).total_seconds()
        if seconds > 0:
            yield Row(
                station,
                event_time.strftime("%Y-%m-%d %H:%M:%S"),
                (temp_c - previous_temp_c) * 3600.0 / seconds,
            )


def query(env):
    readings = (
        env.from_path("weather_readings")
        .where(call_sql("temperature.value_celsius IS NOT NULL"))
        .select(
            col("station"),
            col("event_time"),
            call_sql("temperature.value_celsius").alias("temp_c"),
        )
    )

    changes = (
        env.to_data_stream(readings)
        .key_by(lambda row: row[0])
        .process(
            TemperatureChange(),
            output_type=Types.ROW_NAMED(
                ["station", "event_time_text", "change_c_per_hour"],
                [Types.STRING(), Types.STRING(), Types.DOUBLE()],
            ),
        )
    )

    return env.from_data_stream(
        changes,
        schema(
            [
                ("station", DataTypes.STRING()),
                ("event_time_text", DataTypes.STRING()),
                ("change_c_per_hour", DataTypes.DOUBLE()),
            ]
        ),
    ).select(
        col("station"),
        call_sql("CAST(event_time_text AS TIMESTAMP(3))").alias("event_time"),
        col("change_c_per_hour"),
    )


if __name__ == "__main__":
    run_stream("uc03-temperature-change", SINK, query)
