from pyflink.common import Duration, Row, WatermarkStrategy
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.functions import (
    AggregateFunction,
    KeyedProcessFunction,
    ProcessWindowFunction,
    RuntimeContext,
)
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.table import DataTypes
from pyflink.table.expressions import call_sql, col

from common import (
    datetime_to_utc_millis,
    jdbc_sink,
    run_stream,
    schema,
    utc_millis_to_text,
)


SINK = jdbc_sink(
    "uc07_climate_trend",
    schema(
        [
            ("station", DataTypes.STRING()),
            ("window_start", DataTypes.TIMESTAMP(3)),
            ("window_end", DataTypes.TIMESTAMP(3)),
            ("avg_temp_c", DataTypes.DOUBLE()),
            ("trend_direction", DataTypes.STRING()),
        ],
        ["station", "window_start"],
    ),
)


class EventTimeAssigner(TimestampAssigner):
    def extract_timestamp(self, value, _record_timestamp):
        return datetime_to_utc_millis(value[1])


class AverageTemperature(AggregateFunction):
    def create_accumulator(self):
        return (0.0, 0)

    def add(self, value, accumulator):
        return (accumulator[0] + value[2], accumulator[1] + 1)

    def get_result(self, accumulator):
        return accumulator[0] / accumulator[1]

    def merge(self, a, b):
        return (a[0] + b[0], a[1] + b[1])


class WindowAverage(ProcessWindowFunction):
    def process(self, key, context, values):
        yield Row(
            key,
            utc_millis_to_text(context.window().start),
            utc_millis_to_text(context.window().end),
            next(iter(values)),
        )


class TrendDirection(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        self.previous_avg = runtime_context.get_state(
            ValueStateDescriptor("previous_avg_temp_c", Types.DOUBLE())
        )

    def process_element(self, value, _ctx):
        station, window_start, window_end, avg_temp_c = value
        previous_avg = self.previous_avg.value()
        self.previous_avg.update(avg_temp_c)

        if previous_avg is None:
            trend = "unknown"
        elif avg_temp_c > previous_avg:
            trend = "rising"
        elif avg_temp_c < previous_avg:
            trend = "falling"
        else:
            trend = "stable"

        yield Row(station, window_start, window_end, avg_temp_c, trend)


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

    trends = (
        env.to_data_stream(readings)
        .assign_timestamps_and_watermarks(
            WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_minutes(5))
            .with_timestamp_assigner(EventTimeAssigner())
        )
        .key_by(lambda row: row[0])
        .window(SlidingEventTimeWindows.of(Time.days(30), Time.days(10)))
        .aggregate(
            AverageTemperature(),
            window_function=WindowAverage(),
            accumulator_type=Types.TUPLE([Types.DOUBLE(), Types.LONG()]),
            output_type=Types.ROW_NAMED(
                ["station", "window_start_raw", "window_end_raw", "avg_temp_c"],
                [Types.STRING(), Types.STRING(), Types.STRING(), Types.DOUBLE()],
            ),
        )
        .key_by(lambda row: row[0])
        .process(
            TrendDirection(),
            output_type=Types.ROW_NAMED(
                [
                    "station",
                    "window_start_raw",
                    "window_end_raw",
                    "avg_temp_c",
                    "trend_direction",
                ],
                [
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.DOUBLE(),
                    Types.STRING(),
                ],
            ),
        )
    )

    return env.from_data_stream(
        trends,
        schema(
            [
                ("station", DataTypes.STRING()),
                ("window_start_raw", DataTypes.STRING()),
                ("window_end_raw", DataTypes.STRING()),
                ("avg_temp_c", DataTypes.DOUBLE()),
                ("trend_direction", DataTypes.STRING()),
            ]
        ),
    ).select(
        col("station"),
        call_sql("CAST(window_start_raw AS TIMESTAMP(3))").alias("window_start"),
        call_sql("CAST(window_end_raw AS TIMESTAMP(3))").alias("window_end"),
        col("avg_temp_c"),
        col("trend_direction"),
    )


if __name__ == "__main__":
    run_stream("uc07-climate-trend", SINK, query)
