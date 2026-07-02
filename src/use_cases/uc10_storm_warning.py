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

PRESSURE_DROP_THRESHOLD_HPA_PER_HOUR = 5.0
WIND_THRESHOLD_KMH = 80.0


SINK = jdbc_sink(
    "uc10_storm_warning",
    schema(
        [
            ("station", DataTypes.STRING()),
            ("window_start", DataTypes.TIMESTAMP(3)),
            ("window_end", DataTypes.TIMESTAMP(3)),
            ("pressure_drop_hpa_per_hour", DataTypes.DOUBLE()),
            ("max_wind_kmh", DataTypes.DOUBLE()),
            ("alert", DataTypes.STRING()),
        ],
        ["station", "window_start"],
    ),
)


class EventTimeAssigner(TimestampAssigner):
    def extract_timestamp(self, value, _record_timestamp):
        return datetime_to_utc_millis(value[1])


class PressureDrops(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        self.previous = runtime_context.get_state(
            ValueStateDescriptor("previous_pressure", Types.PICKLED_BYTE_ARRAY())
        )

    def process_element(self, value, _ctx):
        station, event_time, pressure_hpa, wind_kmh = value
        previous = self.previous.value()
        self.previous.update((event_time, pressure_hpa))

        if previous is None:
            return

        previous_time, previous_pressure = previous
        seconds = (event_time - previous_time).total_seconds()
        if seconds > 0:
            yield Row(
                station,
                event_time,
                (previous_pressure - pressure_hpa) * 3600.0 / seconds,
                wind_kmh,
            )


class StormAggregate(AggregateFunction):
    def create_accumulator(self):
        return (float("-inf"), float("-inf"))

    def add(self, value, accumulator):
        return (max(accumulator[0], value[2]), max(accumulator[1], value[3]))

    def get_result(self, accumulator):
        return accumulator

    def merge(self, a, b):
        return (max(a[0], b[0]), max(a[1], b[1]))


class StormWindow(ProcessWindowFunction):
    def process(self, key, context, values):
        pressure_drop, max_wind = next(iter(values))
        if (
            pressure_drop > PRESSURE_DROP_THRESHOLD_HPA_PER_HOUR
            and max_wind > WIND_THRESHOLD_KMH
        ):
            yield Row(
                key,
                utc_millis_to_text(context.window().start),
                utc_millis_to_text(context.window().end),
                pressure_drop,
                max_wind,
                "storm_event",
            )


def query(env):
    readings = (
        env.from_path("weather_readings")
        .where(call_sql("""
            COALESCE(sea_level_pressure.is_valid, FALSE)
            AND COALESCE(wind.speed_is_valid, FALSE)
            AND sea_level_pressure.value_hpa IS NOT NULL
            AND wind.speed_rate IS NOT NULL
            """))
        .select(
            col("station"),
            col("event_time"),
            call_sql("sea_level_pressure.value_hpa").alias("pressure_hpa"),
            call_sql("wind.speed_rate * 3.6").alias("wind_kmh"),
        )
    )

    alerts = (
        env.to_data_stream(readings)
        .key_by(lambda row: row[0])
        .process(
            PressureDrops(),
            output_type=Types.ROW_NAMED(
                ["station", "event_time", "pressure_drop", "wind_kmh"],
                [
                    Types.STRING(),
                    Types.SQL_TIMESTAMP(),
                    Types.DOUBLE(),
                    Types.DOUBLE(),
                ],
            ),
        )
        .assign_timestamps_and_watermarks(
            WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_minutes(5))
            .with_timestamp_assigner(EventTimeAssigner())
        )
        .key_by(lambda row: row[0])
        .window(SlidingEventTimeWindows.of(Time.minutes(60), Time.minutes(10)))
        .aggregate(
            StormAggregate(),
            window_function=StormWindow(),
            accumulator_type=Types.TUPLE([Types.DOUBLE(), Types.DOUBLE()]),
            output_type=Types.ROW_NAMED(
                [
                    "station",
                    "window_start_text",
                    "window_end_text",
                    "pressure_drop_hpa_per_hour",
                    "max_wind_kmh",
                    "alert",
                ],
                [
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.DOUBLE(),
                    Types.DOUBLE(),
                    Types.STRING(),
                ],
            ),
        )
    )

    return env.from_data_stream(
        alerts,
        schema(
            [
                ("station", DataTypes.STRING()),
                ("window_start_text", DataTypes.STRING()),
                ("window_end_text", DataTypes.STRING()),
                ("pressure_drop_hpa_per_hour", DataTypes.DOUBLE()),
                ("max_wind_kmh", DataTypes.DOUBLE()),
                ("alert", DataTypes.STRING()),
            ]
        ),
    ).select(
        col("station"),
        call_sql("CAST(window_start_text AS TIMESTAMP(3))").alias("window_start"),
        call_sql("CAST(window_end_text AS TIMESTAMP(3))").alias("window_end"),
        col("pressure_drop_hpa_per_hour"),
        col("max_wind_kmh"),
        col("alert"),
    )


if __name__ == "__main__":
    run_stream("uc10-storm-warning", SINK, query)
