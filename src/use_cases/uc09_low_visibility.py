from pyflink.common import Row
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.table import DataTypes
from pyflink.table.expressions import call_sql, col

from common import jdbc_sink, run_stream, schema

VISIBILITY_THRESHOLD_M = 200

SINK = jdbc_sink(
    "uc09_low_visibility",
    schema(
        [
            ("station", DataTypes.STRING()),
            ("period_start", DataTypes.TIMESTAMP(3)),
            ("period_end", DataTypes.TIMESTAMP(3)),
            ("duration_hours", DataTypes.BIGINT()),
        ],
        ["station", "period_start"],
    ),
)


class LowVisibilityPeriods(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        self.period_start = runtime_context.get_state(
            ValueStateDescriptor("period_start", Types.SQL_TIMESTAMP())
        )

    def process_element(self, value, _ctx):
        station, event_time, visibility_m = value
        period_start = self.period_start.value()

        if visibility_m < VISIBILITY_THRESHOLD_M:
            if period_start is None:
                self.period_start.update(event_time)
            return

        if period_start is None:
            return

        duration_hours = int((event_time - period_start).total_seconds() // 3600)
        self.period_start.clear()
        yield Row(
            station,
            period_start.strftime("%Y-%m-%d %H:%M:%S"),
            event_time.strftime("%Y-%m-%d %H:%M:%S"),
            duration_hours,
        )


def query(env):
    readings = (
        env.from_path("weather_readings")
        .where(call_sql("visibility.distance_meters IS NOT NULL"))
        .select(
            col("station"),
            col("event_time"),
            call_sql("visibility.distance_meters").alias("visibility_m"),
        )
    )

    periods = (
        env.to_data_stream(readings)
        .key_by(lambda row: row[0])
        .process(
            LowVisibilityPeriods(),
            output_type=Types.ROW_NAMED(
                ["station", "period_start_raw", "period_end_raw", "duration_hours"],
                [
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.LONG(),
                ],
            ),
        )
    )

    return env.from_data_stream(
        periods,
        schema(
            [
                ("station", DataTypes.STRING()),
                ("period_start_raw", DataTypes.STRING()),
                ("period_end_raw", DataTypes.STRING()),
                ("duration_hours", DataTypes.BIGINT()),
            ]
        ),
    ).select(
        col("station"),
        call_sql("CAST(period_start_raw AS TIMESTAMP(3))").alias("period_start"),
        call_sql("CAST(period_end_raw AS TIMESTAMP(3))").alias("period_end"),
        col("duration_hours"),
    )


if __name__ == "__main__":
    run_stream("uc09-low-visibility", SINK, query)
