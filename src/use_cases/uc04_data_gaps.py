from pyflink.common import Row
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.table import DataTypes
from pyflink.table.expressions import call_sql, col

from common import jdbc_sink, run_stream, schema

SINK = jdbc_sink(
    "uc04_data_gaps",
    schema(
        [
            ("station", DataTypes.STRING()),
            ("interval_start", DataTypes.TIMESTAMP(3)),
            ("interval_end", DataTypes.TIMESTAMP(3)),
            ("status", DataTypes.STRING()),
        ],
        ["station", "interval_end"],
    ),
)


class DataGaps(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        self.previous_time = runtime_context.get_state(
            ValueStateDescriptor("previous_time", Types.SQL_TIMESTAMP())
        )

    def process_element(self, value, _ctx):
        station, event_time = value
        previous_time = self.previous_time.value()
        self.previous_time.update(event_time)

        if previous_time is None:
            return

        hours = (event_time - previous_time).total_seconds() / 3600
        yield Row(
            station,
            previous_time.strftime("%Y-%m-%d %H:%M:%S"),
            event_time.strftime("%Y-%m-%d %H:%M:%S"),
            "complete" if hours <= 1 else "incomplete",
        )


def query(env):
    gaps = (
        env.to_data_stream(
            env.from_path("weather_readings").select(col("station"), col("event_time"))
        )
        .key_by(lambda row: row[0])
        .process(
            DataGaps(),
            output_type=Types.ROW_NAMED(
                ["station", "start_text", "end_text", "status"],
                [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()],
            ),
        )
    )

    return env.from_data_stream(
        gaps,
        schema(
            [
                ("station", DataTypes.STRING()),
                ("start_text", DataTypes.STRING()),
                ("end_text", DataTypes.STRING()),
                ("status", DataTypes.STRING()),
            ]
        ),
    ).select(
        col("station"),
        call_sql("CAST(start_text AS TIMESTAMP(3))").alias("interval_start"),
        call_sql("CAST(end_text AS TIMESTAMP(3))").alias("interval_end"),
        col("status"),
    )


if __name__ == "__main__":
    run_stream("uc04-data-gaps", SINK, query)
