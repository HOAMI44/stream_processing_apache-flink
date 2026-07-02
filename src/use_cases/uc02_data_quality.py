from pyflink.table import DataTypes
from pyflink.table.window import Slide
from pyflink.table.expressions import call_sql, col, lit

from common import jdbc_sink, run, schema

SINK = jdbc_sink(
    "uc02_data_quality",
    schema(
        [
            ("station", DataTypes.STRING()),
            ("window_start", DataTypes.TIMESTAMP(3)),
            ("window_end", DataTypes.TIMESTAMP(3)),
            ("bad_count", DataTypes.BIGINT()),
        ],
        ["station", "window_start"],
    ),
)


def query(env):
    return (
        env.from_path("weather_readings")
        .add_columns(
            call_sql(
                "CASE WHEN COALESCE(temperature.is_valid, FALSE) = FALSE THEN 1 ELSE 0 END"
            ).alias("bad")
        )
        .window(
            Slide.over(lit(1).hours)
            .every(lit(10).minutes)
            .on(col("event_time"))
            .alias("w")
        )
        .group_by(col("station"), col("w"))
        .select(
            col("station"),
            col("w").start.alias("window_start"),
            col("w").end.alias("window_end"),
            col("bad").sum.alias("bad_count"),
        )
    )


if __name__ == "__main__":
    run("uc02-data-quality", SINK, query)
