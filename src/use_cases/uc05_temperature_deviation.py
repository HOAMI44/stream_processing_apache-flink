from pyflink.table import DataTypes
from pyflink.table.window import Slide
from pyflink.table.expressions import call_sql, col, lit
from pyflink.table.udf import udf

from common import jdbc_sink, run, schema

SINK = jdbc_sink(
    "uc05_temperature_deviation",
    schema(
        [
            ("station", DataTypes.STRING()),
            ("window_start", DataTypes.TIMESTAMP(3)),
            ("window_end", DataTypes.TIMESTAMP(3)),
            ("deviation_c", DataTypes.DOUBLE()),
        ],
        ["station", "window_start"],
    ),
)


@udf(result_type=DataTypes.DOUBLE())
def historical_avg_temp(station):
    if station is None:
        return 4.0
    return 2.0 + (int(station[:6]) % 50) / 10.0


def query(env):
    rolling = (
        env.from_path("weather_readings")
        .where(call_sql("temperature.value_celsius IS NOT NULL"))
        .window(
            Slide.over(lit(24).hours)
            .every(lit(1).hours)
            .on(col("event_time"))
            .alias("w")
        )
        .group_by(col("station"), col("w"))
        .select(
            col("station"),
            col("w").start.alias("window_start"),
            col("w").end.alias("window_end"),
            col("temperature").get("value_celsius").avg.alias("rolling_avg_temp_c"),
            historical_avg_temp(col("station")).alias("historical_avg_temp_c"),
        )
    )
    return rolling.select(
        col("station"),
        col("window_start"),
        col("window_end"),
        call_sql("rolling_avg_temp_c - historical_avg_temp_c"),
    )


if __name__ == "__main__":
    run("uc05-temperature-deviation", SINK, query)
