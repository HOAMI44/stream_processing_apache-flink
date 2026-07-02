from pyflink.table import DataTypes
from pyflink.table.window import Tumble
from pyflink.table.expressions import call_sql, col, lit

from common import jdbc_sink, run, schema

SINK = jdbc_sink(
    "uc01_temperature_stats",
    schema(
        [
            ("station", DataTypes.STRING()),
            ("window_size", DataTypes.STRING()),
            ("window_start", DataTypes.TIMESTAMP(3)),
            ("window_end", DataTypes.TIMESTAMP(3)),
            ("min_temp_c", DataTypes.DOUBLE()),
            ("max_temp_c", DataTypes.DOUBLE()),
            ("avg_temp_c", DataTypes.DOUBLE()),
        ],
        ["station", "window_size", "window_start"],
    ),
)


def _stats(base, hours, label):
    return (
        base.window(Tumble.over(lit(hours).hours).on(col("event_time")).alias("w"))
        .group_by(col("station"), col("w"))
        .select(
            col("station"),
            lit(label).alias("window_size"),
            col("w").start.alias("window_start"),
            col("w").end.alias("window_end"),
            col("temp_c").min.alias("min_temp_c"),
            col("temp_c").max.alias("max_temp_c"),
            col("temp_c").avg.alias("avg_temp_c"),
        )
    )


def query(env):
    base = (
        env.from_path("weather_readings")
        .where(call_sql("temperature.value_celsius IS NOT NULL"))
        .select(
            col("station"),
            col("event_time"),
            call_sql("temperature.value_celsius").alias("temp_c"),
        )
    )
    return _stats(base, 1, "1h").union_all(_stats(base, 24, "24h"))


if __name__ == "__main__":
    run("uc01-temperature-stats", SINK, query)
