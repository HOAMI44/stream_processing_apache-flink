from pyflink.table import DataTypes
from pyflink.table.expressions import call_sql, col, lit
from pyflink.table.window import Slide

from common import jdbc_sink, run, schema

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


def query(env):
    rolling = (
        env.from_path("weather_readings")
        .where(call_sql("temperature.value_celsius IS NOT NULL"))
        .window(
            Slide.over(lit(30).days)
            .every(lit(10).days)
            .on(col("event_time"))
            .alias("w")
        )
        .group_by(col("station"), col("w"))
        .select(
            col("station"),
            col("w").start.alias("window_start"),
            col("w").end.alias("window_end"),
            col("w").rowtime.alias("window_time"),
            col("temperature").get("value_celsius").avg.alias("avg_temp_c"),
        )
    )
    with_previous = rolling.select(
        col("station"),
        col("window_start"),
        col("window_end"),
        col("avg_temp_c"),
        call_sql(
            "LAG(avg_temp_c) OVER (PARTITION BY station ORDER BY window_time)"
        ).alias("previous_avg_temp_c"),
    )
    return with_previous.select(
        col("station"),
        call_sql("CAST(window_start AS TIMESTAMP(3))"),
        call_sql("CAST(window_end AS TIMESTAMP(3))"),
        col("avg_temp_c"),
        call_sql("""
            CASE WHEN previous_avg_temp_c IS NULL THEN 'unknown'
                 WHEN avg_temp_c > previous_avg_temp_c THEN 'rising'
                 WHEN avg_temp_c < previous_avg_temp_c THEN 'falling'
                 ELSE 'stable'
            END
        """),
    )


if __name__ == "__main__":
    run("uc07-climate-trend", SINK, query)
