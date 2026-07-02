from pyflink.table import DataTypes
from pyflink.table.expressions import call_sql, col, lit
from pyflink.table.udf import udf
from pyflink.table.window import Slide

from common import jdbc_sink, run, schema

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


@udf(result_type=DataTypes.DOUBLE())
def pressure_drop_per_hour(previous_time, event_time, previous_pressure, pressure):
    if None in (previous_time, event_time, previous_pressure, pressure):
        return None
    seconds = (event_time - previous_time).total_seconds()
    if seconds <= 0:
        return None
    return (previous_pressure - pressure) * 3600.0 / seconds


def query(env):
    with_previous = (
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
            call_sql(
                "LAG(event_time) OVER (PARTITION BY station ORDER BY event_time)"
            ).alias("previous_time"),
            call_sql(
                "LAG(sea_level_pressure.value_hpa) OVER "
                "(PARTITION BY station ORDER BY event_time)"
            ).alias("previous_pressure_hpa"),
        )
    )
    storm_windows = (
        with_previous.where(call_sql("previous_time IS NOT NULL"))
        .select(
            col("station"),
            col("event_time"),
            col("wind_kmh"),
            pressure_drop_per_hour(
                col("previous_time"),
                col("event_time"),
                col("previous_pressure_hpa"),
                col("pressure_hpa"),
            ).alias("pressure_drop_hpa_per_hour"),
        )
        .window(
            Slide.over(lit(60).minutes)
            .every(lit(10).minutes)
            .on(col("event_time"))
            .alias("w")
        )
        .group_by(col("station"), col("w"))
        .select(
            col("station"),
            col("w").start.alias("window_start"),
            col("w").end.alias("window_end"),
            col("pressure_drop_hpa_per_hour").max.alias("pressure_drop_hpa_per_hour"),
            col("wind_kmh").max.alias("max_wind_kmh"),
        )
    )
    return storm_windows.where(
        call_sql(
            f"pressure_drop_hpa_per_hour > {PRESSURE_DROP_THRESHOLD_HPA_PER_HOUR} "
            f"AND max_wind_kmh > {WIND_THRESHOLD_KMH}"
        )
    ).select(
        col("station"),
        col("window_start"),
        col("window_end"),
        col("pressure_drop_hpa_per_hour"),
        col("max_wind_kmh"),
        call_sql("'storm_event'"),
    )


if __name__ == "__main__":
    run("uc10-storm-warning", SINK, query)
