from pyflink.table import DataTypes
from pyflink.table.expressions import call_sql, col
from pyflink.table.udf import udf

from common import jdbc_sink, run, schema

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


@udf(result_type=DataTypes.DOUBLE())
def change_per_hour(previous_time, event_time, previous_temp_c, temp_c):
    seconds = (event_time - previous_time).total_seconds()
    if seconds <= 0:
        return None
    return (temp_c - previous_temp_c) * 3600.0 / seconds


def query(env):
    with_previous = (
        env.from_path("weather_readings")
        .where(call_sql("temperature.value_celsius IS NOT NULL"))
        .select(
            col("station"),
            col("event_time"),
            call_sql("temperature.value_celsius").alias("temp_c"),
            call_sql(
                "LAG(event_time) OVER (PARTITION BY station ORDER BY event_time)"
            ).alias("previous_time"),
            call_sql(
                "LAG(temperature.value_celsius) OVER "
                "(PARTITION BY station ORDER BY event_time)"
            ).alias("previous_temp_c"),
        )
    )
    return with_previous.where(
        call_sql(
            "previous_time IS NOT NULL AND TIMESTAMPDIFF(SECOND, previous_time, event_time) > 0"
        )
    ).select(
        col("station"),
        call_sql("CAST(event_time AS TIMESTAMP(3))"),
        change_per_hour(
            col("previous_time"),
            col("event_time"),
            col("previous_temp_c"),
            col("temp_c"),
        ),
    )


if __name__ == "__main__":
    run("uc03-temperature-change", SINK, query)
