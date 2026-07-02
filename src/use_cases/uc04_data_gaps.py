from pyflink.table import DataTypes
from pyflink.table.expressions import call_sql, col

from common import jdbc_sink, run, schema

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


def query(env):
    with_previous = env.from_path("weather_readings").select(
        col("station"),
        col("event_time"),
        call_sql(
            "LAG(event_time) OVER (PARTITION BY station ORDER BY event_time)"
        ).alias("previous_time"),
    )
    return with_previous.where(call_sql("previous_time IS NOT NULL")).select(
        col("station"),
        call_sql("CAST(previous_time AS TIMESTAMP(3))"),
        call_sql("CAST(event_time AS TIMESTAMP(3))"),
        call_sql(
            "CASE WHEN TIMESTAMPDIFF(HOUR, previous_time, event_time) <= 1 "
            "THEN 'complete' ELSE 'incomplete' END"
        ),
    )


if __name__ == "__main__":
    run("uc04-data-gaps", SINK, query)
