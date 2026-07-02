from pyflink.table import DataTypes
from pyflink.table.expressions import call_sql, col

from common import jdbc_sink, run, schema

SINK = jdbc_sink(
    "uc08_user_notifications",
    schema(
        [
            ("user_id", DataTypes.STRING()),
            ("booking_id", DataTypes.STRING()),
            ("resort_id", DataTypes.STRING()),
            ("event_time", DataTypes.TIMESTAMP(3)),
            ("alert", DataTypes.STRING()),
            ("wind_mps", DataTypes.DOUBLE()),
            ("visibility_m", DataTypes.INT()),
        ],
        ["user_id", "booking_id", "event_time"],
    ),
)


def query(env):
    bookings = env.from_elements(
        [
            (
                "u-001",
                "b-001",
                "02907099999",
                "1901-01-01 00:00:00",
                "1901-01-02 00:00:00",
            ),
            (
                "u-002",
                "b-002",
                "02950099999",
                "1901-01-01 00:00:00",
                "1901-01-02 00:00:00",
            ),
            (
                "u-003",
                "b-003",
                "22707099999",
                "1901-01-01 00:00:00",
                "1901-01-02 00:00:00",
            ),
        ],
        [
            "user_id",
            "booking_id",
            "resort_id",
            "booking_start_text",
            "booking_end_text",
        ],
    ).select(
        col("user_id"),
        col("booking_id"),
        col("resort_id"),
        call_sql("TO_TIMESTAMP(booking_start_text)").alias("booking_start"),
        call_sql("TO_TIMESTAMP(booking_end_text)").alias("booking_end"),
    )
    weather = (
        env.from_path("weather_readings")
        .where(call_sql("""
            wind.speed_rate IS NOT NULL
            AND visibility.distance_meters IS NOT NULL
            """))
        .select(
            col("station"),
            col("event_time"),
            call_sql("wind.speed_rate").alias("wind_mps"),
            call_sql("visibility.distance_meters").alias("visibility_m"),
        )
    )
    return (
        weather.join(bookings, col("station") == col("resort_id"))
        .where(call_sql("""
            event_time BETWEEN booking_start AND booking_end
            AND (wind_mps > 18 OR visibility_m < 500)
            """))
        .select(
            col("user_id"),
            col("booking_id"),
            col("resort_id"),
            col("event_time"),
            call_sql("'bad_conditions'"),
            col("wind_mps"),
            col("visibility_m"),
        )
    )


if __name__ == "__main__":
    run("uc08-user-notifications", SINK, query)
