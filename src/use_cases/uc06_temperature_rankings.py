from pyflink.table import DataTypes

from common import jdbc_sink, run, schema

SINK = jdbc_sink(
    "uc06_temperature_rankings",
    schema(
        [
            ("station", DataTypes.STRING()),
            ("window_start", DataTypes.TIMESTAMP(3)),
            ("window_end", DataTypes.TIMESTAMP(3)),
            ("list_name", DataTypes.STRING()),
            ("rank_position", DataTypes.BIGINT()),
            ("temp_c", DataTypes.DOUBLE()),
        ],
        ["window_start", "list_name", "rank_position"],
    ),
)


def query(env):
    return env.sql_query("""
        WITH station_temperatures AS (
            SELECT
                station,
                window_start,
                window_end,
                MAX(temperature.value_celsius) AS hottest_temp_c,
                MIN(temperature.value_celsius) AS coldest_temp_c
            FROM TABLE(
                HOP(
                    TABLE weather_readings,
                    DESCRIPTOR(event_time),
                    INTERVAL '1' HOUR,
                    INTERVAL '24' HOUR
                )
            )
            WHERE temperature.value_celsius IS NOT NULL
            GROUP BY station, window_start, window_end
        ),
        hottest AS (
            SELECT
                station,
                window_start,
                window_end,
                'hottest' AS list_name,
                ROW_NUMBER() OVER (
                    PARTITION BY window_start, window_end
                    ORDER BY hottest_temp_c DESC, station
                ) AS rank_position,
                hottest_temp_c AS temp_c
            FROM station_temperatures
        ),
        coldest AS (
            SELECT
                station,
                window_start,
                window_end,
                'coldest' AS list_name,
                ROW_NUMBER() OVER (
                    PARTITION BY window_start, window_end
                    ORDER BY coldest_temp_c ASC, station
                ) AS rank_position,
                coldest_temp_c AS temp_c
            FROM station_temperatures
        )
        SELECT station, window_start, window_end, list_name, rank_position, temp_c
        FROM hottest
        WHERE rank_position <= 10
        UNION ALL
        SELECT station, window_start, window_end, list_name, rank_position, temp_c
        FROM coldest
        WHERE rank_position <= 10
    """)


if __name__ == "__main__":
    run("uc06-temperature-rankings", SINK, query)
