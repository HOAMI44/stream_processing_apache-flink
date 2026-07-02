from pyflink.common import Duration, Row, WatermarkStrategy
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.functions import ProcessAllWindowFunction
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.table import DataTypes
from pyflink.table.expressions import call_sql, col

from common import (
    iso_to_utc_millis,
    jdbc_sink,
    run_stream,
    schema,
    utc_millis_to_text,
)


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


class EventTimeAssigner(TimestampAssigner):
    def extract_timestamp(self, value, _record_timestamp):
        return iso_to_utc_millis(value[1])


class TemperatureRankings(ProcessAllWindowFunction):
    def process(self, context, elements):
        by_station = {}
        for station, _date, temp_c in elements:
            hottest, coldest = by_station.get(station, (temp_c, temp_c))
            by_station[station] = (max(hottest, temp_c), min(coldest, temp_c))

        start_raw = utc_millis_to_text(context.window().start)
        end_raw = utc_millis_to_text(context.window().end)

        hottest = sorted(by_station.items(), key=lambda item: (-item[1][0], item[0]))
        coldest = sorted(by_station.items(), key=lambda item: (item[1][1], item[0]))

        for rank, (station, temps) in enumerate(hottest[:10], 1):
            yield Row(station, start_raw, end_raw, "hottest", rank, temps[0])
        for rank, (station, temps) in enumerate(coldest[:10], 1):
            yield Row(station, start_raw, end_raw, "coldest", rank, temps[1])


def query(env):
    readings = (
        env.from_path("weather_readings")
        .where(call_sql("temperature.value_celsius IS NOT NULL"))
        .select(
            col("station"),
            col("date"),
            call_sql("temperature.value_celsius").alias("temp_c"),
        )
    )

    rankings = (
        env.to_data_stream(readings)
        .assign_timestamps_and_watermarks(
            WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_minutes(5))
            .with_timestamp_assigner(EventTimeAssigner())
        )
        .window_all(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(1)))
        .process(
            TemperatureRankings(),
            output_type=Types.ROW_NAMED(
                [
                    "station",
                    "window_start_raw",
                    "window_end_raw",
                    "list_name",
                    "rank_position",
                    "temp_c",
                ],
                [
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.LONG(),
                    Types.DOUBLE(),
                ],
            ),
        )
    )

    return env.from_data_stream(
        rankings,
        schema(
            [
                ("station", DataTypes.STRING()),
                ("window_start_raw", DataTypes.STRING()),
                ("window_end_raw", DataTypes.STRING()),
                ("list_name", DataTypes.STRING()),
                ("rank_position", DataTypes.BIGINT()),
                ("temp_c", DataTypes.DOUBLE()),
            ]
        ),
    ).select(
        col("station"),
        call_sql("CAST(window_start_raw AS TIMESTAMP(3))").alias("window_start"),
        call_sql("CAST(window_end_raw AS TIMESTAMP(3))").alias("window_end"),
        col("list_name"),
        col("rank_position"),
        col("temp_c"),
    )


if __name__ == "__main__":
    run_stream("uc06-temperature-rankings", SINK, query)
