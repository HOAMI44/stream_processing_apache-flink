import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src" / "use_cases"), str(ROOT / "src")]

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    EnvironmentSettings,
    StreamTableEnvironment,
    TableDescriptor,
    TableEnvironment,
)

import uc01_temperature_stats as uc01
import uc02_data_quality as uc02
import uc03_temperature_change as uc03
import uc04_data_gaps as uc04
import uc05_temperature_deviation as uc05
import uc06_temperature_rankings as uc06
import uc07_climate_trend as uc07
import uc08_user_notifications as uc08
import uc09_low_visibility as uc09
import uc10_storm_warning as uc10
from common import readings_schema


def reading(
    station,
    date,
    temp,
    name=None,
    dew=0.0,
    wind=5.0,
    visibility=10000,
    pressure=1010.0,
    lat=60.0,
    lon=25.0,
    temp_quality=None,
    dew_quality=None,
    wind_quality=None,
    visibility_quality=None,
    pressure_quality=None,
):
    temp_quality = temp_quality or ("9" if temp is None else "1")
    dew_quality = dew_quality or ("9" if dew is None else "1")
    wind_quality = wind_quality or ("9" if wind is None else "1")
    visibility_quality = visibility_quality or ("9" if visibility is None else "1")
    pressure_quality = pressure_quality or ("9" if pressure is None else "1")

    def valid(value, quality):
        return value is not None and quality not in {"2", "3", "6", "7"}

    return {
        "station": station,
        "date": date,
        "source": "4",
        "latitude": lat,
        "longitude": lon,
        "elevation": 5.0,
        "name": name or station,
        "report_type": "FM-12",
        "call_sign": "99999",
        "quality_control": "V020",
        "wind": {
            "direction_angle": 270,
            "direction_quality": "1",
            "type": "N",
            "speed_rate": wind,
            "speed_quality": wind_quality,
            "speed_is_valid": valid(wind, wind_quality),
        },
        "ceiling": {
            "height_meters": None,
            "quality": "9",
            "determination": "9",
            "cavok": "N",
            "is_valid": False,
        },
        "visibility": {
            "distance_meters": visibility,
            "quality": visibility_quality,
            "variability": "N",
            "variability_quality": "1",
            "is_valid": valid(visibility, visibility_quality),
        },
        "temperature": {
            "value_celsius": temp,
            "quality": temp_quality,
            "is_valid": valid(temp, temp_quality),
        },
        "dew_point": {
            "value_celsius": dew,
            "quality": dew_quality,
            "is_valid": valid(dew, dew_quality),
        },
        "sea_level_pressure": {
            "value_hpa": pressure,
            "quality": pressure_quality,
            "is_valid": valid(pressure, pressure_quality),
        },
    }


class FlinkUseCaseTests(unittest.TestCase):
    def query(self, records, build_table, *, streaming=False):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "readings.json"
            path.write_text(
                "\n".join(json.dumps(record) for record in records), encoding="utf-8"
            )

            settings = (
                EnvironmentSettings.in_streaming_mode()
                if streaming
                else EnvironmentSettings.in_batch_mode()
            )
            if streaming:
                stream_env = StreamExecutionEnvironment.get_execution_environment()
                stream_env.set_parallelism(1)
                env = StreamTableEnvironment.create(
                    stream_execution_environment=stream_env
                )
            else:
                env = TableEnvironment.create(settings)
            env.create_temporary_table(
                "weather_readings",
                TableDescriptor.for_connector("filesystem")
                .schema(readings_schema())
                .option("path", str(path))
                .format("json")
                .build(),
            )
            return [tuple(row) for row in build_table(env).execute().collect()]

    def test_uc01_temperature_stats(self):
        rows = self.query(
            [
                reading("A", "1901-01-01T00:10:00", -5.0),
                reading("A", "1901-01-01T00:40:00", 5.0),
                reading("A", "1901-01-02T00:10:00", 15.0),
                reading("B", "1901-01-01T00:20:00", 20.0),
            ],
            uc01.query,
        )

        got = {(r[0], r[1], str(r[2])): r[4:] for r in rows}
        self.assertEqual((-5.0, 5.0, 0.0), got[("A", "1h", "1901-01-01 00:00:00")])
        self.assertEqual((-5.0, 5.0, 0.0), got[("A", "24h", "1901-01-01 00:00:00")])
        self.assertEqual((15.0, 15.0, 15.0), got[("A", "24h", "1901-01-02 00:00:00")])

    def test_uc02_data_quality(self):
        rows = self.query(
            [
                reading("A", "1901-01-01T00:00:00", 10.0),
                reading("A", "1901-01-01T00:10:00", 12.0, temp_quality="3"),
                reading("A", "1901-01-01T00:20:00", None),
                reading("B", "1901-01-01T00:00:00", 5.0, wind_quality="6"),
                reading("B", "1901-01-01T00:10:00", 6.0),
            ],
            uc02.query,
        )

        full_windows = {r[0]: r for r in rows if str(r[1]) == "1901-01-01 00:00:00"}
        self.assertEqual(2, full_windows["A"][3])
        self.assertEqual(0, full_windows["B"][3])

    def test_uc03_temperature_change(self):
        rows = self.query(
            [
                reading("A", "1901-01-01T00:00:00", 0.0),
                reading("A", "1901-01-01T02:00:00", 4.0),
                reading("A", "1901-01-01T03:00:00", 1.0),
                reading("B", "1901-01-01T00:00:00", 10.0),
                reading("B", "1901-01-01T01:00:00", 13.0),
            ],
            uc03.query,
            streaming=True,
        )

        got = {(r[0], str(r[1])): r[2] for r in rows}
        self.assertEqual(2.0, got[("A", "1901-01-01 02:00:00")])
        self.assertEqual(-3.0, got[("A", "1901-01-01 03:00:00")])
        self.assertEqual(3.0, got[("B", "1901-01-01 01:00:00")])

    def test_uc04_data_gaps(self):
        rows = self.query(
            [
                reading("A", "1901-01-01T00:00:00", 1.0),
                reading("A", "1901-01-01T01:00:00", 2.0),
                reading("A", "1901-01-01T04:00:00", 3.0),
                reading("B", "1901-01-01T00:00:00", 1.0),
                reading("B", "1901-01-01T02:00:00", 2.0),
            ],
            uc04.query,
            streaming=True,
        )

        got = {(r[0], str(r[2])): r[3] for r in rows}
        self.assertEqual("complete", got[("A", "1901-01-01 01:00:00")])
        self.assertEqual("incomplete", got[("A", "1901-01-01 04:00:00")])
        self.assertEqual("incomplete", got[("B", "1901-01-01 02:00:00")])

    def test_uc05_temperature_deviation(self):
        rows = self.query(
            [
                reading("02907099999", "1901-01-01T00:00:00", 5.0),
                reading("02907099999", "1901-01-01T12:00:00", 7.0),
                reading("02950099999", "1901-01-01T00:00:00", 1.0),
            ],
            uc05.query,
        )

        got = {r[0]: r for r in rows if str(r[1]) == "1901-01-01 00:00:00"}
        self.assertEqual(2.0, got["02907099999"][3])
        self.assertEqual(-1.0, got["02950099999"][3])

    def test_uc06_temperature_rankings(self):
        rows = self.query(
            [
                reading("A", "1901-01-01T00:00:00", -1.0),
                reading("A", "1901-01-01T01:00:00", 8.0),
                reading("B", "1901-01-01T00:00:00", 18.0),
                reading("C", "1901-01-01T00:00:00", 31.0),
            ],
            uc06.query,
            streaming=True,
        )

        got = {
            (str(r[1]), r[3], r[4]): (r[0], r[5])
            for r in rows
            if str(r[1]) == "1901-01-01 00:00:00"
        }
        self.assertEqual(("C", 31.0), got[("1901-01-01 00:00:00", "hottest", 1)])
        self.assertEqual(("B", 18.0), got[("1901-01-01 00:00:00", "hottest", 2)])
        self.assertEqual(("A", -1.0), got[("1901-01-01 00:00:00", "coldest", 1)])

    def test_uc07_climate_trend(self):
        rows = self.query(
            [
                reading("A", "1901-01-01T00:00:00", 0.0),
                reading("A", "1901-01-15T00:00:00", 10.0),
                reading("A", "1901-01-30T00:00:00", 20.0),
                reading("B", "1901-01-10T00:00:00", -5.0),
                reading("B", "1901-01-20T00:00:00", 5.0),
            ],
            uc07.query,
            streaming=True,
        )

        got = {(r[0], str(r[1])): r[3:5] for r in rows}
        self.assertEqual((15.0, "rising"), got[("A", "1901-01-03 00:00:00")])
        self.assertEqual((0.0, "stable"), got[("B", "1901-01-03 00:00:00")])

    def test_uc08_user_notifications(self):
        rows = self.query(
            [
                reading(
                    "02907099999",
                    "1901-01-01T00:00:00",
                    -5.0,
                    wind=8.0,
                    visibility=2000,
                ),
                reading(
                    "02950099999",
                    "1901-01-01T00:00:00",
                    -5.0,
                    wind=20.0,
                    visibility=2000,
                ),
                reading("X", "1901-01-01T00:00:00", -5.0, wind=8.0, visibility=2000),
            ],
            uc08.query,
        )

        got = {(r[0], r[1], r[2]): r for r in rows}
        self.assertEqual(
            "bad_conditions",
            got[("u-002", "b-002", "02950099999")][4],
        )
        self.assertEqual(1, len(rows))

    def test_uc09_low_visibility(self):
        rows = self.query(
            [
                reading("A", "1901-01-01T00:00:00", 1.0, visibility=100),
                reading("A", "1901-01-01T05:00:00", 2.0, visibility=150),
                reading("A", "1901-01-01T06:00:00", 3.0, visibility=500),
                reading("A", "1901-01-01T12:00:00", 4.0, visibility=100),
                reading("B", "1901-01-01T00:00:00", 1.0, visibility=1000),
                reading("B", "1901-01-01T01:00:00", 2.0, visibility=100),
            ],
            uc09.query,
            streaming=True,
        )

        got = {(r[0], str(r[1])): r for r in rows}
        self.assertEqual(6, got[("A", "1901-01-01 00:00:00")][3])
        self.assertNotIn(("A", "1901-01-01 12:00:00"), got)
        self.assertNotIn(("B", "1901-01-01 01:00:00"), got)

    def test_uc10_storm_warning(self):
        rows = self.query(
            [
                reading("A", "1901-01-01T00:00:00", 5.0, wind=5.0, pressure=1015.0),
                reading("A", "1901-01-01T00:30:00", 5.0, wind=25.0, pressure=1011.0),
                reading("B", "1901-01-01T00:00:00", 5.0, wind=5.0, pressure=1015.0),
                reading("B", "1901-01-01T00:30:00", 5.0, wind=5.0, pressure=1011.0),
                reading("C", "1901-01-01T00:00:00", 5.0, wind=25.0, pressure=1010.0),
                reading("C", "1901-01-01T00:30:00", 5.0, wind=25.0, pressure=1010.0),
            ],
            uc10.query,
            streaming=True,
        )

        stations = {r[0] for r in rows}
        self.assertIn("A", stations)
        self.assertNotIn("B", stations)
        self.assertNotIn("C", stations)
        self.assertAlmostEqual(8.0, max(r[3] for r in rows if r[0] == "A"))
        self.assertAlmostEqual(90.0, max(r[4] for r in rows if r[0] == "A"))
        self.assertEqual({"storm_event"}, {r[5] for r in rows})


if __name__ == "__main__":
    unittest.main()
