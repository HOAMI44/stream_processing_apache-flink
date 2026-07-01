import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src" / "use_cases"), str(ROOT / "src")]

from pyflink.table import EnvironmentSettings, TableEnvironment

import uc01_temperature_stats as uc01
import uc02_data_quality as uc02
import uc03_temperature_change as uc03
import uc04_data_gaps as uc04
import uc05_temperature_deviation as uc05
import uc06_temperature_histogram as uc06
import uc07_climate_trend as uc07
import uc08_resort_recommendations as uc08
import uc09_low_visibility as uc09
import uc10_fire_risk as uc10


def reading(
    station,
    date,
    temp,
    name=None,
    dew=0.0,
    wind=5.0,
    visibility=10000,
    lat=60.0,
    lon=25.0,
):
    return {
        "station": station,
        "date": date,
        "latitude": lat,
        "longitude": lon,
        "name": name or station,
        "quality_control": "V020",
        "wind": {"speed_rate": wind},
        "visibility": {"distance_meters": visibility},
        "temperature": {"value_celsius": temp, "is_valid": temp is not None},
        "dew_point": {"value_celsius": dew, "is_valid": dew is not None},
        "sea_level_pressure": {"value_hpa": 1010.0},
    }


class FlinkUseCaseTests(unittest.TestCase):
    def query(self, records, sql, *, streaming=False):
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
            env = TableEnvironment.create(settings)
            env.execute_sql(f"""
CREATE TABLE weather_readings (
  station STRING,
  `date` STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  name STRING,
  quality_control STRING,
  wind ROW<speed_rate DOUBLE>,
  visibility ROW<distance_meters INT>,
  temperature ROW<value_celsius DOUBLE, is_valid BOOLEAN>,
  dew_point ROW<value_celsius DOUBLE, is_valid BOOLEAN>,
  sea_level_pressure ROW<value_hpa DOUBLE>,
  event_time AS TO_TIMESTAMP(REPLACE(`date`, 'T', ' ')),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' MINUTE
) WITH (
  'connector' = 'filesystem',
  'path' = '{path}',
  'format' = 'json'
)
""")
            return [tuple(row) for row in env.execute_sql(sql).collect()]

    def test_uc01_temperature_stats(self):
        rows = self.query(
            [
                reading("A", "1901-01-01T00:10:00", -5.0),
                reading("A", "1901-01-01T00:40:00", 5.0),
                reading("A", "1901-01-02T00:10:00", 15.0),
                reading("B", "1901-01-01T00:20:00", 20.0),
            ],
            uc01.QUERY,
        )

        got = {(r[0], r[1], str(r[2])): r[4:] for r in rows}
        self.assertEqual((2, -5.0, 5.0, 0.0), got[("A", "1h", "1901-01-01 00:00:00")])
        self.assertEqual((2, -5.0, 5.0, 0.0), got[("A", "24h", "1901-01-01 00:00:00")])
        self.assertEqual(
            (1, 15.0, 15.0, 15.0), got[("A", "24h", "1901-01-02 00:00:00")]
        )

    def test_uc02_data_quality(self):
        rows = self.query(
            [
                reading("A", "1901-01-01T00:00:00", 10.0),
                reading("A", "1901-01-01T00:10:00", 70.0),
                reading("A", "1901-01-01T00:20:00", None),
                reading("B", "1901-01-01T00:00:00", 5.0, wind=80.0),
                reading("B", "1901-01-01T00:10:00", 6.0),
            ],
            uc02.QUERY,
        )

        full_windows = {r[0]: r for r in rows if str(r[1]) == "1901-01-01 00:00:00"}
        self.assertEqual((3, 2), full_windows["A"][3:5])
        self.assertAlmostEqual(2 / 3, full_windows["A"][5])
        self.assertEqual((2, 1, 0.5), full_windows["B"][3:6])

    def test_uc03_temperature_change(self):
        rows = self.query(
            [
                reading("A", "1901-01-01T00:00:00", 0.0),
                reading("A", "1901-01-01T02:00:00", 4.0),
                reading("A", "1901-01-01T03:00:00", 1.0),
                reading("B", "1901-01-01T00:00:00", 10.0),
                reading("B", "1901-01-01T01:00:00", 13.0),
            ],
            uc03.QUERY,
        )

        got = {(r[0], str(r[1])): r[5] for r in rows}
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
            uc04.QUERY,
        )

        got = {(r[0], str(r[2])): r[3:5] for r in rows}
        self.assertEqual((1, "complete"), got[("A", "1901-01-01 01:00:00")])
        self.assertEqual((3, "incomplete"), got[("A", "1901-01-01 04:00:00")])
        self.assertEqual((2, "incomplete"), got[("B", "1901-01-01 02:00:00")])

    def test_uc05_temperature_deviation(self):
        rows = self.query(
            [
                reading("02907099999", "1901-01-01T00:00:00", 5.0),
                reading("02907099999", "1901-01-01T12:00:00", 7.0),
                reading("02950099999", "1901-01-01T00:00:00", 1.0),
            ],
            uc05.QUERY,
        )

        got = {
            r[0]: r
            for r in rows
            if str(r[1]) == "1901-01-01 00:00:00"
        }
        self.assertEqual((6.0, 3.0, 3.0), got["02907099999"][3:6])
        self.assertEqual((1.0, 4.0, -3.0), got["02950099999"][3:6])

    def test_uc06_temperature_histogram(self):
        rows = self.query(
            [
                reading("A", "1901-01-01T00:00:00", -1.0, name="North"),
                reading("A", "1901-01-01T01:00:00", 8.0, name="North"),
                reading("B", "1901-01-01T00:00:00", 18.0, name="South"),
                reading("B", "1901-01-02T00:00:00", 31.0, name="South"),
            ],
            uc06.QUERY,
        )

        got = {(r[0], str(r[1]), r[3]): r[4] for r in rows}
        self.assertEqual(1, got[("North", "1901-01-01 00:00:00", "freezing")])
        self.assertEqual(1, got[("North", "1901-01-01 00:00:00", "cold")])
        self.assertEqual(1, got[("South", "1901-01-02 00:00:00", "hot")])

    def test_uc07_climate_trend(self):
        rows = self.query(
            [
                reading("A", "1901-01-01T00:00:00", 0.0),
                reading("A", "1901-01-15T00:00:00", 10.0),
                reading("A", "1901-01-30T00:00:00", 20.0),
                reading("B", "1901-01-10T00:00:00", -5.0),
                reading("B", "1901-01-20T00:00:00", 5.0),
            ],
            uc07.QUERY,
        )

        full = {r[0]: r for r in rows if str(r[1]) == "1901-01-01 00:00:00"}
        self.assertEqual((10.0, 3), full["A"][3:5])
        self.assertEqual((0.0, 2), full["B"][3:5])

    def test_uc08_resort_recommendations(self):
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
            uc08.QUERY,
        )

        got = {r[0]: r for r in rows}
        self.assertEqual("go", got["02907099999"][4])
        self.assertEqual("warn", got["02950099999"][4])
        self.assertNotIn("X", got)

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
            uc09.QUERY,
        )

        got = {(r[0], str(r[1])): r for r in rows}
        self.assertEqual((6, 2), got[("A", "1901-01-01 00:00:00")][3:5])
        self.assertEqual((0, 1), got[("A", "1901-01-01 12:00:00")][3:5])
        self.assertEqual((0, 1), got[("B", "1901-01-01 01:00:00")][3:5])

    def test_uc10_fire_risk(self):
        rows = self.query(
            [
                reading(
                    "A", "1901-01-01T00:00:00", 30.0, name="Forest", dew=5.0, wind=10.0
                ),
                reading(
                    "A", "1901-01-01T00:30:00", 28.0, name="Forest", dew=8.0, wind=8.0
                ),
                reading(
                    "B", "1901-01-01T00:00:00", 5.0, name="Lake", dew=4.0, wind=1.0
                ),
            ],
            uc10.QUERY,
        )

        got = {r[0]: r for r in rows}
        self.assertEqual((200.5, "extreme"), got["Forest"][3:5])
        self.assertEqual((16.5, "low"), got["Lake"][3:5])


if __name__ == "__main__":
    unittest.main()
