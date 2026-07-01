import csv
from io import StringIO

NOAA_COLUMNS = [
    "STATION",
    "DATE",
    "SOURCE",
    "LATITUDE",
    "LONGITUDE",
    "ELEVATION",
    "NAME",
    "REPORT_TYPE",
    "CALL_SIGN",
    "QUALITY_CONTROL",
    "WND",
    "CIG",
    "VIS",
    "TMP",
    "DEW",
    "SLP",
]


def parse_csv_line(line):
    return next(csv.reader(StringIO(line)))


def number(value):
    if value in ("", "99999", "999999", "+9999", "9999"):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def signed_tenths(value):
    if value in ("", "+9999", "9999", "-9999"):
        return None
    try:
        return int(value) / 10
    except ValueError:
        return None


def split(value):
    return value.split(",") if value else []


def wind(raw):
    parts = split(raw)
    angle = None if len(parts) < 1 or parts[0] == "999" else number(parts[0])
    speed = None if len(parts) < 4 or parts[3] == "9999" else number(parts[3])
    return {
        "direction_angle": int(angle) if angle is not None else None,
        "direction_quality": parts[1] if len(parts) > 1 else None,
        "type": parts[2] if len(parts) > 2 else None,
        "speed_rate": speed / 10 if speed is not None else None,
        "speed_quality": parts[4] if len(parts) > 4 else None,
    }


def temperature(raw):
    parts = split(raw)
    value = signed_tenths(parts[0]) if parts else None
    quality = parts[1] if len(parts) > 1 else None
    return {
        "value_celsius": value,
        "quality": quality,
        "is_valid": value is not None,
    }


def ceiling(raw):
    parts = split(raw)
    height = None if not parts or parts[0] == "99999" else number(parts[0])
    return {
        "height_meters": int(height) if height is not None else None,
        "quality": parts[1] if len(parts) > 1 else None,
        "determination": parts[2] if len(parts) > 2 else None,
        "cavok": parts[3] if len(parts) > 3 else None,
    }


def visibility(raw):
    parts = split(raw)
    distance = None if not parts or parts[0] == "999999" else number(parts[0])
    return {
        "distance_meters": int(distance) if distance is not None else None,
        "quality": parts[1] if len(parts) > 1 else None,
        "variability": parts[2] if len(parts) > 2 else None,
        "variability_quality": parts[3] if len(parts) > 3 else None,
    }


def pressure(raw):
    parts = split(raw)
    value = None if not parts or parts[0] == "99999" else number(parts[0])
    return {
        "value_hpa": value / 10 if value is not None else None,
        "quality": parts[1] if len(parts) > 1 else None,
        "is_valid": value is not None,
    }


def normalize_raw_event(event):
    values = parse_csv_line(event.get("raw_line", ""))
    record = dict(zip(NOAA_COLUMNS, values))

    return {
        "station": record.get("STATION"),
        "date": record.get("DATE"),
        "source": record.get("SOURCE"),
        "latitude": number(record.get("LATITUDE", "")),
        "longitude": number(record.get("LONGITUDE", "")),
        "elevation": number(record.get("ELEVATION", "")),
        "name": record.get("NAME"),
        "report_type": record.get("REPORT_TYPE"),
        "call_sign": record.get("CALL_SIGN"),
        "quality_control": record.get("QUALITY_CONTROL"),
        "wind": wind(record.get("WND", "")),
        "ceiling": ceiling(record.get("CIG", "")),
        "visibility": visibility(record.get("VIS", "")),
        "temperature": temperature(record.get("TMP", "")),
        "dew_point": temperature(record.get("DEW", "")),
        "sea_level_pressure": pressure(record.get("SLP", "")),
        "columns": {k.lower(): v for k, v in record.items()},
    }


def _demo():
    event = {
        "source_file": "data/1901/02907099999.csv",
        "row_number": 2,
        "raw_line": '"02907099999","1901-01-01T06:00:00","4","64.3333333","23.45","5.0","KALAJOKI ULKOKALLA, FI","FM-12","99999","V020","270,1,N,0159,1","99999,9,9,N","000000,1,N,9","-0078,1","+9999,9","10200,1","08,99,1,99,9,99,9,99999,9,99,9,99,9",""',
    }
    out = normalize_raw_event(event)
    assert out["station"] == "02907099999"
    assert out["temperature"]["value_celsius"] == -7.8
    assert out["dew_point"]["is_valid"] is False
    assert out["wind"]["speed_rate"] == 15.9
    assert "sky_condition" not in out
    assert "gf1" not in out["columns"]


if __name__ == "__main__":
    _demo()
