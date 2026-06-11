import pandas as pd
from pathlib import Path

INPUT_DIR = Path("data/input")
OUTPUT_DIR = Path("data/output")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

all_dataframes = []


def parse_temperature(tmp_value):
    try:
        temp = str(tmp_value).split(",")[0]

        if temp in ("+9999", "-9999"):
            return None

        return int(temp) / 10

    except Exception:
        return None


def parse_wind_speed(wnd_value):
    try:
        parts = str(wnd_value).split(",")

        speed = parts[3]

        if speed == "9999":
            return None

        return int(speed) / 10

    except Exception:
        return None


for csv_file in INPUT_DIR.glob("*.csv"):

    print(f"In processing: {csv_file.name}")

    try:
        df = pd.read_csv(csv_file)

        df["temperature"] = df["TMP"].apply(parse_temperature)
        df["wind_speed"] = df["WND"].apply(parse_wind_speed)

        clean_df = (
    df[
        [
            "STATION",
            "DATE",
            "LATITUDE",
            "LONGITUDE",
            "temperature",
            "wind_speed",
        ]
    ]
    .rename(
        columns={
            "STATION": "station_id",
            "DATE": "event_time",
            "LATITUDE": "latitude",
            "LONGITUDE": "longitude",
        }
    )
)

        clean_df = clean_df.dropna()

        all_dataframes.append(clean_df)

    except Exception as e:
        print(f"  Fehler bei {csv_file.name}: {e}")


# Alle Stationen zusammenführen
if all_dataframes:

    combined_df = pd.concat(
        all_dataframes,
        ignore_index=True
    )

    combined_file = OUTPUT_DIR / "weather_clean.csv"

    combined_df.to_csv(
        combined_file,
        index=False
    )

    print("\nGesamtausgabe erstellt:")
    print(combined_file)

    print(
        f"Count of stations: "
        f"{combined_df['station_id'].nunique()}"
    )

    print(
        f"Count of records: "
        f"{len(combined_df)}"
    )

print("\nFertig.")