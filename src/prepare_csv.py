import pandas as pd

df = pd.read_csv("data/data.csv")

# Temperatur extrahieren
df["temperature"] = (
    df["TMP"]
    .str.split(",")
    .str[0]
    .astype(int)
    / 10
)

# Windgeschwindigkeit extrahieren
df["wind_speed"] = (
    df["WND"]
    .str.split(",")
    .str[3]
    .astype(int)
    / 10
)

# Nur die Spalten behalten, die du für Flink brauchst
clean_df = df[
    [
        "STATION",
        "DATE",
        "LATITUDE",
        "LONGITUDE",
        "temperature",
        "wind_speed"
    ]
]

# Neue CSV speichern
clean_df.to_csv(
    "data/weather_clean.csv",
    index=False
)

print("CSV gespeichert:")
print("../data/weather_clean.csv")

print("\nSpalten:")
print(clean_df.columns)

print("\nErste Zeilen:")
print(clean_df.head())