import pandas as pd

df = pd.read_csv("data/weather_clean.csv")

df["DATE"] = pd.to_datetime(df["DATE"])

result = (
    df
    .groupby("STATION")
    .agg(
        count=("temperature", "count"),
        min_temp=("temperature", "min"),
        max_temp=("temperature", "max"),
        avg_temp=("temperature", "mean"),
        max_wind=("wind_speed", "max")
    )
)

print(result)