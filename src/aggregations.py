import pandas as pd
import pandas as pd
df = pd.read_csv("data/output/weather_clean.csv")

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
    .reset_index()
)





print(df["STATION"].nunique())
print(len(df))
print(df.head())

print(result)