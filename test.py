import pandas as pd
import glob

files = glob.glob("data/bronze/*/*.parquet")

df = pd.concat([pd.read_parquet(f) for f in files[:5]])  # sample

print(df[['pickup_datetime']].head(20))