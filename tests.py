import pandas as pd

df = pd.read_csv("data/exports/extraction_plan.csv")
print(df.columns.tolist())