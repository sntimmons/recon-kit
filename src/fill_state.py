import pandas as pd

path = "outputs/mapped_unmatched_new.csv"
df = pd.read_csv(path)

# Fill blanks only (handles real NaN AND empty strings)
mask = df["location_state"].isna() | (df["location_state"].astype(str).str.strip() == "")

state = (
    df.loc[mask, "address"]
      .astype(str)
      .str.strip()
      .str.extract(r"\b([A-Z]{2})\s*$", expand=False)
)

df.loc[mask, "location_state"] = state.str.lower()

df.to_csv(path, index=False)

print("rows:", len(df))
print("filled candidates:", int(mask.sum()))
print("still blank location_state:", int((df["location_state"].isna() | (df["location_state"].astype(str).str.strip() == "")).sum()))
print(df["location_state"].value_counts().head(10))
