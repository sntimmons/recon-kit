import sqlite3
import pandas as pd
from pathlib import Path

AUDIT_DIR = Path("audit")
DB_PATH = AUDIT_DIR / "audit.db"

con = sqlite3.connect(DB_PATH)
df = pd.read_sql("SELECT * FROM matched_pairs_raw", con)
con.close()

print(f"Original row count: {len(df)}")

for multiplier in [2, 4, 8, 16, 32]:
    scaled = pd.concat([df] * multiplier, ignore_index=True)
    out_path = AUDIT_DIR / f"matched_pairs_raw_{multiplier}x.csv"
    scaled.to_csv(out_path, index=False)
    print(f"{multiplier}x row count: {len(scaled)}  wrote: {out_path}")
