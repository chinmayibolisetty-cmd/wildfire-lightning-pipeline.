"""
Extract ground truth from FPA FOD (6th edition, 1992-2020).

1. Download the SQLite or GeoPackage version of FPA FOD 6th edition from
   https://www.fs.usda.gov/rds/archive/catalog/RDS-2013-0009.6
   and unzip it into this folder.
2. Run:
   python3.14 get_fpa_fod.py PATH_TO_FILE.sqlite      (or .gpkg)

Keeps California, 2019-2020, natural (lightning) causes.
Output: fpa_fod_ca_lightning.csv
"""
import sqlite3
import sys

import pandas as pd

if len(sys.argv) < 2:
    sys.exit("Usage: python3.14 get_fpa_fod.py PATH_TO_FILE.sqlite")

con = sqlite3.connect(sys.argv[1])
tables = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")]
table = next((t for t in tables if t.lower() == "fires"), None) or \
        next((t for t in tables if "fire" in t.lower() and not t.lower().startswith(("gpkg", "rtree"))), None)
if table is None:
    sys.exit(f"Could not find the fires table. Tables in file: {tables}")
print(f"Reading table '{table}'...")

df = pd.read_sql(f'SELECT * FROM "{table}" WHERE STATE = \'CA\' AND FIRE_YEAR IN (2019, 2020)', con)
print(f"California fires 2019-2020: {len(df):,}")

cause_col = next((c for c in ["NWCG_GENERAL_CAUSE", "NWCG_CAUSE_CLASSIFICATION",
                              "STAT_CAUSE_DESCR"] if c in df.columns), None)
mask = df[cause_col].astype(str).str.contains("Natural|Lightning", case=False)
lightning = df[mask].copy()

keep = [c for c in ["FOD_ID", "FIRE_NAME", "COMPLEX_NAME", "FIRE_YEAR", "DISCOVERY_DATE",
                    "DISCOVERY_DOY", "DISCOVERY_TIME", "FIRE_SIZE", "FIRE_SIZE_CLASS",
                    "LATITUDE", "LONGITUDE", cause_col, "NWCG_REPORTING_AGENCY"]
        if c in lightning.columns]
lightning = lightning[keep]
lightning.to_csv("fpa_fod_ca_lightning.csv", index=False)

aug2020 = lightning[(lightning.FIRE_YEAR == 2020) & lightning.DISCOVERY_DOY.between(227, 238)]
complexes = ["AUGUST", "SCU", "LNU", "CZU", "SQF", "NORTH"]
name_text = (aug2020.get("FIRE_NAME", pd.Series(dtype=str)).astype(str) + " " +
             aug2020.get("COMPLEX_NAME", pd.Series(dtype=str)).astype(str)).str.upper()

print("\n===== FPA FOD SUMMARY =====")
print(f"Cause column used:                     {cause_col}")
print(f"CA natural/lightning fires 2019-2020:  {len(lightning):,}")
print(f"  ...2019:                             {(lightning.FIRE_YEAR == 2019).sum():,}")
print(f"  ...2020:                             {(lightning.FIRE_YEAR == 2020).sum():,}")
print(f"Discovered Aug 15-26, 2020:            {len(aug2020):,}")
for c in complexes:
    print(f"  records mentioning '{c}':".ljust(40) + f"{name_text.str.contains(c).sum():,}")
print("Saved to: fpa_fod_ca_lightning.csv")
