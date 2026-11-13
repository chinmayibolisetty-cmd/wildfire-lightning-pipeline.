"""
Download NASA FIRMS VIIRS fire detections for California.

1. Get a free MAP_KEY (arrives by email in minutes):
   https://firms.modaps.eosdis.nasa.gov/api/map_key/
2. Run:
   python3.14 get_firms.py YOUR_MAP_KEY

Default range: Aug 8 - Aug 25, 2020. The week before Aug 15 is history, so the
matching step can tell which fires are new.
Output: firms_aug2020.csv
"""
import io
import sys
import time
import urllib.request
from datetime import date, timedelta

import pandas as pd

if len(sys.argv) < 2:
    sys.exit("Usage: python3.14 get_firms.py YOUR_MAP_KEY")

MAP_KEY = sys.argv[1]
SOURCE = "VIIRS_SNPP_SP"          # VIIRS S-NPP, standard processing (archive)
AREA = "-124.5,32.5,-114,42"      # west,south,east,north (California)
START, END = date(2020, 8, 8), date(2020, 8, 25)
CHUNK = 5                          # days per request (API limit)
OUT = "firms_aug2020.csv"

start_time = time.time()
frames = []
d = START
while d <= END:
    days = min(CHUNK, (END - d).days + 1)
    url = (f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/"
           f"{MAP_KEY}/{SOURCE}/{AREA}/{days}/{d.isoformat()}")
    print(f"Requesting {d} (+{days} days)...")
    with urllib.request.urlopen(url, timeout=120) as resp:
        text = resp.read().decode()
    if not text.startswith("latitude"):
        sys.exit(f"FIRMS returned an error instead of data:\n{text[:300]}")
    df = pd.read_csv(io.StringIO(text))
    print(f"  {len(df):,} detections")
    frames.append(df)
    d += timedelta(days=days)

fires = pd.concat(frames, ignore_index=True).drop_duplicates()
fires.to_csv(OUT, index=False)

print("\n===== FIRMS SUMMARY =====")
print(f"Source:                  {SOURCE}")
print(f"Date range:              {START} to {END}")
print(f"Fire detections:         {len(fires):,}")
if "confidence" in fires:
    print("By confidence:           " + ", ".join(
        f"{k}={v:,}" for k, v in fires["confidence"].value_counts().items()))
print(f"Total time:              {time.time() - start_time:.1f} s")
print(f"Saved to:                {OUT}")
