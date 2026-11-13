"""
Download GOES-17 GLM lightning flashes for California.

Data: NOAA GOES-17 GLM Level 2 (GLM-L2-LCFA), public AWS bucket, no login needed.
Each file covers 20 seconds, so one hour is about 180 files.

Default: Aug 16, 2020 (day-of-year 229), 09:00-14:59 UTC, the start of the
August 2020 lightning siege.

Usage:
    python3.14 get_glm.py
    python3.14 get_glm.py --day 229 --hours 9 10 11 12 13 14
Output: glm_aug2020.csv  (columns: flash_time, lat, lon, energy, area)
"""
import argparse
import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import s3fs
import xarray as xr

# California bounding box
LAT_MIN, LAT_MAX = 32.5, 42.0
LON_MIN, LON_MAX = -124.5, -114.0

parser = argparse.ArgumentParser()
parser.add_argument("--year", type=int, default=2020)
parser.add_argument("--day", type=int, default=229, help="Day of year (229 = Aug 16, 2020)")
parser.add_argument("--hours", type=int, nargs="+", default=[9, 10, 11, 12, 13, 14],
                    help="UTC hours to download")
parser.add_argument("--out", default="glm_aug2020.csv")
args = parser.parse_args()

fs = s3fs.S3FileSystem(anon=True)
tmpdir = tempfile.mkdtemp()


def process(key):
    local = os.path.join(tmpdir, os.path.basename(key))
    try:
        fs.get(key, local)
        with xr.open_dataset(local) as ds:
            df = pd.DataFrame({
                "flash_time": pd.to_datetime(ds["flash_time_offset_of_first_event"].values),
                "lat": ds["flash_lat"].values,
                "lon": ds["flash_lon"].values,
                "energy": ds["flash_energy"].values,
                "area": ds["flash_area"].values,
            })
        return df[df.lat.between(LAT_MIN, LAT_MAX) & df.lon.between(LON_MIN, LON_MAX)]
    except Exception as e:
        print(f"  skipped {os.path.basename(key)}: {e}")
        return pd.DataFrame()
    finally:
        if os.path.exists(local):
            os.remove(local)


start = time.time()
keys = []
for h in args.hours:
    prefix = f"noaa-goes17/GLM-L2-LCFA/{args.year}/{args.day:03d}/{h:02d}/"
    hour_keys = fs.ls(prefix)
    print(f"Hour {h:02d} UTC: {len(hour_keys)} files")
    keys += hour_keys

print(f"Downloading {len(keys)} files...")
frames = []
with ThreadPoolExecutor(max_workers=16) as pool:
    for i, df in enumerate(pool.map(process, keys), 1):
        frames.append(df)
        if i % 100 == 0:
            print(f"  {i}/{len(keys)} files done")

flashes = pd.concat(frames, ignore_index=True).sort_values("flash_time")
flashes.to_csv(args.out, index=False)
elapsed = time.time() - start

print("\n===== GLM SUMMARY =====")
print(f"Files downloaded:        {len(keys):,}")
print(f"Flashes in California:   {len(flashes):,}")
if len(flashes):
    print(f"Time range (UTC):        {flashes.flash_time.min()} to {flashes.flash_time.max()}")
print(f"Total time:              {elapsed / 60:.1f} min")
print(f"Saved to:                {args.out}")
