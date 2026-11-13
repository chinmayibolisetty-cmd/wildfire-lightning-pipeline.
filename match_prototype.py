"""
First result: which new fires had a lightning flash nearby, shortly before?

1. A fire detection counts as NEW if no other detection fell within 2 km in the
   previous 7 days.
2. For each new fire, look for GLM flashes within D km and the prior T hours.

Usage:
    python3.14 match_prototype.py
    python3.14 match_prototype.py --d-km 10 --t-hours 48
Inputs:  firms_aug2020.csv, glm_aug2020.csv
Outputs: new_fires.csv, fire_flash_matches.csv
"""
import argparse
import time

import numpy as np
import pandas as pd
from scipy.spatial import cKDTree

parser = argparse.ArgumentParser()
parser.add_argument("--firms", default="firms_aug2020.csv")
parser.add_argument("--glm", default="glm_aug2020.csv")
parser.add_argument("--d-km", type=float, default=10.0, help="Max fire-flash distance (km)")
parser.add_argument("--t-hours", type=float, default=48.0, help="Max lookback from fire to flash (hours)")
parser.add_argument("--new-km", type=float, default=2.0, help="Radius for 'same fire' check (km)")
parser.add_argument("--new-days", type=float, default=7.0, help="Lookback for 'same fire' check (days)")
parser.add_argument("--start", default="2020-08-15", help="Only count new fires from this date on")
args = parser.parse_args()

LAT0 = np.radians(37.0)  # projection reference latitude for California


def to_km(lat, lon):
    """Equirectangular projection to km (accurate to a few % across California)."""
    return np.column_stack([np.asarray(lon) * 111.32 * np.cos(LAT0), np.asarray(lat) * 110.57])


t0 = time.time()

# ---- Fire detections ----
fires = pd.read_csv(args.firms)
fires["time"] = pd.to_datetime(fires["acq_date"]) + pd.to_timedelta(
    fires["acq_time"].astype(int).astype(str).str.zfill(4).str[:2].astype(int), unit="h") + \
    pd.to_timedelta(fires["acq_time"].astype(int).astype(str).str.zfill(4).str[2:].astype(int), unit="m")
if "confidence" in fires:
    fires = fires[fires["confidence"].astype(str).str.lower() != "l"]  # drop low-confidence
fires = fires.sort_values("time").reset_index(drop=True)

# ---- Step 1: find new fires ----
xy = to_km(fires.latitude, fires.longitude)
tree = cKDTree(xy)
times = fires["time"].values
new_window = np.timedelta64(int(args.new_days * 24 * 3600), "s")
neighbors = tree.query_ball_point(xy, r=args.new_km)
is_new = np.ones(len(fires), dtype=bool)
for i, nbrs in enumerate(neighbors):
    for j in nbrs:
        # an earlier detection (or an earlier pixel of the same overpass) within the window
        if j < i and times[i] - times[j] <= new_window:
            is_new[i] = False
            break
new_fires = fires[is_new & (fires["time"] >= pd.Timestamp(args.start))].copy()

# ---- Step 2: match flashes ----
glm = pd.read_csv(args.glm, parse_dates=["flash_time"]).sort_values("flash_time").reset_index(drop=True)
gtree = cKDTree(to_km(glm.lat, glm.lon))
gtimes = glm["flash_time"].values
gxy = to_km(glm.lat, glm.lon)
lookback = np.timedelta64(int(args.t_hours * 3600), "s")

new_xy = to_km(new_fires.latitude, new_fires.longitude)
matches = []
for (idx, fire), fxy, cands in zip(new_fires.iterrows(), new_xy, gtree.query_ball_point(new_xy, r=args.d_km)):
    if not cands:
        continue
    cands = np.array(cands)
    lag = fire["time"].to_datetime64() - gtimes[cands]
    ok = cands[(lag >= np.timedelta64(0, "s")) & (lag <= lookback)]
    if len(ok):
        dist = np.linalg.norm(gxy[ok] - fxy, axis=1)
        best = ok[np.argmin(dist)]
        matches.append({
            "fire_index": idx, "fire_time": fire["time"], "fire_lat": fire.latitude,
            "fire_lon": fire.longitude, "n_flashes": len(ok),
            "nearest_flash_km": round(float(dist.min()), 2),
            "nearest_flash_time": glm.at[best, "flash_time"],
            "lag_hours": round((fire["time"] - glm.at[best, "flash_time"]).total_seconds() / 3600, 1),
        })
matches = pd.DataFrame(matches)
new_fires.to_csv("new_fires.csv", index=False)
matches.to_csv("fire_flash_matches.csv", index=False)

# New fires that fall inside the GLM sample's coverage (can possibly match)
glm_start, glm_end = glm.flash_time.min(), glm.flash_time.max()
in_window = new_fires[(new_fires["time"] >= glm_start) & (new_fires["time"] <= glm_end + pd.Timedelta(hours=args.t_hours))]

print("\n===== MATCHING SUMMARY =====")
print(f"Parameters:   D = {args.d_km} km, T = {args.t_hours} h; 'new fire' = nothing within "
      f"{args.new_km} km in prior {args.new_days:g} days")
print(f"Fire detections (nominal/high confidence):  {len(fires):,}")
print(f"New fires since {args.start}:                {len(new_fires):,}")
print(f"GLM flashes in sample:                      {len(glm):,}  ({glm_start} to {glm_end} UTC)")
print(f"New fires within the flash sample's window: {len(in_window):,}")
print(f"New fires with >=1 candidate flash:          {len(matches):,}"
      + (f"  ({len(matches) / max(len(in_window), 1):.0%} of those in the window)" if len(in_window) else ""))
if len(matches):
    print(f"Median nearest-flash distance:              {matches.nearest_flash_km.median():.1f} km")
    print(f"Median fire-to-flash lag:                   {matches.lag_hours.median():.1f} h")
print(f"Run time:                                   {time.time() - t0:.1f} s")
print("Saved: new_fires.csv, fire_flash_matches.csv")
