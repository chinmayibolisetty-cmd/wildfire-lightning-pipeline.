import argparse
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree

p = argparse.ArgumentParser()
p.add_argument("--r-km", type=float, default=5.0, help="Max distance to a recorded fire (km)")
p.add_argument("--days", type=int, default=2, help="Max days between satellite and ground discovery")
p.add_argument("--start", default="2020-08-15")
p.add_argument("--end", default="2020-08-22")
a = p.parse_args()

LAT0 = np.radians(37.0)
def km(lat, lon):
    return np.column_stack([np.asarray(lon) * 111.32 * np.cos(LAT0), np.asarray(lat) * 110.57])

start, end = pd.Timestamp(a.start), pd.Timestamp(a.end)

# Ground truth: FPA FOD natural-cause fires discovered in the study window
fod = pd.read_csv("fpa_fod_ca_lightning.csv")
fod["date"] = pd.to_datetime(fod["FIRE_YEAR"].astype(str)) + pd.to_timedelta(fod["DISCOVERY_DOY"] - 1, unit="D")
fod = fod[(fod.date >= start) & (fod.date <= end)].reset_index(drop=True)

# Pipeline output
new = pd.read_csv("new_fires.csv", parse_dates=["time"]).rename(
    columns={"time": "fire_time", "latitude": "fire_lat", "longitude": "fire_lon"})
lit = pd.read_csv("fire_flash_matches.csv", parse_dates=["fire_time"])
new = new[(new.fire_time >= start) & (new.fire_time < end + pd.Timedelta(days=1))]
lit = lit[(lit.fire_time >= start) & (lit.fire_time < end + pd.Timedelta(days=1))]


def score(det):
    """Precision: share of detected fires near a recorded lightning fire.
       Recall: share of recorded lightning fires near a detected fire."""
    if len(det) == 0 or len(fod) == 0:
        return 0, 0.0, np.zeros(len(fod), bool)
    dxy, fxy = km(det.fire_lat, det.fire_lon), km(fod.LATITUDE, fod.LONGITUDE)
    ddate = det.fire_time.dt.normalize().values
    pairs = cKDTree(dxy).query_ball_point(fxy, r=a.r_km)
    tol = np.timedelta64(a.days, "D")
    det_hit = np.zeros(len(det), bool)
    fod_hit = np.zeros(len(fod), bool)
    for i, cands in enumerate(pairs):
        for j in cands:
            if abs(ddate[j] - fod.date.values[i]) <= tol:
                det_hit[j] = fod_hit[i] = True
    return int(det_hit.sum()), det_hit.mean(), fod_hit


n_all, prec_all, hit_all = score(new)
n_lit, prec_lit, hit_lit = score(lit)
big = (fod["FIRE_SIZE"] >= 10).values if "FIRE_SIZE" in fod else np.ones(len(fod), bool)


def f1(pr, rc):
    return 2 * pr * rc / (pr + rc) if pr + rc else 0.0


rc_lit = hit_lit.mean() if len(fod) else 0.0
print("\n===== VALIDATION SUMMARY =====")
print(f"Match rule: within {a.r_km:g} km and {a.days} days of a recorded fire; window {a.start} to {a.end}")
print(f"FPA FOD lightning fires in window:      {len(fod):,}  ({big.sum():,} were 10+ acres)")
print()
print(f"{'':38}{'All new fires':>15}{'Lightning-matched':>20}")
print(f"{'Fires flagged by pipeline':38}{len(new):>15,}{len(lit):>20,}")
print(f"{'Precision (near a recorded fire)':38}{prec_all:>15.0%}{prec_lit:>20.0%}")
print(f"{'Recall, all recorded fires':38}{hit_all.mean():>15.0%}{rc_lit:>20.0%}")
print(f"{'Recall, fires 10+ acres':38}{hit_all[big].mean() if big.any() else 0:>15.0%}"
      f"{hit_lit[big].mean() if big.any() else 0:>20.0%}")
print(f"{'F1 score':38}{f1(prec_all, hit_all.mean()):>15.2f}{f1(prec_lit, rc_lit):>20.2f}")

out = fod.assign(found_by_pipeline=hit_lit)
cols = [c for c in ["FIRE_NAME", "COMPLEX_NAME", "date", "FIRE_SIZE", "LATITUDE", "LONGITUDE", "found_by_pipeline"] if c in out]
out[cols].to_csv("validation_fires.csv", index=False)
print("\nSaved: validation_fires.csv (each recorded fire and whether the pipeline found it)")
