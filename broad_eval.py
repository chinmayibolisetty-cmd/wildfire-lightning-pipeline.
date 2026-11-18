"""
Broader evaluation: a longer window, fire perimeters as a second ground truth,
and a check of the famous August 2020 lightning complexes.

Part 1  Extended window (Aug 15-25 instead of Aug 15-22) against FPA FOD, overall and
        by discovery day. Lightning matching uses every GLM file it finds; fires whose
        48 h lookback is not fully covered by GLM data are reported as "no GLM data",
        not as misses.
Part 2  Fire perimeters (needs fire_perimeters_ca2020.geojson from get_nifc.py; CAL FIRE by default).
        FPA FOD stores one point per fire (its origin), so a detection on the far side
        of a big complex can be "wrong" by the 5 km rule even though it is the right
        fire. Perimeters fix that: a detection counts if it falls inside a perimeter
        (or within --buffer-km of its edge).
        A perimeter counts as a LIGHTNING fire if its recorded cause is lightning (CAL FIRE
        cause code 1). Where no cause is recorded (the NIFC layer has none), it counts if
        its name contains "LIGHTNING" or a siege complex name, or if it contains the origin
        of an FPA FOD natural-cause fire discovered in the window. Where an alarm date is
        recorded, only fires that started in the window (+-days) count for precision and recall.
Part 3  Famous complexes (August, LNU, SCU, CZU): when the pipeline first saw them as
        new fires, and whether those detections were matched to lightning.

Same rules as the rest of the project: new fire = nothing within 5 km in the prior
7 days; lightning match = a GLM flash within 5 km and 48 h before the detection.
This script does its own matching and does not overwrite new_fires.csv or
fire_flash_matches.csv.

Usage:
    python broad_eval.py
    # optional, for lightning after Aug 20 (~1 h):
    for d in 234 235 236 237 238; do python get_glm.py --day $d --hours {0..23} --out glm_day$d.csv; done
    # optional, for Part 2 and full Part 3:
    python get_nifc.py
Outputs: broad_eval_by_day.csv, broad_eval_summary.csv, famous_fires.csv,
         perimeter_results.csv (if perimeters are available)
"""
import argparse
import glob
import json
import os
import re

import numpy as np
import pandas as pd
from matplotlib.path import Path
from scipy.spatial import cKDTree

p = argparse.ArgumentParser()
p.add_argument("--firms", default="firms_aug2020.csv")
p.add_argument("--glm", nargs="+", default=["glm_aug2020.csv", "glm_day*.csv"],
               help="GLM CSV files or patterns (duplicates are removed)")
p.add_argument("--fod", default="fpa_fod_ca_lightning.csv")
p.add_argument("--perimeters", default="fire_perimeters_ca2020.geojson")
p.add_argument("--start", default="2020-08-15")
p.add_argument("--end", default="2020-08-25", help="Last day (inclusive)")
p.add_argument("--d-km", type=float, default=5.0)
p.add_argument("--t-hours", type=float, default=48.0)
p.add_argument("--new-km", type=float, default=5.0)
p.add_argument("--new-days", type=float, default=7.0)
p.add_argument("--r-km", type=float, default=5.0, help="FPA FOD match distance (km)")
p.add_argument("--days", type=int, default=2, help="FPA FOD match tolerance (days)")
p.add_argument("--buffer-km", type=float, default=1.0, help="Perimeter edge tolerance (km)")
p.add_argument("--min-acres", type=float, default=1000, help="Perimeter recall: only fires at least this big")
a = p.parse_args()

LAT0 = np.radians(37.0)
START, END = pd.Timestamp(a.start), pd.Timestamp(a.end)
END_X = END + pd.Timedelta(days=1)            # exclusive end for timestamps
COMPLEXES = ["AUGUST", "LNU", "SCU", "CZU"]


def km(lat, lon):
    return np.column_stack([np.asarray(lon, float) * 111.32 * np.cos(LAT0), np.asarray(lat, float) * 110.57])


def pct(x):
    return f"{x:.0%}" if x == x else "n/a"   # x == x is False for NaN


# ---------------------------------------------------------------------------
# New fires and lightning matches (same rules as match_prototype.py)
# ---------------------------------------------------------------------------
fires = pd.read_csv(a.firms)
hhmm = fires["acq_time"].astype(int).astype(str).str.zfill(4)
fires["time"] = pd.to_datetime(fires["acq_date"]) + pd.to_timedelta(hhmm.str[:2].astype(int), unit="h") \
    + pd.to_timedelta(hhmm.str[2:].astype(int), unit="m")
fires = fires[fires["confidence"].astype(str).str.lower() != "l"]
fires = fires.sort_values("time").reset_index(drop=True)   # same (default) sort as match_prototype.py
xy = km(fires.latitude, fires.longitude)
times = fires["time"].values
window = np.timedelta64(int(a.new_days * 86400), "s")
is_new = np.ones(len(fires), bool)
for i, nb in enumerate(cKDTree(xy).query_ball_point(xy, r=a.new_km)):
    for j in nb:
        if j < i and times[i] - times[j] <= window:
            is_new[i] = False
            break
new = fires[is_new & (fires.time >= START) & (fires.time < END_X)].copy()

glm_files = sorted({f for pat in a.glm for f in glob.glob(pat)})
glm = pd.concat([pd.read_csv(f, parse_dates=["flash_time"]) for f in glm_files if os.path.getsize(f) > 0],
                ignore_index=True)
glm = glm.drop_duplicates(["flash_time", "lat", "lon"]).sort_values("flash_time").reset_index(drop=True)
# Days with GLM data: any day with a flash, plus days named in glm_dayNNN.csv files (quiet days count)
glm_days = set(glm.flash_time.dt.normalize())
for f in glm_files:
    m = re.search(r"glm_day(\d{3})", f)
    if m:
        glm_days.add(pd.Timestamp("2020-01-01") + pd.Timedelta(days=int(m.group(1)) - 1))


def covered(t):
    """Is the whole T-hour lookback before time t inside days that have GLM data?"""
    d0, d1 = (t - pd.Timedelta(hours=a.t_hours)).normalize(), t.normalize()
    return all(d in glm_days for d in pd.date_range(d0, d1))


gxy = km(glm.lat, glm.lon)
gtree, gt = cKDTree(gxy), glm.flash_time.values
look = np.timedelta64(int(a.t_hours * 3600), "s")
nxy = km(new.latitude, new.longitude)
new["glm_covered"] = [covered(t) for t in new.time]
new["lightning"] = False
new["nearest_flash_km"] = np.nan
new["lag_hours"] = np.nan
for k, (c, fxy, t) in enumerate(zip(gtree.query_ball_point(nxy, r=a.d_km), nxy, new.time.values)):
    if not c:
        continue
    c = np.array(c)
    lag = t - gt[c]
    ok = c[(lag >= np.timedelta64(0, "s")) & (lag <= look)]
    if len(ok):
        d = np.linalg.norm(gxy[ok] - fxy, axis=1)
        b = ok[np.argmin(d)]
        new.iloc[k, new.columns.get_loc("lightning")] = True
        new.iloc[k, new.columns.get_loc("nearest_flash_km")] = round(float(d.min()), 2)
        new.iloc[k, new.columns.get_loc("lag_hours")] = round((t - gt[b]) / np.timedelta64(1, "h"), 1)
new["day"] = new.time.dt.normalize()

# ---------------------------------------------------------------------------
# Part 1: FPA FOD, extended window
# ---------------------------------------------------------------------------
fod_all = pd.read_csv(a.fod)
fod_all["date"] = pd.to_datetime(fod_all["FIRE_YEAR"].astype(str)) + pd.to_timedelta(fod_all["DISCOVERY_DOY"] - 1, unit="D")


def fod_score(det, fod):
    """Same rule as validate.py: within r_km and +-days of a recorded fire."""
    det_hit, fod_hit = np.zeros(len(det), bool), np.zeros(len(fod), bool)
    if len(det) and len(fod):
        pairs = cKDTree(km(det.latitude, det.longitude)).query_ball_point(km(fod.LATITUDE, fod.LONGITUDE), r=a.r_km)
        dd, fd = det.day.values, fod.date.values
        for i, c in enumerate(pairs):
            for j in c:
                if abs(dd[j] - fd[i]) <= np.timedelta64(a.days, "D"):
                    det_hit[j] = fod_hit[i] = True
    return det_hit, fod_hit


def f1(pr, rc):
    return 2 * pr * rc / (pr + rc) if pr + rc else 0.0


summary = []


def evaluate(label, s, e):
    fod = fod_all[fod_all.date.between(s, e)].reset_index(drop=True)
    det = new[(new.time >= s) & (new.time < e + pd.Timedelta(days=1))]
    lit = det[det.lightning]
    cov = det[det.glm_covered]
    h_all, r_all = fod_score(det, fod)
    h_lit, r_lit = fod_score(lit, fod)
    big = (fod.FIRE_SIZE >= 10).to_numpy()
    row = {"window": label, "fod_fires": len(fod), "fod_fires_10plus_acres": int(big.sum()),
           "new_fires": len(det), "new_fires_with_glm": len(cov), "lightning_matched": len(lit),
           "precision_all": h_all.mean() if len(det) else np.nan,
           "precision_lightning": h_lit.mean() if len(lit) else np.nan,
           "recall_all": r_all.mean() if len(fod) else np.nan,
           "recall_lightning": r_lit.mean() if len(fod) else np.nan,
           "recall_all_10plus": r_all[big].mean() if big.any() else np.nan,
           "recall_lightning_10plus": r_lit[big].mean() if big.any() else np.nan,
           "match_rate_with_glm": cov.lightning.mean() if len(cov) else np.nan}
    row["f1_all"] = f1(row["precision_all"], row["recall_all"])
    row["f1_lightning"] = f1(np.nan_to_num(row["precision_lightning"]), row["recall_lightning"])
    summary.append(row)
    return fod


evaluate(f"{START:%b %d}-Aug 22 (original)", START, pd.Timestamp("2020-08-22"))
fod_win = evaluate(f"{START:%b %d}-{END:%b %d} (extended)", START, END)
evaluate("Aug 23-" + f"{END:%d} (new days only)", pd.Timestamp("2020-08-23"), END)
summ = pd.DataFrame(summary)
summ.round(3).to_csv("broad_eval_summary.csv", index=False)

by_day = []
for d in pd.date_range(START, END):
    det = new[new.day == d]
    fod_d = fod_all[fod_all.date == d]
    _, r_all = fod_score(new[(new.day >= d - pd.Timedelta(days=a.days)) & (new.day <= d + pd.Timedelta(days=a.days))], fod_d)
    _, r_lit = fod_score(new[new.lightning & (new.day >= d - pd.Timedelta(days=a.days))
                             & (new.day <= d + pd.Timedelta(days=a.days))], fod_d)
    by_day.append({"day": d.date(), "full_glm_lookback": all(x in glm_days for x in pd.date_range(d - pd.Timedelta(hours=a.t_hours), d).normalize()),
                   "glm_flashes": int((glm.flash_time.dt.normalize() == d).sum()),
                   "new_fires": len(det), "lightning_matched": int(det.lightning.sum()),
                   "fod_fires": len(fod_d), "fod_found_any": int(r_all.sum()), "fod_found_lightning": int(r_lit.sum())})
by_day = pd.DataFrame(by_day)
by_day.to_csv("broad_eval_by_day.csv", index=False)

# ---------------------------------------------------------------------------
# Part 2: fire perimeters
# ---------------------------------------------------------------------------
perims = None
if os.path.exists(a.perimeters):
    feats = json.load(open(a.perimeters))["features"]
    rows = []
    for f in feats:
        g = f["geometry"]
        polys = [g["coordinates"]] if g["type"] == "Polygon" else g["coordinates"]
        polys = [[km(np.array(r)[:, 1], np.array(r)[:, 0]) for r in poly] for poly in polys if len(poly)]
        area = sum(abs(0.5 * np.sum(r[:-1, 0] * r[1:, 1] - r[1:, 0] * r[:-1, 1])) * (1 if i == 0 else -1)
                   for poly in polys for i, r in enumerate(poly))
        allpts = np.vstack([r for poly in polys for r in poly])
        pr = f["properties"]
        rows.append({"name": pr.get("name") or "", "cause": pr.get("cause"), "alarm_date": pr.get("alarm_date"),
                     "polys": polys, "acres": area * 247.105,
                     "bbox": (*allpts.min(0), *allpts.max(0))})
    perims = pd.DataFrame(rows)

    def contains(perim, pts, buf):
        """Points inside the perimeter (holes excluded) or within buf km of its edge."""
        x0, y0, x1, y1 = perim["bbox"]
        out = np.zeros(len(pts), bool)
        near_box = (pts[:, 0] >= x0 - buf) & (pts[:, 0] <= x1 + buf) & (pts[:, 1] >= y0 - buf) & (pts[:, 1] <= y1 + buf)
        if not near_box.any():
            return out
        q = pts[near_box]
        hit = np.zeros(len(q), bool)
        for poly in perim["polys"]:
            inside = Path(poly[0]).contains_points(q)
            for hole in poly[1:]:
                inside &= ~Path(hole).contains_points(q)
            hit |= inside
            if buf > 0:
                for ring in poly:     # distance to every edge segment
                    s0, s1 = ring[:-1], ring[1:]
                    v = s1 - s0
                    L2 = np.maximum((v ** 2).sum(1), 1e-12)
                    for k in np.where(~hit)[0]:
                        t = np.clip(((q[k] - s0) * v).sum(1) / L2, 0, 1)
                        if (np.hypot(*(s0 + t[:, None] * v - q[k]).T) <= buf).any():
                            hit[k] = True
        out[near_box] = hit
        return out

    fod_nat = fod_all[fod_all.date.between(START - pd.Timedelta(days=1), END + pd.Timedelta(days=1))]
    fod_pts = km(fod_nat.LATITUDE, fod_nat.LONGITUDE)
    perims["fod_origins"] = [int(contains(r, fod_pts, a.buffer_km).sum()) for _, r in perims.iterrows()]
    by_name = perims.name.str.contains("LIGHTNING") | perims.name.str.contains("|".join(COMPLEXES))
    cause = pd.to_numeric(perims.cause, errors="coerce")
    perims["lightning"] = np.where(cause.notna(), cause == 1, by_name | (perims.fod_origins > 0))
    alarm = pd.to_datetime(perims.alarm_date, errors="coerce")
    tol = pd.Timedelta(days=a.days)
    perims["in_window"] = alarm.isna() | alarm.between(START - tol, END + tol)  # no date -> can't exclude
    has_cause, has_date = int(cause.notna().sum()), int(alarm.notna().sum())

    inside = np.array([contains(r, nxy, a.buffer_km) for _, r in perims.iterrows()]) if len(perims) else np.zeros((0, len(new)), bool)
    new["in_perimeter"] = inside.any(0)
    rel = perims.in_window.to_numpy()
    new["in_window_perimeter"] = inside[rel].any(0)
    new["in_lightning_perimeter"] = inside[perims.lightning.to_numpy() & rel].any(0)
    ltn = new.lightning.to_numpy()
    perims["new_fires_inside"] = inside.sum(1)
    perims["lightning_matched_inside"] = (inside & ltn).sum(1)
    perims["first_new_fire"] = [new.time[m].min() if m.any() else pd.NaT for m in inside]
    perims["first_lightning_match"] = [new.time[m & ltn].min() if (m & ltn).any() else pd.NaT for m in inside]
    perims.drop(columns=["polys", "bbox"]).assign(acres=lambda d: d.acres.round()).sort_values(
        "acres", ascending=False).to_csv("perimeter_results.csv", index=False)

# ---------------------------------------------------------------------------
# Part 3: famous complexes
# ---------------------------------------------------------------------------
famous = []
for key in COMPLEXES:
    fod_c = fod_win[fod_win.COMPLEX_NAME.fillna("").str.upper().str.contains(key)]
    _, r_any = fod_score(new, fod_c)
    _, r_lit = fod_score(new[new.lightning], fod_c)
    row = {"complex": key, "fod_records": len(fod_c), "fod_acres": int(fod_c.FIRE_SIZE.sum()),
           "fod_found_as_new_fire": int(r_any.sum()), "fod_found_lightning": int(r_lit.sum()),
           "fod_acres_found_lightning": int(fod_c.FIRE_SIZE[r_lit].sum()),
           "fod_first_discovered": fod_c.date.min().date() if len(fod_c) else None}
    if perims is not None:
        m = perims.name.str.contains(key)
        hit = inside[m.to_numpy()].any(0) if m.any() else np.zeros(len(new), bool)
        det = new[hit]
        row.update({"perimeters": int(m.sum()), "perimeter_names": "; ".join(sorted(perims.name[m].unique())[:5]),
                    "perimeter_acres": int(perims.acres[m].sum()), "new_fires_inside": len(det),
                    "lightning_matched_inside": int(det.lightning.sum()),
                    "first_new_fire": det.time.min(), "first_lightning_match": det.time[det.lightning].min(),
                    "nearest_flash_km": det.nearest_flash_km.min(), "shortest_lag_h": det.lag_hours.min()})
    famous.append(row)
famous = pd.DataFrame(famous)
famous.to_csv("famous_fires.csv", index=False)

# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
pd.set_option("display.width", 220)
pd.set_option("display.max_columns", 30)
print("\n===== BROADER EVALUATION =====")
print(f"GLM files: {', '.join(glm_files)}  ->  {len(glm):,} flashes, days with data: "
      f"{min(glm_days):%b %d}-{max(glm_days):%b %d} ({len(glm_days)} days)")
print(f"New fires {START:%b %d}-{END:%b %d}: {len(new):,}; with full GLM lookback: {new.glm_covered.sum():,}; "
      f"lightning-matched: {new.lightning.sum():,}")

print("\n--- Part 1: FPA FOD, original vs extended window ---")
show = summ.copy()
for c in [c for c in show if c.startswith(("precision", "recall", "match_rate"))]:
    show[c] = show[c].map(pct)
for c in ["f1_all", "f1_lightning"]:
    show[c] = show[c].round(2)
print(show.T.to_string(header=False))
print("\nBy discovery day (FOD found = a pipeline fire within 5 km / 2 days):")
print(by_day.to_string(index=False))
if not new[~new.glm_covered].empty:
    print(f"\nNote: for {(~new.glm_covered).sum()} new fires the {a.t_hours:g} h before detection is only partly "
          "(or not) covered by GLM data, so their lightning matches are a lower bound. "
          "Download more GLM days (see the docstring) and rerun.")

print("\n--- Part 2: fire perimeters ---")
if perims is None:
    print(f"Skipped: {a.perimeters} not found. Run `python get_nifc.py`, then rerun this script.")
else:
    lit = new[new.lightning]
    big = perims[perims.lightning & perims.in_window & (perims.acres >= a.min_acres)]
    print(f"2020 perimeters: {len(perims):,} ({has_cause:,} with a recorded cause, {has_date:,} with an alarm date); "
          f"started {START - tol:%b %d}-{END + tol:%b %d} or undated: {perims.in_window.sum():,}")
    print(f"Lightning fires in the window: {(perims.lightning & perims.in_window).sum():,} "
          f"({len(big)} of them {a.min_acres:,.0f}+ acres)")
    print(f"{'':48}{'All new fires':>15}{'Lightning-matched':>20}")
    print(f"{'Fires flagged':48}{len(new):>15,}{len(lit):>20,}")
    print(f"{'Inside any 2020 perimeter (real fire)':48}{pct(new.in_perimeter.mean()):>15}{pct(lit.in_perimeter.mean()):>20}")
    print(f"{'Inside a fire that started in the window':48}{pct(new.in_window_perimeter.mean()):>15}"
          f"{pct(lit.in_window_perimeter.mean()):>20}")
    print(f"{'Inside a lightning perimeter (precision)':48}{pct(new.in_lightning_perimeter.mean()):>15}"
          f"{pct(lit.in_lightning_perimeter.mean()):>20}")
    print(f"{f'Lightning perimeters {a.min_acres:,.0f}+ acres found (recall)':48}"
          f"{pct((big.new_fires_inside > 0).mean()):>15}{pct((big.lightning_matched_inside > 0).mean()):>20}")
    tot = big.acres.sum()
    share = lambda m: big.acres[m].sum() / tot if tot else float("nan")
    print(f"{'Acres of those perimeters found':48}{pct(share(big.new_fires_inside > 0)):>15}"
          f"{pct(share(big.lightning_matched_inside > 0)):>20}")
    if len(perims) < 100 or not (perims.acres >= 100_000).any():
        print(f"WARNING: only {len(perims)} perimeters, the largest {perims.acres.max():,.0f} acres. California had "
              "thousands of 2020 fires, several over 100,000 acres, so this download looks incomplete. "
              "Try `python get_nifc.py` (CAL FIRE source) again.")

print("\n--- Part 3: famous complexes ---")
print(famous.to_string(index=False))
if perims is None:
    print("CZU and SCU have no FPA FOD records under those names; their rows need the fire perimeters.")
print("\nSaved: broad_eval_summary.csv, broad_eval_by_day.csv, famous_fires.csv"
      + (", perimeter_results.csv" if perims is not None else ""))
