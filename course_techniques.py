#!/usr/bin/env python3
"""
DATA 226 course techniques added to the wildfire-lightning pipeline.

  1. Bloom filter        - streaming new-fire detection in constant memory
  2. Flajolet-Martin     - approximate count of distinct lightning-struck cells per day
  3. LSH (p-stable)      - near-neighbour flash search vs exact k-d tree
  4. Differential privacy - Laplace noise on released fire counts
  5. Data poisoning      - robustness of attribution to injected fake flashes + a defence

Run AFTER the prototype has produced new_fires.csv with the report's settings:
    python3.14 match_prototype.py --d-km 5 --t-hours 48 --new-km 5
    python3.14 course_techniques.py

Inputs : firms_aug2020.csv, glm_aug2020.csv, new_fires.csv, fpa_fod_ca_lightning.csv
Outputs: results_bloom.csv, results_fm.csv, results_lsh.csv, results_dp.csv, results_poison.csv
"""
import hashlib
import math
import sys
import time

import numpy as np
import pandas as pd
from scipy.spatial import cKDTree

D_KM, T_H = 5.0, 48.0          # attribution window (report defaults)
NEW_KM, NEW_DAYS = 5.0, 7      # new-fire rule (report defaults)
START, END = "2020-08-15", "2020-08-22"
LAT0 = np.radians(37.0)
RNG = np.random.default_rng(42)


def to_km(lat, lon):
    return np.column_stack([np.asarray(lon, float) * 111.32 * np.cos(LAT0),
                            np.asarray(lat, float) * 110.57])


def h64(s: str, salt: int = 0) -> int:
    return int.from_bytes(hashlib.blake2b(s.encode(), digest_size=8, salt=salt.to_bytes(8, "little")).digest(), "little")


def section(title):
    print("\n" + "=" * 70 + f"\n{title}\n" + "=" * 70)


# ---------------------------------------------------------------- data
firms = pd.read_csv("firms_aug2020.csv")
t = firms["acq_time"].astype(int).astype(str).str.zfill(4)
firms["time"] = (pd.to_datetime(firms["acq_date"]) + pd.to_timedelta(t.str[:2].astype(int), unit="h")
                 + pd.to_timedelta(t.str[2:].astype(int), unit="m"))
if "confidence" in firms:
    firms = firms[firms["confidence"].astype(str).str.lower() != "l"]
firms = firms.sort_values("time").reset_index(drop=True)

glm = pd.read_csv("glm_aug2020.csv", parse_dates=["flash_time"]).sort_values("flash_time").reset_index(drop=True)
glm_xy = to_km(glm["lat"], glm["lon"])
glm_t = glm["flash_time"].values

new = pd.read_csv("new_fires.csv", parse_dates=["time"]).sort_values("time").reset_index(drop=True)
new_xy = to_km(new["latitude"], new["longitude"])
new_t = new["time"].values

fod = pd.read_csv("fpa_fod_ca_lightning.csv")
fod["date"] = pd.to_datetime(fod["FIRE_YEAR"].astype(str)) + pd.to_timedelta(fod["DISCOVERY_DOY"] - 1, unit="D")
fod = fod[(fod["date"] >= START) & (fod["date"] <= END)].reset_index(drop=True)
fod_tree = cKDTree(to_km(fod["LATITUDE"], fod["LONGITUDE"]))

print(f"Loaded {len(firms):,} detections, {len(glm):,} flashes, {len(new):,} new fires, {len(fod):,} FPA FOD fires")

T_WIN = np.timedelta64(int(T_H * 3600), "s")


def flashes_near(fire_xy, fire_t, flash_xy, flash_t, tree=None):
    """Exact: for each fire, count of flashes within D_KM and the previous T_H hours."""
    tree = tree or cKDTree(flash_xy)
    out = np.zeros(len(fire_xy), int)
    for i, idx in enumerate(tree.query_ball_point(fire_xy, r=D_KM)):
        if idx:
            dt = fire_t[i] - flash_t[idx]
            out[i] = int(((dt >= np.timedelta64(0, "s")) & (dt <= T_WIN)).sum())
    return out


def precision_vs_fod(flagged_mask):
    """Share of flagged new fires within 5 km and 2 days of an FPA FOD lightning fire."""
    if flagged_mask.sum() == 0:
        return float("nan")
    hits = 0
    fire_days = new["time"].dt.normalize().values
    for i, idx in enumerate(fod_tree.query_ball_point(new_xy, r=5.0)):
        if flagged_mask[i] and idx:
            gap = np.abs((fod["date"].values[idx] - fire_days[i]) / np.timedelta64(1, "D"))
            hits += bool((gap <= 2).any())
    return hits / flagged_mask.sum()


# ====================================================== 1. Bloom filter
class BloomFilter:
    def __init__(self, n_items, fp_rate):
        self.m = int(math.ceil(-n_items * math.log(fp_rate) / math.log(2) ** 2))
        self.k = max(1, round(self.m / n_items * math.log(2)))
        self.bits = bytearray((self.m + 7) // 8)

    def _idx(self, key):
        d = hashlib.blake2b(key.encode(), digest_size=16).digest()
        a, b = int.from_bytes(d[:8], "little"), int.from_bytes(d[8:], "little") | 1
        return [(a + i * b) % self.m for i in range(self.k)]   # Kirsch-Mitzenmacher double hashing

    def add(self, key):
        for i in self._idx(key):
            self.bits[i >> 3] |= 1 << (i & 7)

    def __contains__(self, key):
        return all(self.bits[i >> 3] >> (i & 7) & 1 for i in self._idx(key))

    def nbytes(self):
        return len(self.bits)


CELL_KM = NEW_KM / 2                       # 2.5 km cells
_r = int(math.ceil(NEW_KM / CELL_KM))
# neighbour cells whose nearest point can lie within NEW_KM of the current cell
NEIGH = [(dx, dy) for dx in range(-_r, _r + 1) for dy in range(-_r, _r + 1)
         if (max(abs(dx) - 1, 0) ** 2 + max(abs(dy) - 1, 0) ** 2) * CELL_KM ** 2 <= NEW_KM ** 2]


def stream_new_fires(store):
    """One pass over detections in time order (a simulated stream). Keys = (2.5 km cell, day).
    A detection is NEW if no nearby cell was seen in the last NEW_DAYS days. Memory does not
    grow with the number of detections when `store` is a Bloom filter."""
    xy = to_km(firms["latitude"], firms["longitude"])
    cells = np.floor(xy / CELL_KM).astype(int)
    days = (firms["time"].values.astype("datetime64[D]") - np.datetime64("2020-01-01")).astype(int)
    is_new = np.zeros(len(firms), bool)
    t0 = time.perf_counter()
    for i in range(len(firms)):
        cx, cy, d = cells[i, 0], cells[i, 1], days[i]
        seen = any(f"{cx+dx}:{cy+dy}:{dd}" in store
                   for dd in range(d, d - NEW_DAYS - 1, -1) for dx, dy in NEIGH)
        is_new[i] = not seen
        store.add(f"{cx}:{cy}:{d}")
    us = (time.perf_counter() - t0) / len(firms) * 1e6
    return is_new & (firms["time"] >= START).values, us


section("1. BLOOM FILTER: new-fire detection in one streaming pass")
key = lambda df: set(zip(df["time"].astype(str), df["latitude"].round(4), df["longitude"].round(4)))
exact_new = key(new)

exact_set = set()
mask_set, us_set = stream_new_fires(exact_set)
set_bytes = sys.getsizeof(exact_set) + sum(sys.getsizeof(k) for k in exact_set)
rows = [dict(method="Exact Python set (same cell keys)", target_fp="-", bits=None, k=None,
             memory_kb=round(set_bytes / 1024, 1), new_fires=int(mask_set.sum()), us_per_event=round(us_set, 1))]
ref = key(firms[mask_set])

for fp in (0.01, 0.001, 0.0001):
    bf = BloomFilter(n_items=max(len(exact_set), 1), fp_rate=fp)
    mask, us = stream_new_fires(bf)
    got = key(firms[mask])
    rows.append(dict(method="Bloom filter", target_fp=fp, bits=bf.m, k=bf.k, memory_kb=round(bf.nbytes() / 1024, 1),
                     new_fires=int(mask.sum()), us_per_event=round(us, 1),
                     missed_vs_set=len(ref - got), agree_with_prototype=len(got & exact_new)))
rows[0]["agree_with_prototype"] = len(ref & exact_new)
bloom = pd.DataFrame(rows)
print(bloom.to_string(index=False))
print(f"\nPrototype (exact k-d tree, {NEW_KM:g} km radius) found {len(exact_new)} new fires.")
print(f"Each event does {len(NEIGH) * (NEW_DAYS + 1)} lookups, so the per-event false-positive chance is roughly that many times the target rate.")
print("A Bloom filter never misses a key it has seen, so a false positive can only turn a new fire into 'not new' (missed_vs_set).")
bloom.to_csv("results_bloom.csv", index=False)


# ================================================== 2. Flajolet-Martin
section("2. FLAJOLET-MARTIN: distinct 1-km cells struck by lightning per day")


def trailing_zeros(x):
    return (x & -x).bit_length() - 1 if x else 64


def fm_estimate(items, n_hash=64, group=8):
    R = np.zeros(n_hash, int)
    for it in items:
        for s in range(n_hash):
            R[s] = max(R[s], trailing_zeros(h64(it, s)))
    groups = R.reshape(-1, group)
    est = 0.79402 * 2 ** groups.mean(axis=1)           # average R within a group (LogLog-style constant)
    return float(np.median(est))                        # median across groups


cell_keys = (np.floor(glm_xy / 1.0).astype(int).astype(str))
glm["cell"] = [f"{a}:{b}" for a, b in cell_keys]
glm["day"] = glm["flash_time"].dt.date
fm_rows = []
for day, g in glm.groupby("day"):
    exact = g["cell"].nunique()
    est = fm_estimate(g["cell"].tolist())
    fm_rows.append(dict(day=str(day), flashes=len(g), exact_distinct_cells=exact, fm_estimate=round(est),
                        rel_error_pct=round(100 * (est - exact) / exact, 1)))
fm = pd.DataFrame(fm_rows)
print(fm.to_string(index=False))
print(f"\nMean absolute relative error: {fm.rel_error_pct.abs().mean():.1f}%  |  memory: 64 small counters "
      f"vs a set of up to {fm.exact_distinct_cells.max():,} cell ids")
fm.to_csv("results_fm.csv", index=False)


# ============================================================ 3. LSH
section("3. LOCALITY SENSITIVE HASHING (p-stable, Euclidean) vs exact search")
tree = cKDTree(glm_xy)
exact_counts = flashes_near(new_xy, new_t, glm_xy, glm_t, tree)
exact_pairs = int(exact_counts.sum())
exact_fires = int((exact_counts > 0).sum())
print(f"Exact (k-d tree): {exact_fires} new fires matched, {exact_pairs} fire-flash pairs")

W = 2 * D_KM
lsh_rows = []
for k in (1, 2):
    for L in (1, 2, 4, 8, 16):
        t0 = time.perf_counter()
        tables = []
        for _ in range(L):
            a = RNG.normal(size=(k, 2))
            b = RNG.uniform(0, W, size=k)
            hk = lambda P, a=a, b=b: [tuple(r) for r in np.floor((P @ a.T + b) / W).astype(int)]
            buckets = {}
            for j, hv in enumerate(hk(glm_xy)):
                buckets.setdefault(hv, []).append(j)
            tables.append((hk, buckets))
        build_s = time.perf_counter() - t0
        t0 = time.perf_counter()
        found_pairs, found_fires, cand_total = 0, 0, 0
        for i in range(len(new_xy)):
            cand = set()
            for hk, buckets in tables:
                cand.update(buckets.get(hk(new_xy[i:i + 1])[0], ()))
            cand_total += len(cand)
            if cand:
                c = np.fromiter(cand, int)
                dist = np.hypot(*(glm_xy[c] - new_xy[i]).T)
                dt = new_t[i] - glm_t[c]
                ok = (dist <= D_KM) & (dt >= np.timedelta64(0, "s")) & (dt <= T_WIN)
                found_pairs += int(ok.sum())
                found_fires += bool(ok.any())
        q_ms = (time.perf_counter() - t0) / len(new_xy) * 1000
        lsh_rows.append(dict(k=k, L=L, avg_candidates=round(cand_total / len(new_xy)),
                             pct_of_all_flashes=round(100 * cand_total / len(new_xy) / len(glm), 2),
                             pair_recall_pct=round(100 * found_pairs / max(exact_pairs, 1), 1),
                             fire_recall_pct=round(100 * found_fires / max(exact_fires, 1), 1),
                             build_s=round(build_s, 2), query_ms=round(q_ms, 2)))
lsh = pd.DataFrame(lsh_rows)
print(lsh.to_string(index=False))
print(f"\nBucket width w = {W:g} km. Brute force would compare every fire with all {len(glm):,} flashes.")
lsh.to_csv("results_lsh.csv", index=False)


# =========================================== 4. Differential privacy
section("4. DIFFERENTIAL PRIVACY: Laplace mechanism on released counts")
flagged = exact_counts > 0
daily = new[flagged].groupby(new["time"].dt.date[flagged]).size()
cell1 = new[flagged].assign(c=lambda d: d["latitude"].floordiv(1).astype(int).astype(str) + "N,"
                            + d["longitude"].floordiv(1).astype(int).astype(str) + "E").groupby("c").size()
print("Released statistics: lightning-attributed new fires per DAY and per 1-degree CELL.")
print("Sensitivity = 1 (adding or removing one fire changes one count by 1), noise ~ Laplace(1/epsilon).")
dp_rows = []
for eps in (0.1, 0.5, 1.0, 2.0, 5.0):
    for name, true in (("per day", daily.values), ("per 1-degree cell", cell1.values)):
        noisy = true + RNG.laplace(0, 1 / eps, size=(1000, len(true)))
        noisy = np.clip(np.round(noisy), 0, None)                    # post-processing keeps DP
        mae = np.abs(noisy - true).mean()
        small = true <= 2
        dp_rows.append(dict(epsilon=eps, release=name, n_counts=len(true), mean_true=round(true.mean(), 2),
                            mean_abs_error=round(mae, 2),
                            rel_error_pct=round(100 * mae / true.mean(), 1),
                            rel_error_small_counts_pct=round(100 * np.abs(noisy[:, small] - true[small]).mean()
                                                             / true[small].mean(), 1) if small.any() else None))
dp = pd.DataFrame(dp_rows)
print(dp.to_string(index=False))
dp.to_csv("results_dp.csv", index=False)


# ============================================== 5. Data poisoning
section("5. DATA POISONING: injected fake flashes vs attribution precision")
lat_lo, lat_hi, lon_lo, lon_hi = glm["lat"].min(), glm["lat"].max(), glm["lon"].min(), glm["lon"].max()
t_lo, t_hi = glm_t.min().astype("int64"), glm_t.max().astype("int64")
p_rows = []
for rate in (0.0, 0.1, 0.25, 0.5, 1.0):
    for seed in range(3 if rate else 1):
        r = np.random.default_rng(100 + seed)
        n = int(rate * len(glm))
        fake_xy = to_km(r.uniform(lat_lo, lat_hi, n), r.uniform(lon_lo, lon_hi, n))
        fake_t = r.integers(t_lo, t_hi, n).astype("datetime64[ns]")
        xy = np.vstack([glm_xy, fake_xy]) if n else glm_xy
        tt = np.concatenate([glm_t, fake_t]) if n else glm_t
        counts = flashes_near(new_xy, new_t, xy, tt)
        for m in (1, 3):
            f = counts >= m
            p_rows.append(dict(poison_rate=rate, seed=seed, rule=f">={m} flashes",
                               flagged=int(f.sum()), precision=precision_vs_fod(f)))
pois = (pd.DataFrame(p_rows).groupby(["poison_rate", "rule"], as_index=False)
        .agg(flagged=("flagged", "mean"), precision=("precision", "mean")))
pois["precision_pct"] = (100 * pois["precision"]).round(1)
pois["flagged"] = pois["flagged"].round(1)
print(pois.drop(columns="precision").to_string(index=False))
print("\npoison_rate = fake flashes as a fraction of the real flash count, spread uniformly over the area and time.")
print("Precision uses the same rule as validate.py (FPA FOD fire within 5 km and 2 days); small differences are possible.")
pois.to_csv("results_poison.csv", index=False)
print("\nDone. Paste everything above into the chat so the report can be updated.")
