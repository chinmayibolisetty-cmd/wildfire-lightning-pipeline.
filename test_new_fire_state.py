"""
Offline check of the in-stream new-fire detector (no Spark or Kafka needed).

Feeds the producer's output through new_fire_state.make_detector() the same way
spark_match.py does: explode each detection into its 3x3 grid cells, cut the
stream into micro-batches, group each batch by cell and call the detector with
that cell's state. Then matches the new fires to lightning with the D km / T h
rule and compares both results with the pandas prototype.

Usage:
    python replay_producer.py --to-dir stream_in --speed 0
    python match_prototype.py --d-km 5 --t-hours 48 --new-km 5
    python test_new_fire_state.py
"""
import argparse
import json

import numpy as np
import pandas as pd
from scipy.spatial import cKDTree

from new_fire_state import cell_size_deg, make_detector, _xy

p = argparse.ArgumentParser()
p.add_argument("--in-dir", default="stream_in")
p.add_argument("--new-km", type=float, default=5.0)
p.add_argument("--new-days", type=float, default=7.0)
p.add_argument("--new-from", default="2020-08-15")
p.add_argument("--d-km", type=float, default=5.0)
p.add_argument("--t-hours", type=float, default=48.0)
p.add_argument("--batches", type=int, default=200, help="Number of micro-batches to cut the stream into")
p.add_argument("--seed", type=int, default=0)
a = p.parse_args()


class FakeGroupState:
    """Stand-in for pyspark's GroupState (only what the detector uses)."""
    def __init__(self, value=None):
        self.value = value

    @property
    def exists(self):
        return self.value is not None

    @property
    def get(self):
        return self.value

    def update(self, value):
        self.value = value


fires = pd.read_json(f"{a.in_dir}/firms-fires/part-0.json", lines=True, dtype={"fire_time": str})
fires["ts_ms"] = pd.to_datetime(fires["fire_time"]).astype("datetime64[ms]").astype("int64")
fires["emit"] = pd.to_datetime(fires["fire_time"]) >= pd.Timestamp(a.new_from)
lat_cell, lon_cell = cell_size_deg(a.new_km)
fires["cy"] = np.floor(fires.lat / lat_cell).astype(int)
fires["cx"] = np.floor(fires.lon / lon_cell).astype(int)
rows = [fires.assign(cell=(fires.cy + dy).astype(str) + "_" + (fires.cx + dx).astype(str),
                     is_home=(dy == 0 and dx == 0)) for dy in (-1, 0, 1) for dx in (-1, 0, 1)]
fires["row"] = range(len(fires))  # arrival order (one Kafka partition)
exploded = pd.concat([r.assign(row=range(len(fires))) for r in rows], ignore_index=True)

# Random micro-batch boundaries; within a batch Spark gives no order guarantee, so shuffle.
rng = np.random.default_rng(a.seed)
cuts = np.sort(rng.choice(np.arange(1, len(fires)), a.batches - 1, replace=False))
exploded["batch"] = np.searchsorted(cuts, exploded["row"], side="right")
exploded = exploded.sample(frac=1, random_state=a.seed)

detect = make_detector(a.new_km, a.new_days, now_ms=lambda: 0)
states, out = {}, []
for _, b in exploded.groupby("batch", sort=True):
    for cell, g in b.groupby("cell"):
        st = states.setdefault(cell, FakeGroupState())
        chunks = [g.iloc[i:i + 1000] for i in range(0, len(g), 1000)]  # Arrow batches
        out.extend(detect((cell,), iter(chunks), st))
stream_new = pd.concat(out, ignore_index=True)
max_state = max(len(s.value[0]) for s in states.values())

# Prototype results
proto_new = pd.read_csv("new_fires.csv", parse_dates=["time"])
proto_new = proto_new[proto_new.time <= pd.to_datetime(fires.fire_time).max()]  # replayed window only
proto_new["fire_id"] = [f"{la:.5f},{lo:.5f},{t:%Y-%m-%dT%H:%M}"
                        for la, lo, t in zip(proto_new.latitude, proto_new.longitude, proto_new.time)]
proto_m = pd.read_csv("fire_flash_matches.csv", parse_dates=["fire_time"])
proto_m["fire_id"] = [f"{la:.5f},{lo:.5f},{t:%Y-%m-%dT%H:%M}"
                      for la, lo, t in zip(proto_m.fire_lat, proto_m.fire_lon, proto_m.fire_time)]

# Lightning match of the stream's new fires (what the Spark join does next)
glm = pd.read_json(f"{a.in_dir}/glm-flashes/part-0.json", lines=True, dtype={"flash_time": str})
gt = pd.to_datetime(glm.flash_time).values
gx, gy = _xy(glm.lat, glm.lon)
tree = cKDTree(np.column_stack([gx, gy]))
fx, fy = _xy(stream_new.lat, stream_new.lon)
ft = pd.to_datetime(stream_new.fire_time).values
matched = set()
for fid, x, y, t in zip(stream_new.fire_id, fx, fy, ft):
    c = np.array(tree.query_ball_point([x, y], r=a.d_km), dtype=int)
    lag = t - gt[c]
    if ((lag >= np.timedelta64(0, "s")) & (lag <= np.timedelta64(int(a.t_hours * 3600), "s"))).any():
        matched.add(fid)

s_new, p_new, p_m = set(stream_new.fire_id), set(proto_new.fire_id), set(proto_m.fire_id)
print("\n===== IN-STREAM NEW-FIRE DETECTOR vs PROTOTYPE =====")
print(f"Fire detections streamed:                 {len(fires):,} ({(~fires.emit).sum():,} warm-up)")
print(f"Micro-batches simulated:                  {a.batches}")
print(f"Largest per-cell state (detections):      {max_state:,}")
print(f"New fires (stream / prototype / both):    {len(s_new)} / {len(p_new)} / {len(s_new & p_new)}")
print(f"Lightning-matched (stream / proto / both):{len(matched)} / {len(p_m)} / {len(matched & p_m)}")
ok = s_new == p_new and matched == p_m
print("RESULT:", "PASS" if ok else "FAIL")
raise SystemExit(0 if ok else 1)
