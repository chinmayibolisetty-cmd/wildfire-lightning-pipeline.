"""
In-stream new-fire detection: the per-grid-cell state logic used by spark_match.py.

Rule (same as match_prototype.py): a fire detection is NEW if no earlier detection
lies within NEW_KM km in the previous NEW_DAYS days. "Earlier" means an earlier
time, or the same time but an earlier position in the replay order (seq) - that is
how the prototype treats several pixels from the same satellite overpass.

How it runs in Spark (applyInPandasWithState, keyed by grid cell):
  * Cells are at least NEW_KM wide, so every neighbour within NEW_KM is in the
    detection's own cell or one of the 8 cells around it.
  * Each detection is copied into its 3x3 neighbourhood. The copy in its own
    cell is the "home" row: that is where it gets checked. The 8 other copies
    are only remembered, so neighbouring cells can check against it.
  * Each cell keeps the detections of the last NEW_DAYS days in Spark state
    (checkpointed, so it survives a restart).

This file has no Spark imports, so the logic can be tested with plain pandas
(see test_new_fire_state.py).
"""
import numpy as np
import pandas as pd

LAT0 = np.radians(37.0)   # projection reference latitude, same as match_prototype.py

STATE_SCHEMA = "ts_ms array<long>, seq array<long>, lat array<double>, lon array<double>"
OUTPUT_SCHEMA = ("fire_id string, fire_time string, lat double, lon double, "
                 "sent_ms long, seq long, detected_ms long")
OUTPUT_COLS = ["fire_id", "fire_time", "lat", "lon", "sent_ms", "seq", "detected_ms"]


def cell_size_deg(km):
    """Grid cell (lat, lon) size in degrees, at least `km` tall and wide."""
    return km / 110.57 * 1.01, km / (111.32 * np.cos(LAT0)) * 1.01


def _xy(lat, lon):
    """Equirectangular projection to km, written exactly as in match_prototype.py."""
    return np.asarray(lon, dtype=float) * 111.32 * np.cos(LAT0), np.asarray(lat, dtype=float) * 110.57


def find_new(home, known, new_km, window_ms, chunk=512):
    """Boolean array: which `home` rows have no earlier detection in `known` nearby."""
    hx, hy = _xy(home["lat"], home["lon"])
    kx, ky = _xy(known["lat"], known["lon"])
    hts, hseq = home["ts_ms"].to_numpy(np.int64), home["seq"].to_numpy(np.int64)
    kts, kseq = known["ts_ms"].to_numpy(np.int64), known["seq"].to_numpy(np.int64)
    out = np.ones(len(home), dtype=bool)
    for s in range(0, len(home), chunk):
        e = slice(s, s + chunk)
        dt = hts[e, None] - kts[None, :]
        earlier = (dt > 0) | ((dt == 0) & (kseq[None, :] < hseq[e, None]))
        near = np.hypot(hx[e, None] - kx[None, :], hy[e, None] - ky[None, :]) <= new_km
        out[e] = ~(earlier & (dt <= window_ms) & near).any(axis=1)
    return out


def make_detector(new_km=5.0, new_days=7.0, now_ms=None):
    """Return the function to pass to groupBy("cell").applyInPandasWithState(...).

    Input rows: cell, is_home, emit, fire_id, fire_time, ts_ms, seq, lat, lon, sent_ms.
    Output: one row per NEW fire whose `emit` flag is set (detections before the
    counting start date are only used to fill the state).
    """
    import time
    window_ms = int(new_days * 24 * 3600 * 1000)
    clock = now_ms or (lambda: int(time.time() * 1000))

    def detect(key, pdf_iter, state):
        batch = pd.concat(list(pdf_iter), ignore_index=True)
        cols = ["ts_ms", "seq", "lat", "lon"]
        if state.exists:
            ts, seq, lat, lon = state.get
            known = pd.DataFrame({"ts_ms": ts, "seq": seq, "lat": lat, "lon": lon})
            points = pd.concat([known, batch[cols]], ignore_index=True)
        else:
            points = batch[cols].reset_index(drop=True)

        home = batch[batch["is_home"].astype(bool)]
        result = None
        if len(home):
            new = home[find_new(home, points, new_km, window_ms) & home["emit"].astype(bool).to_numpy()]
            if len(new):
                result = new.assign(detected_ms=clock())[OUTPUT_COLS].astype(
                    {"sent_ms": "int64", "seq": "int64", "detected_ms": "int64"})

        # Keep only what a future detection could still need. Detections arrive in
        # time order (one Kafka partition), so anything older than the newest time
        # in this cell minus the window can never matter again.
        newest = int(points["ts_ms"].max())
        keep = points[points["ts_ms"] >= newest - window_ms]
        state.update((keep["ts_ms"].astype("int64").tolist(), keep["seq"].astype("int64").tolist(),
                      keep["lat"].astype(float).tolist(), keep["lon"].astype(float).tolist()))
        if result is not None:
            yield result

    return detect
