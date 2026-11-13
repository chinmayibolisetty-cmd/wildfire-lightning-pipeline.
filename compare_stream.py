"""
Check the Spark streaming results against the pandas prototype, and report latency.

Usage (on your Mac, after the Spark job):
    python3.14 compare_stream.py
Test mode:
    python3 compare_stream.py --parquet stream_out
"""
import argparse

import pandas as pd

p = argparse.ArgumentParser()
p.add_argument("--parquet", default=None, help="Read Spark output from parquet instead of MongoDB")
p.add_argument("--mongo-uri", default="mongodb://localhost:27017")
a = p.parse_args()

if a.parquet:
    s = pd.read_parquet(a.parquet)
else:
    from pymongo import MongoClient
    s = pd.DataFrame(list(MongoClient(a.mongo_uri)["wildfire"]["stream_matches"].find({}, {"_id": 0})))
if s.empty:
    raise SystemExit("No streaming matches found. Did the Spark job run while the producer was sending?")

per_fire = s.groupby("fire_id").agg(n_flashes=("dist_km", "size"), nearest_km=("dist_km", "min"))

# Prototype results (from: match_prototype.py --d-km 5 --t-hours 48 --new-km 5)
new = pd.read_csv("new_fires.csv", parse_dates=["time"])
new["fire_id"] = [f"{la:.5f},{lo:.5f},{t:%Y-%m-%dT%H:%M}" for la, lo, t in zip(new.latitude, new.longitude, new.time)]
proto = pd.read_csv("fire_flash_matches.csv", parse_dates=["fire_time"])
proto["fire_id"] = [f"{la:.5f},{lo:.5f},{t:%Y-%m-%dT%H:%M}" for la, lo, t in zip(proto.fire_lat, proto.fire_lon, proto.fire_time)]

stream_new = set(per_fire.index) & set(new.fire_id)
proto_set = set(proto.fire_id)
agree = stream_new & proto_set

# Latency: when the match was written minus when the fire was sent to Kafka
lat_s = (s["processed_ms"] - s[["fire_sent_ms", "flash_sent_ms"]].max(axis=1)) / 1000

print("\n===== STREAMING vs PROTOTYPE =====")
print(f"Fire-flash pairs found by Spark:          {len(s):,}")
print(f"Fire detections with >=1 flash (all):     {len(per_fire):,}")
print(f"New fires matched by Spark:               {len(stream_new):,}")
print(f"New fires matched by pandas prototype:    {len(proto_set):,}")
print(f"Agreement:                                {len(agree):,} of {len(proto_set | stream_new):,} "
      f"({len(agree) / max(len(proto_set | stream_new), 1):.0%})")
print(f"End-to-end latency (median / 95th pct):   {lat_s.median():.1f} s / {lat_s.quantile(0.95):.1f} s")
