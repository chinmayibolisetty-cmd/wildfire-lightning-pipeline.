"""
Check the Spark streaming results against the pandas prototype, and report latency.

With in-stream new-fire detection (the default in spark_match.py) the stream itself
decides which detections are new fires, so it is compared directly with the
prototype: first the new fires, then the lightning matches. If the Spark job ran with
--match-all, the new-fire filter is applied here in batch instead (old behaviour).

Usage (on your Mac, after the Spark job):
    python3.14 compare_stream.py
Test mode:
    python3 compare_stream.py --parquet stream_out
"""
import argparse
import glob

import pandas as pd

p = argparse.ArgumentParser()
p.add_argument("--parquet", default=None, help="Read Spark output from parquet instead of MongoDB")
p.add_argument("--mongo-uri", default="mongodb://localhost:27017")
a = p.parse_args()

if a.parquet:
    s = pd.read_parquet(a.parquet)
    nf_dir = a.parquet.rstrip("/") + "_new_fires_table"
    sn = pd.read_parquet(nf_dir) if glob.glob(f"{nf_dir}/*.parquet") else pd.DataFrame()
else:
    from pymongo import MongoClient
    db = MongoClient(a.mongo_uri)["wildfire"]
    s = pd.DataFrame(list(db["stream_matches"].find({}, {"_id": 0})))
    sn = pd.DataFrame(list(db["stream_new_fires"].find({}, {"_id": 0})))
if s.empty:
    raise SystemExit("No streaming matches found. Did the Spark job run while the producer was sending?")
in_stream = not sn.empty
if in_stream:
    sn = sn.drop_duplicates("fire_id")  # a replayed micro-batch could write a fire twice

per_fire = s.groupby("fire_id").agg(n_flashes=("dist_km", "size"), nearest_km=("dist_km", "min"))


def fire_ids(lat, lon, t):
    return [f"{la:.5f},{lo:.5f},{ti:%Y-%m-%dT%H:%M}" for la, lo, ti in zip(lat, lon, t)]


# Prototype results (from: match_prototype.py --d-km 5 --t-hours 48 --new-km 5)
new = pd.read_csv("new_fires.csv", parse_dates=["time"])
new["fire_id"] = fire_ids(new.latitude, new.longitude, new.time)
proto = pd.read_csv("fire_flash_matches.csv", parse_dates=["fire_time"])
proto["fire_id"] = fire_ids(proto.fire_lat, proto.fire_lon, proto.fire_time)

if in_stream:
    # The replay ends before the prototype's data does, so compare within the replayed window.
    last = pd.to_datetime(sn["fire_time"]).max()
    proto_new = set(new.loc[new.time <= last, "fire_id"])
    stream_new_fires = set(sn.fire_id)
    stream_new = set(per_fire.index)                       # matcher only ever sees new fires
else:
    stream_new = set(per_fire.index) & set(new.fire_id)    # batch filter, as before
proto_set = set(proto.fire_id)
agree = stream_new & proto_set

# Latency: when the match was written minus when the fire/flash was sent to Kafka
lat_s = (s["processed_ms"] - s[["fire_sent_ms", "flash_sent_ms"]].max(axis=1)) / 1000

print("\n===== STREAMING vs PROTOTYPE =====")
print(f"New-fire detection:                       {'in the stream' if in_stream else 'batch, after the stream'}")
if in_stream:
    both = stream_new_fires & proto_new
    det_s = (sn["detected_ms"] - sn["sent_ms"]) / 1000
    print(f"New fires detected by Spark:              {len(stream_new_fires):,}")
    print(f"New fires in prototype (same window):     {len(proto_new):,}")
    print(f"New-fire agreement:                       {len(both):,} of {len(stream_new_fires | proto_new):,} "
          f"({len(both) / max(len(stream_new_fires | proto_new), 1):.0%})")
    print(f"New-fire detection latency (median / 95th pct): {det_s.median():.1f} s / {det_s.quantile(0.95):.1f} s")
print(f"Fire-flash pairs found by Spark:          {len(s):,}")
print(f"Fire detections with >=1 flash (all):     {len(per_fire):,}")
print(f"New fires matched by Spark:               {len(stream_new):,}")
print(f"New fires matched by pandas prototype:    {len(proto_set):,}")
print(f"Agreement:                                {len(agree):,} of {len(proto_set | stream_new):,} "
      f"({len(agree) / max(len(proto_set | stream_new), 1):.0%})")
print(f"End-to-end latency (median / 95th pct):   {lat_s.median():.1f} s / {lat_s.quantile(0.95):.1f} s")
