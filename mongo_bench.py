"""
MongoDB geospatial query benchmark.

For every new fire, find GLM flashes within D km and the prior T hours, using
three index setups:
  1. no index (full collection scan)
  2. 2dsphere index on location
  3. compound index: 2dsphere location + flash_time
Reports query times, documents examined per query, and whether the results
match the pandas prototype.

Usage:  python3.14 mongo_bench.py
Needs:  MongoDB running (docker compose up -d mongodb), glm_aug2020.csv,
        new_fires.csv and fire_flash_matches.csv from
        match_prototype.py --d-km 5 --t-hours 48 --new-km 5
Output: mongo_benchmark.csv
"""
import time

import numpy as np
import pandas as pd
from pymongo import ASCENDING, GEOSPHERE, MongoClient

D_KM, T_HOURS = 5.0, 48
EARTH_RADIUS_KM = 6378.1

db = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=5000)["wildfire"]
db.command("ping")

glm = pd.read_csv("glm_aug2020.csv", parse_dates=["flash_time"])
docs = [{"flash_time": t.to_pydatetime(),
         "location": {"type": "Point", "coordinates": [float(lon), float(lat)]}}
        for t, lat, lon in zip(glm.flash_time, glm.lat, glm.lon)]
db.flashes.drop()
t0 = time.time()
db.flashes.insert_many(docs)
load_s = time.time() - t0
print(f"Loaded {len(docs):,} flashes into wildfire.flashes in {load_s:.1f} s")

new = pd.read_csv("new_fires.csv", parse_dates=["time"])
start, end = glm.flash_time.min(), glm.flash_time.max() + pd.Timedelta(hours=T_HOURS)
fires = new[(new.time >= start) & (new.time <= end)]
proto = pd.read_csv("fire_flash_matches.csv")
print(f"Querying for {len(fires):,} new fires (D = {D_KM:g} km, T = {T_HOURS} h)")


def query(fire):
    t = fire.time.to_pydatetime()
    return {"location": {"$geoWithin": {"$centerSphere": [[float(fire.longitude), float(fire.latitude)],
                                                          D_KM / EARTH_RADIUS_KM]}},
            "flash_time": {"$gte": t - pd.Timedelta(hours=T_HOURS).to_pytimedelta(), "$lte": t}}


setups = [
    ("No index (full scan)", None),
    ("2dsphere on location", [("location", GEOSPHERE)]),
    ("2dsphere + flash_time", [("location", GEOSPHERE), ("flash_time", ASCENDING)]),
]
rows = []
for name, index in setups:
    db.flashes.drop_indexes()
    build_s = 0.0
    if index:
        t0 = time.time()
        db.flashes.create_index(index)
        build_s = time.time() - t0
    queries = [query(f) for f in fires.itertuples()]
    db.flashes.count_documents(queries[0])
    times, matched = [], 0
    for q in queries:
        t0 = time.perf_counter()
        n = db.flashes.count_documents(q)
        times.append((time.perf_counter() - t0) * 1000)
        matched += n > 0
    examined = [db.flashes.find(q).explain()["executionStats"]["totalDocsExamined"] for q in queries]
    rows.append({"index": name, "index_build_s": round(build_s, 2),
                 "median_query_ms": round(float(np.median(times)), 2),
                 "p95_query_ms": round(float(np.percentile(times, 95)), 2),
                 "total_s": round(sum(times) / 1000, 2),
                 "avg_docs_examined": int(np.mean(examined)),
                 "fires_matched": matched})
    print(f"  {name}: median {rows[-1]['median_query_ms']} ms, "
          f"{rows[-1]['avg_docs_examined']:,} docs examined per query")

res = pd.DataFrame(rows)
res.to_csv("mongo_benchmark.csv", index=False)
print("\n===== MONGODB GEOSPATIAL BENCHMARK =====")
print(f"Flashes: {len(docs):,} (loaded in {load_s:.1f} s); queries: {len(fires):,} new fires")
print(res.to_string(index=False))
print(f"\nPandas prototype matched {len(proto):,} fires. Small differences are expected:")
print("MongoDB measures true spherical distance, the prototype uses a flat-map approximation.")
print("Saved: mongo_benchmark.csv")
