"""
Load a CSV of points (GLM flashes or FIRMS fire detections) into MongoDB
and create a 2dsphere geospatial index.

Usage examples:
  python3.14 load_to_mongo.py firms_aug2020.csv fires --lat-col latitude --lon-col longitude
  python3.14 load_to_mongo.py glm_aug2020.csv flashes --lat-col lat --lon-col lon
"""
import argparse
import time

import pandas as pd
from pymongo import MongoClient, GEOSPHERE

parser = argparse.ArgumentParser()
parser.add_argument("csv_path", help="Path to the CSV file")
parser.add_argument("collection", help="MongoDB collection name, e.g. fires or flashes")
parser.add_argument("--lat-col", default="latitude", help="Name of the latitude column")
parser.add_argument("--lon-col", default="longitude", help="Name of the longitude column")
parser.add_argument("--db", default="wildfire", help="Database name")
parser.add_argument("--uri", default="mongodb://localhost:27017", help="MongoDB connection string")
args = parser.parse_args()

client = MongoClient(args.uri)
coll = client[args.db][args.collection]

df = pd.read_csv(args.csv_path)
print(f"Read {len(df):,} rows from {args.csv_path}")

# Drop rows without coordinates
df = df.dropna(subset=[args.lat_col, args.lon_col])

records = df.to_dict(orient="records")
for r in records:
    # GeoJSON uses [longitude, latitude] order, not [lat, lon]
    r["location"] = {
        "type": "Point",
        "coordinates": [float(r[args.lon_col]), float(r[args.lat_col])],
    }

start = time.time()
coll.delete_many({})  # start fresh each run so reruns don't duplicate data
if records:
    coll.insert_many(records)
elapsed = time.time() - start
print(f"Inserted {len(records):,} documents into {args.db}.{args.collection} in {elapsed:.1f}s")

coll.create_index([("location", GEOSPHERE)])
print("Created 2dsphere index on 'location'")

# Sanity check: count points within 10 km of the first document
if records:
    sample = records[0]["location"]
    t0 = time.time()
    n = coll.count_documents({
        "location": {"$nearSphere": {"$geometry": sample, "$maxDistance": 10_000}}
    })
    print(f"Test query: {n:,} points within 10 km of the first point "
          f"({(time.time() - t0) * 1000:.0f} ms)")
