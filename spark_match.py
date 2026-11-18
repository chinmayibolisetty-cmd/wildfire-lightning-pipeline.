"""
Spark Structured Streaming: detect NEW fires in the stream, then match them to
prior lightning flashes.

Two streaming queries run in this one Spark application:

  1. New-fire detector  (firms-fires topic -> new-fires topic + MongoDB stream_new_fires)
     A detection is NEW if no earlier detection lies within NEW_KM km in the previous
     NEW_DAYS days. Detections are bucketed into a grid of cells at least NEW_KM wide
     and copied into their 3x3 neighbourhood; each cell keeps the last NEW_DAYS days
     of detections in Spark state (applyInPandasWithState). Logic: new_fire_state.py.

  2. Lightning matcher  (new-fires + glm-flashes topics -> MongoDB stream_matches)
     Stream-stream join: a flash matches a fire if it is within D km and up to T hours
     BEFORE the fire. Both streams are bucketed into a spatial grid; each flash is
     copied into its 3x3 neighbouring cells, so the join only compares points in the
     same cell.

They are two queries (connected by the new-fires Kafka topic) rather than one, which avoids
Spark's limits on chaining a custom stateful operator into a stream-stream join.
--match-all skips step 1 and matches every detection (the old behaviour, where the
new-fire filter was applied afterwards in batch by compare_stream.py).

Needs pandas + pyarrow inside the Spark container (see README, Setup).

Run inside the spark container (see instructions), e.g.:
    spark-submit ... spark_match.py --run-seconds 400
Test mode (files instead of Kafka/Mongo):
    spark-submit spark_match.py --source files --in-dir stream_in --sink parquet --out-dir stream_out
"""
import argparse
import math
import os
import time

from pyspark.sql import SparkSession, functions as F, types as T

p = argparse.ArgumentParser()
p.add_argument("--source", choices=["kafka", "files"], default="kafka")
p.add_argument("--bootstrap", default="kafka:29092")
p.add_argument("--in-dir", default="stream_in")
p.add_argument("--sink", choices=["mongodb", "parquet"], default="mongodb")
p.add_argument("--mongo-uri", default="mongodb://mongodb:27017")
p.add_argument("--out-dir", default="stream_out")
p.add_argument("--checkpoint", default="/tmp/spark_chk")
p.add_argument("--d-km", type=float, default=5.0)
p.add_argument("--t-hours", type=int, default=48)
p.add_argument("--new-km", type=float, default=5.0, help="New-fire rule: radius (km)")
p.add_argument("--new-days", type=float, default=7.0, help="New-fire rule: lookback (days)")
p.add_argument("--new-from", default="2020-08-15",
               help="Only report new fires from this date; earlier detections just fill the state")
p.add_argument("--match-all", action="store_true",
               help="Skip in-stream new-fire detection and match every detection (old behaviour)")
p.add_argument("--run-seconds", type=int, default=400, help="Stop the stream after this many seconds")
a = p.parse_args()

spark = SparkSession.builder.appName("lightning-fire-match") \
    .config("spark.sql.shuffle.partitions", "4").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
HERE = os.path.dirname(os.path.abspath(__file__))
spark.sparkContext.addPyFile(os.path.join(HERE, "new_fire_state.py"))  # ship to Python workers
import new_fire_state as nfs  # noqa: E402

COS0 = math.cos(math.radians(37.0))
LAT_CELL = a.d_km / 110.57 * 1.01            # grid cell at least D km tall
LON_CELL = a.d_km / (111.32 * COS0) * 1.01   # ...and at least D km wide
NEW_LAT_CELL, NEW_LON_CELL = nfs.cell_size_deg(a.new_km)
NEW_FIRES_DIR = a.out_dir.rstrip("/") + "_new_fires"   # files mode: new fires handed to query 2

flash_schema = T.StructType([T.StructField("flash_time", T.StringType()),
                             T.StructField("lat", T.DoubleType()), T.StructField("lon", T.DoubleType()),
                             T.StructField("sent_ms", T.LongType())])
fire_schema = T.StructType([T.StructField("fire_id", T.StringType()), T.StructField("fire_time", T.StringType()),
                            T.StructField("lat", T.DoubleType()), T.StructField("lon", T.DoubleType()),
                            T.StructField("sent_ms", T.LongType()), T.StructField("seq", T.LongType())])


def read(topic, schema, path=None):
    if a.source == "kafka":
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", a.bootstrap)
               .option("subscribe", topic).option("startingOffsets", "earliest").load())
        return raw.select(F.from_json(F.col("value").cast("string"), schema).alias("v")).select("v.*")
    return spark.readStream.schema(schema).option("maxFilesPerTrigger", 1).json(path or f"{a.in_dir}/{topic}")


offsets = F.array(F.lit(-1), F.lit(0), F.lit(1))
queries = []

# ---------------------------------------------------------------------------
# Query 1: new-fire detector
# ---------------------------------------------------------------------------
if not a.match_all:
    from pyspark.sql.streaming.state import GroupStateTimeout

    detections = (read("firms-fires", fire_schema)
                  .withColumn("fire_ts", F.to_timestamp("fire_time"))
                  .withColumn("ts_ms", F.expr("unix_millis(fire_ts)"))
                  .withColumn("emit", F.col("fire_ts") >= F.to_timestamp(F.lit(a.new_from)))
                  .withColumn("cy", F.floor(F.col("lat") / NEW_LAT_CELL).cast("long"))
                  .withColumn("cx", F.floor(F.col("lon") / NEW_LON_CELL).cast("long"))
                  .withColumn("dy", F.explode(offsets)).withColumn("dx", F.explode(offsets))
                  .withColumn("cell", F.concat_ws("_", F.col("cy") + F.col("dy"), F.col("cx") + F.col("dx")))
                  .withColumn("is_home", (F.col("dy") == 0) & (F.col("dx") == 0))
                  .select("cell", "is_home", "emit", "fire_id", "fire_time", "ts_ms", "seq",
                          "lat", "lon", "sent_ms"))

    new_fires = detections.groupBy("cell").applyInPandasWithState(
        nfs.make_detector(a.new_km, a.new_days),
        outputStructType=nfs.OUTPUT_SCHEMA, stateStructType=nfs.STATE_SCHEMA,
        outputMode="append", timeoutConf=GroupStateTimeout.NoTimeout)

    def publish_new_fires(batch_df, batch_id):
        """Send this micro-batch's new fires on to the matcher, and keep a copy."""
        if batch_df.isEmpty():
            return
        batch_df.persist()
        if a.source == "kafka":
            (batch_df.select(F.to_json(F.struct(*batch_df.columns)).alias("value"))
             .write.format("kafka").option("kafka.bootstrap.servers", a.bootstrap)
             .option("topic", "new-fires").save())
        else:
            batch_df.write.mode("append").json(NEW_FIRES_DIR)
        if a.sink == "mongodb":
            (batch_df.write.format("mongodb").mode("append")
             .option("spark.mongodb.connection.uri", a.mongo_uri)
             .option("spark.mongodb.database", "wildfire")
             .option("spark.mongodb.collection", "stream_new_fires").save())
        else:
            batch_df.write.mode("append").parquet(os.path.join(a.out_dir + "_new_fires_table"))
        batch_df.unpersist()

    if a.source == "files":
        os.makedirs(NEW_FIRES_DIR, exist_ok=True)
    queries.append(("new-fire detector", new_fires.writeStream.foreachBatch(publish_new_fires)
                    .option("checkpointLocation", f"{a.checkpoint}/new_fires")
                    .trigger(processingTime="5 seconds").start()))

# ---------------------------------------------------------------------------
# Query 2: lightning matcher
# ---------------------------------------------------------------------------
cell_y = lambda: F.floor(F.col("lat") / LAT_CELL).cast("long")
cell_x = lambda: F.floor(F.col("lon") / LON_CELL).cast("long")

flashes = (read("glm-flashes", flash_schema)
           .withColumn("flash_ts", F.to_timestamp("flash_time"))
           .withWatermark("flash_ts", f"{a.t_hours + 1} hours")
           .withColumn("cy", cell_y()).withColumn("cx", cell_x())
           .withColumn("dy", F.explode(offsets)).withColumn("dx", F.explode(offsets))
           .withColumn("cell", F.concat_ws("_", F.col("cy") + F.col("dy"), F.col("cx") + F.col("dx")))
           .select("cell", "flash_ts", F.col("lat").alias("flash_lat"), F.col("lon").alias("flash_lon"),
                   F.col("sent_ms").alias("flash_sent_ms")))

fire_source = read("firms-fires", fire_schema) if a.match_all else read("new-fires", fire_schema, NEW_FIRES_DIR)
fires = (fire_source
         .withColumn("fire_ts", F.to_timestamp("fire_time"))
         .withWatermark("fire_ts", "1 hour")
         .withColumn("cell", F.concat_ws("_", cell_y(), cell_x()))
         .select("fire_id", "cell", "fire_ts", F.col("lat").alias("fire_lat"),
                 F.col("lon").alias("fire_lon"), F.col("sent_ms").alias("fire_sent_ms")))

joined = fires.alias("f").join(
    flashes.alias("g"),
    F.expr(f"""f.cell = g.cell AND
               g.flash_ts >= f.fire_ts - INTERVAL {a.t_hours} HOURS AND
               g.flash_ts <= f.fire_ts"""))

dist = F.sqrt(((F.col("flash_lon") - F.col("fire_lon")) * 111.32 * COS0) ** 2 +
              ((F.col("flash_lat") - F.col("fire_lat")) * 110.57) ** 2)
matches = (joined.withColumn("dist_km", dist).filter(F.col("dist_km") <= a.d_km)
           .withColumn("lag_hours", (F.col("fire_ts").cast("double") - F.col("flash_ts").cast("double")) / 3600)
           .withColumn("processed_ms", (F.unix_timestamp(F.current_timestamp()) * 1000).cast("long"))
           .select("fire_id", "fire_ts", "fire_lat", "fire_lon", "flash_ts", "flash_lat", "flash_lon",
                   F.round("dist_km", 3).alias("dist_km"), F.round("lag_hours", 2).alias("lag_hours"),
                   "fire_sent_ms", "flash_sent_ms", "processed_ms"))

writer = matches.writeStream.outputMode("append").option("checkpointLocation", f"{a.checkpoint}/matches") \
    .trigger(processingTime="5 seconds")
if a.sink == "mongodb":
    writer = writer.format("mongodb").option("spark.mongodb.connection.uri", a.mongo_uri) \
        .option("spark.mongodb.database", "wildfire").option("spark.mongodb.collection", "stream_matches")
else:
    writer = writer.format("parquet").option("path", a.out_dir)
queries.append(("lightning matcher", writer.start()))

mode = "every detection" if a.match_all else f"new fires only (rule: {a.new_km} km / {a.new_days:g} days)"
print(f"Streaming started (D={a.d_km} km, T={a.t_hours} h; matching {mode}). Running for {a.run_seconds} s...")
t_end = time.time() + a.run_seconds
while time.time() < t_end and all(q.isActive for _, q in queries):
    time.sleep(10)
    for name, q in queries:
        lp = q.lastProgress
        if lp:
            print(f"  {name} batch {lp['batchId']}: {lp['numInputRows']:,} new events")
for name, q in queries:
    if q.exception():
        print(f"{name} FAILED: {q.exception()}")

progress = {name: [p_ for p_ in q.recentProgress if p_["numInputRows"] > 0] for name, q in queries}
for _, q in queries:
    q.stop()


def rate_summary(name):
    pr = progress.get(name, [])
    rates = [p_.get("processedRowsPerSecond", 0) for p_ in pr if p_.get("processedRowsPerSecond")]
    return len(pr), sum(p_["numInputRows"] for p_ in pr), max(rates) if rates else None


print("\n===== SPARK STREAMING SUMMARY =====")
n_batches, rows, peak = rate_summary("lightning matcher")
print(f"Micro-batches with data:  {n_batches}")
print(f"Events processed:         {rows:,}")
if peak:
    print(f"Peak processing rate:     {peak:,.0f} events/s")
if not a.match_all:
    d_batches, d_rows, d_peak = rate_summary("new-fire detector")
    print(f"New-fire detector:        {d_rows:,} detections in {d_batches} micro-batches"
          + (f", peak {d_peak:,.0f} detections/s" if d_peak else ""))
