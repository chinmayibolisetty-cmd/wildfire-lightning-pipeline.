"""
Spark Structured Streaming: match new fire detections to prior lightning flashes.

Reads the glm-flashes and firms-fires Kafka topics and runs a stream-stream join:
a flash matches a fire if it is within D km and up to T hours BEFORE the fire.
To avoid comparing every fire with every flash, both streams are bucketed into a
spatial grid; each flash is copied into its 3x3 neighbouring cells, so the join
only compares points in the same cell.

Run inside the spark container (see instructions), e.g.:
    spark-submit ... spark_match.py --run-seconds 400
Test mode (files instead of Kafka/Mongo):
    spark-submit spark_match.py --source files --in-dir stream_in --sink parquet --out-dir stream_out
"""
import argparse
import math
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
p.add_argument("--run-seconds", type=int, default=400, help="Stop the stream after this many seconds")
a = p.parse_args()

spark = SparkSession.builder.appName("lightning-fire-match") \
    .config("spark.sql.shuffle.partitions", "4").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

COS0 = math.cos(math.radians(37.0))
LAT_CELL = a.d_km / 110.57 * 1.01            # grid cell at least D km tall
LON_CELL = a.d_km / (111.32 * COS0) * 1.01   # ...and at least D km wide

flash_schema = T.StructType([T.StructField("flash_time", T.StringType()),
                             T.StructField("lat", T.DoubleType()), T.StructField("lon", T.DoubleType()),
                             T.StructField("sent_ms", T.LongType())])
fire_schema = T.StructType([T.StructField("fire_id", T.StringType()), T.StructField("fire_time", T.StringType()),
                            T.StructField("lat", T.DoubleType()), T.StructField("lon", T.DoubleType()),
                            T.StructField("sent_ms", T.LongType())])


def read(topic, schema):
    if a.source == "kafka":
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", a.bootstrap)
               .option("subscribe", topic).option("startingOffsets", "earliest").load())
        return raw.select(F.from_json(F.col("value").cast("string"), schema).alias("v")).select("v.*")
    return spark.readStream.schema(schema).option("maxFilesPerTrigger", 1).json(f"{a.in_dir}/{topic}")


cell_y = lambda: F.floor(F.col("lat") / LAT_CELL).cast("long")
cell_x = lambda: F.floor(F.col("lon") / LON_CELL).cast("long")
offsets = F.array(F.lit(-1), F.lit(0), F.lit(1))

flashes = (read("glm-flashes", flash_schema)
           .withColumn("flash_ts", F.to_timestamp("flash_time"))
           .withWatermark("flash_ts", f"{a.t_hours + 1} hours")
           .withColumn("cy", cell_y()).withColumn("cx", cell_x())
           .withColumn("dy", F.explode(offsets)).withColumn("dx", F.explode(offsets))
           .withColumn("cell", F.concat_ws("_", F.col("cy") + F.col("dy"), F.col("cx") + F.col("dx")))
           .select("cell", "flash_ts", F.col("lat").alias("flash_lat"), F.col("lon").alias("flash_lon"),
                   F.col("sent_ms").alias("flash_sent_ms")))

fires = (read("firms-fires", fire_schema)
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

writer = matches.writeStream.outputMode("append").option("checkpointLocation", a.checkpoint) \
    .trigger(processingTime="5 seconds")
if a.sink == "mongodb":
    writer = writer.format("mongodb").option("spark.mongodb.connection.uri", a.mongo_uri) \
        .option("spark.mongodb.database", "wildfire").option("spark.mongodb.collection", "stream_matches")
else:
    writer = writer.format("parquet").option("path", a.out_dir)
query = writer.start()

print(f"Streaming started (D={a.d_km} km, T={a.t_hours} h). Running for {a.run_seconds} s...")
t_end = time.time() + a.run_seconds
while time.time() < t_end and query.isActive:
    time.sleep(10)
    lp = query.lastProgress
    if lp:
        print(f"  batch {lp['batchId']}: {lp['numInputRows']:,} new events")
progress = [p_ for p_ in query.recentProgress if p_["numInputRows"] > 0]
query.stop()

rows = sum(p_["numInputRows"] for p_ in progress)
rates = [p_.get("processedRowsPerSecond", 0) for p_ in progress if p_.get("processedRowsPerSecond")]
print("\n===== SPARK STREAMING SUMMARY =====")
print(f"Micro-batches with data:  {len(progress)}")
print(f"Events processed:         {rows:,}")
if rates:
    print(f"Peak processing rate:     {max(rates):,.0f} events/s")
