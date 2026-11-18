#!/bin/bash
# Benchmark the streaming pipeline at several replay speeds.
# For each speed: reset Kafka topics + results, start Spark, wait until it is
# listening, replay the data, wait for Spark to finish, then check results.
cd "$(dirname "$0")"
PKGS=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.mongodb.spark:mongo-spark-connector_2.12:10.4.0
SPEEDS=${SPEEDS:-"3600 36000 0"}   # 0 = as fast as possible
TOPICS="glm-flashes firms-fires new-fires"

# The in-stream new-fire detector (applyInPandasWithState) needs pandas + pyarrow in the Spark container.
docker exec spark python3 -c "import pandas, pyarrow" 2>/dev/null || \
  docker exec spark pip3 install -q "numpy<2" "pandas<2.3" "pyarrow<15"

for SPEED in $SPEEDS; do
  echo; echo "########## Speed ${SPEED}x (0 = max) ##########"
  for t in $TOPICS; do
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic $t >/dev/null 2>&1
  done
  sleep 15
  for t in $TOPICS; do
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic $t --partitions 3 --replication-factor 1 >/dev/null
  done
  docker exec mongodb mongosh wildfire --quiet --eval "db.stream_matches.drop(); db.stream_new_fires.drop()" >/dev/null
  docker exec spark rm -rf /tmp/spark_chk

  if [ "$SPEED" = "3600" ]; then RUN=300; else RUN=150; fi
  docker exec spark /opt/spark/bin/spark-submit --packages $PKGS --conf spark.jars.ivy=/tmp/.ivy2 \
    spark_match.py --run-seconds $RUN > spark_log_$SPEED.txt 2>&1 &
  SPARK_PID=$!
  echo "Waiting for Spark to start..."
  for i in $(seq 1 120); do
    grep -q "Streaming started" spark_log_$SPEED.txt && break
    sleep 2
  done
  if ! grep -q "Streaming started" spark_log_$SPEED.txt; then
    echo "Spark did not start. Last lines of spark_log_$SPEED.txt:"; tail -20 spark_log_$SPEED.txt; exit 1
  fi
  echo "Spark is listening. Starting replay."
  python3.14 -u replay_producer.py --speed $SPEED | tee replay_log_$SPEED.txt
  echo "Replay done. Waiting for Spark to finish its ${RUN}-second run..."
  wait $SPARK_PID
  grep -A3 "SPARK STREAMING SUMMARY" spark_log_$SPEED.txt
  python3.14 compare_stream.py | tee compare_log_$SPEED.txt
done

python3.14 - "$SPEEDS" <<'PY'
import re, sys
import pandas as pd

def grab(path, pattern):
    try:
        m = re.search(pattern, open(path).read())
        return m.group(1).replace(",", "") if m else ""
    except FileNotFoundError:
        return ""

rows = []
for s in sys.argv[1].split():
    lat = re.search(r"End-to-end latency.*?:\s+([\d.]+) s / ([\d.]+) s", open(f"compare_log_{s}.txt").read()) \
        if grab(f"compare_log_{s}.txt", r"(Agreement)") else None
    rows.append({
        "replay_speed": "max" if s == "0" else f"{int(s):,}x",
        "events": grab(f"replay_log_{s}.txt", r"Events sent:\s+([\d,]+)"),
        "replay_time_s": grab(f"replay_log_{s}.txt", r"Wall time:\s+([\d.]+)"),
        "producer_events_per_s": grab(f"replay_log_{s}.txt", r"Throughput:\s+([\d,]+)"),
        "spark_peak_events_per_s": grab(f"spark_log_{s}.txt", r"Peak processing rate:\s+([\d,]+)"),
        "new_fires_detected": grab(f"compare_log_{s}.txt", r"New fires detected by Spark:\s+([\d,]+)"),
        "new_fire_agreement": grab(f"compare_log_{s}.txt", r"New-fire agreement:.*\((\d+%)\)"),
        "new_fires_matched": grab(f"compare_log_{s}.txt", r"New fires matched by Spark:\s+([\d,]+)"),
        "agreement": grab(f"compare_log_{s}.txt", r"Agreement:.*\((\d+%)\)"),
        "latency_median_s": lat.group(1) if lat else "",
        "latency_p95_s": lat.group(2) if lat else "",
        "detect_latency_median_s": grab(f"compare_log_{s}.txt", r"detection latency.*?:\s+([\d.]+) s"),
    })
df = pd.DataFrame(rows)
df.to_csv("benchmark_results.csv", index=False)
print("\n===== BENCHMARK RESULTS =====")
print(df.to_string(index=False))
print("\nSaved: benchmark_results.csv")
PY
