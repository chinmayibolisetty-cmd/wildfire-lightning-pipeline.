"""
Replay GLM flashes and FIRMS fire detections into Kafka in event-time order.

--speed 3600 means 1 hour of 2020 data is sent per second of real time.
--speed 0 sends as fast as possible (throughput test).

Usage (on your Mac):
    python3.14 replay_producer.py --speed 3600
Test mode (no Kafka, writes JSON files instead):
    python3.14 replay_producer.py --to-dir stream_in --speed 0
"""
import argparse
import json
import os
import time

import pandas as pd

p = argparse.ArgumentParser()
p.add_argument("--speed", type=float, default=3600, help="Replay speed-up; 0 = as fast as possible")
p.add_argument("--bootstrap", default="localhost:9092")
p.add_argument("--start", default="2020-08-15")
p.add_argument("--end", default="2020-08-23", help="Exclusive end date")
p.add_argument("--to-dir", default=None, help="Write JSON files instead of sending to Kafka (testing)")
a = p.parse_args()
start, end = pd.Timestamp(a.start), pd.Timestamp(a.end)

# ---- Load and prepare events ----
glm = pd.read_csv("glm_aug2020.csv", parse_dates=["flash_time"])
glm = glm[(glm.flash_time >= start) & (glm.flash_time < end)]
flashes = pd.DataFrame({"t": glm.flash_time, "topic": "glm-flashes",
                        "lat": glm.lat, "lon": glm.lon})

firms = pd.read_csv("firms_aug2020.csv")
hhmm = firms["acq_time"].astype(int).astype(str).str.zfill(4)
firms["t"] = pd.to_datetime(firms["acq_date"]) + pd.to_timedelta(hhmm.str[:2].astype(int), unit="h") \
    + pd.to_timedelta(hhmm.str[2:].astype(int), unit="m")
firms = firms[firms["confidence"].astype(str).str.lower() != "l"]  # same filter as the prototype
firms = firms[(firms.t >= start) & (firms.t < end)]
fires = pd.DataFrame({"t": firms.t, "topic": "firms-fires",
                      "lat": firms.latitude, "lon": firms.longitude})

events = pd.concat([flashes, fires]).sort_values("t", kind="stable").reset_index(drop=True)
print(f"Replaying {len(flashes):,} flashes + {len(fires):,} fire detections "
      f"({a.start} to {a.end}) at speed {'max' if a.speed == 0 else f'{a.speed:g}x'}")


def to_msg(r):
    ts = r.t.strftime("%Y-%m-%d %H:%M:%S.%f")
    sent_ms = int(time.time() * 1000)
    if r.topic == "glm-flashes":
        return {"flash_time": ts, "lat": float(r.lat), "lon": float(r.lon), "sent_ms": sent_ms}
    fire_id = f"{r.lat:.5f},{r.lon:.5f},{r.t:%Y-%m-%dT%H:%M}"
    return {"fire_id": fire_id, "fire_time": ts, "lat": float(r.lat), "lon": float(r.lon), "sent_ms": sent_ms}


# ---- Send ----
if a.to_dir:
    for topic in ("glm-flashes", "firms-fires"):
        os.makedirs(os.path.join(a.to_dir, topic), exist_ok=True)
    out = {t: open(os.path.join(a.to_dir, t, "part-0.json"), "w") for t in ("glm-flashes", "firms-fires")}
    send = lambda topic, msg: out[topic].write(json.dumps(msg) + "\n")
else:
    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=a.bootstrap, linger_ms=20,
                             value_serializer=lambda v: json.dumps(v).encode())
    send = lambda topic, msg: producer.send(topic, msg)

t0_event, t0_wall = events.t.iloc[0], time.time()
last_report = t0_wall
for i, r in enumerate(events.itertuples(index=False), 1):
    if a.speed > 0:
        due = t0_wall + (r.t - t0_event).total_seconds() / a.speed
        delay = due - time.time()
        if delay > 0:
            time.sleep(delay)
    send(r.topic, to_msg(r))
    if time.time() - last_report >= 10:
        last_report = time.time()
        print(f"  {i:,}/{len(events):,} sent; replay clock at {r.t:%Y-%m-%d %H:%M} UTC")

if a.to_dir:
    for f in out.values():
        f.close()
else:
    producer.flush()
elapsed = time.time() - t0_wall
print("\n===== REPLAY SUMMARY =====")
print(f"Events sent:     {len(events):,}")
print(f"Wall time:       {elapsed:.1f} s")
print(f"Throughput:      {len(events) / max(elapsed, 1e-9):,.0f} events/s")
