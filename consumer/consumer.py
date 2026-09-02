"""
consumer/consumer.py

Subscribes to the 'socialdata' Kafka topic and writes every message
to data/landing/kafka_posts.jsonl (one JSON object per line) — this
is what spark/process_and_load.py reads next.

Requires: pip install kafka-python
Run from the project root: python consumer/consumer.py
It will keep listening — stop it with Ctrl+C once it stops receiving
new messages (or add consumer_timeout_ms as shown below for a clean exit).
"""

import json
import os
from kafka import KafkaConsumer

TOPIC = "socialdata"
OUT_PATH = os.path.join("data", "landing", "kafka_posts.jsonl")
os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    consumer_timeout_ms=10000,  # stop automatically after 10s of no new messages
)

print(f"Subscribed to '{TOPIC}'. Listening...")

count = 0
with open(OUT_PATH, "w") as out:
    for msg in consumer:
        post = msg.value
        out.write(json.dumps(post) + "\n")
        count += 1
        if count <= 5 or count % 200 == 0:
            print(f"  <- received message {count}: id={post.get('id', 'N/A')} user={post.get('username', 'N/A')}")

print(f"Consumer wrote {count} records to {OUT_PATH}")
