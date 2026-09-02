"""
producer/producer.py

Reads every raw JSON post from data/raw/ and publishes it to the
Kafka topic 'socialdata'.

Requires: pip install kafka-python
Run from the project root: python producer/producer.py
Assumes Kafka is reachable at localhost:9092 (matches the docker-compose
port mapping).
"""

import json
import os
import time
from kafka import KafkaProducer

RAW_DIR = os.path.join("data", "raw")
TOPIC = "socialdata"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

files = sorted(f for f in os.listdir(RAW_DIR) if f.endswith(".json"))
print(f"Found {len(files)} raw files in {RAW_DIR}")

sent = 0
for fname in files:
    with open(os.path.join(RAW_DIR, fname)) as f:
        post = json.load(f)
    producer.send(TOPIC, post)
    sent += 1
    if sent <= 5 or sent % 200 == 0:
        print(f"  -> sent message {sent}: id={post['id']} user={post['username']}")

producer.flush()
print(f"Producer flush complete. Total messages sent: {sent}")
