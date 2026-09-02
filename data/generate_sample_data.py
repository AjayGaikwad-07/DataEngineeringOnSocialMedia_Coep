"""
data/generate_sample_data.py

Generates 1,000 AI-related sample social media posts and writes each
as its own JSON file under data/raw/ — this is the "raw JSON data lake"
step from the assignment PDF.

Run:  python data/generate_sample_data.py
(from the SocialMedia_DE_Practicals folder)

Swap this out later for a real PRAW (Reddit) collector once you have
API keys — same output shape (one JSON object per post), so nothing
downstream (producer/Spark) needs to change.
"""

import json
import os
import random
from datetime import datetime, timedelta

random.seed(7)

OUT_DIR = os.path.join("data", "raw")
os.makedirs(OUT_DIR, exist_ok=True)

users = [f"User{i}" for i in range(1, 121)]
hashtags_pool = ["#AI", "#MachineLearning", "#DataScience", "#DeepLearning",
                  "#Kafka", "#Spark", "#BigData", "#Python", "#OpenAI", "#Automation"]
templates_pos = [
    "AI is transforming the future of {topic} and it's amazing {tags}",
    "Just built a great {topic} model, loving the results {tags}",
    "{topic} keeps getting better every year, so excited {tags}",
    "Huge breakthrough in {topic} today, this is fantastic {tags}",
]
templates_neg = [
    "Worried about AI replacing jobs in {topic} {tags}",
    "This {topic} pipeline keeps failing and it's frustrating {tags}",
    "Not impressed with the latest {topic} tool, disappointing {tags}",
]
templates_neu = [
    "Reading about {topic} today {tags}",
    "New paper published on {topic} {tags}",
    "Discussion thread on {topic} trends {tags}",
    "Attending a webinar on {topic} this week {tags}",
]
topics = ["AI", "machine learning", "data engineering", "deep learning",
          "automation", "neural networks", "generative AI", "robotics"]

N = 1000
start = datetime(2026, 8, 1)

for i in range(1, N + 1):
    kind = random.choices(["pos", "neg", "neu"], weights=[0.45, 0.2, 0.35])[0]
    tmpl = random.choice({"pos": templates_pos, "neg": templates_neg, "neu": templates_neu}[kind])
    tags = " ".join(random.sample(hashtags_pool, k=random.randint(1, 3)))
    text = tmpl.format(topic=random.choice(topics), tags=tags)
    ts = start + timedelta(minutes=random.randint(0, 60 * 24 * 30))

    post = {
        "id": 10000 + i,
        "username": random.choice(users),
        "text": text,
        "likes": random.randint(0, 500),
        "retweets": random.randint(0, 150),
        "created_at": ts.isoformat(),
    }

    with open(os.path.join(OUT_DIR, f"post_{i:05d}.json"), "w") as f:
        json.dump(post, f)

print(f"Wrote {N} raw JSON files to {OUT_DIR}")
