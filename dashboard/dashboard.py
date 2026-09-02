"""
dashboard/dashboard.py

Connects to the 'posts' and 'hashtag_counts' tables in PostgreSQL
(populated by spark/process_and_load.py) and renders the four
required dashboard metrics:
  - Total Posts
  - Top Users
  - Trending Hashtags
  - Most Active Hours

Run this from your host (Windows) with:
    pip install pandas sqlalchemy psycopg2-binary matplotlib seaborn
    python dashboard/dashboard.py

It connects to Postgres on localhost:5434 (the port already mapped
by your docker-compose setup).
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine

OUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUT_DIR, exist_ok=True)

engine = create_engine("postgresql+psycopg2://postgres:password@localhost:5434/socialmedia")

posts = pd.read_sql("SELECT * FROM posts", engine)
hashtags = pd.read_sql("SELECT * FROM hashtag_counts ORDER BY count DESC LIMIT 15", engine)

sns.set_theme(style="whitegrid")

# ---------------------------------------------------------------------------
# 1. Total Posts
# ---------------------------------------------------------------------------
total_posts = len(posts)
print(f"Total Posts: {total_posts}")

# ---------------------------------------------------------------------------
# 2. Top Users
# ---------------------------------------------------------------------------
top_users = posts["username"].value_counts().head(10)
plt.figure(figsize=(8, 5))
sns.barplot(x=top_users.values, y=top_users.index, orient="h")
plt.title("Top Users by Post Count")
plt.xlabel("Posts")
plt.ylabel("Username")
plt.tight_layout()
plt.savefig(os.path.join(OUT_DIR, "top_users.png"))
plt.close()

# ---------------------------------------------------------------------------
# 3. Trending Hashtags
# ---------------------------------------------------------------------------
plt.figure(figsize=(8, 5))
sns.barplot(data=hashtags, x="count", y="hashtag", orient="h")
plt.title("Trending Hashtags")
plt.xlabel("Frequency")
plt.ylabel("Hashtag")
plt.tight_layout()
plt.savefig(os.path.join(OUT_DIR, "trending_hashtags.png"))
plt.close()

# ---------------------------------------------------------------------------
# 4. Most Active Hours
# ---------------------------------------------------------------------------
if "post_hour" in posts.columns and posts["post_hour"].notna().any():
    hourly = posts.groupby("post_hour").size().reindex(range(24), fill_value=0)
    plt.figure(figsize=(9, 5))
    sns.barplot(x=hourly.index, y=hourly.values, color="steelblue")
    plt.title("Most Active Hours")
    plt.xlabel("Hour of Day (0-23)")
    plt.ylabel("Posts")
    plt.tight_layout()
    plt.savefig(os.path.join(OUT_DIR, "active_hours.png"))
    plt.close()
else:
    print("No usable 'created_at' timestamps found — skipping active-hours chart.")

# ---------------------------------------------------------------------------
# Bonus: sentiment breakdown (feeds the "Sentiment" chart the PDF also asks for)
# ---------------------------------------------------------------------------
if "sentiment_label" in posts.columns:
    sentiment_counts = posts["sentiment_label"].value_counts()
    plt.figure(figsize=(6, 6))
    plt.pie(sentiment_counts.values, labels=sentiment_counts.index, autopct="%1.1f%%")
    plt.title("Sentiment Breakdown")
    plt.tight_layout()
    plt.savefig(os.path.join(OUT_DIR, "sentiment_breakdown.png"))
    plt.close()

print(f"Charts saved to {OUT_DIR}")
print("\nSummary")
print("-------")
print(f"Total Posts: {total_posts}")
print(f"Top user: {top_users.index[0]} ({top_users.iloc[0]} posts)")
print(f"Top hashtag: #{hashtags.iloc[0]['hashtag']} ({hashtags.iloc[0]['count']} uses)")
