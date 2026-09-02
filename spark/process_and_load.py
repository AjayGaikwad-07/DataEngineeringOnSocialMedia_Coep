"""
spark/process_and_load.py

Simple Python version (no Spark needed) — reads Kafka-landed posts, 
cleans them, extracts hashtags, scores sentiment, writes to PostgreSQL.

Run: python spark/process_and_load.py
"""

import json
import re
from datetime import datetime
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import psycopg2
from psycopg2.extras import execute_values

# Config
INPUT_PATH = "data/landing/kafka_posts.jsonl"
DB_HOST = "localhost"
DB_PORT = 5434
DB_NAME = "socialmedia"
DB_USER = "postgres"
DB_PASSWORD = "password"

print("Starting data processing pipeline...")

# Sentiment analyzer
analyzer = SentimentIntensityAnalyzer()

# Connect to Postgres
try:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    conn_admin = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    conn_admin.autocommit = True
    cur_admin = conn_admin.cursor()
except Exception as e:
    print(f"ERROR: Cannot connect to database: {e}")
    exit(1)

# Drop and recreate tables
print("Dropping old tables...")
try:
    cur_admin.execute("DROP TABLE IF EXISTS hashtag_counts CASCADE")
    cur_admin.execute("DROP TABLE IF EXISTS posts CASCADE")
    cur_admin.execute("COMMIT")  # Ensure drop is committed
    print("✓ Old tables dropped")
except Exception as e:
    print(f"Warning: {e}")

print("Creating tables...")
try:
    cur_admin.execute("""
        CREATE TABLE posts (
            id BIGINT PRIMARY KEY,
            username VARCHAR(255),
            text TEXT,
            likes INT DEFAULT 0,
            retweets INT DEFAULT 0,
            created_at TIMESTAMP,
            post_hour INT,
            hashtags_str TEXT,
            sentiment_score FLOAT,
            sentiment_label VARCHAR(50)
        )
    """)
    cur_admin.execute("""
        CREATE TABLE hashtag_counts (
            hashtag VARCHAR(255) PRIMARY KEY,
            count INT
        )
    """)
    cur_admin.execute("COMMIT")  # Ensure create is committed
    print("✓ Tables created")
except Exception as e:
    print(f"ERROR creating tables: {e}")
    cur_admin.close()
    conn_admin.close()
    conn.close()
    exit(1)

cur_admin.close()
conn_admin.close()

# Read and process posts
posts_data = []
posts_by_id = {}  # Track to deduplicate
hashtag_counts = {}
skipped = 0

print(f"\nReading from {INPUT_PATH}...")
try:
    with open(INPUT_PATH, 'r') as f:
        for line_num, line in enumerate(f, 1):
            try:
                post = json.loads(line.strip())
                
                # Get ID - handle both 'id' and 'tweet_id' fields
                post_id = post.get('id') or post.get('tweet_id')
                if not post_id:
                    skipped += 1
                    continue
                
                # Skip if we've already seen this post ID
                if post_id in posts_by_id:
                    skipped += 1
                    continue
                posts_by_id[post_id] = True
                
                try:
                    post_id = int(post_id)
                except (ValueError, TypeError):
                    skipped += 1
                    continue
                
                # Get text
                text = post.get('text', '')
                if not text:
                    skipped += 1
                    continue
                
                # Extract hashtags - use existing list or extract from text
                hashtags_lower = []
                if 'hashtags' in post and isinstance(post['hashtags'], list):
                    hashtags_lower = [str(h).lower().strip() for h in post['hashtags']]
                else:
                    hashtags = re.findall(r'#(\w+)', text)
                    hashtags_lower = [h.lower() for h in hashtags]
                
                hashtags_str = ','.join(hashtags_lower)
                
                # Count hashtags
                for tag in hashtags_lower:
                    hashtag_counts[tag] = hashtag_counts.get(tag, 0) + 1
                
                # Sentiment score
                sentiment = analyzer.polarity_scores(text)
                compound = float(sentiment['compound'])
                if compound >= 0.05:
                    label = "positive"
                elif compound <= -0.05:
                    label = "negative"
                else:
                    label = "neutral"
                
                # Parse created_at to extract hour
                created_at = post.get('created_at')
                post_hour = None
                if created_at:
                    try:
                        dt = datetime.fromisoformat(created_at)
                        post_hour = dt.hour
                    except:
                        pass
                
                posts_data.append((
                    post_id,
                    post.get('username', 'unknown'),
                    text,
                    int(post.get('likes', 0)),
                    int(post.get('retweets', 0)),
                    created_at,
                    post_hour,
                    hashtags_str,
                    compound,
                    label,
                ))
                
                if line_num % 500 == 0:
                    print(f"  ✓ Processed {line_num} lines, {len(posts_data)} valid posts")
                    
            except json.JSONDecodeError as e:
                skipped += 1
                continue
except FileNotFoundError:
    print(f"ERROR: {INPUT_PATH} not found!")
    exit(1)

print(f"\nRead complete: {len(posts_data)} posts, {skipped} skipped")

# Insert into database
if posts_data:
    cur = conn.cursor()
    try:
        print(f"Inserting {len(posts_data)} posts into database...")
        execute_values(
            cur,
            """
            INSERT INTO posts 
            (id, username, text, likes, retweets, created_at, post_hour, hashtags_str, sentiment_score, sentiment_label)
            VALUES %s
            """,
            posts_data,
        )
        conn.commit()
        print(f"✓ Inserted {len(posts_data)} posts successfully")
    except Exception as e:
        print(f"ERROR inserting posts: {e}")
        conn.rollback()
    cur.close()

# Insert hashtag counts
if hashtag_counts:
    cur = conn.cursor()
    try:
        print(f"Inserting {len(hashtag_counts)} unique hashtags...")
        hashtag_data = [(tag, count) for tag, count in hashtag_counts.items()]
        execute_values(
            cur,
            "INSERT INTO hashtag_counts (hashtag, count) VALUES %s",
            hashtag_data,
        )
        conn.commit()
        print(f"✓ Inserted {len(hashtag_counts)} hashtags successfully")
    except Exception as e:
        print(f"ERROR inserting hashtags: {e}")
        conn.rollback()
    cur.close()

conn.close()
print("\n✓ Pipeline complete!")
