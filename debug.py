import json
import psycopg2

# Check what's in the landing file
print("=== LANDING FILE ===")
with open('data/landing/kafka_posts.jsonl') as f:
    for i, line in enumerate(f):
        if i >= 3:
            break
        post = json.loads(line)
        print(f"Post {i}: id={post.get('id')}, tweet_id={post.get('tweet_id')}, username={post.get('username')}")

# Check database
print("\n=== DATABASE ===")
conn = psycopg2.connect(
    host='localhost',
    port=5434,
    database='socialmedia',
    user='postgres',
    password='password',
)
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM posts')
print(f"Posts in DB: {cur.fetchone()[0]}")
cur.execute('SELECT COUNT(*) FROM hashtag_counts')
print(f"Hashtags in DB: {cur.fetchone()[0]}")

# Try inserting one manually
print("\n=== TESTING MANUAL INSERT ===")
try:
    cur.execute('''
        INSERT INTO posts (id, username, text, likes, retweets, sentiment_score, sentiment_label)
        VALUES (999, 'testuser', 'test text', 10, 5, 0.5, 'positive')
    ''')
    conn.commit()
    print("Manual insert succeeded")
    cur.execute('SELECT COUNT(*) FROM posts')
    print(f"Posts after manual insert: {cur.fetchone()[0]}")
except Exception as e:
    print(f"Manual insert failed: {e}")

cur.close()
conn.close()
