"""
spark/pyspark_analysis.py

Assignment 3 — Apache Spark / PySpark Data Processing
======================================================
Demonstrates PySpark:
  1. SparkSession creation & data loading
  2. Schema definition & DataFrame operations
  3. RDD-style transformations
  4. Cleaning & filtering transformations
  5. Sentiment categorisation (UDF)
  6. Hashtag explosion & aggregation
  7. Spark SQL queries
  8. Final summary metrics

Run:  python spark/pyspark_analysis.py
(Java 17 must be installed and on PATH)
"""

import os
import sys

# ── Setup JAVA_HOME if not set ──────────────────────────────────────────────
# Try to locate the Microsoft OpenJDK 17 installed by winget
_java_candidates = [
    r"C:\Program Files\Microsoft\jdk-17.0.20.1+1",
    r"C:\Program Files\Microsoft\jdk-17.0.20.101-hotspot",
    r"C:\Program Files\Eclipse Adoptium\jdk-17.0.11.9-hotspot",
    r"C:\Program Files\Java\jdk-17",
]
if "JAVA_HOME" not in os.environ:
    for _p in _java_candidates:
        if os.path.exists(_p):
            os.environ["JAVA_HOME"] = _p
            print(f"[INFO] Set JAVA_HOME = {_p}")
            break

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, FloatType, TimestampType
)

# ── 1. SparkSession ──────────────────────────────────────────────────────────
print("\n" + "="*60)
print("ASSIGNMENT 3 — PySpark Social Media Data Processing")
print("="*60)

spark = (
    SparkSession.builder
    .appName("SocialMediaPipeline_Spark")
    .master("local[*]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
print(f"\n[OK] SparkSession started  —  Spark {spark.version}")
print(f"     Master : {spark.sparkContext.master}")
print(f"     App ID : {spark.sparkContext.applicationId}")

# ── 2. Define schema & load data ────────────────────────────────────────────
schema = StructType([
    StructField("id",         LongType(),   True),
    StructField("username",   StringType(), True),
    StructField("text",       StringType(), True),
    StructField("likes",      IntegerType(), True),
    StructField("retweets",   IntegerType(), True),
    StructField("created_at", StringType(), True),
])

INPUT = "data/landing/kafka_posts.jsonl"
print(f"\n[STEP 1] Loading raw JSON from  {INPUT}")
df_raw = (
    spark.read
    .schema(schema)
    .json(INPUT)
)
total_raw = df_raw.count()
print(f"         Raw records loaded : {total_raw}")
print("\nSchema:")
df_raw.printSchema()

print("Sample raw rows:")
df_raw.show(5, truncate=80)

# ── 3. RDD-style demo (using DataFrame for Windows PySpark 4 compat) ─────────
# Note: On Windows with PySpark 4 + Python 3.14, native Python RDD worker
# socket connections can timeout. We demonstrate the equivalent using Spark SQL
# which avoids the JVM ↔ Python socket protocol entirely.
print("\n[STEP 2] Hashtag extraction (RDD-equivalent using DataFrame explode)")
print("   Demonstrating: flatMap -> filter -> reduceByKey pattern via Spark SQL")
df_words = (
    df_raw
    .select(F.explode(F.split(F.col("text"), r"\s+")).alias("word"))
    .filter(F.col("word").startswith("#"))
    .withColumn("word", F.lower(F.regexp_replace("word", r"[.,!?]", "")))
)
top_df = df_words.groupBy("word").count().orderBy(F.desc("count")).limit(10)
print("Top 10 hashtags (DataFrame reduceByKey equivalent):")
top_df.show(10, truncate=False)

# ── 4. DataFrame cleaning ────────────────────────────────────────────────────
print("\n[STEP 3] DataFrame cleaning & transformations")
df_clean = (
    df_raw
    .dropna(subset=["id", "text"])
    .filter(F.col("text") != "")
    .filter(F.length(F.col("text")) > 10)
    .withColumn("created_ts", F.to_timestamp("created_at"))
    .withColumn("post_hour",  F.hour("created_ts"))
    .withColumn("post_date",  F.to_date("created_ts"))
    .withColumn("text_clean", F.regexp_replace("text", r"[^\w\s#@]", ""))
    .withColumn("likes",      F.coalesce(F.col("likes"),    F.lit(0)))
    .withColumn("retweets",   F.coalesce(F.col("retweets"), F.lit(0)))
    .withColumn("engagement", F.col("likes") + F.col("retweets"))
    .dropDuplicates(["id"])
)
clean_count = df_clean.count()
print(f"   Records after cleaning: {clean_count}  (dropped {total_raw - clean_count})")
print("\n   Sample cleaned data:")
df_clean.select("id", "username", "post_hour", "engagement", "text_clean").show(5, truncate=70)

# ── 5. Sentiment scoring (keyword-based, no UDF — avoids PySpark4+Py3.14 socket issue) ──
print("[STEP 4] Sentiment scoring — keyword-based Spark SQL approach")
print("   (Uses native Spark SQL CASE expressions instead of Python UDF)")

# Positive / negative keyword lists embedded as SQL expressions
pos_words = ["amazing", "fantastic", "great", "loving", "excited", "breakthrough", "excellent"]
neg_words = ["worried", "failing", "frustrating", "disappointing", "not impressed", "bad", "worse"]

pos_pattern = "|".join(pos_words)
neg_pattern = "|".join(neg_words)

df_sentiment = (
    df_clean
    .withColumn(
        "sentiment_score",
        F.when(F.lower(F.col("text")).rlike(pos_pattern),  F.lit(0.65))
         .when(F.lower(F.col("text")).rlike(neg_pattern),  F.lit(-0.60))
         .otherwise(F.lit(0.05))
         .cast(FloatType())
    )
    .withColumn(
        "sentiment_label",
        F.when(F.col("sentiment_score") >= 0.05,  F.lit("positive"))
         .when(F.col("sentiment_score") <= -0.05, F.lit("negative"))
         .otherwise(F.lit("neutral"))
    )
)
print("   Sentiment distribution:")
df_sentiment.groupBy("sentiment_label").count().orderBy("count", ascending=False).show()


# ── 6. Hashtag explosion & aggregation ───────────────────────────────────────
print("[STEP 5] Hashtag extraction using regexp_extract_all + explode")
df_tags = (
    df_clean
    .withColumn("hashtag_array",
        F.expr(r"regexp_extract_all(text, '#(\\w+)', 0)"))
    .withColumn("hashtag", F.explode_outer("hashtag_array"))
    .withColumn("hashtag", F.lower(F.col("hashtag")))
    .filter(F.col("hashtag").isNotNull())
)

print("   Top 15 trending hashtags (DataFrame):")
(
    df_tags.groupBy("hashtag")
    .agg(F.count("*").alias("count"))
    .orderBy(F.desc("count"))
    .show(15)
)

# ── 7. Spark SQL ─────────────────────────────────────────────────────────────
print("[STEP 6] Spark SQL queries")
df_sentiment.createOrReplaceTempView("posts")
df_tags.createOrReplaceTempView("post_tags")

print("   SQL: Top 10 most active users")
spark.sql("""
    SELECT username, COUNT(*) AS post_count,
           ROUND(AVG(engagement),1) AS avg_engagement
    FROM posts
    GROUP BY username
    ORDER BY post_count DESC
    LIMIT 10
""").show()

print("   SQL: Hourly activity distribution")
spark.sql("""
    SELECT post_hour, COUNT(*) AS posts
    FROM posts
    WHERE post_hour IS NOT NULL
    GROUP BY post_hour
    ORDER BY post_hour
""").show(24)

print("   SQL: Sentiment breakdown by day")
spark.sql("""
    SELECT post_date, sentiment_label, COUNT(*) AS posts
    FROM posts
    WHERE post_date IS NOT NULL
    GROUP BY post_date, sentiment_label
    ORDER BY post_date, sentiment_label
    LIMIT 20
""").show()

print("   SQL: Top 5 hashtags per sentiment")
spark.sql("""
    SELECT p.sentiment_label, t.hashtag, COUNT(*) AS uses
    FROM posts p
    JOIN post_tags t ON p.id = t.id
    GROUP BY p.sentiment_label, t.hashtag
    ORDER BY p.sentiment_label, uses DESC
    LIMIT 20
""").show(20)

# ── 8. Summary statistics ────────────────────────────────────────────────────
print("[STEP 7] Summary statistics")
spark.sql("""
    SELECT
        COUNT(*)                         AS total_posts,
        COUNT(DISTINCT username)         AS unique_users,
        ROUND(AVG(engagement), 2)        AS avg_engagement,
        MAX(engagement)                  AS max_engagement,
        ROUND(AVG(sentiment_score), 4)   AS avg_sentiment
    FROM posts
""").show()

# ── 9. Save processed data (via toPandas — avoids winutils requirement) ───────
OUT_DIR = "spark/output"
os.makedirs(OUT_DIR, exist_ok=True)
csv_path = os.path.join(OUT_DIR, "processed_posts.csv")

# Collect to driver and save with Python's built-in CSV (no Hadoop needed)
pdf = df_sentiment.select(
    "id", "username", "text", "likes", "retweets",
    "created_at", "post_hour", "engagement",
    "sentiment_score", "sentiment_label"
).toPandas()
pdf.to_csv(csv_path, index=False, encoding="utf-8")
print(f"\n[OK] Processed data written to {csv_path}  ({len(pdf)} rows)")

# Also save top hashtags
tag_path = os.path.join(OUT_DIR, "top_hashtags.csv")
top_tags_pdf = (
    df_tags.groupBy("hashtag")
    .agg(F.count("*").alias("count"))
    .orderBy(F.desc("count"))
    .limit(50)
    .toPandas()
)
top_tags_pdf.to_csv(tag_path, index=False, encoding="utf-8")
print(f"[OK] Top hashtags written to {tag_path}")

spark.stop()
print("\n" + "="*60)
print("ASSIGNMENT 3 — PySpark Processing COMPLETE")
print("="*60 + "\n")
