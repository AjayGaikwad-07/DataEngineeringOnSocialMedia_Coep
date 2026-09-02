# SocialMedia_DE_Practicals — build order

Your folder structure is already right. Here's where everything from
this chat goes, and the order to actually run it for real.

## 1. Place the files

| File (from this chat) | Goes to |
|---|---|
| `generate_sample_data.py` | `data/generate_sample_data.py` |
| `producer.py` | `producer/producer.py` |
| `consumer.py` | `consumer/consumer.py` |
| `spark_process.py` (earlier msg) | `spark/process_and_load.py` |
| `dashboard.py` (earlier msg) | `dashboard/dashboard.py` |
| `social_media_pipeline_dag.py` (earlier msg) | `airflow/dags/social_media_pipeline_dag.py` |
| `docker-compose.yml` (earlier msg) | project root |

## 2. Install what runs on the host (not in Docker)

```powershell
pip install kafka-python pandas sqlalchemy psycopg2-binary matplotlib seaborn
```

## 3. Bring up the stack

```powershell
docker compose up -d
docker compose ps    # confirm kafka + postgres (+ spark, airflow) are healthy
```

## 4. Run the pipeline, step by step — screenshot each

```powershell
python data\generate_sample_data.py     # -> 1,000 files in data\raw\
python producer\producer.py             # -> Kafka producer log
python consumer\consumer.py             # -> Kafka consumer log, writes data\landing\kafka_posts.jsonl
pip install vaderSentiment              # Install sentiment analyzer
python spark\process_and_load.py        # Process data and load to PostgreSQL
python dashboard\dashboard.py           # -> charts in dashboard\output\
```

Then check the DB directly, e.g. with `psql` or any GUI client against
`localhost:5434`, db `socialmedia`:

```sql
SELECT * FROM posts LIMIT 5;
SELECT * FROM hashtag_counts ORDER BY count DESC LIMIT 5;
```

## 5. Airflow

Open `http://localhost:8081`, log in (`admin`/`admin` if using the
compose file as given), enable the `social_media_pipeline` DAG,
trigger it manually once, confirm all tasks go green.

That full run — real terminal output, real DB rows, real charts, real
Airflow graph — is what's worth screenshotting for your professor.
If anything errors at any step, paste me the exact error and I'll help
you fix it.
