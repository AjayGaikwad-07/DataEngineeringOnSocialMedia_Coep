"""
airflow/airflow_assignment4_output.py

Assignment 4 — Apache Airflow Workflow Orchestration
=====================================================
Generates:
  1. DAG structure visualization (ASCII + Python diagram)
  2. Simulated task run log (what you'd see in Airflow UI)
  3. Gantt-style pipeline run timeline
  4. Summary metrics

Run: python airflow/airflow_assignment4_output.py
"""

import json
import os
from datetime import datetime, timedelta

OUT_DIR = "airflow/output"
os.makedirs(OUT_DIR, exist_ok=True)

print("=" * 70)
print("ASSIGNMENT 4 — Apache Airflow: Social Media Pipeline Orchestration")
print("=" * 70)

# ── 1. DAG metadata ──────────────────────────────────────────────────────────
print("""
[DAG DETAILS]
─────────────────────────────────────────────────────────────
  DAG ID          : social_media_data_pipeline
  Description     : End-to-end Social Media Data Engineering Pipeline
  Schedule        : @daily  (runs every day at midnight UTC)
  Start Date      : 2026-09-01
  Catchup         : False
  Tags            : data-engineering, social-media
  Owner           : airflow
  Max Active Runs : 1
  Retries         : 0
""")

# ── 2. DAG graph (ASCII) ─────────────────────────────────────────────────────
print("[DAG GRAPH — Task Dependencies]")
print("─────────────────────────────────────────────────────────────")
print("""
  ┌──────────────┐     ┌──────────────┐     ┌─────────────────┐
  │ collect_data │────▶│ kafka_stream │────▶│ spark_processing│
  └──────────────┘     └──────────────┘     └────────┬────────┘
                                                      │
                                            ┌─────────▼────────┐
                                            │ store_postgresql  │
                                            └─────────┬────────┘
                                                      │
                                            ┌─────────▼────────┐
                                            │ dashboard_ready   │
                                            └──────────────────┘

  Task Flow:
    collect_data >> kafka_stream >> spark_processing >> store_postgresql >> dashboard_ready
""")

# ── 3. Task definitions ─────────────────────────────────────────────────────
tasks = [
    {
        "id": "collect_data",
        "operator": "PythonOperator",
        "description": "Collect data from social media sources (raw JSON files → data/raw/)",
        "duration_s": 8,
    },
    {
        "id": "kafka_stream",
        "operator": "PythonOperator",
        "description": "Stream 1000 posts through Kafka topic 'socialdata' (3 partitions)",
        "duration_s": 15,
    },
    {
        "id": "spark_processing",
        "operator": "PythonOperator",
        "description": "Clean data, extract hashtags, score sentiment via Spark/Python",
        "duration_s": 22,
    },
    {
        "id": "store_postgresql",
        "operator": "PythonOperator",
        "description": "Insert cleaned records into PostgreSQL (posts + hashtag_counts tables)",
        "duration_s": 11,
    },
    {
        "id": "dashboard_ready",
        "operator": "PythonOperator",
        "description": "Generate dashboard charts (top_users, trending_hashtags, active_hours, sentiment)",
        "duration_s": 6,
    },
]

print("[TASK DEFINITIONS]")
print("─────────────────────────────────────────────────────────────")
for t in tasks:
    print(f"  Task ID    : {t['id']}")
    print(f"  Operator   : {t['operator']}")
    print(f"  Purpose    : {t['description']}")
    print(f"  Duration   : ~{t['duration_s']}s")
    print()

# ── 4. Simulated DAG run log ────────────────────────────────────────────────
print("[DAG RUN LOG — Simulated Execution]")
print("─────────────────────────────────────────────────────────────")
print("  Dag Run ID   : manual__2026-10-10T12:00:00+00:00")
print("  Triggered By : manual")
print("  Start Time   : 2026-10-10 12:00:00 UTC")
print()

run_start = datetime(2026, 10, 10, 12, 0, 0)
current   = run_start
for t in tasks:
    task_start = current
    task_end   = current + timedelta(seconds=t["duration_s"])
    current    = task_end + timedelta(seconds=1)

    print(f"  [{task_start.strftime('%H:%M:%S')}] Task '{t['id']}' — RUNNING")
    print(f"           >> {t['description']}")
    print(f"  [{task_end.strftime('%H:%M:%S')}] Task '{t['id']}' — SUCCESS  ✅  "
          f"(duration: {t['duration_s']}s)")
    print()

total_s = int((current - run_start).total_seconds())
print(f"  ✅  DAG RUN COMPLETE in {total_s}s")
print(f"  All 5 tasks succeeded. No retries needed.")
print()

# ── 5. Gantt timeline ───────────────────────────────────────────────────────
print("[GANTT — Pipeline Timeline]")
print("─────────────────────────────────────────────────────────────")
print(f"  {'Task':<22}  {'Start':>8}  {'End':>8}  {'Dur':>5}  Timeline")
print(f"  {'─'*22}  {'─'*8}  {'─'*8}  {'─'*5}  {'─'*30}")

current = run_start
for t in tasks:
    ts  = current
    te  = current + timedelta(seconds=t["duration_s"])
    current = te + timedelta(seconds=1)

    offset = int((ts - run_start).total_seconds())
    bar_start = offset // 2
    bar_len   = max(1, t["duration_s"] // 2)
    bar = " " * bar_start + "█" * bar_len
    print(f"  {t['id']:<22}  {ts.strftime('%H:%M:%S')}  {te.strftime('%H:%M:%S')}  {t['duration_s']:>4}s  {bar}")

print()

# ── 6. Airflow UI navigation steps ──────────────────────────────────────────
print("[AIRFLOW UI — How to Verify]")
print("─────────────────────────────────────────────────────────────")
print("""
  1. Open browser: http://localhost:8081
  2. Login: admin / admin
  3. Click on DAG: social_media_data_pipeline
  4. In Grid view: all task squares should be green ✅
  5. In Graph view: arrows show collect_data → kafka_stream → ... → dashboard_ready
  6. Click any task → View Log to see print() outputs from each task function
  7. In Gantt view: sequential task execution timeline shown as horizontal bars
""")

# ── 7. Summary ───────────────────────────────────────────────────────────────
print("[PIPELINE ORCHESTRATION SUMMARY]")
print("─────────────────────────────────────────────────────────────")
print(f"""
  Total tasks           : {len(tasks)}
  Execution mode        : Sequential (LocalExecutor)
  Total run duration    : {total_s}s
  Status                : ALL TASKS SUCCEEDED
  Schedule              : @daily (automated, no manual trigger needed)
  Database backend      : PostgreSQL (socialmedia db)
  DAG file location     : airflow/dags/social_media_pipeline_dag.py
""")

print("=" * 70)
print("ASSIGNMENT 4 — Airflow Orchestration Output COMPLETE")
print("=" * 70)
