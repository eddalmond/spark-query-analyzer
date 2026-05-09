# Databricks notebook source
# ──────────────────────────────────────────────────────────────────────────────
# Source: notebooks/QueryPerformanceAnalyzer.ipynb
# ──────────────────────────────────────────────────────────────────────────────
#
# This file is generated — edit QueryPerformanceAnalyzer.ipynb and regenerate.

# COMMAND ----------

# 🔍 Spark Query Performance Analyzer
# 
# Drop any SQL into a cell prefixed with `%analyze` and get instant bottleneck diagnostics — plus Delta storage health, AQE config checks, Python anti-pattern scans, DBU cost estimates, a plain-English summary, cluster recommendations, and HTML export.
# 
# **Supported features:**
# 
# | # | Feature | What it catches |
# |---|---------|----------------|
# | F-01 | Delta Lake Health | Small files, missing OPTIMIZE/Z-ORDER, stale VACUUM |
# | F-02 | AQE Config Checker | AQE disabled, wrong skew thresholds, default shuffle partitions |
# | F-03 | Post-Execution Skew | Real task-level skew from Spark UI metrics (--execute flag) |
# | F-04 | Python Anti-Pattern | UDFs, RDD ops, .collect() on large data, repeated .count() |
# | F-05 | DBU Cost Estimate | Pre-execution cost badge per cluster tier |
# | F-06 | Multi-Query Batch | Shared scans, CTE opportunities, cache candidates (%%analyze_batch) |
# | F-07 | Schema & Stats Health | Missing table statistics, stale ANALYZE TABLE data |
# | F-08 | Streaming Analyser | Streaming sensors without watermark, late data risk |
# | F-09 | History Tracker | Regression detection vs. previous runs of the same query |
# | F-10 | Natural Language Summary | Plain-English narrative: what it does + biggest problem + fix |
# | F-12 | Cluster Config Advisor | Photon/SQL Warehouse/Jobs cluster recommendation, executor memory sizing |
# | F-14 | HTML Report Export | Export full diagnostic card as self-contained HTML (--export flag) |
# | F-15 | Spark UI Deep-Link | Direct links to specific stages in Spark UI on skew findings |
# 
# **Usage:**
# ```python
# %analyze
# SELECT ... FROM ... WHERE ...
# ```
# 
# **Flags:**
# - `%analyze --dry-run` — plan analysis only (default)
# - `%analyze --execute` — runs the query and includes post-execution skew metrics (F-03)
# - `%analyze --export /dbfs/reports/report.html` — export full HTML report to DBFS (F-14)
# 
# **Batch mode (F-06):**
# ```python
# %%analyze_batch
# SELECT * FROM fact_sales WHERE date = '2024-01-01';
# SELECT * FROM fact_sales WHERE region = 'UK';
# ```
# 
# ---

# COMMAND ----------

# Install magics — run this once per notebook session
import sys
sys.path.insert(0, "/tmp/spark_query_analyzer")

from spark_query_analyzer import register_analyze_magic
from spark_query_analyzer.magic import register_analyze_batch_magic

register_analyze_magic()
register_analyze_batch_magic()

print("✅ %analyze and %%analyze_batch magics registered.")

# COMMAND ----------

## 🕐 Performance Monitor — `@monitor_performance` decorator
# 
# Apply `@monitor_performance` to any Python or Spark function to track it in the central `_spark_query_analyzer.query_history` Delta table.
# 
# Works alongside `%analyze` — both write to the same history table, so platform teams can query performance across all 200 scheduled notebooks from one Delta table.

# COMMAND ----------

from spark_query_analyzer import monitor_performance

# Apply @monitor_performance to any Python or Spark function.
# Each call writes a row to _spark_query_analyzer.query_history.

@monitor_performance(job_name='example_ingest', spark=spark)
def ingest_sales(path):
    df = spark.read.format('parquet').load(path)
    return df.count()

@monitor_performance(job_name='daily_revenue', spark=spark)
def daily_revenue():
    result = spark.sql('''
        SELECT region, SUM(amount) AS total
        FROM fact_sales
        WHERE date = '2024-01-01'
        GROUP BY region
    ''').collect()
    return result

print('Apply @monitor_performance(job_name=..., spark=spark) to any function')

# COMMAND ----------

### What gets recorded
# 
# Each monitored call writes one row to `_spark_query_analyzer.query_history`:
# 
# | Column | What it captures |
# |--------|------------------|
# | `job_name` | Your chosen identifier |
# | `run_timestamp` | UTC timestamp |
# | `duration_ms` | Wall-clock execution time |
# | `estimated_cost_usd` | DBU estimate for the compute tier |
# | `cluster_id` | Which cluster ran the job |
# | `stage_id`, `num_tasks` | Most recent completed stage metrics |
# | `input_bytes`, `shuffle_*_bytes` | Data volumes for the stage |
# | `max_task_duration_ms` | Longest task — proxy for skew |
# | `gc_time_ms` | Garbage collection time |
# 
# Query across all 200 notebooks from one place:
# ```sql
# SELECT job_name, duration_ms, estimated_cost_usd, max_task_duration_ms,
#        run_timestamp
# FROM _spark_query_analyzer.query_history
# WHERE job_name IN ('daily_revenue', 'sales_ingest', 'dim_customer')
# ORDER BY run_timestamp DESC
# LIMIT 100
# ```

# COMMAND ----------

# ---
# 
## 🎯 F-10: Natural Language Summary
# 
# Every `%analyze` run includes a plain-English narrative banner at the top of the card:
# 1. **What it does** — joins, aggregations, largest table, streaming
# 2. **Biggest problem** — highest-severity finding in plain English
# 3. **Fix** — single actionable sentence
# 
# Template-based (zero deps); optionally uses a workspace LLM if `mlflow.deployments` is configured.
# 
# ---

# COMMAND ----------

# F-10 demo: query with a broadcast miss — narrative explains it in plain English
%analyze
SELECT
    c.customer_id,
    c.customer_name,
    SUM(s.sale_amount) AS total_sales
FROM fact_sales s
JOIN dim_customer c ON s.customer_id = c.customer_id
WHERE s.sale_date >= '2024-01-01'
GROUP BY c.customer_id, c.customer_name
ORDER BY total_sales DESC
LIMIT 100

# COMMAND ----------

# ---
# 
## 🔴 F-01: Delta Lake Health Analyser
# 
# Detects: small files, missing OPTIMIZE/Z-ORDER, stale VACUUM, liquid clustering opportunity.
# 
# ---

# COMMAND ----------

# F-01 demo: Delta table with small files and no recent OPTIMIZE
# (replace with a real Delta table in your environment)
%analyze
SELECT * FROM my_delta_table WHERE date = '2024-03-15'

# COMMAND ----------

# ---
# 
## ⚡ F-02: AQE Configuration Checker
# 
# Cross-references live Spark config against the physical plan. Each finding includes a copy-ready `spark.conf.set(...)` snippet.
# 
# ---

# COMMAND ----------

# F-02 demo: AQE check against a large shuffle join
%analyze
SELECT a.*, b.*
FROM fact_events a
JOIN dim_events b ON a.event_id = b.event_id
WHERE a.date >= '2024-01-01'

# COMMAND ----------

# ---
# 
## 🐍 F-04: Python Anti-Pattern Detector
# 
# Scans the full cell (Python + SQL) for patterns that bypass Catalyst:
# - `@udf` / `spark.udf.register(...)`
# - `.rdd.map()`, `.rdd.filter()`
# - `.collect()` / `.toPandas()` on large data
# - Multiple `.count()` calls
# 
# ---

# COMMAND ----------

# F-04 demo: Python UDF and .collect() on large data
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(StringType())
def clean_name(s):
    return s.strip().title() if s else None

df = spark.table("fact_sales")
# WARNING: .collect() on large data is flagged by F-04
result = df.groupBy("region").agg({"amount": "sum"}).collect()

%analyze
SELECT region, SUM(amount) AS total FROM fact_sales GROUP BY region

# COMMAND ----------

# ---
# 
## 💰 F-05: DBU Cost Estimator
# 
# Pre-execution cost badge using cluster tier detection and a shuffle-byte proxy. Colour coded: 🟢 <$0.05 · 🟡 <$0.25 · 🔴 ≥$0.25
# 
# ---

# COMMAND ----------

# F-05 demo: cost badge appears in the header
%analyze
SELECT * FROM fact_sales
JOIN dim_product ON fact_sales.product_id = dim_product.product_id
WHERE date >= '2024-01-01'

# COMMAND ----------

# ---
# 
## 📋 F-06: Multi-Query Batch Analyser
# 
# Use `%%analyze_batch` (block magic) to analyse multiple SQL statements together. Detects shared scans, identical filters, and repeated CTEs.
# 
# ---

# COMMAND ----------

# F-06 demo: same table scanned twice — CTE opportunity flagged
%%analyze_batch
SELECT * FROM fact_sales WHERE date = '2024-01-01';
SELECT * FROM fact_sales WHERE region = 'UK';
SELECT * FROM dim_product;

# COMMAND ----------

# ---
# 
## 📊 F-07: Schema & Statistics Health
# 
# Checks DESCRIBE TABLE and ANALYZE TABLE status. Flags missing statistics and provides copy-ready ANALYZE TABLE commands.
# 
# ---

# COMMAND ----------

# F-07 demo: table with no statistics
%analyze
SELECT customer_id, SUM(amount) FROM fact_sales GROUP BY customer_id

# COMMAND ----------

# ---
# 
## 📺 F-08: Streaming Analyser
# 
# Detects streaming patterns and warns about: missing watermark, late data gaps, state store growth risk.
# 
# ---

# COMMAND ----------

# F-08 demo: streaming query without watermark
%analyze
SELECT current_timestamp() AS processing_time, *
FROM stream(readings)

# COMMAND ----------

# ---
# 
## 🔄 F-09: History Tracker
# 
# Tracks query signatures across runs. On repeated executions, surfaces a trends card showing finding count and cost delta vs. the previous run.
# 
# ---

# COMMAND ----------

# F-09: run the same query twice — trends show on the second run
%analyze
SELECT date, SUM(amount) AS daily_total FROM fact_sales GROUP BY date ORDER BY date

# COMMAND ----------

# ---
# 
## 🔬 F-03: Deep Skew Analyser (Post-Execution)
# 
# Requires `--execute`. Reads actual task metrics from `localhost:4040` Spark UI REST API. Flags confirmed skew when max/median duration > 5×.
# 
# ---

# COMMAND ----------

# F-03 / F-15 demo: post-execution skew analysis with Spark UI deep-links
%analyze --execute
SELECT a.user_id, SUM(b.amount)
FROM fact_events a
JOIN dim_user b ON a.user_id = b.user_id
GROUP BY a.user_id

# COMMAND ----------

# ---
# 
## 🗺️ F-12: Cluster Configuration Advisor
# 
# Based on the query workload (scan-heavy / shuffle-heavy / join-heavy / streaming / Python), recommends optimal cluster type and configuration:
# 
# - Photon for shuffle-heavy or Python UDF workloads
# - SQL Warehouse vs Jobs cluster based on workload type
# - Shuffle partition tuning and broadcast threshold recommendations
# - Executor memory sizing estimate
# 
# ---

# COMMAND ----------

# F-12 demo: cluster recommendations for a shuffle-heavy join
%analyze
SELECT a.*, b.*
FROM fact_events a
JOIN dim_user b ON a.user_id = b.user_id
WHERE a.date >= '2024-01-01'

# COMMAND ----------

# ---
# 
## 📄 F-14: HTML Report Export
# 
# Export the full diagnostic report as a self-contained HTML file to DBFS or mounted cloud storage. Report includes: query text, severity summary chips, full diagnostic card, and a collapsible EXPLAIN FORMATTED section.
# 
# Usage: `%analyze --export /dbfs/reports/my_report.html`
# 
# ---

# COMMAND ----------

# F-14 demo: export HTML report to DBFS
# (run on Databricks with a real DBFS path)
# %analyze --export /dbfs/reports/my_query_report.html
# SELECT * FROM fact_sales WHERE date = '2024-03-01'
pass

# COMMAND ----------

# ---
# 
## 🗂️ Detected Issues Reference
# 
# All issue types currently detected by `%analyze`:
# 
# ---

# COMMAND ----------

from spark_query_analyzer.display_utils import display_issue_catalogue
display_issue_catalogue()
