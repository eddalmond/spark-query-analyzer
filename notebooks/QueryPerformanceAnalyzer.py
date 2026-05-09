"""
Spark Query Analyzer — Notebook Source

This file is the canonical source for the Databricks notebook. Import it to register
the %analyze and %%analyze_batch magics, then use the magic commands in cells.

Usage in a Databricks notebook:
    # In a setup cell (run once per session):
    from spark_query_analyzer import register_analyze_magic
    from spark_query_analyzer.magic import register_analyze_batch_magic
    register_analyze_magic()
    register_analyze_batch_magic()

    # Then in SQL cells:
    %analyze
    SELECT * FROM fact_sales s
    JOIN dim_date d ON s.date_key = d.date_key
    WHERE d.year = 2024

    # For post-execution skew analysis (F-03):
    %analyze --execute
    SELECT * FROM fact_sales WHERE date >= '2024-01-01'

    # For multiple queries at once (F-06):
    %%analyze_batch
    SELECT * FROM fact_sales WHERE date = '2024-01-01';
    SELECT * FROM fact_sales WHERE region = 'UK';
    SELECT * FROM dim_product;

For the full issue catalogue in a Python cell:
    from spark_query_analyzer.display_utils import display_issue_catalogue
    display_issue_catalogue()

---
Feature summary:
  F-01 · Delta Lake Health Analyser   — small files, missing OPTIMIZE/Z-ORDER, stale VACUUM
  F-02 · AQE Configuration Checker    — plan-aware AQE recommendations + copy-ready conf blocks
  F-03 · Deep Skew Analyser           — post-execution task metrics via Spark UI REST API (--execute)
  F-04 · Python Anti-Pattern Detector — AST scan for UDFs, RDD usage, driver pulls, repeated counts
  F-05 · DBU Cost Estimator           — pre-execution cost badge, tier-aware pricing (green/yellow/red)
  F-06 · Multi-Query Batch Analyser  — cross-query cache/CTE/temp-view opportunities (%%analyze_batch)
  F-07 · Schema & Stats Health        — missing table/column statistics, ANALYZE TABLE snippets
  F-08 · Streaming Analyser           — missing watermark, late data risk, state store growth
  F-09 · History Tracker              — query signature tracking, regression detection across runs
  F-10 · Natural Language Summary    — plain-English narrative: what it does + biggest problem + fix
"""

from spark_query_analyzer import register_analyze_magic
from spark_query_analyzer.magic import register_analyze_batch_magic

# ── Setup ──────────────────────────────────────────────────────────────────────
register_analyze_magic()
register_analyze_batch_magic()


# ── F-01 · Delta Lake Health Analyser ─────────────────────────────────────────
# Automatically triggered when %analyze runs on a query that scans Delta tables.
# Inspects DESCRIBE DETAIL, DESCRIBE HISTORY, and table properties.
# Flags: small files (<32MB avg, >100 files), missing OPTIMIZE (>7 days),
#        missing Z-ORDER on large tables, VACUUM overdue (>30 days),
#        liquid clustering opportunity (DBR 13.3+, >1TB).


# ── F-02 · AQE Configuration Checker ──────────────────────────────────────────
# Automatically triggered with every %analyze run.
# Reads spark.sql.adaptive.* and spark.sql.shuffle.partitions.
# Cross-references with plan symptoms — e.g. AQE off + SortMergeJoin = critical.
# Each finding includes a copy-ready spark.conf.set(...) snippet in the HTML output.


# ── F-03 · Deep Skew Analyser (Post-Execution) ─────────────────────────────────
# Run with --execute flag to trigger query execution and read actual task metrics
# from the Spark UI REST API at localhost:4040 (local driver-side call).
# Not available on Databricks Serverless — gracefully degrades.
# Flags confirmed skew when max_task_duration / median_task_duration > 5x.
#
#   %analyze --execute
#   SELECT a.user_id, SUM(b.amount) FROM fact_events a JOIN dim_user b ON a.user_id = b.user_id GROUP BY a.user_id


# ── F-04 · Python Anti-Pattern Detector ───────────────────────────────────────
# Automatically scans the full magic cell (Python + SQL) for patterns that bypass
# Catalyst optimisation. SQL lines are excluded via heuristic detection.
# Flags:
#   🔴  Python UDF (@udf / spark.udf.register) — row-by-row Python interpreter
#   🔴  RDD ops (.rdd.map, .rdd.filter) — loses all Catalyst optimisation
#   🔴  .collect() / .toPandas() on large data — driver OOM risk
#   🟠  Multiple .count() calls — repeated separate Spark jobs
#   🟡  Single-threaded pandas on large data — use pyspark.pandas instead


# ── F-05 · DBU Cost Estimator ─────────────────────────────────────────────────
# Always runs with %analyze — produces a cost badge in the output header.
# Uses cluster tier detection (All Purpose / SQL Warehouse / Photon / ML Runtime)
# + shuffle byte proxy (Exchange count × 50MB/core) → runtime estimate → DBU cost.
# Colour coded: 🟢 <$0.05  🟡 <$0.25  🔴 ≥$0.25


# ── F-06 · Multi-Query Batch Analyser ─────────────────────────────────────────
# Block magic for analysing multiple SQL statements together.
# Split on semicolons, one EXPLAIN FORMATTED per query, then cross-query analysis.
# Detects: SHARED_SCAN (same table >2× → cache/CTE), IDENTICAL_FILTER (same filter
# on same table ≥2× → CTE), REPEATED_CTE (same CTE in multiple queries → temp view).
#
#   %%analyze_batch
#   SELECT * FROM fact_sales WHERE date = '2024-01-01';
#   SELECT * FROM fact_sales WHERE region = 'UK';
#   SELECT * FROM dim_product WHERE category = 'Ceramics';


# ── F-07 · Schema & Statistics Health ────────────────────────────────────────
# Checks DESCRIBE TABLE and ANALYZE TABLE status for all tables in the query.
# Flags: Statistics: 0 bytes (no table stats), missing column statistics.
# Each finding includes a copy-ready ANALYZE TABLE command as a config snippet.


# ── F-08 · Structured Streaming Analyser ──────────────────────────────────────
# Detects streaming patterns and warns about:
#   - Streaming sensor with no watermark defined
#   - Late data handling gaps (no late数据 policy)
#   - State store size growth risk (no state cleanup configured)
# Automatically triggered for queries with streaming sources.


# ── F-09 · History Tracker ───────────────────────────────────────────────────
# Tracks query signatures (normalised SQL hash) across %analyze runs.
# On repeated executions of the same query, surfaces a trends card showing
# finding count and cost delta vs. the previous run.
# Uses Spark SQL state store — best-effort, degrades gracefully.


# ── F-10 · Natural Language Summary ─────────────────────────────────────────
# Automatically appears at the top of every %analyze output.
# Template-based plain-English narrative (zero external dependencies):
#   1. What it does — joins, aggregations, largest table, streaming
#   2. Biggest problem — highest-severity finding in plain English
#   3. Fix — single actionable imperative sentence
# Optional enhancement: if mlflow.deployments is available and a Databricks
# workspace LLM endpoint is configured, delegates there first and falls back
# to templates silently.
#
#   %analyze
#   SELECT s.*, c.name FROM fact_sales s JOIN dim_customer c ON s.cust_id = c.id
#
# The summary banner renders as a gradient card at the top of the output,
# above the findings list, with a "Summary" or "AI Summary" badge.