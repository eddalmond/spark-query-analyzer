"""
Spark Query Analyzer — Notebook Source

This file is the canonical source for the Databricks notebook. Import it to register
the %analyze and %%analyze_batch magics, then use the magic commands in cells.

Usage in a Databricks notebook:
    # In a setup cell (run once per session):
    from spark_query_analyzer import register_analyze_magic, register_analyze_batch_magic
    register_analyze_magic()
    register_analyze_batch_magic()

    # Then in SQL cells:
    %analyze
    SELECT * FROM fact_sales s
    JOIN dim_date d ON s.date_key = d.date_key
    WHERE d.year = 2024

    # For post-execution skew analysis:
    %analyze --execute
    SELECT * FROM fact_sales WHERE date >= '2024-01-01'

    # For multiple queries at once:
    %%analyze_batch
    SELECT * FROM fact_sales WHERE date = '2024-01-01';
    SELECT * FROM fact_sales WHERE region = 'UK';
    SELECT * FROM dim_product;

For more detail on any finding, call display_issue_catalogue() in a Python cell:
    from spark_query_analyzer import display_issue_catalogue
    display_issue_catalogue()

---
Feature summary:
  F-01 · Delta Lake Health Analyser   — Z-ORDER, small files, OPTIMIZE/VACUUM overdue
  F-02 · AQE Configuration Checker     — plan-aware AQE recommendations + copy-ready conf
  F-03 · Deep Skew Analyser            — post-execution task metrics via Spark UI REST API
  F-04 · Python Anti-Pattern Detector  — AST scan for UDFs, RDD usage, driver pulls, etc.
  F-05 · DBU Cost Estimator            — pre-execution cost badge, tier-aware pricing
  F-06 · Multi-Query Batch Analyser    — cross-query cache/CTE/temp-view opportunities
"""

from spark_query_analyzer import (
    register_analyze_magic,
    register_analyze_batch_magic,
    display_issue_catalogue,
)

# ── Setup ──────────────────────────────────────────────────────────────────────
# Run this cell once per notebook session to register the magics.
# Databricks injects the `spark` variable automatically.

register_analyze_magic()
register_analyze_batch_magic()


# ── F-01 · Delta Lake Health Analyser ─────────────────────────────────────────
# Automatically triggered when %analyze runs on a query that scans Delta tables.
# No special flag needed — the analyser detects Delta tables from the plan and
# inspects their transaction logs.
#
# Example cell:
#   %analyze
#   SELECT * FROM my_delta_table WHERE date = '2024-01-01'


# ── F-02 · AQE Configuration Checker ──────────────────────────────────────────
# Automatically triggered with every %analyze run.
# Checks spark.sql.adaptive.* configs and cross-references with plan symptoms.
# Findings include copy-ready config snippets.
#
# Example cell:
#   %analyze
#   SELECT s.*, d.amount FROM fact_sales s JOIN dim_date d ON s.date_key = d.key


# ── F-03 · Deep Skew Analyser ─────────────────────────────────────────────────
# Run with --execute to trigger query execution and read actual task metrics
# from the Spark UI REST API (localhost:4040).
# Not available on Databricks Serverless — gracefully degrades.
#
# Example cell:
#   %analyze --execute
#   SELECT * FROM fact_sales s JOIN dim_product p ON s.product_id = p.id


# ── F-04 · Python Anti-Pattern Detector ───────────────────────────────────────
# Automatically scans the full cell (SQL + Python) for anti-patterns.
# SQL lines are excluded from AST analysis.
#
# Example cell:
#   %analyze
#   SELECT name, count(*) FROM my_table GROUP BY name
#
# Python code in Python cells is NOT analysed (only magic cells are scanned).


# ── F-05 · DBU Cost Estimator ─────────────────────────────────────────────────
# Always runs with %analyze — produces a cost badge in the output header.
# Uses cluster config + shuffle byte proxy for directional estimate.
# Colour coded: green (<$0.05) · yellow (<$0.25) · red (≥$0.25)
#
# Override the cluster tier with --cost-profile:
#   %analyze --cost-profile photon_all_purpose
#   SELECT ...


# ── F-06 · Multi-Query Batch Analyser ─────────────────────────────────────────
# New block magic for analysing multiple SQL statements together.
# Split on semicolons, one EXPLAIN per query, then cross-query analysis.
#
# Example cell:
#   %%analyze_batch
#   SELECT * FROM fact_sales WHERE date = '2024-01-01';
#   SELECT * FROM fact_sales WHERE region = 'UK';
#   SELECT * FROM dim_product WHERE category = 'Ceramics';


# ── Issue Catalogue ──────────────────────────────────────────────────────────
# Run in a Python cell to see all supported finding codes:
#
# display_issue_catalogue()