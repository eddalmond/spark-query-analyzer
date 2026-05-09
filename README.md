# Databricks Query Performance Analyzer

A Databricks notebook tool that analyses Spark SQL execution plans, surfaces performance findings with actionable fixes, and detects Python anti-patterns — via a `%analyze` cell magic and a `%%analyze_batch` block magic.

## Quick Start

1. Open `notebooks/QueryPerformanceAnalyzer.ipynb` or `notebooks/QueryPerformanceAnalyzer.py` in Databricks
2. Run the setup cell once per session:
```python
from spark_query_analyzer import register_analyze_magic, register_analyze_batch_magic
register_analyze_magic()
register_analyze_batch_magic()
```
3. In any SQL cell, prefix your query with `%analyze`:

```python
%analyze
SELECT a.id, b.name, c.value
FROM facts a
JOIN dim_b b ON a.b_id = b.id
JOIN dim_c c ON b.id = c.id
WHERE a.date >= '2024-01-01'
```

## Features

| Feature | Magic | What it does |
|---------|-------|-------------|
| **F-01** Delta Lake Health | `%analyze` (auto) | Checks Z-ORDER, small files, OPTIMIZE/VACUUM overdue for Delta tables |
| **F-02** AQE Config Checker | `%analyze` (auto) | Cross-references plan symptoms with Spark config; emits copy-ready conf blocks |
| **F-03** Deep Skew Analyser | `%analyze --execute` | Post-execution task metrics via Spark UI REST API; confirmed skew with actual ratios |
| **F-04** Python Anti-Pattern | `%analyze` (auto) | AST scan for UDFs, RDD usage, .collect(), repeated .count(), single-threaded Pandas |
| **F-05** DBU Cost Estimator | `%analyze` (auto) | Pre-execution cost badge; tier-aware (All Purpose, Photon, SQL Warehouse, ML) |
| **F-06** Multi-Query Batch | `%%analyze_batch` | Cross-query: shared scans → cache; identical filters → CTE; repeated CTE → temp view |

## Output

Each `%analyze` run produces an HTML diagnostic card:
- Severity badges (🔴 🟠 🟡 ℹ️) with counts
- Cost estimate badge (green/yellow/red by cost tier)
- Findings with node IDs, tables, and copy-ready suggestions
- Collapsible Configuration panel (F-02)
- Delta Storage section (F-01)
- Post-Execution section (F-03)
- Python Patterns section (F-04)

## Architecture

```
spark_query_analyzer/
├── analyzer.py              # run_analysis() — orchestrates all analysers
├── display_utils.py        # format_diagnostics() — HTML rendering
├── magic.py                # %analyze and %%analyze_batch magic registration
├── python_scanner.py       # F-04: AST-based Python anti-pattern scanner
├── aqe_checker.py         # F-02: AQE config reader + plan-aware findings
├── delta_analyser.py       # F-01: Delta Lake transaction log analyser
├── cost_estimator.py      # F-05: DBU cost estimation + badge renderer
├── cross_query_optimiser.py # F-06: multi-query batch analysis
├── post_execution_analyser.py # F-03: Spark UI REST API task metrics
└── system_info.py         # Shared SparkConf / AQE config helpers
```

## Requirements

- Databricks Runtime 11.0+ (Spark 3.3+)
- No external Python dependencies — uses only Spark internals and Databricks built-ins
- F-03 (Deep Skew) requires `localhost:4040` Spark UI — not available on Serverless compute; gracefully degrades
- Works on Databricks Community Edition

## `%analyze` Flags

| Flag | Behaviour |
|------|-----------|
| (default) | Plan-only analysis, no query execution |
| `--dry-run` | Same as default |
| `--execute` | Executes query with LIMIT 100000 cap + post-execution skew analysis (F-03) |

## `%%analyze_batch`

Accepts multiple SQL statements separated by semicolons:

```python
%%analyze_batch
SELECT * FROM fact_sales WHERE date = '2024-01-01';
SELECT * FROM fact_sales WHERE region = 'UK';
SELECT * FROM dim_product WHERE category = 'Ceramics';
```

Output: HTML card showing shared scan candidates, CTE candidates, and cache candidates across the queries.

## Issue Catalogue

To see all supported finding codes and their fixes, run in a Python cell:

```python
from spark_query_analyzer import display_issue_catalogue
display_issue_catalogue()
```