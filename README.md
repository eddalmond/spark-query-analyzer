# Databricks Query Performance Analyzer

A Databricks notebook that analyzes Spark SQL query execution plans, identifies performance bottlenecks, and suggests optimisations via a `%analyze` cell magic command.

## Quick Start

1. Open `notebooks/QueryPerformanceAnalyzer.ipynb` in Databricks
2. Run the setup cell to install the magic
3. In any cell, prefix your SQL with `%analyze`

```python
%analyze
SELECT a.id, b.name, c.value
FROM facts a
JOIN dim_b b ON a.b_id = b.id
JOIN dim_c c ON b.id = c.id
WHERE a.date >= '2024-01-01'
```

## What It Detects

| Severity | Issue | Detection Method |
|----------|-------|-----------------|
| 🔴 Critical | Broadcast join suggested but not used | Exchange node estimated size exceeds broadcast threshold |
| 🔴 Critical | Cartesian product (cross join) | Join with no condition or single-table filter |
| 🟠 High | Sort merge join on large tables | Exchange + Sort nodes before Join |
| 🟠 High | Full table scan | Scan node without filter predicates |
| 🟠 High | Missing predicate pushdown | Filter applied after scan |
| 🟡 Medium | Repeated scans of same table | Table appears multiple times in plan |
| 🟡 Medium | Large shuffle without limit | Wide transformation with no LIMIT |
| 🟡 Medium | Data skew indicators | Partition size variance across nodes |
| 🟢 Info | Partition pruning opportunity | Scan on partitioned table without filter |
| 🟢 Info | Bucketing opportunity | Large table joined without bucketing |

## Architecture

- `%analyze` — Cell magic that intercepts SQL, runs `EXPLAIN FORMATTED`, parses the plan, and emits diagnostics
- `spark_plan_parser.py` — Parses the logical/physical plan tree into a structured object
- `bottleneck_detector.py` — Pattern-matches against known anti-patterns
- `recommendation_engine.py` — Maps detected issues to specific optimisation suggestions
- `display_utils.py` — Renders HTML diagnostics in the notebook cell

## Requirements

- Databricks Runtime 11.0+ (Spark 3.3+)
- No external Python dependencies — uses only Spark internals and Databricks built-ins
- Works on Databricks Community Edition

## Output Example

```
╔══════════════════════════════════════════════════════════════════════╗
║  🔴 CRITICAL: Broadcast join recommended but not used              ║
║  Table: dim_b (2.1M rows) is being shuffled instead of broadcasted  ║
║  → Add hint: JOIN b BROADCAST(t) or increase spark.sql.autoBroadcastJoinThreshold
╠══════════════════════════════════════════════════════════════════════╣
║  🟠 HIGH: Full table scan on facts                                  ║
║  Table: facts (180M rows) scanned without partition pruning        ║
║  → Add date filter or restructure partition column
╠══════════════════════════════════════════════════════════════════════╣
║  🟡 MEDIUM: Repeated scan of dim_c                                   ║
║  dim_c appears 3 times in the plan                                 ║
║  → Consider CTE or caching dim_c
╚══════════════════════════════════════════════════════════════════════╝
```