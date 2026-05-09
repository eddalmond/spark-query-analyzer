# 🔍 Spark Query Analyzer

**A zero-dependency Databricks notebook tool that turns Spark execution plans into plain-English diagnoses with copy-ready fixes.**

Drop `%analyze` in front of any SQL query and get an HTML diagnostic card with severity-tiered findings, a natural-language summary, Delta storage health, AQE config checks, Python anti-pattern scans, DBU cost estimates, Spark UI deep-links, cluster recommendations, and an HTML export option — all without leaving the notebook.

```
%analyze
SELECT a.id, b.name, c.value
FROM facts a
JOIN dim_b b ON a.b_id = b.id
JOIN dim_c c ON b.id = c.id
WHERE a.date >= '2024-01-01'
GROUP BY a.id, b.name, c.value
```

**Output:**

```
┌─────────────────────────────────────────────────────────────┐
│ 🔍 Spark Query Analyzer — 3 findings                       │
│ 🔴 1  🟠 1  🟡 1                                             │
└─────────────────────────────────────────────────────────────┘

  What it does:  joins 3 tables with 1 aggregation against
                 `facts` (? rows)

  Biggest problem:  Table 'dim_b' (unknown size) is being
                    shuffled across all executors when it's
                    small enough to broadcast — this is
                    causing an unnecessary full data exchange.

  Fix:  Add a BROADCAST hint:
        JOIN /*+ BROADCAST(dim_b) */ dim_b ON ...

  🔴 MISSING_BROADCAST  dim_b  │  🟠 SORT_MERGE_JOIN
  🟡 FULL_TABLE_SCAN    facts  │  🟢 BROADCAST_USED

  ⚡ Est. $0.04 (0.07 DBU-hr, 8 cores, all_purpose_compute)

  🔗 View Stage 3 in Spark UI  ← (with --execute)
  🗺️ Cluster Advisor           ← Photon recommended
```

---

## ✨ All 13 Features

| # | Feature | Trigger | What it does |
|---|---------|---------|-------------|
| F-01 | **Delta Lake Health** | `%analyze` (auto) | Small files, missing OPTIMIZE/Z-ORDER, stale VACUUM, liquid clustering |
| F-02 | **AQE Config Checker** | `%analyze` (auto) | AQE disabled, wrong skew thresholds, default shuffle partitions — with copy-ready `spark.conf.set(...)` blocks |
| F-03 | **Post-Execution Skew** | `%analyze --execute` | Real task metrics via Spark UI REST API — confirmed skew with max/median ratio |
| F-04 | **Python Anti-Pattern** | `%analyze` (auto) | AST scan: UDFs, RDD ops, `.collect()` on large data, repeated `.count()` |
| F-05 | **DBU Cost Estimate** | `%analyze` (auto) | Pre-execution cost badge — tier-aware (All Purpose, Photon, SQL Warehouse, ML) |
| F-06 | **Multi-Query Batch** | `%%analyze_batch` | Shared scans → cache; identical filters → CTE; repeated CTEs → temp view |
| F-07 | **Schema & Stats Health** | `%analyze` (auto) | Missing table/column statistics; copy-ready `ANALYZE TABLE` commands |
| F-08 | **Streaming Analyser** | `%analyze` (auto) | Missing watermark, late data gaps, state store growth risk |
| F-09 | **History Tracker** | `%analyze` (auto) | Query signature tracking — regression detection across runs |
| F-10 | **Natural Language Summary** | `%analyze` (auto) | Plain-English narrative: what it does, biggest problem, fix sentence |
| F-12 | **Cluster Advisor** | `%analyze` (auto) | Photon/SQL Warehouse/Jobs cluster rec, executor memory sizing |
| F-14 | **HTML Export** | `%analyze --export <path>` | Export the full diagnostic card as a self-contained HTML file |
| F-15 | **Spark UI Deep-Links** | `%analyze --execute` | Direct `🔗 View Stage X in Spark UI` links on skew findings |

> F-11 (Automated Query Rewriter) and F-13 (CI/CD Lint Mode) require external packages or API calls — see the [roadmap][].

---

## 🚀 Quick Start

### 1. Install the package

**Option A — Databricks Repos (recommended)**
```
%sh
pip install git+https://github.com/eddalmond/spark-query-analyzer.git --quiet
```

**Option B — Clone into DBFS**
```python
%sh
git clone https://github.com/eddalmond/spark-query-analyzer.git /tmp/spark_query_analyzer
```

**Option C — Community Edition (no git)**
Upload `spark_query_analyzer/` as a library or copy it to a workspace directory.

### 2. Register the magic (once per session)
```python
import sys
sys.path.insert(0, "/tmp/spark_query_analyzer")

from spark_query_analyzer import register_analyze_magic
from spark_query_analyzer.magic import register_analyze_batch_magic

register_analyze_magic()
register_analyze_batch_magic()

print("✅ Ready — use %analyze in any SQL cell")
```

### 3. Run it
```python
%analyze
SELECT a.id, b.name, SUM(a.value) AS total
FROM facts a
JOIN dim_b b ON a.b_id = b.id
WHERE a.date >= '2024-01-01'
GROUP BY a.id, b.name
```

---

## `%analyze` Flags

| Flag | Behaviour |
|------|-----------|
| (default) | Plan-only analysis — no query execution |
| `--dry-run` | Same as default |
| `--execute` | Executes query (LIMIT 100k cap) + post-execution skew analysis (F-03) |
| `--export <path>` | Saves a self-contained HTML report to DBFS or mounted storage (F-14) |

## `%%analyze_batch`

Analyses multiple SQL statements together and surfaces cross-query opportunities:

```python
%%analyze_batch
SELECT * FROM fact_sales WHERE date = '2024-01-01';
SELECT * FROM fact_sales WHERE region = 'UK';
SELECT * FROM dim_product WHERE category = 'Ceramics';
```

Output: HTML card with shared scan candidates, CTE candidates, and cache recommendations.

---

## 📐 Architecture

```
spark_query_analyzer/
├── analyzer.py              # run_analysis() — orchestrates all analysers
├── display_utils.py          # format_diagnostics() — HTML rendering
├── magic.py                  # %analyze / %%analyze_batch IPython magics
├── narrative_explainer.py    # F-10: template-based NL summary + optional LLM path
├── cluster_advisor.py        # F-12: workload classifier + cluster recommendations
├── report_exporter.py        # F-14: self-contained HTML export
├── post_execution_analyser.py # F-03 + F-15: Spark UI REST API + deep-links
├── python_scanner.py         # F-04: AST-based Python anti-pattern scanner
├── aqe_checker.py            # F-02: AQE config reader + plan-aware findings
├── stats_checker.py          # F-07: DESCRIBE TABLE / ANALYZE TABLE checks
├── streaming_analyser.py     # F-08: streaming sensor / watermark checks
├── delta_analyser.py         # F-01: Delta Lake transaction log analyser
├── cost_estimator.py         # F-05: DBU cost estimation + badge renderer
├── cross_query_optimiser.py  # F-06: multi-query batch analysis
├── history_tracker.py        # F-09: query signature tracking + regression
└── system_info.py             # SparkConf / AQE config helpers
```

---

## 📋 Issue Catalogue

See all supported finding codes and their fixes:

```python
from spark_query_analyzer.display_utils import display_issue_catalogue
display_issue_catalogue()
```

---

## ✅ Requirements

- **Databricks Runtime 11.0+** (Spark 3.3+)
- **No external Python dependencies** — uses only Spark internals and Databricks built-ins
- **F-03 / F-15** require `localhost:4040` Spark UI — not available on Serverless; gracefully degrades
- Works on **Databricks Community Edition**

---

## 🗺️ Roadmap

All features, implementation details, and the priority matrix are documented in [spark-query-analyzer-roadmap.md][].

---

## 👤 Author

**Edd Almond** — [eddalmond/spark-query-analyzer](https://github.com/eddalmond/spark-query-analyzer)

[roadmap]: https://github.com/eddalmond/spark-query-analyzer/blob/master/spark-query-analyzer-roadmap.md
[spark-query-analyzer-roadmap.md]: https://github.com/eddalmond/spark-query-analyzer/blob/master/spark-query-analyzer-roadmap.md