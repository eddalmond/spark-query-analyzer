# Contributing to spark-query-analyzer

🎉 Thanks for contributing! This guide explains the architecture and the exact process for adding a new finding type.

---

## Architecture

```
SQL input
    │
    ▼
analyzer.py · run_analysis()
    │
    ├── EXPLAIN FORMATTED → plan text
    │
    ▼
bottleneck_detector.py · parse_plan()
    │   (parses plan text into structured Finding objects)
    │
    ├── narrative_explainer.py · NarrativeExplainer.explain()
    │   (F-10: plain-English summary)
    ├── cluster_advisor.py · ClusterAdvisor
    │   (F-12: cluster recommendations)
    ├── post_execution_analyser.py · SkewAnalyser
    │   (F-03 / F-15: Spark UI metrics)
    └── display_utils.py · format_diagnostics()
        (renders HTML card)
```

**Adding a new finding = 2 steps:**

1. Add detection logic to `bottleneck_detector.py` → returns a `Finding`
2. Add the fix template to `display_utils.py` (or the relevant renderer)

All findings are `Finding` dataclass objects:
```python
from spark_query_analyzer.analyzer import Finding

Finding(
    code="MY_FINDING_CODE",     # unique uppercase ID
    severity="high",            # critical | high | medium | info
    message="...",              # short human-readable description
    suggestion="...",          # copy-ready fix (code snippet preferred)
    table="...",                # optional: table this applies to
    column="..."                # optional: column this applies to
)
```

---

## Adding a New Detection

**Step 1 — Detect it** in `bottleneck_detector.py`:

```python
def parse_plan(plan_text: str, sql: str) -> list[Finding]:
    findings = []
    # ... existing logic ...

    # Example: detect missing predicate pushdown
    if "Scan" in node and "Filter" not in parent_nodes:
        findings.append(Finding(
            code="MISSING_PUSHDOWN",
            severity="medium",
            message=f"Table '{table}' is scanned without a filter pushed down.",
            suggestion=f"Add a WHERE clause directly on the scan:\n"
                      f"SELECT * FROM {table} WHERE date >= '2024-01-01'",
            table=table,
        ))
    return findings
```

**Step 2 — Add the fix template** in `display_utils.py` under `ISSUE_FIXES`:

```python
ISSUE_FIXES = {
    # ... existing entries ...
    "MISSING_PUSHDOWN": {
        "label": "MISSING_PUSHDOWN",
        "fix": "Add a partition filter directly on the scan node to avoid scanning the full table.",
        "example": "SELECT * FROM table WHERE date = '2024-01-01'",
    },
}
```

**Step 3 — Test it:**

```bash
# Run the test suite (when tests exist)
python3 -m pytest tests/

# Smoke-test by running %analyze on a query that triggers the finding
```

---

## Code Style

- **Python 3.10+** — use `from __future__ import annotations` for forward refs
- **Line length: 120** — configured in `pyproject.toml`
- **Imports:** `ruff check` in CI — run `ruff check . --fix` before committing
- **`python3 -m py_compile`** on every modified `.py` file before pushing
- **No new dependencies** — the tool must work with Databricks Runtime pre-installed packages only

---

## File Layout

```
spark_query_analyzer/
├── analyzer.py           # run_analysis() — entry point, EXPLAIN call
├── display_utils.py      # format_diagnostics() — HTML renderer + ISSUE_FIXES dict
├── magic.py              # %analyze / %%analyze_batch IPython magics
├── bottleneck_detector.py # parse_plan() — detection logic
├── narrative_explainer.py # F-10: plain-English summary
├── cluster_advisor.py    # F-12: cluster recommendations
├── report_exporter.py    # F-14: HTML export
├── post_execution_analyser.py # F-03 + F-15: Spark UI REST API
├── python_scanner.py    # F-04: AST-based Python anti-pattern scanner
├── aqe_checker.py       # F-02: AQE config checker
├── stats_checker.py      # F-07: DESCRIBE TABLE / ANALYZE TABLE checks
├── streaming_analyser.py # F-08: streaming sensor / watermark checks
├── delta_analyser.py     # F-01: Delta Lake transaction log analyser
├── cost_estimator.py     # F-05: DBU cost estimation
├── cross_query_optimiser.py # F-06: multi-query batch analysis
├── history_tracker.py    # F-09: query signature tracking
├── performance_monitor.py # F-09 extension: @monitor_performance decorator
└── system_info.py        # SparkConf / AQE config helpers
```

---

## Magic Registration

`%analyze` is a **line magic** — the SQL follows on the same line:

```
%analyze SELECT * FROM ...
```

`%%analyze_batch` is a **cell magic** — all SQL is in the cell body.

If you change the magic registration in `magic.py`, verify multi-line SQL works:

```python
%analyze
SELECT a.id, b.name
FROM fact_a a
JOIN dim_b b ON a.b_id = b.id
WHERE a.date >= '2024-01-01'
```

---

## Pull Request Checklist

- [ ] `python3 -m py_compile <modified file(s)>`
- [ ] `ruff check spark_query_analyzer/` (zero errors)
- [ ] New finding tested with a query that triggers it
- [ ] `pyproject.toml` version bumped if publishing new feature
- [ ] `spark-query-analyzer-roadmap.md` updated if adding a new feature
