# spark-query-analyzer — Full Review & Action Plan

> **Repo:** eddalmond/spark-query-analyzer · master · reviewed May 2026
> **Stack:** Python 92% · Jupyter 8% · Databricks Runtime 11+ (Spark 3.3+)
> **Status:** Early-stage · 2 commits

---

## Scores

| Dimension | Score |
|---|---|
| Concept strength | 8/10 |
| Feature completeness | 5/10 |
| Code robustness | 3/10 |
| Test coverage | 0/10 |
| Production readiness | 2/10 |
| Docs quality | 6/10 |

---

## Executive Summary

The concept and notebook DX are genuinely good — a well-structured magic command with a rich feature catalogue is exactly what Databricks users need. The four-module split (plan parser → bottleneck detector → recommendation engine → display utils) is the right architecture.

However, the implementation is at MVP or notebook-demo stage. The repo has only 2 commits, zero tests, no CI, no installable package, and several features described in the notebook appear to be stubs. The priority is hardening core reliability before expanding features.

---

## Architecture Assessment

### [HIGH] Module boundary is sound, but plan parser is the critical weak point

`spark_plan_parser.py` parses `EXPLAIN FORMATTED` output, which is semi-structured human-readable text — not a stable API. Databricks runtime upgrades between 11.x and 15.x change this output format (node naming, indentation, stats formatting). There is no versioned parser or runtime-detection fallback, so a runtime upgrade silently breaks all detection.

### [HIGH] Installation model creates re-run friction

The setup cell does `sys.path.insert(0, "/tmp/spark_query_analyzer")` but the source is never automatically copied there. The user must manually upload or clone the repo into `/tmp` before running. There is no `pip install` path, no `%pip install git+https://...` cell, no Databricks init script. Every new cluster session requires manual setup.

### [MEDIUM] Magic registration is session-global and not idempotent

`register_analyze_magic()` likely calls `ip.register_magic_function()` on the IPython shell. Re-running the setup cell in a live kernel will silently re-register, potentially doubling output. There is no guard for "already registered" state. The `%%analyze_batch` cell magic registration should also verify the IPython shell is available (Databricks uses a custom shell wrapper) before registering.

### [MEDIUM] F-11 is missing entirely

The feature table in the notebook jumps from F-10 to F-12. F-11 was presumably removed or never implemented. This leaves a gap in the ID sequence and signals an incomplete planning pass. The feature list should be renumbered or F-11 should be documented as intentionally omitted.

### [LOW] Generated notebook comment is misleading

The file opens with *"This file is generated — edit QueryPerformanceAnalyzer.ipynb and regenerate."* but there is no generation script in the repo. This will confuse contributors who find the `.py` export and try to edit it directly. Either remove the comment or add the generation script.

---

## Feature-by-Feature Status

| ID | Feature | Status | Key concern |
|---|---|---|---|
| F-01 | Delta Lake health | ⚠️ Partial | Small-file detection via `DESCRIBE HISTORY` is expensive on large tables; no sampling option. VACUUM staleness check needs configurable threshold. |
| F-02 | AQE config checker | ✅ Solid | Copy-ready `spark.conf.set()` snippets are the right DX. Verify thresholds are kept in sync with Databricks defaults (they changed in DBR 14). |
| F-03 | Post-execution skew | ❌ Fragile | Hardcoded `localhost:4040` will not work on multi-driver clusters or SQL Warehouses. The Spark UI REST API URL must be derived from `spark.sparkContext.uiWebUrl`. |
| F-04 | Python anti-pattern | ⚠️ Partial | Regex scan on the cell string is brittle — it will false-positive on commented-out code and string literals. Needs an AST-based pass (`ast.parse()`) for reliable detection. |
| F-05 | DBU cost estimate | ⚠️ Partial | Shuffle-byte proxy is a rough heuristic. DBU rates are hardcoded and will become stale. Should read from cluster tags or a configurable rate map, not constants. |
| F-06 | Multi-query batch | ⚠️ Partial | Shared-scan detection requires parsing multiple SQL statements; the SQL splitter must handle semicolons inside string literals and CTEs. This is a common edge-case failure. |
| F-07 | Schema & stats health | ✅ Solid | Copy-ready `ANALYZE TABLE` commands are good UX. Ensure `DESCRIBE EXTENDED` fallback exists for non-Delta tables. |
| F-08 | Streaming analyser | ❌ Likely stub | The demo uses `stream(readings)` syntax which is not standard Spark SQL. Streaming plan detection from `EXPLAIN` is unreliable — streaming plans are not fully represented in the logical plan text. |
| F-09 | History tracker | ⚠️ Partial | Writing to `_spark_query_analyzer.query_history` requires the Delta table to pre-exist. There is no documented auto-create step. First run on a fresh cluster will fail silently or with a cryptic table-not-found error. |
| F-10 | Natural language summary | ✅ Solid | Template-based fallback is the right choice for zero-dependency envs. The `mlflow.deployments` path needs a documented config example; without it users won't discover the LLM upgrade path. |
| F-12 | Cluster config advisor | ⚠️ Partial | Photon/SQL Warehouse recommendations are heuristic-only. No check for whether Photon is actually available on the current cluster tier. Recommendations may be irrelevant to Community Edition users. |
| F-14 | HTML export | ⚠️ Demo only | The demo cell is commented out with `pass`. There is no validation that DBFS paths are writable, no size cap on the exported HTML, and no test of the self-contained assertion (external CSS references?). |
| F-15 | Spark UI deep-link | ❌ Fragile | Same `localhost:4040` issue as F-03. Deep-links will be dead in any environment where the Spark UI is proxied (all Databricks managed clusters). |
| @monitor_performance | Performance decorator | ⚠️ Partial | The decorator reads Spark UI REST API for stage metrics after the function returns — it will capture the most-recently-completed stage, not necessarily the stage belonging to the wrapped function's query. Stage attribution is unreliable without a job-group tag. |

---

## Code Quality Findings

### [CRITICAL] Zero test coverage

There is no `tests/` directory, no CI workflow, and no mention of testing in the README. Every module is untested. The plan parser in particular is parsing free-text and is near-impossible to verify correct without a corpus of known `EXPLAIN FORMATTED` outputs at different DBR versions.

**Minimum viable fix:** add a `tests/fixtures/` folder with canned EXPLAIN outputs and pytest assertions on bottleneck detection results.

### [CRITICAL] No error handling visible in the magic layer

If `EXPLAIN FORMATTED` fails (syntax error in the user's SQL, missing table, permission denied), the magic must surface a clean error to the notebook cell — not a Python traceback. Similarly, if the Spark UI REST API is unreachable (F-03, F-15), the tool must degrade gracefully to dry-run mode. None of this error boundary is visible in the notebook or documented as implemented.

### [HIGH] Hardcoded Spark UI URL

`localhost:4040` is used for Spark UI REST API calls. On Databricks, the UI is proxied — the real URL is `spark.sparkContext.uiWebUrl`. On a cluster with multiple running applications, the port may be 4041 or 4042.

**Fix:**
```python
base_url = spark.sparkContext.uiWebUrl or "http://localhost:4040"
```

### [HIGH] `@monitor_performance` stage attribution is wrong

The decorator reads `stage_id` and `num_tasks` from "most recent completed stage" after the function exits. If any other Spark job ran concurrently (background Delta log compaction, for example), the metrics belong to the wrong job.

**Fix:** set a Spark job group before execution and filter REST API results by that group ID:
```python
spark.sparkContext.setJobGroup(job_name, description, interruptOnCancel=False)
# ... run the wrapped function ...
spark.sparkContext.clearJobGroup()
```

### [HIGH] SQL statement splitter does not handle edge cases

Splitting on `;` to parse batch queries will break on semicolons inside string literals (`WHERE comment = 'end; of'`), inside inline views, and inside CTE definitions.

**Fix:** use `sqlparse` or `sqlfluff` for statement splitting — both are available in DBR environments.

### [MEDIUM] No version pinning or compatibility matrix

The README says "DBR 11.0+" but gives no upper bound. DBR 15.x (Spark 3.5) has changed `EXPLAIN` output format significantly, especially for adaptive plans. The parser should detect DBR/Spark version at startup and warn if outside the tested range.

**Fix:**
```python
spark_version = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "unknown")
TESTED_VERSIONS = ["11.", "12.", "13.", "14."]
if not any(spark_version.startswith(v) for v in TESTED_VERSIONS):
    warn(f"Untested runtime {spark_version} — results may be inaccurate")
```

### [MEDIUM] `display_issue_catalogue()` is coupled to display environment

Calling `displayHTML()` or `display()` inside a library function couples the library to Databricks notebook context. Any non-notebook usage (unit tests, CI) will crash.

**Fix:** render functions should return HTML strings; the caller decides whether to `display()` or `print()`.

---

## Security & Compliance Notes

### [HIGH] History table stores raw SQL — PII exposure risk

`_spark_query_analyzer.query_history` records the full query text. If users run queries referencing PII column names or embed literal values in SQL, that data lands in a Delta table readable by anyone with access to the `_spark_query_analyzer` database.

**Required:** implement a `--no-record` flag and document the data retention posture. For regulated environments (GDPR, HIPAA), this is a blocker before any enterprise adoption.

### [MEDIUM] HTML export is not sanitised

The F-14 HTML report inlines the original SQL query. If the query contains `<script>` tags (possible via table aliases or string literals), the exported HTML could execute arbitrary JavaScript when opened in a browser.

**Fix:** escape all user-supplied content before inlining:
```python
import html
safe_sql = html.escape(original_sql)
```

---

## Prioritised Action Plan

### Phase 1 — Fix the foundation (target: 1–2 weeks)

**Action 1 — Add a `%pip install` setup cell and proper packaging**

Add a `setup.py` / `pyproject.toml` so users can run:
```
%pip install git+https://github.com/eddalmond/spark-query-analyzer
```
in any cluster without manual file copying. Move the `/tmp` path hack to a fallback only. The package entry point should call `register_analyze_magic()` automatically on import so that the only setup required is `import spark_query_analyzer`.

**Action 2 — Fix Spark UI URL (unblocks F-03 and F-15)**

Replace all `localhost:4040` references with `spark.sparkContext.uiWebUrl`. Add a connectivity check with a 2-second timeout and fall back to dry-run mode with an info banner if the UI is unreachable. This is the single change that makes the two most prominent "power features" actually work on real clusters.

**Action 3 — Add error boundaries to the magic command**

Wrap the entire magic execution in a try/except that catches:
- `AnalysisException` (SQL syntax errors, missing tables)
- `PermissionException` (access denied on tables)
- `requests.exceptions.ConnectionError` (Spark UI unreachable)
- `Exception` (catch-all with traceback captured to a collapsible section)

Render each as a styled warning card in the notebook output rather than a raw traceback.

**Action 4 — Auto-create the history table**

Add an `ensure_history_table(spark)` call at magic registration time that creates `_spark_query_analyzer.query_history` if absent:
```sql
CREATE TABLE IF NOT EXISTS _spark_query_analyzer.query_history (
  job_name STRING,
  run_timestamp TIMESTAMP,
  duration_ms LONG,
  estimated_cost_usd DOUBLE,
  cluster_id STRING,
  query_text STRING,
  stage_id INT,
  num_tasks INT,
  input_bytes LONG,
  shuffle_read_bytes LONG,
  shuffle_write_bytes LONG,
  max_task_duration_ms LONG,
  gc_time_ms LONG,
  schema_version INT
) USING DELTA
```

Include a `schema_version` column for future migrations.

---

### Phase 2 — Tests and CI (target: 2–3 weeks)

**Action 5 — Build a plan-parser test corpus**

Collect `EXPLAIN FORMATTED` outputs for 8–10 representative query patterns at DBR 11, 13, and 15:
- Broadcast miss (large table, no hint)
- Cartesian product (cross join)
- Full table scan (no filter)
- Sort merge join on large tables
- Repeated scan of same table
- Missing predicate pushdown
- AQE-rewritten plan (coalesced partitions)
- Streaming plan

Store as `tests/fixtures/explain_dbr{version}_{pattern}.txt`. Write pytest assertions against `bottleneck_detector.detect(plan_text)` for each. This is the single highest-value test investment in the project.

**Action 6 — Add GitHub Actions CI**

Add `.github/workflows/ci.yml`:
```yaml
name: CI
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.10" }
      - run: pip install -e ".[dev]"
      - run: ruff check .
      - run: mypy spark_query_analyzer/
      - run: pytest tests/ -v
```

Use a mock SparkSession (`pyspark.sql.SparkSession.builder.master("local[1]")`) for unit tests — no Databricks cluster needed for CI.

**Action 7 — Fix `@monitor_performance` stage attribution**

```python
def monitor_performance(job_name, spark):
    def decorator(fn):
        def wrapper(*args, **kwargs):
            spark.sparkContext.setJobGroup(job_name, f"monitored: {job_name}", interruptOnCancel=False)
            try:
                start = time.time()
                result = fn(*args, **kwargs)
                duration_ms = int((time.time() - start) * 1000)
                # now fetch REST API and filter by job group ID
                _record_metrics(spark, job_name, duration_ms)
                return result
            finally:
                spark.sparkContext.clearJobGroup()
        return wrapper
    return decorator
```

**Action 8 — Replace regex anti-pattern detection with AST parsing**

```python
import ast

def detect_python_antipatterns(cell_source: str) -> list[dict]:
    try:
        tree = ast.parse(cell_source)
    except SyntaxError:
        return []  # not valid Python, skip

    findings = []
    for node in ast.walk(tree):
        # UDF decorator
        if isinstance(node, ast.FunctionDef):
            for dec in node.decorator_list:
                if isinstance(dec, ast.Call) and getattr(dec.func, 'id', '') == 'udf':
                    findings.append({"pattern": "udf_decorator", "line": node.lineno})
        # .collect() / .toPandas() / .rdd
        if isinstance(node, ast.Attribute):
            if node.attr in ('collect', 'toPandas', 'rdd'):
                findings.append({"pattern": node.attr, "line": node.lineno})
    return findings
```

This eliminates false positives from commented-out code and string literals.

---

### Phase 3 — Polish and grow (target: 4–6 weeks)

**Action 9 — Sanitise HTML export and add `--no-record` flag**

- Run `html.escape()` on all user-supplied strings before inlining into F-14 report.
- Add `--no-record` flag to `%analyze` that skips writing to `query_history`.
- Add a `PRIVACY.md` documenting what is stored, default retention, and how to delete.

**Action 10 — Add DBR version detection and parser compatibility warnings**

At magic registration, read the runtime version and emit a yellow banner if outside the tested range:
```python
version = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "")
TESTED = ["11.", "12.", "13.", "14."]
if version and not any(version.startswith(v) for v in TESTED):
    display_warning(f"Runtime {version} is outside the tested range (DBR 11–14). "
                    "Plan parsing results may be inaccurate.")
```

**Action 11 — Fix or honestly document F-08 (streaming analyser)**

The `stream(readings)` syntax in the demo is not valid Spark SQL and will not execute. Two options:

- **Implement properly:** detect streaming plans by inspecting `StreamingQuery` objects via `spark.streams.active` and the Structured Streaming REST API (`/api/v1/streams`), rather than via `EXPLAIN`.
- **Remove from the feature table** and add to a `ROADMAP.md` section with a clear description of what proper implementation requires.

Do not leave it as a demo that silently fails.

**Action 12 — Add CHANGELOG, roadmap, and contributing guide**

- `CONTRIBUTING.md`: dev setup, how to add a new detection rule, how to add test fixtures, PR checklist.
- `CHANGELOG.md`: start from current state so future releases are trackable.
- `ROADMAP.md`: capture F-11 (document what it was meant to be), F-08 status, and any future feature ideas.
- GitHub issue labels: `bug`, `enhancement`, `detection-rule`, `good-first-issue`.
- Resolve the F-11 gap in the feature numbering (either implement it or renumber F-12–F-15 down by one).

---

## Quick Reference: Files to Create / Modify

| File | Action |
|---|---|
| `pyproject.toml` | Create — enables `pip install`, declares dependencies |
| `spark_query_analyzer/__init__.py` | Modify — auto-register magic on import |
| `spark_query_analyzer/magic.py` | Modify — fix error handling, `--no-record` flag |
| `spark_query_analyzer/spark_plan_parser.py` | Modify — version detection, compatibility warning |
| `spark_query_analyzer/bottleneck_detector.py` | Modify — AST-based anti-pattern detection |
| `spark_query_analyzer/display_utils.py` | Modify — return HTML strings, don't call `display()` internally |
| `spark_query_analyzer/history.py` | Modify — auto-create table, add `schema_version` |
| `spark_query_analyzer/monitor.py` | Modify — job group tagging for stage attribution |
| `tests/fixtures/` | Create — canned EXPLAIN outputs per DBR version |
| `tests/test_bottleneck_detector.py` | Create — pytest assertions against fixture corpus |
| `tests/test_antipattern.py` | Create — AST detection unit tests |
| `.github/workflows/ci.yml` | Create — lint, type-check, pytest |
| `CONTRIBUTING.md` | Create |
| `CHANGELOG.md` | Create |
| `ROADMAP.md` | Create |
| `PRIVACY.md` | Create |
