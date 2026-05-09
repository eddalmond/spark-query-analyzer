# Spark Query Analyzer — Feature Roadmap

> Research-backed roadmap for `eddalmond/spark-query-analyzer`, covering competitive gaps, community pain points, and implementation detail sufficient for direct development.

---

## Competitive Landscape Summary

| Tool | Positioning | Key Differentiators |
|---|---|---|
| **Databricks Query Profile** (native) | Built-in DBSQL visual DAG | Task-level metrics, Spark UI integration; only works post-execution in DBSQL warehouses |
| **Unravel Data** | Enterprise SaaS observability | Full-stack visibility, AI-powered root cause, FinOps, pipeline lineage; expensive, external agent required |
| **Pepperdata** | Autonomous resource optimizer | Real-time Spark tuning without code changes; infrastructure-level, not query-level |
| **Databricks `spark-sql-perf`** | Benchmarking harness | TPC-DS/TPC-H benchmark runner; no recommendations |
| **Acceldata** | Data observability platform | Multi-cloud, data quality + pipeline monitoring; enterprise only |
| **Chaos Genius** | DataOps for Snowflake/Databricks | Query cost allocation, instance rightsizing; limited Spark plan depth |

**The gap this project fills:** a free, notebook-native, zero-dependency Spark plan analyser with actionable recommendations. The roadmap below extends it to close gaps against paid tools while staying true to the notebook-first, open-source model.

---

## Environment Constraints

These constraints apply to all features and were used to determine phasing and implementation approach.

| Constraint | Impact |
|---|---|
| **No outbound Databricks API calls** | Rules out LLM API calls to external providers (OpenAI, Anthropic). F-10 uses template-based generation instead. F-13 (CLI lint mode) is the one exception — it runs *outside* the cluster and calls the Databricks Jobs API deliberately; flagged clearly below. |
| **No additional Python packages** | All features must work with the packages pre-installed on Databricks Runtime. The one exception is F-11 (Query Rewriter), which needs `sqlglot`; it is deprioritised and marked optional. |
| **`localhost:4040` Spark UI REST API** | Used by F-03 and F-15. This is a local driver-side call, not an external network call, so it is allowed. Note: not available on Databricks Serverless compute — both features gracefully degrade in that environment. |
| **`mlflow.deployments` (internal workspace LLM endpoints)** | Available if the workspace has a served model configured. Used as an *optional enhancement* in F-10 only, never as a required path. |

Features are labelled in the priority matrix as: ✅ **Zero-dep notebook** · ⚠️ **Needs package** · 🌐 **External call**

---

## Existing Features (v0 baseline)

- `%analyze` cell magic on SQL cells
- `EXPLAIN FORMATTED` plan parser (`spark_plan_parser.py`)
- Pattern-matched bottleneck detection: broadcast miss, cartesian joins, sort-merge joins, full table scans, missing predicate pushdown, repeated scans, large shuffles, skew indicators, partition pruning, bucketing
- Severity-tiered HTML diagnostic output

---

## Roadmap

Features are grouped into four phases. Each includes: **what it is**, **why it matters** (with competitive context), **implementation notes** detailed enough to hand to another model.

---

### Phase 1 — Core Analysis Depth (Highest ROI, lowest effort)

---

#### F-01 · Delta Lake Health Analyser ✅ Zero-dep notebook

**What:** When a scanned table is a Delta table, inspect its transaction log and file layout to surface storage-layer anti-patterns before or alongside the plan analysis.

**Why it matters:** Delta-specific issues (small files, lack of Z-ORDER, stale VACUUM) are among the top causes of slow queries in Databricks but are invisible in a `EXPLAIN` plan. Databricks' own documentation flags this as the single biggest non-obvious performance lever. Unravel and Chaos Genius both surface this; the project currently has no Delta awareness.

**Implementation:**

1. After parsing the plan, identify all `Scan` nodes and extract the table name and location path.
2. For each Delta table (check via `spark.sql(f"DESCRIBE DETAIL {table_name}")`), collect:
   - `numFiles`, `sizeInBytes`, `avgFileSize` = `sizeInBytes / numFiles`
   - Last `OPTIMIZE` and `VACUUM` timestamps from `DESCRIBE HISTORY {table_name} LIMIT 10`
   - Whether `delta.autoOptimize.optimizeWrite` and `delta.autoOptimize.autoCompact` are enabled (from `tblproperties`)
   - Whether a `ZORDER BY` clause appears in recent OPTIMIZE history entries
3. Raise issues:
   - 🔴 **Small files:** `avgFileSize < 32MB` and `numFiles > 100` → recommend `OPTIMIZE {table}` with estimated improvement.
   - 🟠 **No recent OPTIMIZE:** last OPTIMIZE > 7 days ago → recommend scheduled OPTIMIZE job.
   - 🟡 **No Z-ORDER:** table is large (> 1GB) and queried with a filter, but OPTIMIZE history has no ZORDER → recommend `OPTIMIZE {table} ZORDER BY ({filter_columns})` where `filter_columns` is extracted from WHERE clauses in the user's SQL.
   - 🟡 **VACUUM overdue:** latest VACUUM > 30 days → warn about storage bloat; suggest `VACUUM {table} RETAIN 168 HOURS`.
   - 🟢 **Liquid clustering opportunity:** if table > 1TB and on DBR 13.3+, suggest migrating from Z-ORDER to Liquid Clustering.
4. Add a `DeltaHealthAnalyser` class in a new `delta_analyser.py` module. Call it from the main `%analyze` pipeline before `display_utils` renders output. Results slot into a new "Delta Storage" section in the HTML output card.

---

#### F-02 · AQE Configuration Checker & Recommender ✅ Zero-dep notebook

**What:** Inspect the active Spark configuration and, in combination with the physical plan, determine whether AQE is misconfigured or whether its sub-features would change this specific query's plan.

**Why it matters:** AQE is the highest-ROI single lever for most Spark workloads — community posts cite 2–5× speed improvements. The tool currently detects plan-level issues but never checks whether AQE is simply turned off, or whether its thresholds are wrong for this query.

**Implementation:**

1. Add an `AQEChecker` class in `aqe_checker.py`.
2. Read these configs at analysis time:
   - `spark.sql.adaptive.enabled`
   - `spark.sql.adaptive.coalescePartitions.enabled`
   - `spark.sql.adaptive.skewJoin.enabled`
   - `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` (default 256MB)
   - `spark.sql.autoBroadcastJoinThreshold`
   - `spark.sql.shuffle.partitions`
3. Cross-reference with plan findings:
   - If AQE is disabled AND the plan has a `SortMergeJoin` on tables > autoBroadcastJoinThreshold → 🔴 "AQE disabled — enabling it may auto-switch this to a BroadcastHashJoin at runtime."
   - If `skewJoin.enabled = false` AND the plan shows a large `Exchange` before a `Join` → 🟠 "Skew join handling disabled — if partition sizes are uneven, tasks will stall."
   - If `shuffle.partitions` is the default 200 AND estimated output rows > 50M → 🟡 "Default shuffle partitions (200) may produce oversized partitions; consider `spark.sql.shuffle.partitions = 800`."
4. For each finding, emit a **copy-ready config block** in the HTML output, e.g.:
   ```python
   spark.conf.set("spark.sql.adaptive.enabled", "true")
   spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
   ```
5. Surface config findings in a collapsible "Configuration" panel in the HTML card, separate from plan findings.

---

#### F-03 · Deep Skew Analyser (Post-Execution) ✅ Zero-dep notebook (`localhost:4040` — not available on Serverless)

**What:** After the query runs (not just from the plan), read actual task-level metrics from the SparkContext to detect skew with real numbers rather than estimates.

**Why it matters:** The current skew detection is plan-based heuristics. Real skew is only visible after shuffle stages run. Unravel's primary value proposition is exactly this task-level analysis. This feature would replicate the core of it in-notebook for free.

**Implementation:**

1. Add a `PostExecutionAnalyser` that is triggered when the user runs `%analyze` — the magic should execute the query (optionally with `LIMIT 0` for safety, or the full query) and capture the `SparkContext` listener events.
2. Use `spark.sparkContext.statusTracker()` and the Spark REST API (`http://localhost:4040/api/v1/applications/{appId}/stages`) to retrieve stage-level task metrics post-execution. Parse the JSON response into a list of task metrics per stage.
3. For each stage that has a shuffle:
   - Compute `max_task_duration`, `median_task_duration`, `p95_task_duration`
   - Compute `max_bytes_read` vs `median_bytes_read` across tasks
   - If `max_task_duration / median_task_duration > 5.0` → flag as 🔴 **Confirmed skew** (not just inferred), including the stage ID, the skew ratio, and the number of straggler tasks.
4. Recommend the appropriate fix based on AQE state:
   - AQE enabled → "AQE should handle this; if it persists, increase `skewedPartitionThresholdInBytes`."
   - AQE disabled → "Enable AQE skew join handling, or manually salt the join key on `{join_key}`."
5. This feature requires the query to actually execute. Wrap in a toggle: `%analyze --dry-run` skips execution and only analyses the plan; `%analyze` (default) runs the query and adds post-execution metrics.

---

#### F-04 · Python Anti-Pattern Detector ✅ Zero-dep notebook

**What:** Scan the cell text (not just the SQL) for PySpark/Python patterns that bypass Catalyst optimisation, and warn before execution.

**Why it matters:** Python UDFs, RDD operations, and single-threaded pandas usage are cited in every Databricks best-practice guide as the most common causes of poor performance among intermediate users. No existing notebook-native tool catches these statically.

**Implementation:**

1. Extend the `%analyze` magic to also receive the full cell content (not just the SQL portion).
2. Add a `PythonAntiPatternScanner` in `python_scanner.py` using Python's `ast` module to parse the non-SQL Python in the cell.
3. Detect and flag:
   - **Python UDFs:** `@udf` decorator or `spark.udf.register(...)` → 🔴 "Python UDF detected — runs row-by-row in Python interpreter, bypassing Catalyst and Photon. Rewrite as a native Spark SQL function or Pandas UDF."
   - **RDD usage:** `.rdd.`, `.map(`, `.filter(` on a DataFrame → 🔴 "RDD operation converts DataFrame to RDD, losing all Catalyst optimisations."
   - **`.collect()` or `.toPandas()`:** on a large DataFrame (estimated rows > 1M from plan) → 🔴 "Pulling >1M rows to driver — risk of OOM. Consider `display()` with a LIMIT or write results to Delta."
   - **`count()` in a loop or repeated:** multiple `df.count()` calls → 🟠 "Multiple `.count()` actions trigger separate Spark jobs. Cache the DataFrame or rewrite as a single aggregation."
   - **Single-threaded pandas:** `import pandas` + direct pandas operations on large data → 🟡 "Use `pandas on Spark` (`pyspark.pandas`) to retain distributed execution."
4. Findings appear in a "Python Patterns" section in the HTML card.

---

#### F-05 · DBU Cost Estimator ✅ Zero-dep notebook

**What:** Estimate the approximate Databricks Unit (DBU) cost of the query based on cluster configuration, data volumes from the plan, and pricing tiers.

**Why it matters:** Cost is the primary concern for data platform teams, and it's the central feature of every paid observability tool (Unravel, Pepperdata, Chaos Genius all lead with it). A rough estimate in-notebook lets engineers catch expensive queries before they run in production jobs. No free notebook-native tool does this.

**Implementation:**

1. Add a `CostEstimator` class in `cost_estimator.py`.
2. Read cluster configuration via `spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags")` (returns a JSON string with cluster metadata) and `spark.sparkContext.defaultParallelism` for core count.
3. Determine cluster type heuristically:
   - If `clusterUsageTags` contains `"sqlWarehouseId"` → SQL Warehouse DBU rate
   - If `spark.databricks.photon.enabled = true` → Photon-enabled compute rate
   - Else → Standard compute rate
4. DBU rates (store as a configurable dict, defaulting to 2025 list prices):
   ```python
   DBU_RATES = {
       "jobs_compute": 0.10,          # per DBU-hour
       "all_purpose_compute": 0.55,
       "sql_warehouse_small": 0.22,
       "photon_jobs": 0.30,
   }
   ```
5. Estimate query runtime from plan: use the sum of estimated row counts across all `Exchange` nodes × a tunable `bytes_per_row` constant (default 100 bytes) to estimate shuffle bytes, then model runtime as `shuffle_bytes_GB * 60 / cluster_cores` seconds (rough but directional).
6. `estimated_dbu_cost = (runtime_seconds / 3600) * dbu_rate * num_cores`
7. Display as: `⚡ Estimated cost: ~$0.04 (0.07 DBU-hours on 8-core all-purpose compute)` with a disclaimer that it's a pre-execution estimate.
8. Add a `--cost-profile {tier}` flag to `%analyze` to override cluster type.

---

### Phase 2 — Advanced Diagnostics

---

#### F-06 · Multi-Query Batch Analyser ✅ Zero-dep notebook

**What:** A new cell magic `%analyze_batch` that accepts multiple SQL statements, analyses them together, and identifies cross-query optimisations — shared table scans, CTE opportunities, caching candidates.

**Why it matters:** Real notebooks almost always have several cells querying the same tables. Each query is currently analysed in isolation, so the tool misses the biggest source of waste: redundant full scans of the same large table across cells.

**Implementation:**

1. Register a `%%analyze_batch` block magic (double `%%`) that captures the entire cell, splits on `;`, and runs each SQL through the existing `spark_plan_parser`.
2. Build a `CrossQueryOptimiser` in `cross_query_optimiser.py`:
   - Collect all `Scan` nodes across all queries and group by table name.
   - If the same table appears in > 2 queries → 🟡 "Table `{t}` scanned {n} times across this cell. Consider `t_df = spark.table('{t}'); t_df.cache()` before these queries."
   - If two queries have identical `Filter` predicates on the same table → 🟡 "Queries 1 and 3 apply the same filter to `{t}`. Extract as a CTE or temp view."
   - If queries share a large intermediate join result → suggest materialising as a Delta temp table.
3. Produce a combined HTML report showing individual query diagnostics plus a "Batch Summary" card at the top.

---

#### F-07 · Schema & Statistics Health Checker ✅ Zero-dep notebook

**What:** For each table touched by a query, check whether column statistics are present and current, and recommend `ANALYZE TABLE` commands where missing.

**Why it matters:** The Databricks Cost-Based Optimizer (CBO) relies on column statistics to choose join order and strategies. Stale or missing stats cause the planner to make poor decisions even when AQE is enabled. Databricks' own best practice guide lists weekly `ANALYZE TABLE` runs as essential; predictive auto-ANALYZE is still in public preview. This fills the gap with targeted, per-query guidance.

**Implementation:**

1. Add `StatsHealthChecker` in `stats_checker.py`.
2. For each table in the plan, run `DESCRIBE TABLE EXTENDED {table}` and check the `Statistics` row. If absent or if `rowCount = -1` → stats are missing.
3. Compare last stats collection timestamp (from `DESCRIBE HISTORY`) against table's last write timestamp. If stats are older than the last `WRITE` or `MERGE` commit → stats are stale.
4. Correlate with plan findings: if the plan has a `SortMergeJoin` that the tool suspects could be a broadcast join, and stats are missing on the smaller table → 🟠 "Statistics missing on `{table}` — planner may be overestimating size and avoiding broadcast join. Run: `ANALYZE TABLE {table} COMPUTE STATISTICS FOR ALL COLUMNS`."
5. Output a ready-to-run `ANALYZE` command block in the HTML card.

---

#### F-08 · Structured Streaming Plan Analyser ✅ Zero-dep notebook

**What:** Extend `%analyze` to work with Structured Streaming queries, detecting streaming-specific anti-patterns in addition to batch plan issues.

**Why it matters:** Structured Streaming is the second most common Databricks workload type, yet all existing notebook analysis tools (including DBSQL Query Profile) are batch-only. Users have no static analysis tool for streaming plans.

**Implementation:**

1. Detect streaming queries by checking if the SQL contains `readStream`, or if the user uses `%analyze` on a PySpark cell that includes `.writeStream`.
2. Parse the streaming plan via `query.explain(True)` (captured from the active `StreamingQuery` object, obtained via `spark.streams.active[-1]`).
3. Add a `StreamingAntiPatternDetector` in `streaming_analyser.py` with detections:
   - **`foreachBatch` with non-idempotent operations:** if `foreachBatch` lambda contains `INSERT` without `MERGE` → 🔴 "Non-idempotent write in foreachBatch — reprocessing will produce duplicates. Use `MERGE INTO` for exactly-once semantics."
   - **Missing watermark on stateful operations:** plan contains `FlatMapGroupsWithState` or `dropDuplicates` without a watermark → 🔴 "Stateful operation without watermark — state will grow unboundedly. Add `.withWatermark('{event_time_col}', '10 minutes')`."
   - **Trigger interval too aggressive:** if `Trigger.ProcessingTime("1 second")` is set on a micro-batch query reading from a large Delta table → 🟡 "High-frequency trigger on Delta source — consider `Trigger.AvailableNow()` for cost-efficient batch-style streaming."
   - **AQE in streaming:** warn if AQE is disabled for Photon clusters running foreachBatch (known limitation pre-DBR 13.1).
4. Show streaming-specific findings in a "Streaming" card alongside standard plan findings.

---

#### F-09 · Query History Performance Tracker ✅ Zero-dep notebook

**What:** Persist query signatures and their diagnostic findings to a Delta table in the user's workspace, enabling trend tracking — "did this query get worse after the schema change last Tuesday?"  Also includes a `@monitor_performance` decorator for tracking arbitrary Python/Spark functions in the same history table.

**Why it matters:** Unravel's core commercial value is longitudinal performance visibility. This adds a free, notebook-native equivalent. It also unlocks the ability to detect performance regressions in CI/CD pipelines (see F-13).

**Implementation:**

1. `history_tracker.py` — `HistoryTracker` called at the end of every `%analyze` run.
2. **Query signature:** stable SHA-256 hash of normalised SQL (strip literals, aliases; collapse whitespace). Groups `WHERE date = '2024-01-01'` and `WHERE date = '2025-01-01'` as the same shape.
3. `_spark_query_analyzer.query_history` Delta table (auto-created on first run):
   ```
   query_signature STRING, run_timestamp TIMESTAMP,
   query_text STRING, severity_critical/HIGH/MEDIUM/INFO INT,
   estimated_dbu_cost DOUBLE, cluster_id STRING,
   findings_json STRING, duration_ms BIGINT,
   tables_json STRING, codes_json STRING,
   -- F-09 extension (monitor_performance):
   job_name STRING, spark_ui_url STRING, stage_id INT,
   num_tasks INT, input_bytes BIGINT,
   shuffle_read_bytes BIGINT, shuffle_write_bytes BIGINT,
   gc_time_ms BIGINT, max_task_duration_ms BIGINT
   ```
4. After writing, query the last 30 runs of the same signature and display a **sparkline trend** showing how `estimated_dbu_cost` and severity counts have changed over time.
5. `@monitor_performance` decorator (`performance_monitor.py`): apply to any Python or Spark function to track it in the same history table.  Inline HTML card after each call; regression vs last run auto-computed; Spark UI stage metrics captured via `localhost:4040`.
6. Make the tracker opt-in (disabled by default) via: `spark.conf.set("spark_query_analyzer.history_enabled", "true")`.

---

### Phase 3 — AI & Intelligence Layer

---

#### F-10 · Natural Language Query Explainer ✅ Zero-dep notebook

**What:** An `--explain` flag on `%analyze` that produces a concise, plain-English narrative summarising what the query does, what its biggest performance problem is, and the single most impactful fix — generated entirely from structured findings with no external API calls.

**Why it matters:** The Unravel platform offers AI-generated root cause explanations as a key selling point. This replicates that output through deterministic template generation, which is faster, free, and works in any environment. The output transforms the tool from "shows a finding list" to "tells you a story about what's wrong."

**Implementation:**

1. Add a `NarrativeExplainer` in `narrative_explainer.py`. No external dependencies — pure Python string logic operating on the structured `findings` list produced by `bottleneck_detector.py`.

2. **Query summary sentence:** infer a one-sentence description of what the query does by inspecting the plan tree:
   - Count `Join` nodes → "This query joins {n} tables"
   - Identify `Aggregate` nodes → "...with {n} aggregations"
   - Identify the largest `Scan` node by estimated rows → "...against `{largest_table}` ({n}M rows)"
   - Example output: *"This query joins 3 tables with 2 aggregations against `sales_fact` (180M rows)."*

3. **Lead finding sentence:** pick the single highest-severity finding and convert it to a plain-English sentence using a template keyed on finding type:
   ```python
   TEMPLATES = {
       "broadcast_miss":   "The biggest problem is that `{table}` ({size}) is being shuffled "
                           "across all executors when it is small enough to broadcast — "
                           "this is causing an unnecessary full data exchange.",
       "full_table_scan":  "The biggest problem is that `{table}` ({rows}M rows) is being "
                           "read in full with no partition filter applied.",
       "cartesian_join":   "The biggest problem is a cartesian product between `{t1}` and "
                           "`{t2}` — every row of each table is being paired, producing "
                           "an exploding row count.",
       "skew":             "The biggest problem is data skew on `{join_key}` — one or more "
                           "tasks are processing far more data than the others, stalling "
                           "the entire stage.",
       # ... one template per finding type in bottleneck_detector.py
   }
   ```

4. **Fix sentence:** append the top recommendation from `recommendation_engine.py` reformatted as a single imperative sentence, e.g. *"The highest-impact fix is to add a BROADCAST hint: `JOIN /*+ BROADCAST(dim_b) */ dim_b ON ...`"*.

5. **Compose and render:** join the three sentences into a paragraph and render it in a highlighted "Summary" banner at the top of the HTML output card, above the finding list. Always shown (not just with `--explain`) — the flag can be used to request a more detailed multi-paragraph version covering all findings, not just the top one.

6. **Optional enhancement (internal workspace LLM only):** if `QueryAnalyzerConfig.use_workspace_llm = True` *and* `mlflow.deployments` is importable and a deployment named `"databricks-meta-llama"` (or the configured endpoint) is available, pass the structured findings as a JSON prompt to that endpoint instead of using templates. This path must never make outbound HTTP calls — it routes only through the internal Databricks model serving gateway. Fail silently back to templates if unavailable.

---

#### F-11 · Automated Query Rewriter ⚠️ Needs package (`sqlglot`)

**What:** For a subset of detectable, mechanical anti-patterns, automatically generate a corrected version of the SQL and display it as a diff.

**Why it matters:** Showing a problem is useful; showing the fix is 10× more useful. This is the feature most commonly requested in Databricks community posts ("just tell me what to write"). No free tool does this; Unravel offers it only for some patterns and only at enterprise tier.

**Note on dependencies:** This feature requires `sqlglot` for SQL AST manipulation. `sqlglot` is not pre-installed on Databricks Runtime. Installation via `%pip install sqlglot` at the top of the notebook is required. Because of this, F-11 is deprioritised relative to all zero-dependency features. Consider implementing the broadcast hint and repeated-scan rewrites first using regex + string manipulation as a no-package fallback (those two patterns are structurally simple enough not to need a full AST parser), then migrating to `sqlglot` for the more complex rewrites.

**Implementation:**
2. Implement rewrites for the following mechanical patterns:
   - **Missing broadcast hint:** if F-01 or existing detector flags a broadcast opportunity on `table_x`, rewrite `JOIN table_x ON ...` → `JOIN /*+ BROADCAST(table_x) */ table_x ON ...`.
   - **Missing predicate pushdown:** if a subquery or CTE scans a large table and the outer WHERE clause has a filter on a column that exists in that table, push the filter inside: rewrite `SELECT * FROM (SELECT * FROM big_table) t WHERE t.date = '...'` → `SELECT * FROM (SELECT * FROM big_table WHERE date = '...') t`.
   - **Repeated table scan → CTE:** if the same table appears in > 2 scan nodes, wrap it in a CTE at the top of the query.
   - **`SELECT *` on wide table:** if a `Scan` node has > 50 columns and the plan shows only a subset are used downstream, rewrite `SELECT *` to list only the referenced columns.
3. Display the original and rewritten SQL side-by-side in an HTML diff (using Python's `difflib.HtmlDiff`).
4. Include a copy button for the rewritten SQL. Add a disclaimer: "Review before using — rewrites are mechanical and may change semantics in edge cases."
5. Only surface rewrites when confidence is high (i.e., pattern exactly matches; do not attempt rewrites on ambiguous plans).

---

#### F-12 · Cluster Configuration Advisor ✅ Zero-dep notebook

**What:** Based on the query plan characteristics and detected bottlenecks, recommend the optimal cluster type, size, and configuration for running this query in production.

**Why it matters:** Engineers regularly ask "should I use a SQL Warehouse or a Jobs cluster? How many cores do I need?" for a given query. Pepperdata's core product is autonomous cluster tuning; this brings a rule-based version of that insight into the notebook at zero cost.

**Implementation:**

1. Add a `ClusterAdvisor` in `cluster_advisor.py`.
2. Classify the query workload based on plan characteristics:
   - **Scan-heavy / read-mostly (many Scan nodes, few Exchanges):** recommend SQL Warehouse with Photon enabled + Delta caching. Config: `spark.databricks.io.cache.enabled = true`.
   - **Shuffle-heavy (many Exchange nodes):** recommend Jobs cluster with memory-optimised instances (r-family on AWS, Edsv5 on Azure). Suggest `spark.sql.shuffle.partitions = 2 * num_cores * 4`.
   - **Join-heavy with small dimension tables:** recommend enabling `spark.sql.autoBroadcastJoinThreshold = 104857600` (100MB).
   - **Python-heavy (from F-04 findings):** recommend Photon-enabled cluster (Photon skips Python UDFs, so if UDFs are rewritten to SQL, Photon will accelerate them).
3. Estimate minimum executor memory: `estimated_shuffle_bytes * 3 / num_executors`, and recommend the next standard instance size above that.
4. Output a ready-to-paste cluster JSON policy snippet or a `spark.conf.set(...)` block.

---

### Phase 4 — Developer Experience & Integration

---

#### F-13 · CI/CD Lint Mode (`--lint`) 🌐 External call (by design)

**Environment note:** This is the one feature that intentionally runs *outside* the Databricks cluster and makes outbound calls to the Databricks Jobs API and GitHub API. It is not a notebook feature — it runs in a CI runner (GitHub Actions, Azure DevOps, etc.). It requires `DATABRICKS_HOST` and `DATABRICKS_TOKEN` env vars and the `databricks-sdk` Python package in the CI environment (`pip install databricks-sdk`). These constraints are expected and appropriate for a CI tool; they do not affect any other features.

**What:** A headless, non-interactive mode that runs the full analysis pipeline and exits with a non-zero code if any Critical or High findings are detected. Designed for use in Databricks Repos CI pipelines or GitHub Actions running `databricks bundle run`.

**Why it matters:** Performance regressions introduced by code changes are currently only caught in production. A lint gate lets teams enforce performance standards the same way they enforce code style. No open-source Spark tool provides this.

**Implementation:**

1. Add a CLI entry point in `spark_query_analyzer/cli.py`:
   ```bash
   python -m spark_query_analyzer lint --sql-file queries/transform.sql \
       --cluster-id <id> --severity-gate HIGH
   ```
2. The CLI authenticates to the Databricks workspace via environment variables (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`) and submits an ephemeral job that runs the analysis in a single-node cluster.
3. Results are written to stdout as JSON: `{"findings": [...], "max_severity": "HIGH", "exit_code": 1}`.
4. Exit codes: `0` = no findings above gate, `1` = findings at or above gate, `2` = analysis error.
5. Provide a sample GitHub Actions workflow in `.github/workflows/spark-lint.yml` that:
   - Triggers on PRs that modify `.sql` or `.py` files
   - Runs the lint check against all changed SQL files
   - Posts findings as a PR comment via the GitHub API
6. Provide a `databricks.yml` bundle task example for use in Databricks Asset Bundles.

---

#### F-14 · Standalone HTML Report Export ✅ Zero-dep notebook

**What:** A `%analyze --export {path}` flag that saves the full diagnostic report as a self-contained HTML file to DBFS or a mounted cloud storage path.

**Why it matters:** Notebook output is ephemeral and hard to share with stakeholders (DBAs, platform teams) who don't have Databricks access. An exported report closes the loop between the developer who found the problem and the team who needs to act on it.

**Implementation:**

1. Extend `display_utils.py` to accept an `export_path` parameter.
2. Generate a self-contained HTML file: inline all CSS (already present in the HTML card), add a report header with query text, timestamp, cluster info, and severity summary.
3. Include a collapsible section for the full `EXPLAIN FORMATTED` output (hidden by default, shown on click).
4. Write to path using `dbutils.fs.put(export_path, html_content, overwrite=True)`.
5. At the bottom of the notebook output, show a clickable DBFS file browser link and a `displayHTML` download trigger.
6. Support a `--format json` variant that exports findings as structured JSON for downstream tooling.

---

#### F-15 · Spark UI Deep-Link Integration ✅ Zero-dep notebook (`localhost:4040` — not available on Serverless)

**What:** After post-execution analysis (F-03), automatically embed direct links into the diagnostic HTML card that jump to the relevant stage or query in the Spark UI.

**Why it matters:** The current output tells the user a problem exists but leaves them to manually navigate the Spark UI to confirm it. Databricks' Query Profile does this natively for DBSQL but not for interactive cluster queries. Bridging the gap removes the most frustrating friction point in Spark debugging.

**Implementation:**

1. In `PostExecutionAnalyser` (F-03), capture the Spark application ID from `spark.sparkContext.applicationId` and the active Spark UI URL from `spark.sparkContext.uiWebUrl`.
2. When emitting a finding that references a specific stage (e.g., skew in stage 5), append a hyperlink:
   `<a href="{uiWebUrl}/stages/stage/?id=5&attempt=0" target="_blank">→ View Stage 5 in Spark UI</a>`
3. For query-level links (SQL tab), use the `executionId` captured from the `SQLAppStatusPlugin` REST endpoint: `GET {uiWebUrl}/api/v1/applications/{appId}/sql` — find the execution matching the query start time.
4. For Databricks clusters where the Spark UI is proxied, construct the correct proxy URL format: `https://{workspace_host}/driver-proxy/o/{orgId}/{clusterId}/4040/...`.
5. Gracefully degrade: if the UI URL is unavailable (e.g., serverless compute), omit the links silently.

---

## Priority Matrix

Labels: ✅ Zero-dep notebook · ⚠️ Needs package · 🌐 External call (by design)

Features are ordered within each phase by implementability: zero-dep notebook features first, constrained ones last.

| # | Feature | Constraint | Effort | Impact |
|---|---|---|---|---|
| F-02 | AQE Config Checker | ✅ | Low | 🔴 Critical |
| F-04 | Python Anti-Pattern Detector | ✅ | Low | 🟠 High |
| F-07 | Schema & Stats Health Checker | ✅ | Low | 🟠 High |
| F-12 | Cluster Config Advisor | ✅ | Low | 🟡 Medium |
| F-14 | HTML Report Export | ✅ | Low | 🟡 Medium |
| F-01 | Delta Lake Health Analyser | ✅ | Medium | 🔴 Critical |
| F-05 | DBU Cost Estimator | ✅ | Medium | 🟠 High |
| F-06 | Multi-Query Batch Analyser | ✅ | Medium | 🟠 High |
| F-09 | Query History Tracker | ✅ | Medium | 🟡 Medium |
| F-10 | Natural Language Explainer | ✅ | Medium | 🟠 High |
| F-03 | Deep Skew Analyser | ✅ `localhost:4040`* | Medium | 🔴 Critical |
| F-15 | Spark UI Deep-Link | ✅ `localhost:4040`* | Low | 🟡 Medium |
| F-08 | Structured Streaming Analyser | ✅ | High | 🟡 Medium |
| F-11 | Automated Query Rewriter | ⚠️ `sqlglot` | High | 🔴 Critical |
| F-13 | CI/CD Lint Mode | 🌐 Databricks + GitHub APIs | Medium | 🟠 High |

\* `localhost:4040` is a local driver-side call, not an outbound network call. Not available on Databricks Serverless — both features degrade gracefully.

---

## Suggested File Structure After Roadmap

```
spark_query_analyzer/
├── spark_plan_parser.py        # existing
├── bottleneck_detector.py      # existing
├── recommendation_engine.py    # existing
├── display_utils.py            # existing — extend for export (F-14)
├── aqe_checker.py              # new — F-02  ✅
├── python_scanner.py           # new — F-04  ✅
├── stats_checker.py            # new — F-07  ✅
├── cluster_advisor.py          # new — F-12  ✅
├── delta_analyser.py           # new — F-01  ✅
├── cost_estimator.py           # new — F-05  ✅
├── cross_query_optimiser.py    # new — F-06  ✅
├── history_tracker.py          # new — F-09  ✅
├── narrative_explainer.py      # new — F-10  ✅ (replaces llm_explainer.py)
├── skew_analyser.py            # new — F-03  ✅ (localhost:4040 only)
├── streaming_analyser.py       # new — F-08  ✅
├── query_rewriter.py           # new — F-11  ⚠️ requires sqlglot
└── cli.py                      # new — F-13  🌐 external calls, CI use only
```

**Dependency summary:**
- All features except F-11 and F-13 require zero new packages and make zero outbound network calls.
- F-11 requires `sqlglot` (`%pip install sqlglot`). Implement broadcast hint and repeated-scan rewrites with regex first as a no-package fallback.
- F-13 requires `databricks-sdk` in the CI environment only. It is not a notebook feature and its external calls are intentional.
