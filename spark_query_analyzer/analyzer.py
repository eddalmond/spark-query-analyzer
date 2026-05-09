"""
Core analysis engine: takes SQL, runs EXPLAIN FORMATTED, returns structured findings.
"""

import re
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Finding:
    severity: str  # "critical" | "high" | "medium" | "info"
    code: str      # short identifier e.g. "MISSING_BROADCAST"
    message: str   # human-readable description
    node: str      # plan node or table involved
    suggestion: str # actionable fix
    table: Optional[str] = None
    detail: Optional[str] = None
    config_snippet: Optional[str] = None  # copy-ready Spark conf set() block


@dataclass
class AnalysisResult:
    findings: list[Finding] = field(default_factory=list)
    delta_results: list = field(default_factory=list)  # DeltaHealthResult from F-01
    plan_text: str = ""
    query: str = ""

    def has_critical(self) -> bool:
        return any(f.severity == "critical" for f in self.findings)

    def has_high(self) -> bool:
        return any(f.severity == "high" for f in self.findings)

    @property
    def severity_counts(self) -> dict:
        return {s: sum(1 for f in self.findings if f.severity == s) for s in ["critical", "high", "medium", "info"]}


def run_analysis(spark, sql: str, line: str = "", full_cell: str = "", dry_run: bool = True) -> str:
    """Run EXPLAIN FORMATTED on the query, parse the plan, return HTML diagnostics."""
    # Run EXPLAIN FORMATTED
    try:
        explained = spark.sql(f"EXPLAIN FORMATTED {sql}")
        plan_text = "\n".join(row[0] for row in explained.collect())
    except Exception as e:
        raise RuntimeError(f"EXPLAIN FORMATTED failed: {e}")

    result = parse_plan(plan_text, sql)
    tables = _extract_table_names(sql)
    plan_lines = plan_text.split("\n")

    # --- AQE config diagnostics (F-02: plan-aware AQE recommendations) ---
    from spark_query_analyzer.aqe_checker import read_aqe_config, build_recommendations
    aqe_cfg = read_aqe_config(spark)
    for f in build_recommendations(aqe_cfg, plan_text, sql):
        result.findings.append(Finding(
            severity=f.severity,
            code=f.code,
            message=f.message,
            node=None,
            suggestion=f.suggestion,
            table=None,
            detail=f.detail,
            config_snippet=f.config_snippet,
        ))

    # --- File size / small file diagnostics ---
    if tables:
        from spark_query_analyzer.system_info import check_file_size_stats
        for item in check_file_size_stats(spark, sql, tables):
            result.findings.append(Finding(
                severity=item["severity"],
                code=item["code"],
                message=item["message"],
                node=None,
                suggestion=item.get("suggestion", ""),
                table=None,
                detail=item.get("detail"),
            ))

    # --- Query history regression check ---
    from spark_query_analyzer.system_info import check_query_history_for_slow_runs
    for item in check_query_history_for_slow_runs(spark, sql):
        result.findings.append(Finding(
            severity=item["severity"],
            code=item["code"],
            message=item["message"],
            node=None,
            suggestion=item.get("suggestion", ""),
            table=None,
            detail=item.get("detail"),
        ))

    # --- Exploding join detection ---
    for finding in _detect_exploding_joins(plan_text, plan_lines):
        result.findings.append(finding)

    # --- F-01: Delta Lake Health Analyser ---
    delta_results = []
    if tables:
        from spark_query_analyzer.delta_analyser import analyse_all_tables
        delta_results = analyse_all_tables(spark, tables, sql)
        result.delta_results = delta_results
        for dr in delta_results:
            for df in dr.findings:
                result.findings.append(Finding(
                    severity=df.severity,
                    code=df.code,
                    message=df.message,
                    node=None,
                    suggestion=df.suggestion,
                    table=df.table,
                    detail=df.detail,
                ))

    # --- F-04: Python Anti-Pattern Scanner ---
    if full_cell:
        from spark_query_analyzer.python_scanner import scan_cell
        py_findings = scan_cell(full_cell)
        result.python_findings = py_findings
        for pf in py_findings:
            result.findings.append(Finding(
                severity=pf.severity,
                code=pf.code,
                message=pf.message,
                node=str(pf.line) if pf.line else None,
                suggestion=pf.suggestion,
                table=None,
                detail=pf.detail,
            ))

    # --- F-03: Deep Skew Analyser (Post-Execution) ---
    skew_findings = []
    if not dry_run:
        from spark_query_analyzer.post_execution_analyser import run_post_execution_skew_analysis
        from spark_query_analyzer.aqe_checker import read_aqe_config
        aqe_cfg = read_aqe_config(spark)
        skew_findings = run_post_execution_skew_analysis(
            spark, sql=sql, aqe_enabled=aqe_cfg.adaptive_enabled
        )
        result.skew_findings = skew_findings
        for sf in skew_findings:
            detail = None
            if sf.metrics:
                detail = (f"max={sf.metrics.max_task_duration_ms/1000:.1f}s | "
                          f"median={sf.metrics.median_task_duration_ms/1000:.1f}s | "
                          f"ratio={sf.metrics.skew_ratio:.1f}x | "
                          f"stragglers={sf.metrics.num_stragglers}")
            result.findings.append(Finding(
                severity=sf.severity,
                code=sf.code,
                message=sf.message,
                node=f"stage {sf.stage_id}",
                suggestion=sf.suggestion,
                table=None,
                detail=detail,
            ))

    # --- F-05: DBU Cost Estimator ---
    from spark_query_analyzer.cost_estimator import build_cost_estimate, format_cost_badge
    cost_estimate = build_cost_estimate(spark, plan_text)
    cost_badge = format_cost_badge(cost_estimate)
    result.cost_estimate = cost_estimate

    # Display via display_utils
    from spark_query_analyzer.display_utils import format_diagnostics
    html = format_diagnostics(
        result, delta_results,
        getattr(result, 'python_findings', None),
        skew_findings,
        cost_badge,
    )
    from IPython.display import HTML, display
    display(HTML(html))
    return ""


def parse_plan(plan_text: str, query: str = "") -> AnalysisResult:
    """Parse the raw EXPLAIN output into structured findings."""
    result = AnalysisResult(plan_text=plan_text, query=query)
    lines = plan_text.split("\n")

    tables_in_query = _extract_table_names(query)

    for i, line in enumerate(lines):
        stripped = line.strip()

        # Broadcast miss: large table shuffled instead of broadcasted
        if "Exchange" in stripped and i + 1 < len(lines):
            next_lines = "\n".join(lines[i+1:i+5])
            if "BroadcastExchange" not in next_lines and any(t in next_lines for t in tables_in_query):
                for table in tables_in_query:
                    if table in next_lines and "Scan" in next_lines:
                        result.findings.append(Finding(
                            severity="critical",
                            code="MISSING_BROADCAST",
                            node=_extract_node_id(stripped),
                            message=f"Table '{table}' is being shuffled instead of broadcasted. "
                                    f"Estimated plan shows an Exchange but no BroadcastExchange for this join.",
                            suggestion=f"Add BROADCAST hint: JOIN {table} BROADCAST(t) "
                                       f"or increase spark.sql.autoBroadcastJoinThreshold "
                                       f"(currently defaults to 10MB).",
                            table=table,
                            detail=_extract_detail(next_lines)
                        ))
                        break

        # Cartesian product
        if re.match(r"(-+\+)+", stripped) and "Join" in stripped:
            join_line = stripped
            prev_lines = "\n".join(lines[max(0, i-10):i])
            if "Filter" not in prev_lines or prev_lines.count("Filter") <= 1:
                if "Condition" not in join_line and "Inner" in join_line:
                    result.findings.append(Finding(
                        severity="critical",
                        code="CARTESIAN_PRODUCT",
                        node=_extract_node_id(stripped),
                        message="Join has no condition — this is a Cartesian (cross) product. "
                                "Every row in the left table will be joined to every row in the right.",
                        suggestion="Add an explicit JOIN condition or filter one side to a single partition "
                                   "if a cross join is intended.",
                        table=None,
                        detail=join_line
                    ))

        # Full table scan
        if stripped.startswith("+- ") or stripped.startswith("+- "):
            scan_match = re.search(r"Scan (?:.*? )?(\w+)", stripped)
            filter_match = re.search(r"Filter.*?\[(.*?)\]", stripped)
            if scan_match and not filter_match:
                table = scan_match.group(1)
                if table != "<unknown>" and table not in ["", " "]:
                    result.findings.append(Finding(
                        severity="high",
                        code="FULL_TABLE_SCAN",
                        node=_extract_node_id(stripped),
                        message=f"Full table scan on '{table}' with no filter predicates. "
                                f"All partitions will be read regardless of partition pruning.",
                        suggestion=f"Add partition column filter (WHERE partition_col = ...) "
                                   f"or rewrite query to reference partition column in filter.",
                        table=table,
                        detail=stripped
                    ))

        # Sort merge join
        if "Sort" in stripped and i > 0:
            prev_lines = "\n".join(lines[max(0, i-5):i])
            if "Exchange" in prev_lines:
                result.findings.append(Finding(
                    severity="high",
                    code="SORT_MERGE_JOIN",
                    node=_extract_node_id(stripped),
                    message="Sort-based join detected. Data is being sorted before a shuffle-merge join.",
                    suggestion="If joining large tables, consider BROADCAST or shuffle-hash join instead. "
                               "Use hint: JOIN ... SHUFFLE_HASH(t) or BROADCAST(t).",
                    table=None,
                    detail=stripped
                ))

        # Repeated table scans
        table_counts = _count_table_occurrences(plan_text, tables_in_query)
        for table, count in table_counts.items():
            if count >= 3:
                result.findings.append(Finding(
                    severity="medium",
                    code="REPEATED_SCAN",
                    node=None,
                    message=f"Table '{table}' appears {count} times in the execution plan. "
                            f"It is being scanned multiple times across the query.",
                    suggestion=f"Consider using a CTE (WITH clause) to materialise '{table}' once, "
                               f"or cache the table with df.cache() if it's reused.",
                    table=table
                ))

        # BroadcastExchange present
        if "BroadcastExchange" in stripped:
            broadcast_match = re.search(r"BroadcastExchange\s+(?:.*? )?(\w+)", stripped)
            if broadcast_match:
                result.findings.append(Finding(
                    severity="info",
                    code="BROADCAST_USED",
                    node=_extract_node_id(stripped),
                    message=f"Broadcast join in use (estimated {broadcast_match.group(1)} bytes).",
                    suggestion="No action needed — broadcast is optimal for this table.",
                    table=broadcast_match.group(1) if broadcast_match else None,
                    detail=stripped
                ))

        # Skew indicator
        exchanges = [l.strip() for l in lines if "Exchange" in l and "partition=" in l]
        if len(exchanges) > 2:
            partition_counts = re.findall(r"partition=(\d+)", "\n".join(exchanges))
            if len(set(partition_counts)) > 3:
                result.findings.append(Finding(
                    severity="medium",
                    code="SKEW_INDICATOR",
                    node=None,
                    message=f"Detected {len(exchanges)} exchanges with varying partition counts: {partition_counts}. "
                            f"This may indicate data skew.",
                    suggestion="Check partition size distribution with REPL. "
                              "Consider using skewed join optimisation: spark.sql.adaptive.skewJoinEnabled=true.",
                    table=None
                ))

    return result


def _extract_table_names(sql: str) -> list[str]:
    tables = set()
    sql_clean = re.sub(r"--.*", "", sql)
    sql_clean = re.sub(r"'[^']*'", "", sql_clean)
    patterns = [r"(?:FROM|JOIN)\s+(\w+(?:\.\w+)?)", r"(?:FROM|JOIN)\s+(\w+)"]
    for pattern in patterns:
        for match in re.finditer(pattern, sql_clean, re.IGNORECASE):
            name = match.group(1).split(".")[-1]
            if name.upper() not in ("SELECT", "WHERE", "AND", "OR", "ON"):
                tables.add(name)
    return list(tables)


def _extract_node_id(line: str) -> str:
    m = re.match(r"^(\d+)", line.strip())
    return m.group(1) if m else "?"


def _extract_detail(context: str) -> str:
    lines = [l.strip() for l in context.split("\n") if l.strip()]
    return lines[0][:200] if lines else ""


def _count_table_occurrences(plan_text: str, tables: list[str]) -> dict[str, int]:
    counts = {}
    for table in tables:
        counts[table] = len(re.findall(re.escape(table), plan_text, re.IGNORECASE))
    return {t: c for t, c in counts.items() if c >= 2}


def _detect_exploding_joins(plan_text: str, lines: list[str]) -> list[Finding]:
    """Detect joins that output far more rows than their inputs (exploding join)."""
    findings = []
    for i, line in enumerate(lines):
        stripped = line.strip()
        if re.match(r"\d+\s+\[.*\]", stripped) and "Join" in stripped:
            prev_context = "\n".join(lines[max(0, i-20):i])
            next_context = "\n".join(lines[i+1:i+15])
            input_scan_lines = [l for l in prev_context.split("\n") if "Scan" in l and "num_rows" in l.lower()]
            output_join_lines = [l for l in next_context.split("\n") if "Join" in l]
            if input_scan_lines and output_join_lines:
                if "Aggregate" in next_context and "count" not in next_context.lower():
                    findings.append(Finding(
                        severity="critical",
                        code="EXPLODING_JOIN",
                        node=_extract_node_id(stripped),
                        message="Join appears to be outputting significantly more rows than input. "
                                "This typically happens when join selectivity is poor (missing filter on large table, "
                                "or join on non-unique key creating a Cartesian product within partitions).",
                        suggestion="Check that filters on both join inputs are selective enough. "
                                   "Verify join key has proper cardinality. "
                                   "Consider adding a pre-aggregate step to reduce left side before joining.",
                        table=None,
                        detail=stripped[:200],
                    ))
    return findings