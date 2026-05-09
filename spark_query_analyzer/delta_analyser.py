"""
Delta Lake Health Analyser — inspects transaction log and file layout for storage anti-patterns.
F-01 of the spark-query-analyzer roadmap.
"""

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional


@dataclass
class DeltaFinding:
    severity: str  # "critical" | "high" | "medium" | "info"
    code: str
    message: str
    table: str
    suggestion: str
    detail: Optional[str] = None
    improvement_bytes: Optional[int] = None


@dataclass
class DeltaHealthResult:
    table: str
    location: str
    is_delta: bool = False
    findings: list[DeltaFinding] = field(default_factory=list)
    # Stats collected
    num_files: int = 0
    size_bytes: int = 0
    avg_file_size_bytes: int = 0
    last_optimize: Optional[str] = None
    last_vacuum: Optional[str] = None
    optimize_write_enabled: bool = False
    auto_compact_enabled: bool = False
    zorder_columns: list[str] = field(default_factory=list)
    partition_columns: list[str] = field(default_factory=list)
    table_size_gb: float = 0.0
    dbr_version: str = ""
    liquid_clustering_enabled: bool = False


def analyse_table(spark, table: str, sql_filter_columns: list[str] = None) -> Optional[DeltaHealthResult]:
    """
    Run Delta Lake health checks on a single table.
    Returns DeltaHealthResult with findings, or None if the table is not a Delta table.
    """
    result = DeltaHealthResult(table=table, location="")

    # 1. Check if it's a Delta table via DESCRIBE DETAIL
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {table}").collect()
        if not detail:
            return None
        row = detail[0]
        format_val = str(row.get("format", "")).lower()
        location = str(row.get("location", ""))

        result.is_delta = format_val == "delta"
        result.location = location

        if not result.is_delta:
            return None

        # Collect stats from DESCRIBE DETAIL
        result.num_files = int(row.get("numFiles", 0) or 0)
        result.size_bytes = int(row.get("sizeInBytes", 0) or 0)
        result.avg_file_size_bytes = result.size_bytes // result.num_files if result.num_files > 0 else 0
        result.table_size_gb = result.size_bytes / (1024 ** 3)

        # 2. DESCRIBE HISTORY — last 10 operations
        history = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 20").collect()

        optimize_operations = [r for r in history if str(r.get("operation", "")).upper() == "OPTIMIZE"]
        vacuum_operations = [r for r in history if str(r.get("operation", "")).upper() == "VACUUM"]

        if optimize_operations:
            latest_opt = optimize_operations[0]
            result.last_optimize = str(latest_opt.get("timestamp", "") or "")
            # Check for Z-ORDER in parameters
            for op in optimize_operations:
                params = str(op.get("operationParameters", "") or "")
                if "zOrderBy" in params or "clusteringExpression" in params:
                    # Extract column names from ZORDER BY clause
                    zorder_match = re.search(r"(?:zOrderBy|clusteringExpression)\[([^\]]+)\]", params)
                    if zorder_match:
                        cols_raw = zorder_match.group(1)
                        result.zorder_columns = [c.strip() for c in cols_raw.split(",")]
                        break

        if vacuum_operations:
            result.last_vacuum = str(vacuum_operations[0].get("timestamp", "") or "")

        # 3. Read table properties for autoOptimize settings
        props_result = spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
        props = {str(r["key"]): str(r["value"]) for r in props_result}

        result.optimize_write_enabled = props.get("delta.autoOptimize.optimizeWrite", "false") == "true"
        result.auto_compact_enabled = props.get("delta.autoOptimize.autoCompact", "false") == "true"
        result.liquid_clustering_enabled = props.get("delta.enableLiquid", "false") == "true"

        # 4. Read partition columns from DESCRIBE
        desc_result = spark.sql(f"DESCRIBE {table}").collect()
        result.partition_columns = [
            str(r["col_name"]) for r in desc_result
            if str(r.get("col_name", "")) and str(r.get("data_type", "")).startswith("#")
            or str(r.get("partition_id", "")) != ""
        ]

        # Also get from DESCRIBE EXTENDED
        try:
            extended = spark.sql(f"DESCRIBE EXTENDED {table}").collect()
            for r in extended:
                if str(r.get("col_name", "")).lower() in ("# partition information", "# detailed table information"):
                    break
                if r.get("data_type") and "#" in str(r.get("data_type", "")):
                    part_match = re.search(r"# (\w+)", str(r["data_type"]))
                    if part_match:
                        col_name = str(r["col_name"])
                        if col_name and col_name not in result.partition_columns:
                            result.partition_columns.append(col_name)
        except Exception:
            pass

    except Exception:
        # Table may be a view, external non-Delta table, or invalid — degrade gracefully
        return None

    # 5. Run health checks
    _run_health_checks(result, sql_filter_columns)

    return result


def _run_health_checks(result: DeltaHealthResult, sql_filter_columns: list[str]) -> None:
    """Populate findings based on collected stats."""

    # ── Small files: avg < 32MB AND numFiles > 100 ──────────────────────
    if result.num_files > 100 and result.avg_file_size_bytes < 32 * 1024 * 1024:
        improvement_est = result.size_bytes * 0.6  # rough estimate: compaction reduces file count 60%
        result.findings.append(DeltaFinding(
            severity="critical",
            code="DELTA_SMALL_FILES",
            message=f"Table has {result.num_files:,} files with avg size {result.avg_file_size_bytes / (1024*1024):.1f} MB. "
                    f"Small files cause excessive metadata overhead and slow scans.",
            table=result.table,
            suggestion=f"Run: OPTIMIZE {result.table}",
            detail=f"Files: {result.num_files:,} | Avg: {result.avg_file_size_bytes/(1024*1024):.1f}MB | "
                   f"Estimated improvement: ~{improvement_est/(1024**3):.1f}GB saved from compaction",
            improvement_bytes=improvement_est,
        ))

    # ── No recent OPTIMIZE: last OPTIMIZE > 7 days ago ────────────────
    if result.last_optimize:
        try:
            opt_time = datetime.strptime(result.last_optimize, "%Y-%m-%dT%H:%M:%S.%fZ")
            age_days = (datetime.now() - opt_time).days
            if age_days > 7:
                result.findings.append(DeltaFinding(
                    severity="medium",
                    code="DELTA_STALE_OPTIMIZE",
                    message=f"Last OPTIMIZE was {age_days} days ago. Without regular compaction, "
                            f"file count grows and scan performance degrades.",
                    table=result.table,
                    suggestion=f"Schedule: OPTIMIZE {result.table} (daily for high-write tables, weekly for others)",
                    detail=f"Last OPTIMIZE: {result.last_optimize} ({age_days}d ago)",
                ))
        except Exception:
            pass

    # ── No Z-ORDER on large queried table ─────────────────────────────
    if result.table_size_gb > 1.0 and not result.zorder_columns and sql_filter_columns:
        # Table is large, has filter columns in the SQL, but no Z-ORDER
        result.findings.append(DeltaFinding(
            severity="medium",
            code="DELTA_MISSING_ZORDER",
            message=f"Table is {result.table_size_gb:.1f} GB but has no Z-ORDER index. "
                    f"Filter columns detected in query: {', '.join(sql_filter_columns)}. "
                    f"Without Z-ORDER, Databricks reads entire partitions before filtering.",
            table=result.table,
            suggestion=f"Run: OPTIMIZE {result.table} ZORDER BY ({', '.join(sql_filter_columns[:3])})",
            detail=f"Size: {result.table_size_gb:.1f}GB | Filter cols: {sql_filter_columns}",
        ))

    # ── VACUUM overdue: last VACUUM > 30 days ──────────────────────────
    if result.last_vacuum:
        try:
            vacuum_time = datetime.strptime(result.last_vacuum, "%Y-%m-%dT%H:%M:%S.%fZ")
            vacuum_age_days = (datetime.now() - vacuum_time).days
            if vacuum_age_days > 30:
                result.findings.append(DeltaFinding(
                    severity="medium",
                    code="DELTA_STALE_VACUUM",
                    message=f"Last VACUUM was {vacuum_age_days} days ago. "
                            f"Stale files accumulate and consume storage unnecessarily.",
                    table=result.table,
                    suggestion=f"Run: VACUUM {result.table} RETAIN 168 HOURS (7 days)",
                    detail=f"Last VACUUM: {result.last_vacuum} ({vacuum_age_days}d ago)",
                ))
        except Exception:
            pass

    # ── Auto-optimize not enabled ─────────────────────────────────────
    if result.table_size_gb > 5.0 and not result.optimize_write_enabled:
        result.findings.append(DeltaFinding(
            severity="medium",
            code="DELTA_AUTOOPTIMIZE_DISABLED",
            message=f"Table is {result.table_size_gb:.1f} GB but delta.autoOptimize.optimizeWrite is not enabled. "
                    f"Writes will produce small files without compaction.",
            table=result.table,
            suggestion=f"Enable write optimization: ALTER TABLE {result.table} SET TBLPROPERTIES "
                      f"(delta.autoOptimize.optimizeWrite = true)",
            detail=f"Size: {result.table_size_gb:.1f}GB | optimizeWrite: {result.optimize_write_enabled}",
        ))

    # ── Liquid clustering opportunity ──────────────────────────────────
    if result.table_size_gb > 1.0 and not result.liquid_clustering_enabled:
        result.findings.append(DeltaFinding(
            severity="info",
            code="DELTA_LIQUID_CLUSTERING",
            message=f"Table is {result.table_size_gb:.1f} GB on DBR 13.3+. "
                    f"Migrating from Z-ORDER to Liquid Clustering would eliminate manual OPTIMIZE jobs.",
            table=result.table,
            suggestion=f"Consider: ALTER TABLE {result.table} SET CLUSTERING KEYS ({', '.join(sql_filter_columns[:3])})",
            detail=f"Size: {result.table_size_gb:.1f}GB | DBR 13.3+ required | liquid: {result.liquid_clustering_enabled}",
        ))

    # ── Good state confirmations ───────────────────────────────────────
    if result.num_files <= 100 and result.avg_file_size_bytes >= 64 * 1024 * 1024:
        result.findings.append(DeltaFinding(
            severity="info",
            code="DELTA_STORAGE_HEALTHY",
            message=f"Delta storage healthy. {result.num_files} files, avg {result.avg_file_size_bytes/(1024*1024):.0f}MB per file.",
            table=result.table,
            suggestion="No action needed.",
        ))


def extract_filter_columns_from_sql(sql: str) -> list[str]:
    """Extract potential filter/join column names from SQL to suggest Z-ORDER candidates."""
    columns = set()
    sql_clean = re.sub(r"--.*", "", sql)  # strip comments

    # Extract columns from WHERE clauses
    where_match = re.search(r"WHERE\s+(.+?)(?:GROUP\s+BY|ORDER\s+BY|LIMIT|HAVING|$)", sql_clean, re.IGNORECASE)
    if where_match:
        where_clause = where_match.group(1)
        # Find AND/OR separated conditions
        conditions = re.split(r"\s+(?:AND|OR)\s+", where_clause)
        for cond in conditions:
            # Look for column names on left side of comparisons
            col_match = re.match(r"(\w+)\s*[<>=!]+", cond.strip(), re.IGNORECASE)
            if col_match:
                col = col_match.group(1)
                if col.upper() not in ("SELECT", "FROM", "AND", "OR", "NOT", "IN", "LIKE", "BETWEEN"):
                    columns.add(col)

    # Extract JOIN ON columns
    join_matches = re.finditer(r"JOIN\s+\w+(?:\.\w+)?\s+ON\s+([^\s]+)\s*=", sql_clean, re.IGNORECASE)
    for m in join_matches:
        on_clause = m.group(1)
        # strip table qualifiers: table.col -> col
        col = on_clause.split(".")[-1] if "." in on_clause else on_clause
        col = re.sub(r"\s+.*", "", col)  # strip anything after first space
        if col.upper() not in ("SELECT", "WHERE"):
            columns.add(col)

    return list(columns)


def analyse_all_tables(spark, tables: list[str], sql: str) -> list[DeltaHealthResult]:
    """Run Delta health analysis on all tables found in the query."""
    filter_cols = extract_filter_columns_from_sql(sql)
    results = []

    for table in tables:
        result = analyse_table(spark, table, filter_cols)
        if result and result.is_delta:
            results.append(result)

    return results