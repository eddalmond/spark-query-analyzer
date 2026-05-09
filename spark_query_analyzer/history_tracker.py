"""
Query History Performance Tracker — F-09 of the spark-query-analyzer roadmap.

Persists query signatures and diagnostic findings to a Delta table,
enabling longitudinal trend tracking and regression detection.

Opt-in: disabled by default. Enable via:
    spark.conf.set("spark_query_analyzer.history_enabled", "true")
    spark.conf.set("spark_query_analyzer.history_path", "/path/to/history/delta")
"""

from dataclasses import dataclass, field
from typing import Optional
import hashlib
import json
import re


HISTORY_TABLE_NAME = "_spark_query_analyzer.query_history"
HISTORY_DB = "_spark_query_analyzer"


@dataclass
class HistoryEntry:
    query_signature: str
    run_timestamp: str
    query_text: str
    severity_counts: dict  # {"critical": 0, "high": 0, "medium": 0, "info": 0}
    estimated_dbu_cost: Optional[float]
    cluster_id: str
    findings_json: str
    duration_ms: Optional[int]
    tables: list[str] = field(default_factory=list)
    codes: list[str] = field(default_factory=list)


def _normalise_sql(sql: str) -> str:
    """
    Normalise SQL for signature computation:
    - Strip comments
    - Lowercase keywords
    - Remove string/numeric literals
    - Collapse whitespace
    This makes 'WHERE date = 2024-01-01' and 'WHERE date = 2025-01-01'
    produce the same signature.
    """
    # Remove block comments
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    # Remove line comments
    sql = re.sub(r"--.*", "", sql)
    # Remove string literals (replace with _STR_)
    sql = re.sub(r"'[^']*'", "_STR_", sql)
    # Remove numeric literals
    sql = re.sub(r"\b\d+\.\d+\b", "_NUM_", sql)
    sql = re.sub(r"\b\d+\b", "_NUM_", sql)
    # Normalise whitespace
    sql = re.sub(r"\s+", " ", sql).strip()
    return sql


def _compute_signature(sql: str) -> str:
    """Compute a stable SHA-256 hash from the normalised SQL."""
    normalised = _normalise_sql(sql)
    digest = hashlib.sha256(normalised.encode("utf-8")).hexdigest()[:16]
    return digest


def _extract_tables_from_sql(sql: str) -> list[str]:
    """Extract table names from SQL query."""
    tables = set()
    sql_clean = re.sub(r"--.*", "", sql)
    sql_clean = re.sub(r"'[^']*'", "", sql_clean)
    patterns = [r"(?:FROM|JOIN)\s+(\w+(?:\.\w+)?)", r"(?:FROM|JOIN)\s+(\w+)"]
    for pattern in patterns:
        for match in re.finditer(pattern, sql_clean, re.IGNORECASE):
            name = match.group(1).split(".")[-1]
            if name.upper() not in ("SELECT", "WHERE", "AND", "OR", "ON", "AS", "TABLE"):
                tables.add(name)
    return list(tables)[:20]  # cap at 20 tables


def _history_table_exists(spark) -> bool:
    """Check if the history Delta table already exists."""
    try:
        spark.sql(f"SHOW TABLES IN {HISTORY_DB}")
        tables = [r.tableName() for r in spark.sql(f"SHOW TABLES IN {HISTORY_DB}").collect()]
        return HISTORY_TABLE_NAME.split(".")[-1] in tables
    except Exception:
        return False


def _ensure_history_table(spark, path: str) -> None:
    """Create the history Delta table if it doesn't exist."""
    if _history_table_exists(spark):
        return

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {HISTORY_DB}.query_history (
        query_signature STRING,
        run_timestamp TIMESTAMP,
        query_text STRING,
        severity_critical INT,
        severity_high INT,
        severity_medium INT,
        severity_info INT,
        estimated_dbu_cost DOUBLE,
        cluster_id STRING,
        findings_json STRING,
        duration_ms BIGINT,
        tables_json STRING,
        codes_json STRING
    )
    USING DELTA
    LOCATION '{path}'
    """
    spark.sql(create_sql)


def _build_severity_counts(result) -> dict:
    counts = {"critical": 0, "high": 0, "medium": 0, "info": 0}
    for f in result.findings:
        key = f.severity.lower()
        if key in counts:
            counts[key] += 1
    return counts


def _get_cluster_id(spark) -> str:
    """Get a cluster identifier from SparkConf."""
    try:
        conf = spark.sparkContext.getConf()
        cluster_id = conf.get("spark.databricks.clusterUsageTags.clusterId", "")
        if not cluster_id:
            cluster_id = conf.get("spark.databricks.clusterUsageTags.clusterName", "unknown")
        return cluster_id
    except Exception:
        return "unknown"


def track_analysis_run(spark, result, sql: str, duration_ms: Optional[int] = None) -> Optional[str]:
    """
    Write an analysis record to the history Delta table.
    Called at the end of run_analysis().
    Returns the query signature if written, else None.
    """
    try:
        # Check opt-in flag
        try:
            conf = spark.sparkContext.getConf()
            enabled = conf.get("spark_query_analyzer.history_enabled", "false").lower() == "true"
            if not enabled:
                return None
            history_path = conf.get(
                "spark_query_analyzer.history_path",
                f"/tmp/{HISTORY_DB.replace('.', '/')}/query_history",
            )
        except Exception:
            return None

        _ensure_history_table(spark, history_path)

        signature = _compute_signature(sql)
        counts = _build_severity_counts(result)
        tables = _extract_tables_from_sql(sql)
        codes = list(set(f.code for f in result.findings))
        findings_json = json.dumps([
            {"code": f.code, "severity": f.severity, "message": f.message, "suggestion": f.suggestion}
            for f in result.findings
        ])

        estimated_cost = None
        if result.cost_estimate:
            estimated_cost = result.cost_estimate.estimated_cost_usd

        cluster_id = _get_cluster_id(spark)
        from datetime import datetime, timezone
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        insert_sql = f"""
        INSERT INTO {HISTORY_DB}.query_history VALUES (
            '{signature}',
            '{timestamp}',
            {repr(sql[:5000])},
            {counts['critical']},
            {counts['high']},
            {counts['medium']},
            {counts['info']},
            {estimated_cost},
            '{cluster_id}',
            {repr(findings_json)},
            {duration_ms if duration_ms else 'NULL'},
            {repr(json.dumps(tables))},
            {repr(json.dumps(codes))}
        )
        """
        spark.sql(insert_sql)
        return signature

    except Exception:
        return None


def _render_ascii_sparkline(values: list[float], width: int = 20) -> str:
    """Render a simple ASCII sparkline from a list of values."""
    if not values:
        return ""

    min_v, max_v = min(values), max(values)
    range_v = max_v - min_v
    if range_v == 0:
        range_v = 1.0

    chars = "▁▂▃▅▇"
    n_chars = len(chars)

    result = ""
    for v in values:
        normalised = (v - min_v) / range_v
        idx = min(int(normalised * (n_chars - 1)), n_chars - 1)
        result += chars[idx]

    return result


def _render_html_sparkline(values: list[float], width: int = 120, height: int = 24) -> str:
    """Render an HTML inline sparkline using SVG."""
    if not values or len(values) < 2:
        return ""

    min_v, max_v = min(values), max(values)
    range_v = max_v - min_v
    if range_v == 0:
        range_v = 1.0

    n = len(values)
    step = width / max(n - 1, 1)

    points = []
    for i, v in enumerate(values):
        x = i * step
        y = height - ((v - min_v) / range_v * height)
        points.append(f"{x:.1f},{y:.1f}")

    polyline_points = " ".join(points)
    min_label = f"{min_v:.4f}" if min_v < 1 else f"{min_v:.2f}"
    max_label = f"{max_v:.4f}" if max_v < 1 else f"{max_v:.2f}"

    svg = (
        f'<svg width="{width}" height="{height + 8}" viewBox="0 0 {width} {height + 8}" '
        f'style="overflow:visible;">'
        f'<polyline points="{polyline_points}" fill="none" stroke="#0f172a" stroke-width="1.5" '
        f'stroke-linejoin="round" stroke-linecap="round"/>'
        f'<text x="{width - 2}" y="{height + 6}" text-anchor="end" font-size="9" fill="#64748b">{max_label}</text>'
        f'<text x="{width - 2}" y="8" text-anchor="end" font-size="9" fill="#64748b">{min_label}</text>'
        f'</svg>'
    )
    return svg


def get_history_for_signature(spark, signature: str, limit: int = 30) -> list[dict]:
    """Fetch the last `limit` history entries for a given query signature."""
    try:
        query = f"""
        SELECT * FROM {HISTORY_DB}.query_history
        WHERE query_signature = '{signature}'
        ORDER BY run_timestamp DESC
        LIMIT {limit}
        """
        rows = spark.sql(query).collect()
        return [row.asDict() for row in rows]
    except Exception:
        return []


def get_history_for_table(spark, table: str, limit: int = 30) -> list[dict]:
    """Fetch the last `limit` history entries for queries touching a given table."""
    try:
        query = f"""
        SELECT * FROM {HISTORY_DB}.query_history
        WHERE tables_json LIKE '%{table}%'
        ORDER BY run_timestamp DESC
        LIMIT {limit}
        """
        rows = spark.sql(query).collect()
        return [row.asDict() for row in rows]
    except Exception:
        return []


def format_history_trends(spark, signature: str, table: Optional[str] = None) -> str:
    """
    Render a history trends HTML card.
    Shows sparklines for severity counts and estimated cost over time.
    """
    if table:
        entries = get_history_for_table(spark, table)
    else:
        entries = get_history_for_signature(spark, signature)

    if not entries:
        return (
            '<div style="padding:12px 16px;font-size:13px;color:#64748b;">'
            '&#x1F4CB; No history found for this query. '
            'Run %analyze with history_enabled=true to start tracking.'
            '</div>'
        )

    # Build time-series from oldest to newest (for sparkline direction)
    chronological = list(reversed(entries))

    critical_trend = [float(r.get("severity_critical", 0) or 0) for r in chronological]
    high_trend = [float(r.get("severity_high", 0) or 0) for r in chronological]
    medium_trend = [float(r.get("severity_medium", 0) or 0) for r in chronological]
    cost_trend = [float(r.get("estimated_dbu_cost", 0) or 0) for r in chronological]

    # ASCII sparklines
    crit_spark = _render_ascii_sparkline(critical_trend)
    high_spark = _render_ascii_sparkline(high_trend)
    med_spark = _render_ascii_sparkline(medium_trend)
    cost_spark = _render_ascii_sparkline(cost_trend)

    timestamps = [str(r.get("run_timestamp", "")[:16]) for r in chronological]

    # Format each row
    rows_html = ""
    for r, ts in zip(chronological, timestamps):
        run_link = f"#{len(chronological) - (chronological.index(r))}"
        severity_parts = []
        for sev, label in [("critical", "🔴"), ("high", "🟠"), ("medium", "🟡"), ("info", "🟢")]:
            n = r.get(f"severity_{sev}", 0) or 0
            if n:
                severity_parts.append(f"{label}{n}")
        sev_display = " · ".join(severity_parts) or "✅ none"

        cost_val = r.get("estimated_dbu_cost")
        cost_display = f"${cost_val:.4f}" if cost_val else "—"

        duration_val = r.get("duration_ms")
        dur_display = f"{duration_val}ms" if duration_val else "—"

        rows_html += (
            f'<tr style="border-bottom:1px solid #f1f5f9;font-size:12px;">'
            f'<td style="padding:6px 10px;color:#64748b;">{ts}</td>'
            f'<td style="padding:6px 10px;font-family:monospace;color:#0f172a;">{sev_display}</td>'
            f'<td style="padding:6px 10px;color:#475569;">{cost_display}</td>'
            f'<td style="padding:6px 10px;color:#475569;">{dur_display}</td>'
            f'</tr>'
        )

    # Latest summary
    latest = entries[0]
    latest_critical = latest.get("severity_critical", 0) or 0
    latest_high = latest.get("severity_high", 0) or 0
    latest_cost = latest.get("estimated_dbu_cost")

    header = (
        f'<div style="padding:10px 14px;background:#f8fafc;border-bottom:1px solid #e2e8f0;'
        f'font-size:12px;font-weight:600;color:#0f172a;">'
        f'&#x1F4CB; Query History &mdash; {len(entries)} runs tracked'
        f'</div>'
    )

    trend_html = (
        f'<div style="display:flex;gap:16px;padding:8px 14px;background:#f1f5f9;font-size:11px;">'
        f'<span>Critical: <code>{crit_spark}</code></span>'
        f'<span>High: <code>{high_spark}</code></span>'
        f'<span>Medium: <code>{med_spark}</code></span>'
        f'<span>Cost: <code>{cost_spark}</code></span>'
        f'</div>'
    )

    table_html = (
        '<table style="width:100%;border-collapse:collapse;font-size:12px;">'
        '<tr style="background:#f8fafc;font-weight:600;text-align:left;border-bottom:1px solid #e2e8f0;">'
        '<th style="padding:6px 10px;">Time</th>'
        '<th style="padding:6px 10px;">Findings</th>'
        '<th style="padding:6px 10px;">Est. Cost</th>'
        '<th style="padding:6px 10px;">Duration</th>'
        '</tr>'
        f'{rows_html}'
        '</table>'
    )

    return (
        f'<div style="border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;margin-top:8px;">'
        f'{header}{trend_html}{table_html}</div>'
    )