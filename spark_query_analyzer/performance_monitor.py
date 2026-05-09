"""
Performance Monitor — F-09 decorator extension for spark-query-analyzer.

Wraps any Python or Spark callable with instrumentation that:
  1. Records execution time, DBU estimate, and findings to the central
     Delta history table (shared with F-09 %analyze tracking)
  2. Shows an inline HTML output card after each run
  3. Compares against the previous run and flags regressions

Usage (in a setup cell — run once per session):
    from spark_query_analyzer.performance_monitor import monitor_performance

    @monitor_performance(job_name="daily_sales_aggregate", spark=spark)
    def run_aggregation():
        df = spark.sql("SELECT region, SUM(amount) AS total ...")
        return df.collect()

    result = run_aggregation()   # card appears inline after execution

The history table is shared with %analyze F-09 tracking, so all metrics land in
the same place and can be queried together for platform-level dashboards.

Requires:
    spark.conf.set("spark_query_analyzer.history_enabled", "true")
    spark.conf.set("spark_query_analyzer.history_path", "/path/to/history/delta")

(Or accept the defaults — it falls back to /tmp/_spark_query_analyzer/query_history)
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import wraps
from typing import Callable, Optional, Any
import hashlib
import json
import re
import time

# ── Internal helpers (mirror of history_tracker.py) ────────────────────────

HISTORY_DB = "_spark_query_analyzer"
HISTORY_TABLE = f"{HISTORY_DB}.query_history"

SQL_NORMALISATION_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
COMMENT_RE = re.compile(r"--.*")
STRING_RE = re.compile(r"'[^']*'")


def _normalise_sql(sql: str) -> str:
    """Normalise SQL for signature: strip comments/literals, collapse whitespace."""
    sql = SQL_NORMALISATION_RE.sub("", sql)
    sql = COMMENT_RE.sub("", sql)
    sql = STRING_RE.sub("_STR_", sql)
    sql = re.sub(r"\b\d+\.\d+\b", "_NUM_", sql)
    sql = re.sub(r"\b\d+\b", "_NUM_", sql)
    sql = re.sub(r"\s+", " ", sql).strip()
    return sql


def _compute_signature(text: str) -> str:
    """Stable SHA-256 hash (first 16 chars) of normalised text."""
    digest = hashlib.sha256(_normalise_sql(text).encode("utf-8")).hexdigest()
    return digest[:16]


def _get_cluster_id(spark) -> str:
    try:
        conf = spark.sparkContext.getConf()
        return conf.get("spark.databricks.clusterUsageTags.clusterId", "unknown")
    except Exception:
        return "unknown"


def _get_compute_tier(spark) -> str:
    try:
        conf = spark.sparkContext.getConf()
        tags = {
            conf.get(k, "")
            for k in (
                "spark.databricks.clusterUsageTags.clusterId",
                "spark.databricks.clusterUsageTags.orgId",
                "spark.databricks.clusterUsageTags.clusterName",
            )
        }
        tags = {t for t in tags if t}
        if "photon" in " ".join(tags).lower():
            return "photon"
        for t in tags:
            if t.startswith("job"):
                return "jobs_compute"
        return "all_purpose_compute"
    except Exception:
        return "unknown"


# ── Dataclasses for the monitor ──────────────────────────────────────────────

@dataclass
class MonitorResult:
    """Returned by the monitor decorator after each call."""

    job_name: str
    run_timestamp: str
    duration_ms: int
    estimated_cost_usd: Optional[float]
    estimated_dbu_hours: float
    cluster_id: str
    signature: str
    tables: list[str]
    finding_codes: list[str]
    severity_counts: dict
    spark_ui_url: Optional[str] = None
    spark_ui_stage_link: Optional[str] = None
    regression_vs_ms: Optional[int] = None  # positive = slower than last run
    regression_vs_cost: Optional[float] = None  # positive = costlier
    is_first_run: bool = False


@dataclass
class RunResult:
    """
    Result of a monitored function call.
    `result` is the function's own return value.
    `monitor` is the MonitorResult with all the perf metadata.
    """

    result: Any
    monitor: MonitorResult


# ── Cost estimation ───────────────────────────────────────────────────────────

DBU_RATES = {
    "jobs_compute": 0.10,
    "all_purpose_compute": 0.55,
    "sql_warehouse_serverless": 0.22,
    "photon_jobs": 0.30,
    "photon_all_purpose": 0.42,
    "ml_runtime": 0.70,
    "unknown": 0.40,
}


def _estimate_dbu_cost(duration_ms: int, spark, finding_count: int = 0) -> tuple[float, float]:
    """
    Estimate DBU cost for a monitored run.
    Returns (estimated_cost_usd, estimated_dbu_hours).
    """
    tier = _get_compute_tier(spark)
    rate = DBU_RATES.get(tier, DBU_RATES["unknown"])
    hours = duration_ms / 3_600_000
    dbu_hours = hours * rate
    cost = dbu_hours

    # Heuristic: each finding adds ~5% overhead
    if finding_count > 0:
        cost *= 1 + (finding_count * 0.05)

    return round(cost, 6), round(dbu_hours, 6)


# ── Spark UI helpers ────────────────────────────────────────────────────────────

def _get_spark_ui_url(spark) -> Optional[str]:
    """Get the Spark UI URL from the active Spark context."""
    try:
        sc = spark.sparkContext
        app_id = sc.applicationId
        web_url = sc.uiWebUrl
        if web_url and "4040" not in web_url:
            return web_url.rstrip("/")
        return None
    except Exception:
        return None


def _get_stage_metrics(spark) -> dict:
    """
    Fetch the most recent completed stage's metrics from the Spark UI REST API
    (localhost:4040 — driver-local, no outbound call).
    Returns a dict with stage_id, task_count, input_bytes, duration_ms, etc.
    """
    import urllib.request

    metrics = {}
    try:
        ui_url = _get_spark_ui_url(spark)
        if not ui_url or "4040" not in ui_url:
            return {}

        base = ui_url.split("/proxy/")[-1] if "/proxy/" in ui_url else ui_url
        api_base = f"http://localhost:4040/api"

        # Get list of stages, find the most recent completed one
        req = urllib.request.Request(f"{api_base}/v1/applications/{spark.sparkContext.applicationId}/stages")
        with urllib.request.urlopen(req, timeout=3) as resp:
            stages = json.loads(resp.read())

        completed = [s for s in stages if s.get("status") == "COMPLETED"]
        if not completed:
            return {}
        latest = max(completed, key=lambda s: s.get("submissionTime", ""))

        stage_id = latest["stageId"]
        metrics["stage_id"] = stage_id
        metrics["stage_name"] = latest.get("name", f"Stage {stage_id}")
        metrics["attempt_id"] = latest.get("attemptId", 0)

        # Get task-level summary for this stage
        req2 = urllib.request.Request(
            f"{api_base}/v1/applications/{spark.sparkContext.applicationId}/stages/{stage_id}/{metrics['attempt_id']}/taskSummary"
        )
        with urllib.request.urlopen(req2, timeout=3) as resp2:
            summary = json.loads(resp2.read())

        metrics["num_tasks"] = latest.get("numberOfTasks", 0)
        metrics["input_bytes"] = summary.get("inputBytes", 0)
        metrics["output_bytes"] = summary.get("outputBytes", 0)
        metrics["shuffle_read_bytes"] = summary.get("shuffleReadBytes", 0)
        metrics["shuffle_write_bytes"] = summary.get("shuffleWriteBytes", 0)
        metrics["duration_ms"] = summary.get("executorRunTime", 0)
        metrics["gc_time_ms"] = summary.get("gcTime", 0)
        metrics["max_task_duration_ms"] = summary.get("maxTaskDurationMs", 0)
        metrics["median_task_duration_ms"] = summary.get("medianTaskDurationMs", 0)
        return metrics

    except Exception:
        return {}


# ── History table read/write ───────────────────────────────────────────────────

def _history_table_exists(spark) -> bool:
    try:
        rows = spark.sql(f"SHOW TABLES IN {HISTORY_DB}").collect()
        names = [r.tableName() for r in rows]
        return "query_history" in names
    except Exception:
        return False


def _ensure_history_table(spark, path: str) -> None:
    if _history_table_exists(spark):
        return
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {HISTORY_DB}.query_history (
            query_signature STRING,
            job_name STRING,
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
            codes_json STRING,
            spark_ui_url STRING,
            stage_id INT,
            num_tasks INT,
            input_bytes BIGINT,
            shuffle_read_bytes BIGINT,
            shuffle_write_bytes BIGINT,
            gc_time_ms BIGINT,
            max_task_duration_ms BIGINT
        )
        USING DELTA
        LOCATION '{path}'
    """)


def _get_previous_run(spark, job_name: str) -> Optional[dict]:
    """Fetch the most recent history entry for this job_name."""
    try:
        rows = spark.sql(f"""
            SELECT * FROM {HISTORY_TABLE}
            WHERE job_name = '{job_name}'
            ORDER BY run_timestamp DESC
            LIMIT 1
        """).collect()
        return rows[0].asDict() if rows else None
    except Exception:
        return None


def _write_history_entry(
    spark,
    mon: MonitorResult,
    query_text: str,
    path: str,
    finding_codes: list[str],
    severity_counts: dict,
    stage_metrics: dict,
) -> None:
    """Append a row to the shared history Delta table."""
    _ensure_history_table(spark, path)

    insert_sql = f"""
        INSERT INTO {HISTORY_TABLE} VALUES (
            '{mon.signature}',
            '{mon.job_name}',
            '{mon.run_timestamp}',
            {repr(query_text[:5000])},
            {severity_counts.get('critical', 0)},
            {severity_counts.get('high', 0)},
            {severity_counts.get('medium', 0)},
            {severity_counts.get('info', 0)},
            {mon.estimated_cost_usd or 'NULL'},
            '{mon.cluster_id}',
            {repr(json.dumps([{"code": c} for c in finding_codes]))},
            {mon.duration_ms},
            {repr(json.dumps(mon.tables))},
            {repr(json.dumps(mon.finding_codes))},
            {'NULL' if not mon.spark_ui_url else repr(mon.spark_ui_url)},
            {stage_metrics.get('stage_id', 'NULL')},
            {stage_metrics.get('num_tasks', 'NULL')},
            {stage_metrics.get('input_bytes', 'NULL')},
            {stage_metrics.get('shuffle_read_bytes', 'NULL')},
            {stage_metrics.get('shuffle_write_bytes', 'NULL')},
            {stage_metrics.get('gc_time_ms', 'NULL')},
            {stage_metrics.get('max_task_duration_ms', 'NULL')}
        )
    """
    spark.sql(insert_sql)


# ── HTML rendering ──────────────────────────────────────────────────────────────

_SEVERITY_COLOUR = {
    "critical": "#dc2626",
    "high": "#ea580c",
    "medium": "#ca8a04",
    "info": "#16a34a",
}


def _severity_chip(sev: str, count: int) -> str:
    colour = _SEVERITY_COLOUR.get(sev.lower(), "#64748b")
    label = sev.capitalize()
    emoji = {"critical": "🔴", "high": "🟠", "medium": "🟡", "info": "🟢"}.get(sev.lower(), "ℹ️")
    if count == 0:
        return ""
    return f'<span style="background:{colour}18;color:{colour};border-radius:999px;padding:2px 8px;font-size:11px;font-weight:600;white-space:nowrap;">{emoji} {count} {label}</span>'


def _render_inline_card(
    mon: MonitorResult,
    finding_codes: list[str],
    severity_counts: dict,
    stage_metrics: dict,
) -> str:
    """Render the HTML card shown inline after a monitored call."""

    chips = "  ".join(
        c for c in (
            _severity_chip("critical", severity_counts.get("critical", 0)),
            _severity_chip("high", severity_counts.get("high", 0)),
            _severity_chip("medium", severity_counts.get("medium", 0)),
        )
        if c
    )
    if not chips:
        chips = '<span style="color:#16a34a;font-size:12px;">✅ No findings</span>'

    cost_display = f"${mon.estimated_cost_usd:.4f}" if mon.estimated_cost_usd else "—"

    # Regression indicator
    regress_note = ""
    if mon.regression_vs_ms is not None and mon.regression_vs_ms != 0:
        sign = "+" if mon.regression_vs_ms > 0 else ""
        emoji = "🐢" if mon.regression_vs_ms > 500 else "⏱️"
        regress_note = (
            f'<span style="font-size:11px;color:#ea580c;margin-left:8px;">'
            f'{emoji} {sign}{mon.regression_vs_ms}ms vs last run</span>'
           )
    elif mon.is_first_run:
        regress_note = '<span style="font-size:11px;color:#64748b;margin-left:8px;">1st run — baseline not yet set</span>'

    # Spark UI link
    ui_links = ""
    if mon.spark_ui_url:
        ui_links += f'<a href="{mon.spark_ui_url}" target="_blank" style="margin-right:10px;">🔗 Spark UI</a>'
    if mon.spark_ui_stage_link:
        ui_links += f'<a href="{mon.spark_ui_stage_link}" target="_blank" style="margin-right:10px;">🔗 View Stage {stage_metrics.get("stage_id","")}</a>'

    # Stage metrics summary
    stage_info = ""
    if stage_metrics:
        tasks = stage_metrics.get("num_tasks", "?")
        inp = stage_metrics.get("input_bytes", 0)
        inp_display = f"{inp/1024**2:.1f}MB" if inp else "—"
        max_dur = stage_metrics.get("max_task_duration_ms", 0)
        max_display = f"{max_dur}ms" if max_dur else "—"
        stage_info = (
            f'<div style="font-size:11px;color:#64748b;margin-top:4px;">'
            f'Tasks:{tasks} · Input:{inp_display} · Max task:{max_display}'
            f'</div>'
        )

    dur_display = f"{mon.duration_ms:,}ms"
    timestamp = mon.run_timestamp.replace(" ", "T")[:19] + "Z"

    return f"""
    <div style="
        border:1px solid #e2e8f0;
        border-radius:10px;
        padding:14px 16px;
        margin:10px 0;
        background:#fff;
        font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
        max-width:720px;
    ">
        <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:8px;">
            <span style="font-weight:700;font-size:14px;color:#0f172a;">🕐 {mon.job_name}</span>
            <span style="font-size:13px;color:#475569;">{dur_display}</span>
            <span style="font-size:12px;color:#94a3b8;">⚡ {cost_display}</span>
            {chips}
            {regress_note}
        </div>
        <div style="font-size:12px;color:#64748b;">{timestamp} · Cluster:{mon.cluster_id}</div>
        {stage_info}
        {"<div style='margin-top:8px;'>" + ui_links + "</div>" if ui_links else ""}
    </div>
    """


# ── Main decorator ─────────────────────────────────────────────────────────────

def monitor_performance(
    job_name: str,
    spark=None,
    enabled: Optional[bool] = None,
    record: bool = True,
    display_card: bool = True,
    tags: Optional[dict] = None,
):
    """
    Decorator that instruments a function with performance monitoring.

    Args:
        job_name:     Identifier for this job (used as the primary key in history)
        spark:        SparkSession instance (required for history writes + cost estimate)
                      If omitted, reads from globals()['spark'] at call time.
        enabled:      If False, runs the function without instrumentation.
                      Defaults to None (auto — reads spark_query_analyzer.monitor_enabled conf).
        record:       Write a row to the history Delta table after each run (default True).
        display_card: Show an inline HTML card after each run (default True).
        tags:         Optional dict of arbitrary key-value metadata to store with the run.

    Returns a RunResult(result, monitor) if display_card=False,
    otherwise just the function's own return value (card is displayed as a side-effect).

    Example:
        @monitor_performance(job_name="daily_agg", spark=spark)
        def daily_agg():
            return spark.sql("SELECT SUM(amount) ...").collect()

        daily_agg()   # card appears inline, result returned
    """
    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        def wrapper(*args, **kwargs):
            # ── Resolve spark session ────────────────────────────────────────────
            _spark = spark
            if _spark is None:
                _spark = globals().get("spark")
            if _spark is None:
                raise RuntimeError(
                    "monitor_performance: spark session not available. "
                    "Pass spark=spark to the decorator or ensure 'spark' is in notebook globals."
                )

            # ── Opt-in check ───────────────────────────────────────────────────────
            do_record = record
            if enabled is None:
                try:
                    conf = _spark.sparkContext.getConf()
                    do_record = conf.get("spark_query_analyzer.monitor_enabled", "true").lower() == "true"
                except Exception:
                    do_record = True  # default on if conf unreadable

            # ── Read history path ────────────────────────────────────────────────
            try:
                conf = _spark.sparkContext.getConf()
                history_path = conf.get(
                    "spark_query_analyzer.history_path",
                    f"/tmp/{HISTORY_DB.replace('.', '/')}/query_history",
                )
            except Exception:
                history_path = f"/tmp/{HISTORY_DB.replace('.', '/')}/query_history"

            # ── Get previous run for regression ──────────────────────────────────
            prev_run = _get_previous_run(_spark, job_name) if do_record else None

            # ── Execute ───────────────────────────────────────────────────────────
            start = time.perf_counter()
            result = fn(*args, **kwargs)
            duration_ms = int((time.perf_counter() - start) * 1000)

            mon_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

            # ── Cost estimate ──────────────────────────────────────────────────────
            est_cost, est_dbu_hours = _estimate_dbu_cost(duration_ms, _spark)

            # ── Spark UI metrics ──────────────────────────────────────────────────
            spark_ui_url = _get_spark_ui_url(_spark)
            stage_metrics = _get_stage_metrics(_spark) if spark_ui_url else {}

            # ── Build monitor result ──────────────────────────────────────────────
            mon = MonitorResult(
                job_name=job_name,
                run_timestamp=mon_timestamp,
                duration_ms=duration_ms,
                estimated_cost_usd=est_cost,
                estimated_dbu_hours=est_dbu_hours,
                cluster_id=_get_cluster_id(_spark),
                signature=_compute_signature(job_name),  # job_name is the signature key
                tables=[],  # not easily available for arbitrary Python functions
                finding_codes=[],  # filled below if analyzer result available
                severity_counts={"critical": 0, "high": 0, "medium": 0, "info": 0},
                spark_ui_url=spark_ui_url,
                spark_ui_stage_link=None,
                regression_vs_ms=None,
                regression_vs_cost=None,
                is_first_run=(prev_run is None),
            )

            if prev_run:
                prev_dur = prev_run.get("duration_ms", 0) or 0
                mon.regression_vs_ms = duration_ms - prev_dur
                if prev_run.get("estimated_dbu_cost"):
                    mon.regression_vs_cost = est_cost - float(prev_run["estimated_dbu_cost"])

            if spark_ui_url and stage_metrics.get("stage_id"):
                stage_id = stage_metrics["stage_id"]
                # Try driver-proxy URL format
                mon.spark_ui_stage_link = f"{spark_ui_url}/?o={_spark.sparkContext.applicationId}/spark_ui/{stage_id}"
                # Fallback to direct Spark UI stage URL
                mon.spark_ui_stage_link = (
                    f"{spark_ui_url}/#SparkStage/0/{stage_metrics['attempt_id']}/{stage_id}"
                )

            # ── Auto-detect findings from result if it's an analyzer result ─────────
            finding_codes: list[str] = []
            severity_counts = {"critical": 0, "high": 0, "medium": 0, "info": 0}
            if display_card:
                # Try to detect if the function returned an AnalysisResult-like object
                if hasattr(result, "findings"):
                    for f in result.findings:
                        sev = f.severity.lower() if hasattr(f, "severity") else "info"
                        if sev in severity_counts:
                            severity_counts[sev] += 1
                        if hasattr(f, "code"):
                            finding_codes.append(f.code)

            # ── Write to history table ────────────────────────────────────────────
            if do_record and record:
                try:
                    _write_history_entry(
                        _spark, mon,
                        query_text=json.dumps({"tags": tags or {}, "job_name": job_name}),
                        path=history_path,
                        finding_codes=finding_codes,
                        severity_counts=severity_counts,
                        stage_metrics=stage_metrics,
                    )
                except Exception as e:
                    # Never fail the wrapped function due to history write errors
                    pass

            # ── Display inline card ───────────────────────────────────────────────
            if display_card:
                from IPython.display import HTML, display
                display(HTML(_render_inline_card(mon, finding_codes, severity_counts, stage_metrics)))
                return result
            else:
                return RunResult(result=result, monitor=mon)

        return wrapper
    return decorator
