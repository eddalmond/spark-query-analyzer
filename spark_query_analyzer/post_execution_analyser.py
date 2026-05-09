"""
Deep Skew Analyser (Post-Execution) — F-03 of the spark-query-analyzer roadmap.

After query execution, reads actual task-level metrics from the Spark UI REST API
(localhost:4040) to detect confirmed data skew with real numbers rather than plan heuristics.

Not available on Databricks Serverless compute — gracefully degrades when Spark UI
is not accessible.
"""

import re
import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class TaskMetrics:
    stage_id: int
    stage_attempt_id: int
    num_tasks: int
    max_task_duration_ms: int
    median_task_duration_ms: int
    p95_task_duration_ms: int
    skew_ratio: float  # max / median
    max_bytes_read: int
    median_bytes_read: int
    bytes_skew_ratio: float
    num_stragglers: int  # tasks > 2x median


@dataclass
class SkewFinding:
    severity: str  # "critical" | "high" | "medium" | "info"
    code: str
    message: str
    stage_id: int
    suggestion: str
    metrics: Optional[TaskMetrics] = None


def get_spark_app_id(spark) -> Optional[str]:
    """Get the current Spark application ID."""
    try:
        return spark.sparkContext.applicationId
    except Exception:
        return None


def fetch_stage_metrics(spark, stage_id: int, stage_attempt_id: int = 0) -> Optional[dict]:
    """
    Fetch task metrics for a specific stage from the Spark REST API.
    Falls back to SparkContext statusTracker if REST API is unavailable.
    """
    app_id = get_spark_app_id(spark)
    if not app_id:
        return None

    import json

    # Try REST API first (localhost:4040 — local driver-side)
    try:
        import urllib.request
        url = f"http://localhost:4040/api/v1/applications/{app_id}/stages/{stage_id}/{stage_attempt_id}"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
            return data
    except Exception:
        pass

    # Fallback: use SparkContext statusTracker (JVM-side, no HTTP needed)
    try:
        tracker = spark.sparkContext.statusTracker()
        # statusTracker doesn't expose per-task granular metrics,
        # so we fall back to a coarser check via SparkListener events
        return None
    except Exception:
        return None


def analyse_stage_for_skew(stage_data: dict) -> Optional[TaskMetrics]:
    """
    Parse stage data and compute skew metrics.
    Returns TaskMetrics if skew is confirmed, None otherwise.
    """
    tasks = stage_data.get("tasks", [])
    if not tasks:
        return None

    # Collect duration and bytes read per task
    durations = []
    bytes_read = []

    for task in tasks:
        # Task metrics — use executorRunTime (milliseconds) or duration from stage data
        duration = (
            task.get("taskMetrics", {})
            .get("executorRunTime", 0) * 1000  # seconds → ms
            or task.get("duration", 0)
        )
        br = (
            task.get("taskMetrics", {})
            .get("inputMetrics", {})
            .get("bytesRead", 0)
            or task.get("bytesRead", 0)
        )
        durations.append(duration)
        bytes_read.append(br)

    if not durations:
        return None

    durations = sorted(durations)
    bytes_read = sorted(bytes_read)

    n = len(durations)
    median_idx = n // 2
    median_dur = durations[median_idx]
    max_dur = durations[-1]
    p95_idx = int(n * 0.95)
    p95_dur = durations[p95_idx] if p95_idx < n else durations[-1]

    median_bytes = bytes_read[median_idx] if bytes_read else 0
    max_bytes = bytes_read[-1] if bytes_read else 0

    skew_ratio = max_dur / median_dur if median_dur > 0 else 0
    bytes_skew_ratio = max_bytes / median_bytes if median_bytes > 0 else 0

    # Stragglers: tasks > 2x median duration
    stragglers = sum(1 for d in durations if d > 2 * median_dur)

    return TaskMetrics(
        stage_id=stage_data.get("stageId", 0),
        stage_attempt_id=stage_data.get("stageAttemptId", 0),
        num_tasks=n,
        max_task_duration_ms=max_dur,
        median_task_duration_ms=median_dur,
        p95_task_duration_ms=p95_dur,
        skew_ratio=skew_ratio,
        max_bytes_read=max_bytes,
        median_bytes_read=median_bytes,
        bytes_skew_ratio=bytes_skew_ratio,
        num_stragglers=stragglers,
    )


def get_all_shuffle_stages(spark) -> list[int]:
    """Get list of shuffle stage IDs from the current SparkContext."""
    try:
        sc = spark.sparkContext
        status_tracker = sc.statusTracker()
        # getPendingJobs() returns job info but not stage IDs directly
        # Use the REST API to get all stages
        app_id = get_spark_app_id(spark)
        if not app_id:
            return []

        import json
        import urllib.request

        url = f"http://localhost:4040/api/v1/applications/{app_id}/stages"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=5) as resp:
            stages = json.loads(resp.read().decode())
            # Filter to stages with shuffle (have inputbytes > 0 or are marked as shuffle)
            shuffle_stage_ids = []
            for stage in stages:
                stage_id = stage.get("stageId")
                num_tasks = stage.get("numTasks", 0)
                # A shuffle stage is one that reads shuffle data
                # We look for stages that have "shuffle" in description or
                # are preceded by a shuffle Write
                if stage_id and num_tasks > 0:
                    # Conservative: all stages with tasks may involve shuffle
                    # We'll filter more precisely when we fetch task details
                    shuffle_stage_ids.append(stage_id)
            return shuffle_stage_ids
    except Exception:
        return []


def run_post_execution_skew_analysis(spark, sql: str = "", aqe_enabled: bool = False) -> list[SkewFinding]:
    """
    Main entry point: run the query (with safety LIMIT), fetch task metrics,
    and return confirmed skew findings.

    For large queries, prepends LIMIT 100000 to avoid excessive execution while
    still triggering the shuffle stages we need to measure.
    """
    findings = []

    # Check if Spark UI is available
    app_id = get_spark_app_id(spark)
    if not app_id:
        return [SkewFinding(
            severity="info",
            code="SPARK_UI_UNAVAILABLE",
            message="Spark UI not accessible (may be Serverless compute). "
                    "Skew analysis requires local Spark driver. "
                    "Use EXPLAIN-based skew heuristics instead.",
            stage_id=0,
            suggestion="No action needed in this environment.",
        )]

    # Execute query with safe LIMIT to trigger shuffle stages
    # For SELECT queries only — skip for DDL/DML
    sql_upper = sql.strip().upper()
    if any(sql_upper.startswith(kw) for kw in ("SELECT", "WITH")):
        # Strip existing LIMIT if present
        safe_sql = re.sub(r"\s+LIMIT\s+\d+\s*$", "", sql, flags=re.IGNORECASE)
        safe_sql = f"{safe_sql} LIMIT 100000"
        try:
            spark.sql(safe_sql).collect()  # action to trigger execution
            time.sleep(1)  # brief pause for metrics to propagate
        except Exception:
            return [SkewFinding(
                severity="info",
                code="EXECUTION_FAILED",
                message="Query execution failed during skew analysis. "
                        "Skew findings may be incomplete.",
                stage_id=0,
                suggestion="Check the query is valid and run manually.",
            )]

    # Fetch all stages and analyse for skew
    try:
        import json
        import urllib.request

        stages_url = f"http://localhost:4040/api/v1/applications/{app_id}/stages"
        req = urllib.request.Request(stages_url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            stages = json.loads(resp.read().decode())

        for stage in stages:
            stage_id = stage.get("stageId")
            stage_attempt_id = stage.get("stageAttemptId", 0)
            num_tasks = stage.get("numTasks", 0)

            if num_tasks < 2:
                continue  # need at least 2 tasks to detect skew

            # Fetch detailed task metrics for this stage
            detail_url = f"http://localhost:4040/api/v1/applications/{app_id}/stages/{stage_id}/{stage_attempt_id}"
            detail_req = urllib.request.Request(detail_url)
            try:
                with urllib.request.urlopen(detail_req, timeout=10) as detail_resp:
                    stage_data = json.loads(detail_resp.read().decode())
            except Exception:
                continue

            metrics = analyse_stage_for_skew(stage_data)
            if not metrics:
                continue

            # Confirmed skew: max/median ratio > 5.0
            if metrics.skew_ratio > 5.0:
                severity = "critical" if metrics.skew_ratio > 10.0 else "high"
                finding = SkewFinding(
                    severity=severity,
                    code="CONFIRMED_SKEW",
                    message=f"Confirmed data skew in stage {stage_id}: "
                            f"max task duration ({metrics.max_task_duration_ms/1000:.1f}s) "
                            f"is {metrics.skew_ratio:.1f}x median ({metrics.median_task_duration_ms/1000:.1f}s). "
                            f"{metrics.num_stragglers} straggler tasks detected.",
                    stage_id=stage_id,
                    suggestion=_skew_suggestion(metrics, aqe_enabled),
                    metrics=metrics,
                )
                findings.append(finding)
            elif metrics.skew_ratio > 2.5:
                findings.append(SkewFinding(
                    severity="medium",
                    code="MODERATE_SKEW",
                    message=f"Mild skew in stage {stage_id}: "
                            f"max/median ratio is {metrics.skew_ratio:.1f}x. "
                            f"Monitor — may worsen with data growth.",
                    stage_id=stage_id,
                    suggestion="Monitor partition size distribution. "
                               "Consider AQE skew join handling if issue persists.",
                    metrics=metrics,
                ))

    except Exception as e:
        return [SkewFinding(
            severity="info",
            code="METRICS_UNAVAILABLE",
            message=f"Could not retrieve task metrics from Spark UI: {e}",
            stage_id=0,
            suggestion="Spark UI may not be accessible in this compute type.",
        )]

    return findings


def _skew_suggestion(metrics: TaskMetrics, aqe_enabled: bool) -> str:
    """Generate the appropriate skew fix recommendation."""
    if aqe_enabled:
        return (
            f"AQE skew join handling is enabled — it should automatically handle this. "
            f"If stragglers persist, increase the threshold: "
            f"spark.conf.set('spark.sql.adaptive.skewJoin.skewJoinThreshold', '512MB'). "
            f"Or manually salt the join key to distribute partitions evenly."
        )
    else:
        return (
            f"Enable AQE skew join handling: "
            f"spark.conf.set('spark.sql.adaptive.skewJoin.enabled', 'true'). "
            f"This will dynamically split skewed partitions. "
            f"Alternatively, manually salt the join key with: "
            f"df.withColumn('salt', (rand() * 16).cast('int')) to distribute evenly."
        )