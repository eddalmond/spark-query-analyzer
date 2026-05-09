"""
F-12 · Cluster Configuration Advisor

Based on query plan characteristics and detected findings, recommend the
optimal cluster type, size, and configuration for running this query.

Zero external dependencies — reads only from SparkConf and plan text.
"""

from dataclasses import dataclass
from typing import Optional


# --------------------------------------------------------------------------------
# Dataclasses
# --------------------------------------------------------------------------------

@dataclass
class ClusterRecommendation:
    """A single cluster configuration recommendation."""
    severity: str           # "critical" | "high" | "medium" | "info"
    code: str               # short identifier e.g. "PHOTON_RECOMMENDED"
    heading: str            # short bold heading
    message: str            # plain-English explanation
    spark_conf: Optional[str] = None   # copy-ready spark.conf.set(...) block
    cluster_policy: Optional[str] = None  # cluster policy JSON snippet
    detail: Optional[str] = None


# --------------------------------------------------------------------------------
# Workload classifier
# --------------------------------------------------------------------------------

@dataclass
class WorkloadProfile:
    """Inferred workload characteristics from the plan."""
    num_scans: int = 0
    num_exchanges: int = 0
    num_joins: int = 0
    num_aggregates: int = 0
    has_python_udf: bool = False
    has_broadcast: bool = False
    has_large_shuffle: bool = False
    has_streaming: bool = False
    estimated_shuffle_mb: float = 0.0

    @property
    def is_scan_heavy(self) -> bool:
        return self.num_scans > 0 and self.num_exchanges <= 1

    @property
    def is_shuffle_heavy(self) -> bool:
        return self.num_exchanges > 2 or self.has_large_shuffle

    @property
    def is_join_heavy(self) -> bool:
        return self.num_joins > 1

    @property
    def workload_type(self) -> str:
        if self.has_streaming:
            return "streaming"
        if self.has_python_udf:
            return "python"
        if self.is_shuffle_heavy:
            return "shuffle_heavy"
        if self.is_join_heavy:
            return "join_heavy"
        if self.is_scan_heavy:
            return "scan_heavy"
        return "general"


def classify_workload(plan_text: str, findings: list = None, python_findings: list = None) -> WorkloadProfile:
    """Infer workload characteristics from plan text and findings."""
    import re

    profile = WorkloadProfile()

    lines = plan_text.lower()

    # Node counts from plan tree
    profile.num_scans = len(re.findall(r"scan\s", lines))
    profile.num_exchanges = len(re.findall(r"exchange\s", lines))
    profile.num_joins = len(re.findall(r"\bjoin\b", lines))
    profile.num_aggregates = len(re.findall(r"aggregate", lines))

    # Presence checks
    profile.has_broadcast = "broadcastexchange" in lines
    profile.has_streaming = "streaming" in lines or "stream" in lines

    # Large shuffle heuristic: Exchange with high partition count
    partition_counts = re.findall(r"partition[=\s]*(\d+)", plan_text, re.IGNORECASE)
    if partition_counts:
        max_partitions = max(int(p) for p in partition_counts)
        profile.has_large_shuffle = max_partitions > 500
        # Estimate shuffle MB: num_exchanges × max_partitions × 50MB
        profile.estimated_shuffle_mb = profile.num_exchanges * max_partitions * 50 / 1024

    # Python UDF check from findings
    if python_findings:
        profile.has_python_udf = any(
            "UDF" in getattr(pf, "code", "") or "udf" in getattr(pf, "message", "").lower()
            for pf in python_findings
        )

    # Also check from plan text
    if not profile.has_python_udf:
        profile.has_python_udf = "python" in lines or "udf" in lines

    return profile


# --------------------------------------------------------------------------------
# Advisor logic
# --------------------------------------------------------------------------------

def recommend_cluster(
    spark,
    plan_text: str = "",
    findings: list = None,
    python_findings: list = None,
    stats_findings: list = None,
) -> list[ClusterRecommendation]:
    """
    Return a list of ClusterRecommendations based on plan + cluster profile.
    """
    findings = findings or []
    python_findings = python_findings or []
    stats_findings = stats_findings or []

    recommendations = []
    profile = classify_workload(plan_text, findings, python_findings)

    conf = spark.sparkContext.getConf()
    get = conf.get

    current_cores = int(get("spark.executor.cores", "0") or "0") or 4
    current_executors = int(get("spark.executor.instances", "0") or "0") or 2
    current_memory_mb = int(get("spark.executor.memory", "0") or "0") or (8 * 1024)
    photon_enabled = get("spark.databricks.photon.enabled", "false") == "true"
    current_shuffle_partitions = int(get("spark.sql.shuffle.partitions", "200"))

    # ---- 1. Photon recommendation ----
    if profile.has_python_udf and not photon_enabled:
        recommendations.append(ClusterRecommendation(
            severity="high",
            code="PHOTON_RECOMMENDED",
            heading="Enable Photon for Python UDF acceleration",
            message=(
                "This query contains Python UDFs which run in the Python interpreter, "
                "bypassing Photon. Photon cannot accelerate Python UDFs directly, but "
                "rewriting them as native Spark SQL functions or Pandas UDFs would bring "
                "them into Photon's execution path — typically 3–10× faster."
            ),
            spark_conf=None,
            detail="Rewrite Python UDFs to Spark SQL / Pandas UDFs + enable Photon",
        ))
    elif not profile.has_python_udf and not photon_enabled and profile.is_shuffle_heavy:
        recommendations.append(ClusterRecommendation(
            severity="medium",
            code="PHOTON_OPTIONAL",
            heading="Consider Photon for shuffle-heavy workloads",
            message=(
                f"Photon significantly accelerates sort-merge joins and aggregations. "
                f"Since this query has {profile.num_exchanges} exchanges and is shuffle-heavy, "
                f"enabling Photon could provide 2–5× speedup on the shuffle-heavy stages."
            ),
            spark_conf='spark.conf.set("spark.databricks.photon.enabled", "true")',
        ))

    # ---- 2. SQL Warehouse vs Jobs cluster ----
    if profile.is_scan_heavy and not profile.is_shuffle_heavy and not profile.has_python_udf and not profile.has_streaming:
        recommendations.append(ClusterRecommendation(
            severity="info",
            code="SQL_WAREHOUSE_RECOMMENDED",
            heading="SQL Warehouse may be sufficient for this workload",
            message=(
                "This query is read-heavy with minimal shuffling — a Serverless SQL Warehouse "
                "would be cost-effective and provides Delta caching for repeated scans."
            ),
            spark_conf='spark.conf.set("spark.databricks.io.cache.enabled", "true")',
        ))
    elif profile.is_shuffle_heavy or profile.has_python_udf:
        recommendations.append(ClusterRecommendation(
            severity="medium",
            code="JOBS_CLUSTER_RECOMMENDED",
            heading="Use a Jobs cluster for shuffle-heavy or Python workloads",
            message=(
                f"Shuffle-heavy and Python workloads perform better on Jobs clusters "
                f"with memory-optimised instances (r-family on AWS, Edsv5 on Azure). "
                f"SQL Warehouses are not optimised for large shuffles."
            ),
        ))

    # ---- 3. Shuffle partition tuning ----
    if current_shuffle_partitions == 200 and profile.estimated_shuffle_mb > 1000:
        recommended_partitions = min(800, current_cores * 4)
        recommendations.append(ClusterRecommendation(
            severity="medium",
            code="SHUFFLE_PARTITION_TUNE",
            heading=f"Default shuffle partitions may cause small tasks",
            message=(
                f"Default shuffle partitions (200) produces tasks that are too small "
                f"for this query's shuffle volume ({profile.estimated_shuffle_mb:.0f}MB). "
                f"Increase partitions to keep task granularity healthy."
            ),
            spark_conf=f'spark.conf.set("spark.sql.shuffle.partitions", "{recommended_partitions}")',
            detail=f"Current: 200 → Recommended: {recommended_partitions} (4× {current_cores} cores)",
        ))

    # ---- 4. Broadcast threshold for join-heavy queries ----
    if profile.is_join_heavy and not profile.has_broadcast:
        recommendations.append(ClusterRecommendation(
            severity="medium",
            code="BROADCAST_THRESHOLD",
            heading="Increase broadcast threshold for join-heavy queries",
            message=(
                "Multiple joins detected and no broadcasts in the plan. "
                "Increasing the broadcast threshold lets AQE and Spark decide "
                "when to broadcast small tables automatically."
            ),
            spark_conf='spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "104857600")',
            detail="Raise from default 10MB to 100MB",
        ))

    # ---- 5. Memory estimation ----
    if profile.estimated_shuffle_mb > 0:
        estimated_executor_memory_gb = (profile.estimated_shuffle_mb * 3) / 1024
        recommendations.append(ClusterRecommendation(
            severity="info",
            code="EXECUTOR_MEMORY",
            heading="Executor memory sizing",
            message=(
                f"Estimated shuffle volume: ~{profile.estimated_shuffle_mb:.0f}MB. "
                f"Minimum executor memory needed: ~{estimated_executor_memory_gb:.1f}GB per executor. "
                f"Current setting: {current_memory_mb // 1024}GB."
            ),
            detail=f"Shuffle estimate: {profile.estimated_shuffle_mb:.0f}MB | "
                   f"Current executor memory: {current_memory_mb // 1024}GB | "
                   f"Estimated min: {estimated_executor_memory_gb:.1f}GB",
        ))

    # ---- 6. Streaming recommendations ----
    if profile.has_streaming:
        recommendations.append(ClusterRecommendation(
            severity="high",
            code="STREAMING_CLUSTER",
            heading="Streaming requires dedicated cluster configuration",
            message=(
                "Streaming queries benefit from Photon and need stable, long-running clusters. "
                "Configure for continuous workloads: enable Photon, set "
                "spark.sql.shuffle.partitions to a moderate value (e.g. 8), and "
                "ensure spark.sql.streaming.stateStore.stateSchemaCheck is enabled."
            ),
            spark_conf=(
                'spark.conf.set("spark.databricks.photon.enabled", "true")\n'
                'spark.conf.set("spark.sql.shuffle.partitions", "8")\n'
                'spark.conf.set("spark.sql.streaming.stateStore.stateSchemaCheck", "true")'
            ),
        ))

    # ---- 7. Stats missing → ANALYZE recommended ----
    if any(f.code in ("MISSING_STATS", "STALE_STATS") for f in (stats_findings or [])):
        recommendations.append(ClusterRecommendation(
            severity="info",
            code="ANALYZE_TABLE",
            heading="Run ANALYZE TABLE to enable cost-based optimisation",
            message=(
                "Table statistics are missing — the Catalyst optimizer is working blind. "
                "Running ANALYZE TABLE populates statistics so Spark can make better "
                "join selection, broadcast, and partition pruning decisions."
            ),
            detail="Run ANALYZE TABLE for all tables in this query to improve plan quality",
        ))

    return recommendations


# --------------------------------------------------------------------------------
# Formatting
# --------------------------------------------------------------------------------

def format_cluster_advisor(recommendations: list[ClusterRecommendation]) -> str:
    """Render recommendations as HTML sections."""
    if not recommendations:
        return ""

    sections_html = ""
    for rec in recommendations:
        conf_html = ""
        if rec.spark_conf:
            escaped_conf = rec.spark_conf.replace("\n", "<br>").replace(" ", "&nbsp;")
            conf_html = (
                f'<div class="sqa-suggestion" style="background:#1e293b;color:#f8fafc;margin-top:4px;font-size:11px;">'
                f'<strong style="color:#60a5fa;">&#x2699; Config:</strong><br>'
                f'<code style="font-size:10px;">{escaped_conf}</code></div>'
            )
        detail_html = f'<div class="sqa-node">{rec.detail}</div>' if rec.detail else ""

        sections_html += (
            f'<div class="sqa-finding" style="border-left-color:#7c3aed;">'
            f'<div class="sqa-severity" style="color:#7c3aed;">&#x1F5FA; {rec.heading}</div>'
            f'<div class="sqa-message">{rec.message}</div>'
            f'{detail_html}{conf_html}'
            f'</div>'
        )

    header = (
        '<div style="padding:8px 14px;background:#f8fafc;border-top:1px solid #e2e8f0;'
        'font-size:12px;font-weight:600;color:#0f172a;">'
        '&#x1F5FA; Cluster Configuration Advisor (F-12)</div>'
    )
    return '<div style="border-top:2px solid #e2e8f0;margin-top:4px;">' + header + sections_html + '</div>'