"""
DBU Cost Estimator — F-05 of the spark-query-analyzer roadmap.

Estimates approximate Databricks Unit (DBU) cost of a query based on:
- Cluster configuration from SparkConf
- Data volumes from the physical plan
- Databricks pricing tiers (2025 list prices)
"""

from dataclasses import dataclass


@dataclass
class CostEstimate:
    tier: str
    dbu_rate: float  # $/DBU-hour
    estimated_runtime_seconds: float
    num_cores: int
    estimated_dbu_hours: float
    estimated_cost_usd: float
    confidence: str  # "high" | "medium" | "low"
    breakdown: str
    disclaimer: str = (
        'Pre-execution estimate based on plan heuristics. '
        'Actual cost depends on data spill, garbage collection, and cluster utilisation. '
        'Use Databricks Cost Analysis dashboard for post-execution actuals.'
    )


# 2025 Databricks DBU list prices (default rates, USD per DBU-hour)
DBU_RATES = {
    'jobs_compute': 0.10,
    'all_purpose_compute': 0.55,
    'sql_warehouse_serverless': 0.22,
    'sql_warehouse_pro': 0.30,
    'photon_jobs': 0.30,
    'photon_all_purpose': 0.42,
    'ml_runtime': 0.70,
    'unknown': 0.40,
}

# Tunable bytes-per-row estimate for shuffle cost modelling
BYTES_PER_ROW_DEFAULT = 100


def detect_compute_tier(spark) -> tuple[str, float, bool]:
    """
    Detect the compute tier from SparkConf and Databricks cluster tags.
    Returns (tier_name, dbu_rate_per_hour, is_photon_enabled).
    """
    conf = spark.sparkContext.getConf()
    get = conf.get

    photon_enabled = get('spark.databricks.photon.enabled', 'false') == 'true'
    cluster_profile = get('spark.databricks.cluster.profile', 'unknown')

    # Check for SQL warehouse indicators
    cluster_tags_raw = get('spark.databricks.clusterUsageTags.clusterAllTags', '{}')
    is_sql_warehouse = False
    cluster_name = ''

    try:
        import json

        tags = json.loads(cluster_tags_raw)
        cluster_name = tags.get('clusterName', '') or tags.get('cluster_name', '')
        is_sql_warehouse = bool(tags.get('sqlWarehouseId') or tags.get('sql_warehouse_id'))
        # Also check for photon in sql warehouse
        if is_sql_warehouse and not photon_enabled:
            photon_enabled = bool(tags.get('photonEnabled'))
    except Exception:
        pass

    # Determine tier
    if is_sql_warehouse:
        tier = 'sql_warehouse_serverless' if 'serverless' in cluster_name.lower() else 'sql_warehouse_pro'
        rate = DBU_RATES.get(tier, DBU_RATES['sql_warehouse_pro'])
    elif photon_enabled:
        tier = 'photon_all_purpose'
        rate = DBU_RATES['photon_all_purpose']
    elif cluster_profile in DBU_RATES:
        tier = cluster_profile
        rate = DBU_RATES[cluster_profile]
    else:
        tier = 'all_purpose_compute'
        rate = DBU_RATES['all_purpose_compute']

    return tier, rate, photon_enabled


def estimate_shuffle_bytes_from_plan(plan_text: str) -> int:
    """
    Estimate total shuffle bytes from the physical plan.
    Uses Exchange node count + partition counts as a proxy.
    This is crude but gives a directional runtime estimate.
    """
    import re

    # Count Exchange nodes and sum their estimated output sizes
    # EXPLAIN FORMATTED doesn't give byte estimates — we use a proxy:
    # number of exchanges × estimated rows per exchange × bytes_per_row

    exchanges = re.findall(r'Exchange\s+\([^)]*\)', plan_text, re.IGNORECASE)
    num_exchanges = len(exchanges)

    if num_exchanges == 0:
        return 0

    # Estimate row counts from scan nodes (very rough heuristic)
    scan_rows = re.findall(r'num_rows=(\d+)', plan_text, re.IGNORECASE)
    if scan_rows:
        total_rows = sum(int(r) for r in scan_rows[:num_exchanges])
    else:
        # Fall back to partition counts as proxy for row count
        partition_counts = re.findall(r'partition[=\s]+(\d+)', plan_text, re.IGNORECASE)
        total_rows = sum(int(p) for p in partition_counts[:num_exchanges]) if partition_counts else 100_000

    estimated_bytes = total_rows * BYTES_PER_ROW_DEFAULT * num_exchanges
    return estimated_bytes


def estimate_runtime_seconds(shuffle_bytes: int, num_cores: int) -> float:
    """
    Model runtime from shuffle bytes and cluster parallelism.
    Shuffle bytes / (cluster throughput bytes/sec) ≈ seconds.

    Rough throughput assumption: 50MB/s per core for shuffle I/O
    """
    if shuffle_bytes == 0:
        return 1.0  # minimal runtime for plan-only queries

    bytes_per_sec_per_core = 50 * 1024 * 1024  # 50MB/s
    throughput = bytes_per_sec_per_core * max(num_cores, 1)
    runtime = shuffle_bytes / throughput
    return max(runtime, 0.5)  # minimum 0.5s


def build_cost_estimate(spark, plan_text: str = '') -> CostEstimate:
    """
    Build a cost estimate from cluster config + plan shuffle volume.
    """
    conf = spark.sparkContext.getConf()
    get = conf.get

    # Core count
    default_parallelism = int(get('spark.default.parallelism', '0') or '0')
    executor_cores = int(get('spark.executor.cores', '0') or '0')
    num_executors = int(get('spark.executor.instances', '0') or '0')

    # Determine actual parallelism
    if default_parallelism > 0:
        num_cores = default_parallelism
    elif executor_cores > 0 and num_executors > 0:
        num_cores = executor_cores * num_executors
    else:
        # Fall back to Spark's default parallelism setting
        num_cores = int(get('spark.sql.shuffle.partitions', '200'))

    tier, dbu_rate, photon = detect_compute_tier(spark)

    # Estimate shuffle bytes from plan
    shuffle_bytes = estimate_shuffle_bytes_from_plan(plan_text)

    # Model runtime
    runtime_seconds = estimate_runtime_seconds(shuffle_bytes, num_cores)

    # Add base overhead for query compilation + plan parsing (non-zero even on fast queries)
    overhead_seconds = 3.0  # Spark driver overhead
    total_runtime = runtime_seconds + overhead_seconds

    # DBU calculation
    # DBU-hours = (runtime_seconds / 3600) * num_cores * dbu_rate (simplified)
    # But that's not quite right — DBU is per executor hour, not per core hour
    # More accurate: DBU-hours = (runtime_seconds / 3600) * dbu_rate * num_executors
    # If we only have core count, use: DBU-hours = (runtime_seconds / 3600) * dbu_rate * num_cores

    if num_executors > 0:
        dbu_hours = (total_runtime / 3600.0) * dbu_rate * num_executors
    else:
        # Rough proxy: assume ~4 cores per executor unit
        dbu_hours = (total_runtime / 3600.0) * dbu_rate * max(num_cores, 4) / 4

    estimated_cost = dbu_hours
    confidence = 'high' if shuffle_bytes > 0 else 'medium'

    breakdown_parts = [
        f'tier: {tier}',
        f'rate: ${dbu_rate:.2f}/DBU-hr',
        f'cores/parallelism: {num_cores}',
        f'estimated shuffle: {shuffle_bytes / (1024**2):.1f}MB',
        f'runtime: {total_runtime:.1f}s',
        f'DBU-hours: {dbu_hours:.4f}',
    ]

    return CostEstimate(
        tier=tier,
        dbu_rate=dbu_rate,
        estimated_runtime_seconds=total_runtime,
        num_cores=num_cores,
        estimated_dbu_hours=dbu_hours,
        estimated_cost_usd=estimated_cost,
        confidence=confidence,
        breakdown=' | '.join(breakdown_parts),
    )


def format_cost_badge(estimate: CostEstimate) -> str:
    """Format cost estimate as a colour-coded badge string."""
    cost = estimate.estimated_cost_usd
    estimate.tier.replace('_', ' ').replace('sql warehouse', 'SQL').title()

    if cost < 0.01:
        cost_str = '<$0.01'
    else:
        cost_str = f'${cost:.2f}'

    # Colour by cost tier
    if cost < 0.05:
        colour = '#16a34a'  # green — cheap
    elif cost < 0.25:
        colour = '#ca8a04'  # yellow — moderate
    else:
        colour = '#dc2626'  # red — expensive

    badge = (
        f'<span style="'
        f'background:{colour}22;'
        f'color:{colour};'
        f'border:1px solid {colour};'
        f'border-radius:6px;'
        f'padding:2px 8px;'
        f'font-size:12px;'
        f'font-weight:600;'
        f'font-family:-apple-system,sans-serif;'
        f'">'
        f'\u26a1 Est. {cost_str} ({estimate.estimated_dbu_hours:.3f} DBU-hr, {estimate.num_cores} cores, {estimate.tier})'
        f'</span>'
    )
    return badge
