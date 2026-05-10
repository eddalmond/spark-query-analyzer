"""
AQE Configuration Checker & Recommender — F-02 of the spark-query-analyzer roadmap.

Reads current Spark AQE configuration, cross-references with the physical plan to determine
which AQE sub-features would change this specific query's plan, and emits copy-ready config
recommendations with severity-coded findings.
"""

from dataclasses import dataclass


@dataclass
class AQEConfig:
    adaptive_enabled: bool = False
    coalesce_partitions_enabled: bool = False
    skew_join_enabled: bool = False
    coalesce_shuffle_partitions_enabled: bool = False
    broadcast_wait_time_bytes: int = 10 * 1024 * 1024  # default 10MB
    skew_join_threshold_bytes: int = 256 * 1024 * 1024  # default 256MB
    shuffle_partitions: int = 200
    spark_version: str = ''
    cluster_name: str = ''
    executor_cores: int = 0
    num_executors: int = 0


@dataclass
class AQEFinding:
    severity: str  # "critical" | "high" | "medium" | "info"
    code: str
    message: str
    suggestion: str
    config_snippet: str | None = None
    detail: str | None = None


def read_aqe_config(spark) -> AQEConfig:
    """Read all AQE-related configs from the active SparkSession."""
    conf = spark.sparkContext.getConf()
    get = conf.get

    cluster_tags_raw = get('spark.databricks.clusterUsageTags.clusterAllTags', '{}')
    cluster_name = ''
    try:
        import json

        tags = json.loads(cluster_tags_raw)
        cluster_name = tags.get('clusterName', '')
    except Exception:
        pass

    executor_cores = int(get('spark.executor.cores', '1') or '1')
    num_executors = int(get('spark.executor.instances', '1') or '1')

    return AQEConfig(
        adaptive_enabled=get('spark.sql.adaptive.enabled', 'false') == 'true',
        coalesce_partitions_enabled=get('spark.sql.adaptive.coalescePartitions.enabled', 'false') == 'true',
        skew_join_enabled=get('spark.sql.adaptive.skewJoin.enabled', 'false') == 'true',
        coalesce_shuffle_partitions_enabled=get('spark.sql.adaptive.coalesceShufflePartitions.enabled', 'true')
        == 'true',
        broadcast_wait_time_bytes=int(get('spark.sql.adaptive.autoBroadcastJoinThreshold', str(10 * 1024 * 1024))),
        skew_join_threshold_bytes=int(get('spark.sql.adaptive.skewJoin.skewJoinThreshold', str(256 * 1024 * 1024))),
        shuffle_partitions=int(get('spark.sql.shuffle.partitions', '200')),
        spark_version=spark.sparkContext.version,
        cluster_name=cluster_name,
        executor_cores=executor_cores,
        num_executors=num_executors,
    )


def build_recommendations(aqe: AQEConfig, plan_text: str, query: str = '') -> list[AQEFinding]:
    """
    Cross-reference AQE config against the physical plan to produce targeted findings.
    Only fires when there's a plan-level symptom that AQE could fix.
    """
    findings = []
    plan_upper = plan_text.upper()

    # ── AQE globally disabled ──────────────────────────────────────────
    if not aqe.adaptive_enabled:
        has_sort_merge = 'SORTMERGEJOIN' in plan_upper or 'SORT MERGE' in plan_upper
        has_shuffle_join = 'EXCHANGE' in plan_upper and 'JOIN' in plan_upper

        if has_sort_merge or has_shuffle_join:
            findings.append(
                AQEFinding(
                    severity='critical',
                    code='AQE_DISABLED',
                    message='AQE is disabled globally. '
                    'Enabling it allows Spark to dynamically switch SortMergeJoin → BroadcastHashJoin '
                    'at runtime based on actual row counts, which can eliminate large shuffles.',
                    suggestion='Enable AQE before running this query.',
                    config_snippet=(
                        'spark.conf.set("spark.sql.adaptive.enabled", "true")\n'
                        'spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")'
                    ),
                    detail='Plan shows SortMergeJoin or shuffle-join; AQE would evaluate broadcast '
                    'feasibility at runtime and choose the optimal strategy.',
                )
            )
        else:
            findings.append(
                AQEFinding(
                    severity='high',
                    code='AQE_DISABLED',
                    message='AQE is disabled globally. '
                    'This prevents Spark from dynamically adapting the plan at runtime.',
                    suggestion='Enable AQE.',
                    config_snippet='spark.conf.set("spark.sql.adaptive.enabled", "true")',
                )
            )

    # ── AQE enabled but skew join disabled ────────────────────────────
    if aqe.adaptive_enabled and not aqe.skew_join_enabled:
        has_exchange = 'EXCHANGE' in plan_upper
        # Check for signs of potential skew: multiple exchanges with varying sizes
        exchanges = _extract_exchange_partitions(plan_text)
        if has_exchange and len(exchanges) >= 2:
            findings.append(
                AQEFinding(
                    severity='high',
                    code='AQE_SKEW_JOIN_DISABLED',
                    message='AQE skew join handling is disabled. '
                    'If data is unevenly distributed across partitions, one task will process '
                    'significantly more rows than others and become a straggler.',
                    suggestion='Enable AQE skew join handling.',
                    config_snippet='spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")',
                    detail=f'Plan has {len(exchanges)} shuffle stages. Skew detection would identify '
                    'which ones are affected and dynamically split the skewed partitions.',
                )
            )

    # ── AQE enabled but coalesce partitions disabled ──────────────────
    if aqe.adaptive_enabled and not aqe.coalesce_partitions_enabled:
        # Post-filter reduction can leave many small partitions — coalesce helps
        has_filter_before_exchange = _has_filter_before_exchange(plan_text)
        if has_filter_before_exchange:
            findings.append(
                AQEFinding(
                    severity='medium',
                    code='AQE_COALESCE_DISABLED',
                    message='AQE partition coalescing is disabled. '
                    'After filtering reduces row counts, Spark may keep too many small partitions '
                    'causing per-partition overhead.',
                    suggestion='Enable partition coalescing.',
                    config_snippet='spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")',
                )
            )

    # ── Low broadcast threshold for the tables in this query ─────────
    if aqe.adaptive_enabled:
        broadcast_threshold_mb = aqe.broadcast_wait_time_bytes / (1024 * 1024)
        if broadcast_threshold_mb <= 10:
            # Look for large tables that might benefit from higher threshold
            large_tables = _extract_large_table_hints(plan_text, query)
            if large_tables:
                findings.append(
                    AQEFinding(
                        severity='medium',
                        code='LOW_BROADCAST_THRESHOLD',
                        message=f'Broadcast join threshold is {broadcast_threshold_mb:.0f}MB — relatively low. '
                        f'Large dimension tables ({", ".join(large_tables)}) may be shuffled instead '
                        f'of broadcast if they exceed this threshold at runtime.',
                        suggestion='Consider increasing the threshold for this workload.',
                        config_snippet=f'spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "{int(broadcast_threshold_mb * 2 * 1024 * 1024)}")',
                        detail=f'Current: {broadcast_threshold_mb:.0f}MB | Suggested: {broadcast_threshold_mb * 2:.0f}MB',
                    )
                )

    # ── Default shuffle partitions may be wrong ───────────────────────
    if aqe.shuffle_partitions == 200:
        # Heuristic: wide query with multiple stages suggests many tasks
        num_joins = plan_upper.count('JOIN')
        num_exchanges = plan_upper.count('EXCHANGE')
        if num_exchanges >= 3 or num_joins >= 3:
            findings.append(
                AQEFinding(
                    severity='medium',
                    code='HIGH_SHUFFLE_PARTITIONS',
                    message=f'Shuffle partitions is set to the default ({aqe.shuffle_partitions}). '
                    f'For this query ({num_joins} joins, {num_exchanges} exchanges), '
                    f'this creates many tasks for what may be a moderate-sized result set.',
                    suggestion='For ad-hoc exploration: reduce to ~80 partitions to avoid overhead. '
                    'For large ETL: leave at 200+ but ensure AQE coalesce is active.',
                    config_snippet='spark.conf.set("spark.sql.shuffle.partitions", "80")  # for interactive queries',
                )
            )

    # ── Good state confirmations ──────────────────────────────────────
    if aqe.adaptive_enabled and aqe.skew_join_enabled and aqe.coalesce_partitions_enabled:
        findings.append(
            AQEFinding(
                severity='info',
                code='AQE_HEALTHY',
                message='AQE is fully enabled (adaptive + skew join + partition coalescing). '
                "Spark will adapt this query's plan at runtime.",
                suggestion='No action needed.',
            )
        )

    return findings


def _extract_exchange_partitions(plan_text: str) -> list[int]:
    """Extract partition counts from Exchange nodes in the plan."""
    import re

    counts = re.findall(r'partition[=\s]+(\d+)', plan_text, re.IGNORECASE)
    return [int(c) for c in counts]


def _has_filter_before_exchange(plan_text: str) -> bool:
    """Check if there's a Filter node just before an Exchange — a sign that coalesce would help."""
    # Look for lines like: Filter [...] followed within a few lines by Exchange
    lines = plan_text.split('\n')
    for i, line in enumerate(lines):
        if 'Filter' in line and i + 1 < len(lines):
            next_few = '\n'.join(lines[i + 1 : i + 4])
            if 'Exchange' in next_few:
                return True
    return False


def _extract_large_table_hints(plan_text: str, query: str) -> list[str]:
    """Heuristic: tables referenced in JOINs with large aliases (e.g., fact_, dim_, dw_)"""
    import re

    tables = set()
    # Find table names in FROM/JOIN clauses
    patterns = [r'JOIN\s+(\w+(?:\.\w+)?)', r'FROM\s+(\w+(?:\.\w+)?)']
    for pattern in patterns:
        for match in re.finditer(pattern, query, re.IGNORECASE):
            name = match.group(1).split('.')[-1]
            if any(prefix in name.lower() for prefix in ('fact', 'dim', 'dw', 'large', 'big')):
                tables.add(name)
    return list(tables)[:5]  # limit to 5 to keep message readable
