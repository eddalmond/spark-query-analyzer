"""
Query performance system info — check Spark/AQE config and system tables.
"""

from dataclasses import dataclass


@dataclass
class AQEConfig:
    adaptive_enabled: bool = False
    coalesce_partitions_enabled: bool = False
    skew_join_enabled: bool = False
    coalesce_shuffle_partitions_enabled: bool = False
    broadcast_wait_time_ms: int = 0
    skew_join_threshold_bytes: int = 0
    shuffle_partitions: int = 0


@dataclass
class SystemInfo:
    aqe_config: AQEConfig
    spark_version: str = ""
    runtime_ms: int = 0
    bytes_read: int = 0
    records_read: int = 0


def get_aqe_config(spark) -> AQEConfig:
    """Read current AQE config from Spark session."""
    conf = spark.sparkContext.getConf()
    get = conf.get

    return AQEConfig(
        adaptive_enabled=get("spark.sql.adaptive.enabled", "false") == "true",
        coalesce_partitions_enabled=get("spark.sql.adaptive.coalescePartitions.enabled", "false") == "true",
        skew_join_enabled=get("spark.sql.adaptive.skewJoin.enabled", "false") == "true",
        coalesce_shuffle_partitions_enabled=get("spark.sql.adaptive.coalesceShufflePartitions.enabled", "true") == "true",
        broadcast_wait_time_ms=int(get("spark.sql.adaptive.autoBroadcastJoinThreshold", "10485760")) // 1024,
        skew_join_threshold_bytes=int(get("spark.sql.adaptive.skewJoin.skewJoinThreshold", "67108864")),
        shuffle_partitions=int(get("spark.sql.shuffle.partitions", "200")),
    )


def get_system_info(spark, query: str, runtime_ms: int = 0) -> SystemInfo:
    """Gather system info for the current query."""
    aqe = get_aqe_config(spark)

    sc = spark.sparkContext
    return SystemInfo(
        aqe_config=aqe,
        spark_version=sc.version,
        runtime_ms=runtime_ms,
    )


def build_aqe_diagnostics(aqe: AQEConfig) -> list[dict]:
    """Build a list of AQE-related findings from config."""
    findings = []

    if not aqe.adaptive_enabled:
        findings.append({
            "severity": "high",
            "code": "AQE_DISABLED",
            "message": "Adaptive Query Execution (AQE) is disabled globally. "
                       "This prevents Spark from dynamically optimizing the plan at runtime.",
            "suggestion": "Enable AQE: spark.conf.set('spark.sql.adaptive.enabled', 'true'). "
                           "AQE provides dynamic join selection, skew handling, and partition coalescing.",
        })
    else:
        findings.append({
            "severity": "info",
            "code": "AQE_ENABLED",
            "message": "AQE is enabled. Spark will dynamically adapt the physical plan at runtime.",
        })

    if aqe.adaptive_enabled and not aqe.skew_join_enabled:
        findings.append({
            "severity": "medium",
            "code": "AQE_SKEW_JOIN_DISABLED",
            "message": "AQE skew join optimization is disabled. "
                       "Skewed partitions will be processed by a single task causing hot spots.",
            "suggestion": "Enable skew join: spark.conf.set('spark.sql.adaptive.skewJoin.enabled', 'true').",
        })

    if aqe.adaptive_enabled and not aqe.coalesce_partitions_enabled:
        findings.append({
            "severity": "medium",
            "code": "AQE_COALESCE_DISABLED",
            "message": "AQE partition coalescing is disabled. "
                       "Post-shuffle partitions may not be combined after filtering.",
            "suggestion": "Enable: spark.conf.set('spark.sql.adaptive.coalescePartitions.enabled', 'true').",
        })

    broadcast_mb = aqe.broadcast_wait_time_ms // 1024
    if aqe.adaptive_enabled and broadcast_mb <= 10:
        findings.append({
            "severity": "medium",
            "code": "LOW_BROADCAST_THRESHOLD",
            "message": f"Broadcast join threshold is {broadcast_mb}MB. "
                       f"Large dimension tables may be shuffled instead of broadcast.",
            "suggestion": f"Consider increasing: spark.conf.set('spark.sql.adaptive.autoBroadcastJoinThreshold', '{broadcast_mb * 2 * 1024 * 1024}').",
        })

    if aqe.shuffle_partitions > 200:
        findings.append({
            "severity": "medium",
            "code": "HIGH_SHUFFLE_PARTITIONS",
            "message": f"Shuffle partitions set to {aqe.shuffle_partitions}. "
                       f"This creates many small tasks for simple queries.",
            "suggestion": "For ad-hoc queries, try reducing: spark.conf.set('spark.sql.shuffle.partitions', '10'). "
                          "For production ETL, leave higher but use AQE coalesce to adapt.",
        })

    return findings


def check_query_history_for_slow_runs(spark, query: str, days: int = 7) -> list[dict]:
    """Query system tables for recent slow runs of the same SQL text."""
    findings = []

    try:
        import datetime
        cutoff = (datetime.datetime.utcnow() - datetime.timedelta(days=days)).strftime("%Y-%m-%d")

        # Normalize query text for matching
        query_normalized = query.replace("%", "").replace("\n", " ").strip()[:200]

        history = spark.sql(f"""
            SELECT
                query_text,
                execution_time_ms,
                query_id,
                start_time,
                bytes_read,
                rows_read
            FROM system.query.history
            WHERE start_time >= '{cutoff}'
              AND query_text LIKE '%{query_normalized[:50]}%'
            ORDER BY start_time DESC
            LIMIT 20
        """).collect()

        if not history:
            return []

        runtimes = [r["execution_time_ms"] for r in history if r["execution_time_ms"]]
        if len(runtimes) >= 2:
            avg_runtime = sum(runtimes) / len(runtimes)
            latest = runtimes[0]
            if latest > avg_runtime * 1.5 and avg_runtime > 5000:
                findings.append({
                    "severity": "high",
                    "code": "QUERY_REGRESSION",
                    "message": f"Latest run took {latest:,}ms vs {avg_runtime:,.0f}ms avg over {len(runtimes)} recent runs. "
                               f"This query may be regressing in performance.",
                    "suggestion": "Check system.query.history for datasource changes or cluster config differences. "
                                  "Consider pinning the query to a specific cluster.",
                    "detail": f"Latest: {latest:,}ms | Avg: {avg_runtime:,.0f}ms | Trend: {'SLOWER' if latest > avg_runtime else 'stable'}",
                })

        if history:
            total_bytes = sum(r["bytes_read"] or 0 for r in history)
            findings.append({
                "severity": "info",
                "code": "QUERY_HISTORY",
                "message": f"Found {len(history)} historical runs of this query. "
                           f"Total bytes read across runs: {total_bytes / (1024**3):.1f} GB.",
                "suggestion": "Run '%analyze --history " + query_normalized[:50] + "' for full trend analysis.",
            })

    except Exception:
        # System tables may not be available on all Databricks tiers
        pass

    return findings


def check_file_size_stats(spark, query: str, tables: list[str]) -> list[dict]:
    """Check for small file issues in tables used by the query."""
    findings = []

    for table in tables:
        try:
            # Get file size stats via DESCRIBE DETAIL (fast, no sampling)
            desc = spark.sql(f"DESCRIBE DETAIL {table}").collect()
            if not desc:
                continue

            # Spark 3.x puts file size info in Delta log
            # Use table sample for avg file size
            files = spark.sql(f"""
                SELECT
                    count(*) as file_count,
                    avg(size_bytes) as avg_file_size,
                    min(size_bytes) as min_file_size,
                    max(size_bytes) as max_file_size,
                    sum(size_bytes) as total_size
                FROM (
                    SELECT input_file_name() as f, length(content) as size_bytes
                    FROM {table} LIMIT 10000
                )
            """).collect()[0]

            avg_size = files["avg_file_size"] or 0
            file_count = files["file_count"] or 0

            if file_count > 100 and avg_size < 32 * 1024 * 1024:  # > 100 files, avg < 32MB
                findings.append({
                    "severity": "high",
                    "code": "SMALL_FILES",
                    "message": f"Table '{table}' has {file_count:,} files with avg size {avg_size / (1024*1024):.1f} MB. "
                               f"Small files severely impact scan performance.",
                    "suggestion": f"Run OPTIMIZE {table} ZORDER BY (...). "
                                 f"For high-volume tables, consider liquid clustering or incremental compaction.",
                    "detail": f"Files: {file_count:,} | Avg: {avg_size/(1024*1024):.1f}MB | Total: {files['total_size']/(1024**3):.1f}GB",
                })
            elif file_count > 1000 and avg_size < 128 * 1024 * 1024:
                findings.append({
                    "severity": "medium",
                    "code": "MANY_FILES",
                    "message": f"Table '{table}' has {file_count:,} files. "
                               f"High file count increases metadata overhead even if avg size is acceptable.",
                    "suggestion": f"Consider OPTIMIZE {table} to coalesce small files.",
                    "detail": f"Files: {file_count:,} | Avg: {avg_size/(1024*1024):.1f}MB",
                })

        except Exception:
            # DESCRIBE or table scan may fail on external tables, views, etc.
            pass

    return findings