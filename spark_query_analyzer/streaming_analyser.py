"""
Structured Streaming Plan Analyser — F-08 of the spark-query-analyzer roadmap.

Detects streaming-specific anti-patterns in addition to batch plan issues.
Works with both Spark SQL streaming queries and PySpark writeStream cells.
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class StreamingFinding:
    severity: str  # "critical" | "high" | "medium" | "info"
    code: str
    message: str
    suggestion: str
    detail: Optional[str] = None


@dataclass
class StreamingAnalysisResult:
    is_streaming: bool
    query_name: Optional[str]
    findings: list[StreamingFinding]
    watermark_configured: bool
    trigger_interval: Optional[str]
    sink_type: Optional[str]


def _get_active_streaming_queries(spark):
    """Get the list of active streaming queries."""
    try:
        return spark.streams.active
    except Exception:
        return []


def _detect_streaming_query(spark, sql: str, full_cell: str = "") -> bool:
    """Detect if the cell contains a streaming query."""
    sql_upper = sql.upper().strip()

    # SQL-based streaming: FROM (stream source)
    if any(kw in sql_upper for kw in ("READSTREAM", "STREAM", "KAFKA", "RATE(", "SOCKET")):
        return True

    # PySpark writeStream cell — check full cell for .writeStream
    if full_cell and ".writeStream" in full_cell:
        return True

    # Check active streaming queries
    active = _get_active_streaming_queries(spark)
    if active:
        # Check if any active query's logical plan matches the SQL
        for q in active:
            try:
                if sql in q.lastProgress().get("sourceNames", []):
                    return True
            except Exception:
                pass

    return False


def _get_query_info(query) -> dict:
    """Extract key info from a StreamingQuery."""
    info = {"name": None, "sink": None, "trigger": None, "watermark": False}

    try:
        info["name"] = query.name or query.id
    except Exception:
        pass

    try:
        # Sink info is in the query plan
        explain_str = query.explain(True)
        sink_types = ["Delta", "Kafka", "Foreach", "Memory", "Console", "Parquet", "Orc"]
        for st in sink_types:
            if st in explain_str:
                info["sink"] = st
                break
    except Exception:
        pass

    try:
        # Trigger from last progress
        lp = query.lastProgress()
        if lp:
            trigger_info = lp.get("trigger", {})
            if isinstance(trigger_info, dict):
                info["trigger"] = trigger_info.get("triggerType", "unknown")
            else:
                info["trigger"] = str(trigger_info)
    except Exception:
        pass

    try:
        # Watermark from last progress
        lp = query.lastProgress()
        if lp:
            event_time_cols = lp.get("eventTime", {})
            watermark = event_time_cols.get("watermark", None) if isinstance(event_time_cols, dict) else None
            info["watermark"] = watermark is not None and watermark != ""
    except Exception:
        pass

    return info


def _check_foreach_batch_antipatterns(query) -> list[StreamingFinding]:
    """Detect foreachBatch with non-idempotent writes."""
    findings = []

    try:
        explain_str = query.explain(True)

        # foreachBatch detected
        if "ForeachBatch" in explain_str or "foreachBatch" in explain_str:
            # Try to get the batch function source
            try:
                import json
                last_progress = query.lastProgress()
                if last_progress:
                    sources = last_progress.get("sources", [])
                    for source in sources:
                        description = source.get("description", "")
                        if "foreach" in description.lower():
                            findings.append(StreamingFinding(
                                severity="critical",
                                code="FOREACH_BATCH_NON_IDEMPOTENT",
                                message="foreachBatch detected with non-idempotent write operations. "
                                        "When the query is restarted after a failure, foreachBatch "
                                        "will re-execute the batch function, potentially producing "
                                        "duplicate rows in the sink.",
                                suggestion="Use MERGE INTO for exactly-once semantics in foreachBatch: "
                                           "```python\ndef process_batch(batch_df, batch_id):\n    batch_df.createOrReplaceTempView('updates')\n    batch_df._jdf.sparkSession().sql('''\n        MERGE INTO target t\n        USING updates u\n        ON t.id = u.id\n        WHEN MATCHED THEN UPDATE SET *\n        WHEN NOT MATCHED THEN INSERT *\n    ''')\n``` "
                                           "Or use Delta Lake's native streaming write with "
                                           "`df.writeStream.option('txnVersion', ...)` for idempotent writes.",
                                detail="foreachBatch is used for custom write logic per micro-batch",
                            ))
            except Exception:
                pass

            # Also check if it's writing to Delta
            if "Delta" in explain_str:
                findings.append(StreamingFinding(
                    severity="high",
                    code="FOREACH_BATCH_DELTA_IDEMPOTENCY",
                    message="foreachBatch writing to Delta — verify idempotency. "
                            "If this is a production streaming job, ensure the batch function "
                            "uses MERGE or has other deduplication logic.",
                    suggestion="Consider removing foreachBatch and using native Delta streaming write: "
                               "```python\ndf.writeStream.format('delta').option('checkpointLocation', '/path/to/checkpoint') \\\n    .outputMode('append').start('/path/to/output')\n``` "
                               "Native streaming writes are idempotent by default with checkpointing.",
                    detail="foreachBatch + Delta sink combination — check idempotency guarantees",
                ))

    except Exception:
        pass

    return findings


def _check_watermark_antipatterns(query, has_watermark: bool) -> list[StreamingFinding]:
    """Detect stateful operations without watermark."""
    findings = []

    try:
        explain_str = query.explain(True)

        # Stateful operations that require watermark
        stateful_ops = [
            "FlatMapGroupsWithState",
            "MapGroupsWithState",
            "dropDuplicates",
            " deduplicates",
        ]

        has_stateful = any(op in explain_str for op in stateful_ops)

        if has_stateful and not has_watermark:
            findings.append(StreamingFinding(
                severity="critical",
                code="MISSING_WATERMARK",
                message="Stateful operation (FlatMapGroupsWithState / dropDuplicates) detected "
                        "without an event-time watermark. Without a watermark, Spark cannot drop "
                        "old state and state will grow unboundedly — eventually causing OOM.",
                suggestion="Add a watermark on the event-time column before stateful operations: "
                           "```python\ndf \\\n    .withWatermark('event_time', '10 minutes') \\\n    .groupBy('key') \\\n    .agg(...)\n``` "
                           "The watermark delay must be at least as large as the maximum allowed "
                           "late-arrival for your data.",
                detail="Stateful streaming operation without watermark protection",
            ))

        # Also warn if watermark is configured but very short (< 5 minutes)
        if has_watermark:
            try:
                lp = query.lastProgress()
                if lp:
                    event_time = lp.get("eventTime", {})
                    if isinstance(event_time, dict):
                        wm_str = event_time.get("watermark", "")
                        if wm_str:
                            # Parse watermark string like "10 minutes"
                            import re
                            match = re.search(r"(\d+)\s*minutes", wm_str, re.IGNORECASE)
                            if match and int(match.group(1)) < 5:
                                findings.append(StreamingFinding(
                                    severity="medium",
                                    code="WATERMARK_TOO_SHORT",
                                    message=f"Watermark interval is very short ({wm_str}). "
                                            "Late events arriving after the watermark are dropped. "
                                            "A very short watermark may cause data loss if your data "
                                            "has natural delays or out-of-order arrivals.",
                                    suggestion="Consider setting a more conservative watermark interval "
                                               "if your data has known ordering delays, e.g. '30 minutes'.",
                                    detail=f"watermark={wm_str}",
                                ))
            except Exception:
                pass

    except Exception:
        pass

    return findings


def _check_trigger_antipatterns(query, trigger_interval: Optional[str]) -> list[StreamingFinding]:
    """Detect problematic trigger configurations."""
    findings = []

    if not trigger_interval:
        return findings

    trigger_lower = trigger_interval.lower()

    # High-frequency trigger on Delta source (common cost mistake)
    if "1 second" in trigger_lower or trigger_lower == "1s":
        try:
            explain_str = query.explain(True)
            if "Delta" in explain_str:
                findings.append(StreamingFinding(
                    severity="medium",
                    code="TRIGGER_TOO_FREQUENT",
                    message="Trigger interval of 1 second on a Delta source is very aggressive. "
                            "Each micro-batch acquisition and checkpoint write adds overhead. "
                            "For Delta sources with potentially large volumes, this can increase "
                            "DBU cost significantly without improving latency.",
                    suggestion="Consider `Trigger.AvailableNow()` for cost-efficient batch-style streaming: "
                               "```python\ndf.writeStream \\\n    .format('delta') \\\n    .option('checkpointLocation', '...') \\\n    .trigger(triggering=Trigger.AvailableNow()) \\\n    .outputMode('append') \\\n    .start('/path/to/output')\n``` "
                               "This processes all accumulated data in one batch and shuts down the cluster "
                               "between batches.",
                    detail=f"trigger={trigger_interval} on Delta source",
                ))
        except Exception:
            pass

    return findings


def _check_aqe_streaming_limitations(spark, query) -> list[StreamingFinding]:
    """Warn about AQE/Photon limitations for streaming workloads."""
    findings = []

    try:
        from spark_query_analyzer.aqe_checker import read_aqe_config
        aqe_cfg = read_aqe_config(spark)

        if not aqe_cfg.adaptive_enabled:
            findings.append(StreamingFinding(
                severity="info",
                code="AQE_DISABLED_STREAMING",
                message="AQE is disabled. For streaming queries with complex joins or aggregations, "
                        "AQE provides meaningful performance improvements (dynamic partition coalescing, "
                        "skew join handling). Consider enabling it.",
                suggestion="Enable AQE: spark.conf.set('spark.sql.adaptive.enabled', 'true')",
            ))

        # Photon-specific streaming note
        try:
            conf = spark.sparkContext.getConf()
            photon = conf.get("spark.databricks.photon.enabled", "false") == "true"
            if photon:
                # Check if foreachBatch is used (Photon has known limitations pre-DBR 13.1)
                explain_str = query.explain(True)
                if "ForeachBatch" in explain_str or "foreachBatch" in explain_str:
                    findings.append(StreamingFinding(
                        severity="info",
                        code="PHOTON_FOREACH_BATCH",
                        message="Photon + foreachBatch combination — ensure you are on DBR 13.1+ "
                                "for full compatibility. Earlier versions had known issues with "
                                "Photon accelerator and foreachBatch state management.",
                        suggestion="Confirm your DBR version supports Photon + foreachBatch. "
                                   "Upgrade to DBR 13.1+ if needed.",
                    ))
        except Exception:
            pass

    except Exception:
        pass

    return findings


def run_streaming_analysis(spark, sql: str = "", full_cell: str = "") -> StreamingAnalysisResult:
    """
    Main entry point for streaming analysis.
    Detects if the cell contains streaming content and runs appropriate checks.
    Returns StreamingAnalysisResult.
    """
    is_streaming = _detect_streaming_query(spark, sql, full_cell)

    if not is_streaming:
        return StreamingAnalysisResult(
            is_streaming=False,
            query_name=None,
            findings=[],
            watermark_configured=False,
            trigger_interval=None,
            sink_type=None,
        )

    # Get active streaming query
    active_queries = _get_active_streaming_queries(spark)

    if not active_queries:
        return StreamingAnalysisResult(
            is_streaming=True,
            query_name=None,
            findings=[StreamingFinding(
                severity="info",
                code="NO_ACTIVE_QUERY",
                message="Streaming query detected in SQL/cell but no active StreamingQuery found. "
                        "Ensure the query is running before using %analyze.",
                suggestion="Start the stream first, then run %analyze to inspect it.",
            )],
            watermark_configured=False,
            trigger_interval=None,
            sink_type=None,
        )

    # Use the most recent streaming query
    query = active_queries[-1]
    query_info = _get_query_info(query)

    all_findings: list[StreamingFinding] = []

    # foreachBatch anti-patterns
    all_findings.extend(_check_foreach_batch_antipatterns(query))

    # Watermark checks
    all_findings.extend(_check_watermark_antipatterns(query, query_info["watermark"]))

    # Trigger checks
    all_findings.extend(_check_trigger_antipatterns(query, query_info["trigger"]))

    # AQE / Photon limitations
    all_findings.extend(_check_aqe_streaming_limitations(spark, query))

    return StreamingAnalysisResult(
        is_streaming=True,
        query_name=query_info["name"],
        findings=all_findings,
        watermark_configured=query_info["watermark"],
        trigger_interval=query_info["trigger"],
        sink_type=query_info["sink"],
    )


def format_streaming_diagnostics(result: StreamingAnalysisResult) -> str:
    """Render streaming analysis results as HTML."""
    if not result.is_streaming:
        return ""

    header_parts = ["&#x1F300; Structured Streaming Analysis"]
    if result.query_name:
        header_parts.append(f" — {result.query_name}")
    header_html = (
        f'<div style="padding:8px 14px;background:#f8fafc;border-bottom:1px solid #e2e8f0;'
        f'font-size:12px;font-weight:600;color:#0f172a;">'
        f'{"".join(header_parts)}</div>'
    )

    if not result.findings:
        no_issues = (
            '<div style="padding:14px 16px;font-size:13px;color:#16a34a;">'
            '&#x2705; No streaming-specific issues detected.</div>'
        )
        return (
            f'<div style="border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;margin-top:8px;">'
            f'{header_html}{no_issues}</div>'
        )

    findings_html = ""
    for f in result.findings:
        border = {"critical": "#dc2626", "high": "#ea580c", "medium": "#ca8a04", "info": "#16a34a"}.get(f.severity, "#ccc")
        sym = {"critical": "&#x1F534;", "high": "&#x1F7E0;", "medium": "&#x1F7E1;", "info": "&#x2705;"}.get(f.severity, "&#x26AA;")
        label = f.severity.upper()
        detail_html = f'<div class="sqa-node">{f.detail}</div>' if f.detail else ""

        # Format suggestion with code blocks
        suggestion_html = ""
        if f.suggestion:
            # Check if suggestion has a code block (```python ... ```)
            if "```" in f.suggestion:
                parts = f.suggestion.split("```")
                for i, part in enumerate(parts):
                    if i % 2 == 1:  # Code block
                        escaped = part.replace("\n", "<br>").replace(" ", "&nbsp;")
                        suggestion_html += f'<pre style="background:#1e293b;color:#f8fafc;padding:10px;border-radius:6px;font-size:11px;overflow-x:auto;margin-top:4px;"><code>{escaped}</code></pre>'
                    elif part.strip():
                        suggestion_html += f'<div style="font-size:12px;color:#475569;margin-top:4px;">{part.strip()}</div>'
            else:
                suggestion_html = f'<div style="font-size:12px;color:#475569;">{f.suggestion}</div>'

        findings_html += (
            f'<div style="border-left:4px solid {border};padding:10px 14px;border-bottom:1px solid #f1f5f9;">'
            f'<div style="font-weight:600;font-size:12px;color:{border};">{sym} {label} &mdash; {f.code}</div>'
            f'<div style="font-size:13px;color:#1e293b;margin-top:4px;">{f.message}</div>'
            f'{detail_html}{suggestion_html}'
            f'</div>'
        )

    # Metadata strip
    meta_parts = []
    if result.watermark_configured:
        meta_parts.append(f"&#x1F4CC; Watermark: configured")
    else:
        meta_parts.append(f"&#x1F4CC; Watermark: not set")
    if result.trigger_interval:
        meta_parts.append(f"&#x23F1; Trigger: {result.trigger_interval}")
    if result.sink_type:
        meta_parts.append(f"&#x1F4E6; Sink: {result.sink_type}")

    meta_html = (
        f'<div style="padding:6px 14px;background:#f1f5f9;font-size:11px;color:#64748b;display:flex;gap:14px;">'
        f'{" &nbsp;&middot;&nbsp; ".join(meta_parts)}</div>'
    )

    return (
        f'<div style="border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;margin-top:8px;">'
        f'{header_html}{meta_html}{findings_html}</div>'
    )