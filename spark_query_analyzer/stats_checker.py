"""
Schema & Statistics Health Checker — F-07 of the spark-query-analyzer roadmap.

Checks whether column statistics are present and current for each table in the query.
Correlates missing/stale stats with plan findings to give precise recommendations.
"""

import re
from dataclasses import dataclass


@dataclass
class StatsFinding:
    severity: str  # "critical" | "high" | "medium" | "info"
    code: str
    message: str
    table: str
    suggestion: str
    analyze_command: str | None = None  # copy-ready ANALYZE command
    detail: str | None = None


@dataclass
class TableStats:
    table: str
    row_count: int | None
    total_size_bytes: int | None
    has_statistics: bool
    stats_age_hours: float | None  # hours since last ANALYZE
    last_write_timestamp: str | None
    last_analyze_timestamp: str | None
    columns_analyzed: list[str]
    is_stale: bool


def check_table_stats(spark, table: str) -> TableStats:
    """
    Run DESCRIBE TABLE EXTENDED and DESCRIBE HISTORY to build a TableStats snapshot.
    Returns TableStats for the given table.
    """
    # Get basic stats from DESCRIBE EXTENDED
    try:
        extended_df = spark.sql(f'DESCRIBE TABLE EXTENDED {table}')
        extended_rows = [row.asDict() for row in extended_df.collect()]
    except Exception:
        return TableStats(
            table=table,
            row_count=None,
            total_size_bytes=None,
            has_statistics=False,
            stats_age_hours=None,
            last_write_timestamp=None,
            last_analyze_timestamp=None,
            columns_analyzed=[],
            is_stale=False,
        )

    # Parse Statistics row
    stats_row = None
    for row in extended_rows:
        col_name = str(row.get('col_name', '') or row.get('col_name', '')).strip()
        if col_name.lower() == 'statistics':
            stats_row = row.get('data_type', '') or row.get('data_type', '')
            break

    has_statistics = stats_row is not None and 'rowCount' in stats_row

    # Extract row count
    row_count = None
    rc_match = re.search(r'(\d+(?:,\d+)*)\s+rows?', stats_row or '', re.IGNORECASE)
    if rc_match:
        row_count = int(rc_match.group(1).replace(',', ''))

    # Extract size
    total_size_bytes = None
    size_match = re.search(r'(\d+(?:,\d+)*)\s+bytes?', stats_row or '', re.IGNORECASE)
    if size_match:
        total_size_bytes = int(size_match.group(1).replace(',', ''))

    # Get last write from DESCRIBE HISTORY
    last_write_ts = None
    last_analyze_ts = None
    columns_analyzed: list[str] = []

    try:
        history_df = spark.sql(f'DESCRIBE HISTORY {table} LIMIT 20')
        history_rows = [row.asDict() for row in history_df.collect()]

        writes = []
        analyze_entries = []

        for row in history_rows:
            operation = str(row.get('operation', '') or row.get('operationName', '')).strip()
            timestamp = str(row.get('timestamp', '') or row.get('create_time', '') or '')

            if operation.upper() in ('WRITE', 'INSERT', 'MERGE', 'UPDATE', 'DELETE'):
                writes.append(timestamp)
            elif operation.upper() in ('ANALYZE',):
                analyze_entries.append(timestamp)

        if writes:
            last_write_ts = writes[-1]  # oldest write = least recent = earliest chronologically
        if analyze_entries:
            last_analyze_ts = analyze_entries[-1]
    except Exception:
        pass

    # Compute stats age
    stats_age_hours = None
    if last_analyze_ts:
        try:
            from datetime import datetime, timezone

            # Parse timestamp — various formats
            parsed = None
            for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S.%f'):
                try:
                    parsed = datetime.strptime(last_analyze_ts[:19], fmt).replace(tzinfo=timezone.utc)
                    break
                except ValueError:
                    continue
            if parsed:
                delta = datetime.now(timezone.utc) - parsed
                stats_age_hours = delta.total_seconds() / 3600
        except Exception:
            pass

    # Determine staleness: stats are stale if they were collected before the last write
    is_stale = False
    if last_analyze_ts and last_write_ts:
        try:
            from datetime import datetime, timezone

            fmt = '%Y-%m-%d %H:%M:%S'
            at = datetime.strptime(last_analyze_ts[:19], fmt).replace(tzinfo=timezone.utc)
            wt = datetime.strptime(last_write_ts[:19], fmt).replace(tzinfo=timezone.utc)
            if wt > at:  # last write is more recent than last analyze → stale
                is_stale = True
        except Exception:
            pass

    # Check for column-level statistics
    try:
        col_df = spark.sql(f'DESCRIBE TABLE {table}')
        col_rows = [row.asDict() for row in col_df.collect()]
        for row in col_rows:
            col_name = str(row.get('col_name', '') or row.get('column_name', '')).strip()
            str(row.get('data_type', '') or row.get('col_type', '')).strip()
            comment = str(row.get('comment', '') or '').strip()
            # Columns with stats have a comment referencing numNulls/numDistinct etc.
            if 'numNulls' in comment or 'numDistinct' in comment or 'min' in comment:
                columns_analyzed.append(col_name)
    except Exception:
        pass

    return TableStats(
        table=table,
        row_count=row_count,
        total_size_bytes=total_size_bytes,
        has_statistics=has_statistics,
        stats_age_hours=stats_age_hours,
        last_write_timestamp=last_write_ts,
        last_analyze_timestamp=last_analyze_ts,
        columns_analyzed=columns_analyzed,
        is_stale=is_stale,
    )


def run_stats_health_check(spark, sql: str, plan_tables: list[str]) -> list[StatsFinding]:
    """
    Check statistics health for all tables in the plan.
    Returns a list of StatsFindings ordered by severity.
    """
    findings = []

    for table in plan_tables:
        if not table or table in ('', '<unknown>'):
            continue

        stats = check_table_stats(spark, table)

        # --- Missing statistics ---
        if not stats.has_statistics:
            severity = 'medium'
            if stats.row_count and stats.row_count > 5_000_000:
                severity = 'high'
            elif stats.row_count and stats.row_count > 50_000_000:
                severity = 'critical'

            size_str = ''
            if stats.total_size_bytes:
                gb = stats.total_size_bytes / (1024**3)
                size_str = f' ({gb:.1f}GB)' if gb >= 1 else f' ({stats.total_size_bytes / 1024**2:.0f}MB)'

            findings.append(
                StatsFinding(
                    severity=severity,
                    code='STATS_MISSING',
                    message=f'No column statistics found for `{table}`{size_str}. '
                    f'The Catalyst planner is operating without data size estimates for this table.',
                    table=table,
                    suggestion='Run ANALYZE TABLE to collect statistics:',
                    analyze_command=f'ANALYZE TABLE {table} COMPUTE STATISTICS FOR ALL COLUMNS',
                    detail=f'row_count={stats.row_count}, size={stats.total_size_bytes}',
                )
            )
            continue

        # --- Stale statistics ---
        if stats.is_stale:
            age_days = stats.stats_age_hours / 24 if stats.stats_age_hours else None
            age_str = f'{age_days:.0f} days' if age_days else 'unknown'

            severity = 'medium'
            if stats.row_count and stats.row_count > 10_000_000:
                severity = 'high'

            findings.append(
                StatsFinding(
                    severity=severity,
                    code='STATS_STALE',
                    message=f'Column statistics for `{table}` are stale (collected {age_str} ago). '
                    f'The planner may be making decisions based on outdated row count estimates.',
                    table=table,
                    suggestion='Re-run ANALYZE TABLE to refresh statistics:',
                    analyze_command=f'ANALYZE TABLE {table} COMPUTE STATISTICS FOR ALL COLUMNS',
                    detail=f'last_analyze={stats.last_analyze_ts}, last_write={stats.last_write_ts}',
                )
            )

        # --- Partial column stats ---
        # If table is large (>100M rows) and <50% of columns have stats, flag it
        if stats.row_count and stats.row_count > 100_000_000:
            # We don't know total column count from here, so use a heuristic:
            # if stats_age_hours is stale but some columns are analyzed, flag partial stats
            if stats.columns_analyzed and stats.stats_age_hours and stats.stats_age_hours > 168:
                findings.append(
                    StatsFinding(
                        severity='medium',
                        code='STATS_PARTIAL',
                        message=f'Large table `{table}` ({stats.row_count:,} rows) has some column statistics '
                        f'but they may be incomplete or outdated. '
                        f'Only {len(stats.columns_analyzed)} columns flagged with statistics.',
                        table=table,
                        suggestion='Re-run full column statistics and verify all filter/join columns are included:',
                        analyze_command=f'ANALYZE TABLE {table} COMPUTE STATISTICS FOR ALL COLUMNS',
                        detail=f'stats_age_hours={stats.stats_age_hours:.0f}, columns_with_stats={len(stats.columns_analyzed)}',
                    )
                )

    return findings


def format_stats_findings_table(findings: list[StatsFinding]) -> str:
    """Render stats findings as a compact HTML table."""
    if not findings:
        return ''

    rows_html = ''
    for f in findings:
        border = {'critical': '#dc2626', 'high': '#ea580c', 'medium': '#ca8a04', 'info': '#16a34a'}.get(
            f.severity, '#ccc'
        )
        sym = {'critical': '&#x1F534;', 'high': '&#x1F7E0;', 'medium': '&#x1F7E1;', 'info': '&#x2705;'}.get(
            f.severity, '&#x26AA;'
        )
        label = f.severity.upper()
        if f.analyze_command:
            f.analyze_command.replace('\n', '<br>').replace(' ', '&nbsp;')

        rows_html += (
            f'<tr style="border-bottom:1px solid #e2e8f0;">'
            f'<td style="padding:8px 12px;color:{border};font-weight:600;font-size:12px;">{sym} {label}</td>'
            f'<td style="padding:8px 12px;font-size:12px;font-family:monospace;color:#0f172a;">{f.code}</td>'
            f'<td style="padding:8px 12px;font-size:12px;color:#475569;">{f.table}</td>'
            f'<td style="padding:8px 12px;font-size:12px;color:#1e293b;">{f.message}</td>'
            f'</tr>'
        )

    html = (
        f'<div style="margin-top:8px;border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;">'
        f'<div style="padding:8px 14px;background:#f8fafc;border-bottom:1px solid #e2e8f0;'
        f'font-size:12px;font-weight:600;color:#0f172a;">&#x1F4CA; Schema &amp; Statistics Health</div>'
        f'<table style="width:100%;border-collapse:collapse;font-size:12px;">'
        f'<tr style="background:#f1f5f9;font-weight:600;text-align:left;">'
        f'<th style="padding:6px 12px;border-bottom:1px solid #e2e8f0;">Severity</th>'
        f'<th style="padding:6px 12px;border-bottom:1px solid #e2e8f0;">Code</th>'
        f'<th style="padding:6px 12px;border-bottom:1px solid #e2e8f0;">Table</th>'
        f'<th style="padding:6px 12px;border-bottom:1px solid #e2e8f0;">Finding</th>'
        f'</tr>'
        f'{rows_html}'
        f'</table>'
        f'</div>'
    )
    return html
