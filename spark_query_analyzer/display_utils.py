"""
HTML diagnostics renderer for the %analyze output.
"""

from spark_query_analyzer.analyzer import AnalysisResult, Finding

SEVERITY_ORDER = ["critical", "high", "medium", "info"]
SEVERITY_SYMBOL = {
    "critical": "🔴",
    "high": "🟠",
    "medium": "🟡",
    "info": "🟢",
}
SEVERITY_LABEL = {
    "critical": "CRITICAL",
    "high": "HIGH",
    "medium": "MEDIUM",
    "info": "INFO",
}
SEVERITY_BORDER = {
    "critical": "#dc2626",
    "high": "#ea580c",
    "medium": "#ca8a04",
    "info": "#16a34a",
}

_css = """
<style>
.sqa {font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;margin:8px 0;}
.sqa-header {background:#0f172a;color:#f8fafc;padding:12px 16px;border-radius:8px 8px 0 0;display:flex;justify-content:space-between;align-items:center;}
.sqa-badge {display:inline-flex;gap:8px;}
.sqa-badge-item {display:inline-flex;align-items:center;gap:4px;font-size:12px;font-weight:600;padding:2px 8px;border-radius:12px;}
.sqa-badge-critical{background:#dc2626;}
.sqa-badge-high{background:#ea580c;}
.sqa-badge-medium{background:#ca8a04;}
.sqa-badge-info{background:#16a34a;}
.sqa-body{border:1px solid #e2e8f0;border-top:none;border-radius:0 0 8px 8px;overflow:hidden;}
.sqa-finding{border-left:4px solid #ccc;padding:10px 14px;border-bottom:1px solid #f1f5f9;}
.sqa-finding:last-child{border-bottom:none;}
.sqa-severity{font-size:11px;font-weight:700;letter-spacing:.08em;margin-bottom:4px;}
.sqa-code{font-family:'Courier New',monospace;font-size:10px;background:#f1f5f9;color:#475569;padding:1px 5px;border-radius:4px;margin-left:6px;}
.sqa-message{font-size:13px;color:#1e293b;margin:4px 0;font-weight:500;}
.sqa-node{font-size:11px;color:#64748b;font-family:'Courier New',monospace;margin:2px 0;}
.sqa-suggestion{font-size:12px;color:#0f172a;background:#f8fafc;border:1px solid #e2e8f0;border-radius:6px;padding:6px 10px;margin-top:6px;}
.sqa-suggestion strong{color:#16a34a;}
.sqa-summary{padding:8px 14px;background:#f8fafc;font-size:12px;color:#475569;border-top:1px solid #e2e8f0;}
</style>
"""


def format_diagnostics(result: AnalysisResult, delta_results: list = None, python_findings: list = None, skew_findings: list = None, cost_badge: str = "", stats_findings: list = None) -> str:
    """Render an AnalysisResult as a self-contained HTML fragment."""
    counts = result.severity_counts
    total = len(result.findings)
    plural = "s" if total != 1 else ""
    header_title = f"Spark Query Analyzer \u2014 {total} finding{pplural}"

    badge_html = ""
    for sev in SEVERITY_ORDER:
        n = counts.get(sev, 0)
        if n > 0:
            sym = SEVERITY_SYMBOL[sev]
            badge_html += f'<span class="sqa-badge-item sqa-badge-{sev}">{sym} {n}</span>'
    if cost_badge:
        badge_html += cost_badge

    # Build findings HTML (existing plan findings)
    findings_html = ""
    if not result.findings:
        findings_html = '<div style="padding:16px;font-size:13px;color:#16a34a;">&#x2705; No obvious performance issues detected.</div>'
    else:
        for f in result.findings:
            border = SEVERITY_BORDER.get(f.severity, "#ccc")
            sym = SEVERITY_SYMBOL.get(f.severity, "⚪")
            label = SEVERITY_LABEL.get(f.severity, f.severity.upper())
            node_html = f'<div class="sqa-node">Node: {f.node}</div>' if f.node else ""
            table_html = f'<div class="sqa-node">Table: {f.table}</div>' if f.table else ""
            suggestion_html = (
                f'<div class="sqa-suggestion"><strong>\u2192 Fix:</strong> {f.suggestion}</div>'
                if f.suggestion else ""
            )
            if f.config_snippet:
                escaped_snippet = f.config_snippet.replace("\n", "<br>").replace(" ", "&nbsp;")
                suggestion_html += (
                    f'<div class="sqa-suggestion" style="background:#1e293b;color:#f8fafc;margin-top:4px;font-size:11px;">'
                    f'<strong style="color:#60a5fa;">Config:</strong><br>'
                    f'<code style="font-size:10px;">{escaped_snippet}</code></div>'
                )
            findings_html += (
                f'<div class="sqa-finding" style="border-left-color:{border}">'
                f'<div class="sqa-severity" style="color:{border}">{sym} {label}'
                f'<span class="sqa-code">{f.code}</span></div>'
                f'<div class="sqa-message">{f.message}</div>'
                f'{node_html}{table_html}{suggestion_html}'
                f'</div>'
            )

    # Build Python Patterns section (F-04)
    python_html = ""
    if python_findings:
        py_findings_html = ""
        for pf in python_findings:
            border = SEVERITY_BORDER.get(pf.severity, "#ccc")
            sym = SEVERITY_SYMBOL.get(pf.severity, "⚪")
            label = SEVERITY_LABEL.get(pf.severity, pf.severity.upper())
            line_html = f'<div class="sqa-node">Line: {pf.line}</div>' if pf.line else ""
            suggestion_html = f'<div class="sqa-suggestion"><strong>\u2192 Fix:</strong> {pf.suggestion}</div>' if pf.suggestion else ""
            py_findings_html += (
                f'<div class="sqa-finding" style="border-left-color:{border}">'
                f'<div class="sqa-severity" style="color:{border}">{sym} {label}<span class="sqa-code">{pf.code}</span></div>'
                f'<div class="sqa-message">{pf.message}</div>'
                f'{line_html}{suggestion_html}'
                f'</div>'
            )
        python_header = '<div style="padding:8px 14px;background:#f8fafc;border-top:1px solid #e2e8f0;font-size:12px;font-weight:600;color:#0f172a;">&#x1F40D; Python Patterns</div>'
        python_html = '<div style="border-top:2px solid #e2e8f0;margin-top:4px;">' + python_header + py_findings_html + '</div>'

    # Build Delta Storage section (F-01)
    delta_html = ""
    if delta_results:
        delta_sections = []
        for dr in delta_results:
            if not dr.findings:
                continue
            table_header = f'<div style="padding:8px 14px;background:#f8fafc;border-top:1px solid #e2e8f0;font-size:12px;font-weight:600;color:#0f172a;">&#x1F4C4; Delta Storage: {dr.table} &nbsp;<span style="color:#64748b;font-weight:400">({dr.num_files:,} files, {dr.table_size_gb:.1f}GB)</span></div>'
            delta_findings_html = ""
            for df in dr.findings:
                border = SEVERITY_BORDER.get(df.severity, "#ccc")
                sym = SEVERITY_SYMBOL.get(df.severity, "⚪")
                label = SEVERITY_LABEL.get(df.severity, df.severity.upper())
                detail_html = f'<div class="sqa-node">{df.detail}</div>' if df.detail else ""
                suggestion_html = f'<div class="sqa-suggestion"><strong>\u2192 Fix:</strong> {df.suggestion}</div>' if df.suggestion else ""
                delta_findings_html += (
                    f'<div class="sqa-finding" style="border-left-color:{border}">'
                    f'<div class="sqa-severity" style="color:{border}">{sym} {label}<span class="sqa-code">{df.code}</span></div>'
                    f'<div class="sqa-message">{df.message}</div>'
                    f'{detail_html}{suggestion_html}'
                    f'</div>'
                )
            delta_sections.append(table_header + delta_findings_html)

        if delta_sections:
            delta_html = '<div style="border-top:2px solid #e2e8f0;margin-top:4px;">' + ''.join(delta_sections) + '</div>'

    # Build Post-Execution Skew section (F-03)
    skew_html = ""
    if skew_findings:
        skew_findings_html = ""
        for sf in skew_findings:
            border = SEVERITY_BORDER.get(sf.severity, "#ccc")
            sym = SEVERITY_SYMBOL.get(sf.severity, "⚪")
            label = SEVERITY_LABEL.get(sf.severity, sf.severity.upper())
            detail_html = f'<div class="sqa-node">{sf.detail}</div>' if sf.detail else ""
            suggestion_html = f'<div class="sqa-suggestion"><strong>\u2192 Fix:</strong> {sf.suggestion}</div>' if sf.suggestion else ""
            skew_findings_html += (
                f'<div class="sqa-finding" style="border-left-color:{border}">'
                f'<div class="sqa-severity" style="color:{border}">{sym} {label}<span class="sqa-code">{sf.code}</span></div>'
                f'<div class="sqa-message">{sf.message}</div>'
                f'{detail_html}{suggestion_html}'
                f'</div>'
            )
        skew_header = '<div style="padding:8px 14px;background:#f8fafc;border-top:1px solid #e2e8f0;font-size:12px;font-weight:600;color:#0f172a;">&#x1F4CA; Post-Execution (Actual Task Metrics)</div>'
        skew_html = '<div style="border-top:2px solid #e2e8f0;margin-top:4px;">' + skew_header + skew_findings_html + '</div>'

    # Build Stats Health section (F-07)
    stats_html = ""
    if stats_findings:
        stats_findings_html = ""
        for sf in stats_findings:
            border = SEVERITY_BORDER.get(sf.severity, "#ccc")
            sym = SEVERITY_SYMBOL.get(sf.severity, "⚪")
            label = SEVERITY_LABEL.get(sf.severity, sf.severity.upper())
            table_html = f'<div class="sqa-node">Table: {sf.table}</div>' if sf.table else ""
            suggestion_html = f'<div class="sqa-suggestion"><strong>\u2192 Fix:</strong> {sf.suggestion}</div>' if sf.suggestion else ""
            if sf.analyze_command:
                escaped_cmd = sf.analyze_command.replace("\n", "<br>").replace(" ", "&nbsp;")
                suggestion_html += (
                    f'<div class="sqa-suggestion" style="background:#1e293b;color:#f8fafc;margin-top:4px;font-size:11px;">'
                    f'<strong style="color:#60a5fa;">\u2714 Run:</strong><br>{escaped_cmd}</div>'
                )
            stats_findings_html += (
                f'<div class="sqa-finding" style="border-left-color:{border}">'
                f'<div class="sqa-severity" style="color:{border}">{sym} {label}<span class="sqa-code">{sf.code}</span></div>'
                f'<div class="sqa-message">{sf.message}</div>'
                f'{table_html}{suggestion_html}'
                f'</div>'
            )
        stats_header = '<div style="padding:8px 14px;background:#f8fafc;border-top:1px solid #e2e8f0;font-size:12px;font-weight:600;color:#0f172a;">&#x1F4CA; Schema &amp; Statistics Health</div>'
        stats_html = '<div style="border-top:2px solid #e2e8f0;margin-top:4px;">' + stats_header + stats_findings_html + '</div>'

    footer_html = (
        '<div class="sqa-summary">'
        '&#x1F4DD; Run <code>EXPLAIN FORMATTED &lt;query&gt;</code> in a separate cell for the full plan.'
        '<br>&#x26A1; Run <code>%analyze --execute</code> to include post-execution skew analysis (F-03).'
        '</div>'
    )

    return (
        f'<div class="sqa">'
        f'<div class="sqa-header"><span>&#x1F50D; {header_title}</span>'
        f'<div class="sqa-badge">{badge_html}</div></div>'
        f'<div class="sqa-body">{findings_html}{python_html}{delta_html}{skew_html}{stats_html}</div>'
        f'{footer_html}</div>'
    )


def display_issue_catalogue() -> None:
    """Print the supported issue catalogue as HTML."""
    catalogue = [
        ("critical", "MISSING_BROADCAST", "Broadcast join recommended but not used",
         "Add BROADCAST hint: JOIN ... BROADCAST(t) or increase spark.sql.autoBroadcastJoinThreshold"),
        ("critical", "CARTESIAN_PRODUCT", "Join with no condition \u2014 cross join",
         "Add an explicit JOIN condition or restructure the query"),
        ("high", "FULL_TABLE_SCAN", "Scan without partition filter",
         "Add partition column to WHERE clause"),
        ("high", "SORT_MERGE_JOIN", "Sort-based join on large tables",
         "Use BROADCAST or SHUFFLE_HASH hint instead"),
        ("high", "MISSING_PUSHDOWN", "Filter applied after scan instead of during",
         "Rewrite filter to push into scan source"),
        ("medium", "REPEATED_SCAN", "Same table scanned multiple times",
         "Use CTE or cache the table"),
        ("medium", "WIDE_TRANSFORM", "Wide transformation without LIMIT",
         "Add LIMIT if applicable, or use AQE skew optimisation"),
        ("medium", "SKEW_INDICATOR", "Partition count variance suggests skew",
         "Enable AQE: spark.sql.adaptive.enabled=true"),
        ("info", "BROADCAST_USED", "Broadcast join is in use",
         "No action needed"),
        ("info", "PARTITION_PRUNED", "Partition pruning detected",
         "No action needed"),
    ]

    rows = ""
    for sev, code, issue, fix in catalogue:
        border = SEVERITY_BORDER.get(sev, "#ccc")
        sym = SEVERITY_SYMBOL.get(sev, "⚪")
        rows += (
            f"<tr>"
            f"<td style='padding:6px 10px;border:1px solid #e2e8f0;color:{border};font-weight:600'>{sym} {sev.upper()}</td>"
            f"<td style='padding:6px 10px;border:1px solid #e2e8f0;font-family:monospace;font-size:11px'>{code}</td>"
            f"<td style='padding:6px 10px;border:1px solid #e2e8f0'>{issue}</td>"
            f"<td style='padding:6px 10px;border:1px solid #e2e8f0;color:#475569'>{fix}</td>"
            f"</tr>"
        )

    html = (
        "<div style='font-family:-apple-system,sans-serif;font-size:13px;'>"
        "<h3>Supported Issue Detections</h3>"
        "<table style='border-collapse:collapse;width:100%'>"
        "<tr style='background:#f1f5f9;font-weight:600;text-align:left'>"
        "<th style='padding:6px 10px;border:1px solid #e2e8f0'>Severity</th>"
        "<th style='padding:6px 10px;border:1px solid #e2e8f0'>Code</th>"
        "<th style='padding:6px 10px;border:1px solid #e2e8f0'>Issue</th>"
        "<th style='padding:6px 10px;border:1px solid #e2e8f0'>Fix</th>"
        "</tr>"
        f"{rows}"
        "</table></div>"
    )

    from IPython.display import HTML, display
    display(HTML(html))