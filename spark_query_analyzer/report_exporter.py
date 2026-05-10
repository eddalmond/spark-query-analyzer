"""
F-14 · Standalone HTML Report Export

Allows saving the full diagnostic report as a self-contained HTML file
to DBFS or a mounted cloud storage path.

Usage:
    %analyze --export /dbfs/reports/query_report_2024_01_15.html
    %analyze --export /mnt/reports/analysis.html
"""

import datetime
import html
import uuid


def build_export_html(
    report_html: str,
    query: str,
    severity_counts: dict,
    narrative_banner: str = '',
) -> str:
    """
    Wrap the diagnostic HTML in a full self-contained HTML document
    with report header, metadata, and a collapsible EXPLAIN section.
    """
    timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')
    report_id = str(uuid.uuid4())[:8]

    total_findings = sum(severity_counts.values())
    counts_str = ' | '.join(f'{s.upper()}: {n}' for s, n in severity_counts.items() if n > 0)

    export_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Spark Query Analyzer Report — {timestamp}</title>
  <style>
    /* ── Reset & base ──────────────────────────────────────── */
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      background: #f8fafc;
      color: #1e293b;
      padding: 24px;
      font-size: 14px;
      line-height: 1.6;
    }}

    /* ── Report header ────────────────────────────────────── */
    .report-header {{
      background: linear-gradient(135deg, #0f172a 0%, #1e3a5f 100%);
      color: #f8fafc;
      border-radius: 12px;
      padding: 20px 24px;
      margin-bottom: 20px;
    }}
    .report-header h1 {{
      font-size: 18px;
      font-weight: 700;
      color: #f8fafc;
      margin-bottom: 8px;
    }}
    .report-meta {{
      display: flex;
      flex-wrap: wrap;
      gap: 16px;
      font-size: 12px;
      color: #94a3b8;
    }}
    .report-meta-item {{
      display: flex;
      align-items: center;
      gap: 4px;
    }}
    .report-meta-item strong {{ color: #cbd5e1; }}

    /* ── Query box ─────────────────────────────────────────── */
    .query-box {{
      background: #1e293b;
      color: #e2e8f0;
      border-radius: 8px;
      padding: 14px 16px;
      margin-bottom: 20px;
      font-family: 'Courier New', monospace;
      font-size: 12px;
      overflow-x: auto;
      white-space: pre-wrap;
      word-break: break-all;
    }}
    .query-label {{
      font-size: 11px;
      font-weight: 600;
      color: #64748b;
      letter-spacing: .05em;
      text-transform: uppercase;
      margin-bottom: 6px;
    }}

    /* ── Severity summary bar ──────────────────────────────── */
    .severity-summary {{
      display: flex;
      gap: 12px;
      margin-bottom: 20px;
      flex-wrap: wrap;
    }}
    .sev-chip {{
      display: inline-flex;
      align-items: center;
      gap: 4px;
      padding: 4px 10px;
      border-radius: 20px;
      font-size: 12px;
      font-weight: 600;
    }}
    .sev-critical {{ background: #dc2626; color: #fff; }}
    .sev-high      {{ background: #ea580c; color: #fff; }}
    .sev-medium    {{ background: #ca8a04; color: #fff; }}
    .sev-info      {{ background: #16a34a; color: #fff; }}

    /* ── Report card (the diagnostic HTML, injected) ───────── */
    .report-card {{
      background: #fff;
      border-radius: 10px;
      overflow: hidden;
      box-shadow: 0 1px 4px rgba(0,0,0,.08);
    }}

    /* ── EXPLAIN toggle ────────────────────────────────────── */
    .explain-toggle {{
      background: #f1f5f9;
      border: 1px solid #e2e8f0;
      border-radius: 8px;
      margin-bottom: 20px;
    }}
    .explain-toggle summary {{
      padding: 10px 14px;
      font-size: 12px;
      font-weight: 600;
      color: #475569;
      cursor: pointer;
      user-select: none;
      list-style: none;
      display: flex;
      align-items: center;
      gap: 6px;
    }}
    .explain-toggle summary::before {{
      content: "\\25B6";
      font-size: 10px;
      transition: transform .15s;
    }}
    .explain-toggle[open] summary::before {{
      transform: rotate(90deg);
    }}
    .explain-content {{
      padding: 12px 14px;
      font-family: 'Courier New', monospace;
      font-size: 11px;
      color: #475569;
      white-space: pre-wrap;
      border-top: 1px solid #e2e8f0;
      max-height: 400px;
      overflow-y: auto;
    }}

    /* ── Footer ────────────────────────────────────────────── */
    .report-footer {{
      text-align: center;
      margin-top: 24px;
      font-size: 11px;
      color: #94a3b8;
    }}
  </style>
</head>
<body>

<div class="report-header">
  <h1>&#x1F50D; Spark Query Analyzer Report</h1>
  <div class="report-meta">
    <div class="report-meta-item">
      <span>&#x1F4C5;</span>
      <span>Generated:</span>
      <strong>{timestamp}</strong>
    </div>
    <div class="report-meta-item">
      <span>&#x1F4CB;</span>
      <span>Report ID:</span>
      <strong>{report_id}</strong>
    </div>
    <div class="report-meta-item">
      <span>&#x26A0;</span>
      <span>Findings:</span>
      <strong>{total_findings} ({counts_str})</strong>
    </div>
  </div>
</div>

<div class="query-box">
  <div class="query-label">Analysed Query</div>
{html.escape(query)}
</div>

<div class="severity-summary">
  {
        ''.join(
            f'<span class="sev-chip sev-{sev}">{sym} {count} {sev.upper()}</span>'
            for sev, sym, count in [
                ('critical', '❌', severity_counts.get('critical', 0)),
                ('high', '🟠', severity_counts.get('high', 0)),
                ('medium', '🟡', severity_counts.get('medium', 0)),
                ('info', '✅', severity_counts.get('info', 0)),
            ]
            if count > 0
        )
    }
</div>

<div class="report-card">
  {narrative_banner}
  {report_html}
</div>

<details class="explain-toggle">
  <summary>&#x1F4CA; EXPLAIN FORMATTED Output</summary>
  <div class="explain-content" id="explain-output">
    (Injected by the export call — pass plan_text to export_report to populate this)
  </div>
</details>

<div class="report-footer">
  Generated by Spark Query Analyzer &mdash; eddalmond/spark-query-analyzer
</div>

</body>
</html>"""

    return export_html


def export_report(
    spark,
    report_html: str,
    query: str,
    plan_text: str,
    severity_counts: dict,
    narrative_banner: str = '',
    export_path: str = '/dbfs/reports/spark_analyzer_report.html',
) -> str:
    """
    Write the full HTML report to the specified path.

    Supports:
      - /dbfs/...   paths (Databricks DBFS)
      - /mnt/...    paths (mounted cloud storage)
      - file:///... absolute local paths (Docker / local testing)

    Returns the path the file was written to.
    """
    if not export_path:
        raise ValueError('export_path must be specified')

    # Normalise DBFS path for dbutils
    dbfs_path = export_path
    if dbfs_path.startswith('/dbfs/'):
        dbfs_path = dbfs_path[5:]  # strip /dbfs prefix for dbutils

    full_html = build_export_html(report_html, query, severity_counts, narrative_banner)

    # Inject plan text into the explain toggle
    if plan_text:
        escaped_plan = plan_text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
        full_html = full_html.replace(
            'id="explain-output">\n    (Injected',
            f'id="explain-output">\n{escaped_plan}\n    (Injected',
        )

    try:
        # Try Databricks dbutils first
        import dbutils

        dbutils.fs.put(dbfs_path, full_html, overwrite=True)
        return export_path
    except (ImportError, NameError):
        pass

    # Try Python file I/O (local / Docker)
    if export_path.startswith('file://'):
        local_path = export_path[7:]
    elif export_path.startswith('/'):
        local_path = export_path
    else:
        local_path = export_path

    try:
        import os

        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, 'w', encoding='utf-8') as f:
            f.write(full_html)
        return export_path
    except OSError as e:
        raise RuntimeError(
            f"Could not write to export path '{export_path}'. "
            f'Ensure the path is writable or use a DBFS/mnt path on Databricks. Error: {e}'
        ) from e
