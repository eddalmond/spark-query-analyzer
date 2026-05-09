"""
Multi-Query Batch Analyser — F-06 of the spark-query-analyzer roadmap.

Accepts multiple SQL statements, analyses them together, and surfaces
cross-query optimisation opportunities:
- Shared table scans (same table scanned multiple times → cache candidate)
- Identical filter predicates across queries (CTE / temp view candidate)
- Large intermediate join results (materialise as Delta temp table)
- Repeated CTE definitions

Usage:
    %%analyze_batch
    SELECT * FROM fact_sales WHERE date = '2024-01-01';
    SELECT * FROM fact_sales WHERE region = 'UK';
    SELECT * FROM dim_product WHERE category = 'Ceramics';
"""

from dataclasses import dataclass, field
from typing import Optional
import re


@dataclass
class BatchFinding:
    severity: str  # "critical" | "high" | "medium" | "info"
    code: str
    message: str
    suggestion: str
    tables: list[str] = field(default_factory=list)
    queries: list[int] = field(default_factory=list)  # 1-based query indices


@dataclass
class TableScan:
    table: str
    query_index: int  # 1-based
    estimated_rows: Optional[int] = None
    has_filter: bool = False
    filter_columns: list[str] = field(default_factory=list)
    scan_node: str = ""


@dataclass
class BatchAnalysisResult:
    num_queries: int
    findings: list[BatchFinding]
    shared_scan_candidates: list[dict]  # tables that appear in >1 query
    cte_candidates: list[dict]  # identical predicates across queries
    cache_candidates: list[dict]  # high-scan tables worth materialising


def _split_queries(cell: str) -> list[str]:
    """Split cell on semicolons, strip, skip blanks."""
    queries = []
    for chunk in cell.split(";"):
        stripped = chunk.strip()
        if stripped:
            # Remove -- comment lines
            stripped = "\n".join(
                line for line in stripped.split("\n")
                if not line.strip().startswith("--")
            )
            queries.append(stripped)
    return queries


def _extract_table_from_scan_line(line: str) -> Optional[str]:
    """Extract table name from a Scan node line."""
    # Format: "Scan ... table_name [...]" or "Project [...] +- Scan ... table_name"
    m = re.search(r"Scan\s+(?:.*?\s+)?(\w+(?:\.\w+)?)", line, re.IGNORECASE)
    if m:
        name = m.group(1).split(".")[-1]
        if name.upper() not in ("none", "<unknown>", ""):
            return name
    return None


def _extract_filter_columns(plan_text: str) -> list[str]:
    """Extract column names from Filter predicates in the plan."""
    cols = set()
    for m in re.finditer(r"Filter\[(.*?)\]", plan_text, re.IGNORECASE):
        filter_expr = m.group(1)
        # Pull out column references (simple heuristic: bare identifiers)
        for col in re.findall(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b", filter_expr):
            if col.upper() not in ("AND", "OR", "NOT", "IS", "NULL", "TRUE", "FALSE", "IN", "LIKE"):
                cols.add(col)
    return list(cols)


def _run_explain(spark, sql: str) -> str:
    """Run EXPLAIN FORMATTED and return the plan text."""
    try:
        explained = spark.sql(f"EXPLAIN FORMATTED {sql}")
        return "\n".join(row[0] for row in explained.collect())
    except Exception:
        return ""


def _get_num_rows(plan_text: str) -> Optional[int]:
    """Extract num_rows from Scan nodes as a heuristic for data size."""
    matches = re.findall(r"num_rows=(\d+)", plan_text, re.IGNORECASE)
    return max(int(m) for m in matches) if matches else None


def _extract_cte_definitions(sql: str) -> dict[str, str]:
    """Extract CTE definitions (WITH clause) from a SQL query."""
    ctes = {}
    m = re.match(r"WITH\s+(.*)", sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return ctes
    body = m.group(1)
    # Split on non-WITH keywords that terminate CTE definitions
    # CTEs end before the final SELECT/INSERT/UPDATE/MERGE
    cte_pattern = re.compile(
        r"(\w+)\s+AS\s*\(\s*([\s\S]*?)\s*\)",
        re.IGNORECASE
    )
    for match in cte_pattern.finditer(body):
        name = match.group(1).strip()
        definition = match.group(2).strip()
        ctes[name] = definition
    return ctes


def analyse_batch(spark, cell: str) -> BatchAnalysisResult:
    """
    Run cross-query analysis on a multi-statement cell.
    Returns BatchAnalysisResult with findings and opportunity summaries.
    """
    queries = _split_queries(cell)
    if not queries:
        return BatchAnalysisResult(num_queries=0, findings=[], shared_scan_candidates=[], cte_candidates=[], cache_candidates=[])

    # Collect scans per query
    all_scans: list[TableScan] = []
    all_plans: list[str] = []
    all_ctes: list[dict[str, str]] = []

    for i, sql in enumerate(queries, start=1):
        plan = _run_explain(spark, sql)
        all_plans.append(plan)

        # Find Scan nodes
        scan_lines = [l.strip() for l in plan.split("\n") if "Scan" in l]
        for line in scan_lines:
            table = _extract_table_from_scan_line(line)
            if table:
                scan = TableScan(
                    table=table,
                    query_index=i,
                    estimated_rows=_get_num_rows(plan),
                    has_filter=bool(re.search(r"Filter", line, re.IGNORECASE)),
                    filter_columns=_extract_filter_columns(plan),
                    scan_node=line[:80],
                )
                all_scans.append(scan)

        all_ctes.append(_extract_cte_definitions(sql))

    # --- Shared Scan Detection ---
    from collections import defaultdict
    table_to_scans: dict[str, list[TableScan]] = defaultdict(list)
    for scan in all_scans:
        table_to_scans[scan.table].append(scan)

    shared_scan_candidates = []
    findings = []

    for table, scans in table_to_scans.items():
        if len(scans) > 1:
            query_indices = [s.query_index for s in scans]
            avg_rows = max((s.estimated_rows or 0) for s in scans)
            scan_count = len(scans)

            # Severity: high if large table scanned many times
            if avg_rows and avg_rows > 1_000_000 and scan_count >= 3:
                severity = "high"
            elif scan_count >= 3:
                severity = "medium"
            else:
                severity = "info"

            findings.append(BatchFinding(
                severity=severity,
                code="SHARED_SCAN",
                message=f"Table '{table}' is scanned {scan_count} times across this batch "
                        f"(in queries: {', '.join(f'#{q}' for q in query_indices)}). "
                        f"Estimated ~{avg_rows:,} rows per scan.",
                suggestion=f"Materialise before the batch: `{table}_df = spark.table('{table}'); {table}_df.cache()` "
                          f"or extract to a CTE: `WITH {table}_cte AS (SELECT * FROM {table}) SELECT * FROM {table}_cte` "
                          f"used in each query.",
                tables=[table],
                queries=query_indices,
            ))
            shared_scan_candidates.append({
                "table": table,
                "scan_count": scan_count,
                "query_indices": query_indices,
                "estimated_rows": avg_rows,
            })

    # --- Identical Filter Detection ---
    # Build a signature for each query's filter pattern per table
    filter_signatures: dict[str, list[tuple[int, frozenset]]] = defaultdict(list)
    for scan in all_scans:
        if scan.has_filter:
            sig = frozenset(scan.filter_columns)
            filter_signatures[scan.table].append((scan.query_index, sig))

    cte_candidates = []
    for table, sigs in filter_signatures.items():
        # Group queries by identical filter column set
        from collections import defaultdict as dd
        identical_groups: dict[frozenset, list[int]] = dd(list)
        for q_idx, sig in sigs:
            identical_groups[sig].append(q_idx)

        for sig, q_indices in identical_groups.items():
            if len(q_indices) >= 2:
                findings.append(BatchFinding(
                    severity="medium",
                    code="IDENTICAL_FILTER",
                    message=f"Queries {', '.join(f'#{q}' for q in q_indices)} apply identical filters on '{table}' "
                            f"using columns: {set(sig)}.",
                    suggestion=f"Extract the filtered table as a CTE: "
                              f"`WITH {table}_filtered AS (SELECT * FROM {table} WHERE <filter>) SELECT * FROM {table}_filtered` "
                              f"to avoid scanning '{table}' {len(q_indices)} times with the same predicate.",
                    tables=[table],
                    queries=q_indices,
                ))
                cte_candidates.append({
                    "table": table,
                    "filter_columns": list(sig),
                    "query_indices": q_indices,
                })

    # --- Repeated CTE Detection ---
    all_cte_defs: dict[str, list[tuple[int, str]]] = defaultdict(list)
    for i, ctes in enumerate(all_ctes, start=1):
        for name, definition in ctes.items():
            all_cte_defs[name].append((i, definition))

    for cte_name, occurrences in all_cte_defs.items():
        if len(occurrences) > 1:
            # Check if definitions are identical
            uniq_defs = list(set(d for _, d in occurrences))
            if len(uniq_defs) == 1:
                query_indices = [i for i, _ in occurrences]
                findings.append(BatchFinding(
                    severity="info",
                    code="REPEATED_CTE",
                    message=f"CTE '{cte_name}' is defined identically in {len(occurrences)} queries "
                            f"(queries: {', '.join(f'#{q}' for q in query_indices)}).",
                    suggestion=f"Move '{cte_name}' to a shared temp view at the top of the notebook: "
                              f"`{cte_name}_view = spark.sql('''{uniq_defs[0]}'''); {cte_name}_view.createOrReplaceTempView('{cte_name}')` "
                              f"and reference it directly in each query.",
                    tables=[],
                    queries=query_indices,
                ))

    # --- Cache Candidates: high-row scans with no shared scan finding ---
    cache_candidates = []
    for table, scans in table_to_scans.items():
        if len(scans) == 1:
            scan = scans[0]
            if scan.estimated_rows and scan.estimated_rows > 5_000_000:
                cache_candidates.append({
                    "table": table,
                    "estimated_rows": scan.estimated_rows,
                    "query_index": scan.query_index,
                })

    return BatchAnalysisResult(
        num_queries=len(queries),
        findings=findings,
        shared_scan_candidates=shared_scan_candidates,
        cte_candidates=cte_candidates,
        cache_candidates=cache_candidates,
    )


def format_batch_diagnostics(result: BatchAnalysisResult) -> str:
    """Render batch analysis results as HTML."""

    if not result.findings:
        header = (
            '<div style="padding:12px 16px;font-size:13px;color:#16a34a;">'
            '&#x2705; No cross-query optimisation opportunities detected.'
            '</div>'
        )
        return header

    header = (
        f'<div style="padding:10px 16px;background:#f8fafc;border-bottom:1px solid #e2e8f0;'
        f'font-size:12px;color:#64748b;">'
        f'&#x1F4CA; Batch Analysis &mdash; {result.num_queries} queries scanned'
        f'</div>'
    )

    findings_html = ""
    for f in result.findings:
        border = {
            "critical": "#dc2626",
            "high": "#ea580c",
            "medium": "#ca8a04",
            "info": "#16a34a",
        }.get(f.severity, "#ccc")
        sym = {"critical": "&#x1F534;", "high": "&#x1F7E0;", "medium": "&#x1F7E1;", "info": "&#x2705;"}.get(f.severity, "&#x26AA;")
        label = f.severity.upper()
        queries_str = f"Queries: {', '.join(f'#{q}' for q in f.queries)}" if f.queries else ""
        tables_str = f"Tables: {', '.join(f'`{t}`' for t in f.tables)}" if f.tables else ""

        findings_html += (
            f'<div style="border-left:4px solid {border};padding:10px 14px;border-bottom:1px solid #f1f5f9;">'
            f'<div style="font-weight:600;font-size:12px;color:{border};">{sym} {label} &mdash; {f.code}</div>'
            f'<div style="font-size:13px;color:#1e293b;margin-top:4px;">{f.message}</div>'
            f'<div style="font-size:11px;color:#64748b;margin-top:2px;">{queries_str} &nbsp;&middot;&nbsp; {tables_str}</div>'
            f'<div style="margin-top:6px;padding:8px;background:#f8fafc;border-radius:6px;font-size:12px;color:#475569;">'
            f'<strong style="color:#0f172a;">&#x2192;</strong> {f.suggestion}</div>'
            f'</div>'
        )

    # Summary chips
    shared = len(result.shared_scan_candidates)
    cte = len(result.cte_candidates)
    cache = len(result.cache_candidates)
    summary = (
        f'<div style="padding:8px 14px;background:#f1f5f9;font-size:11px;color:#64748b;display:flex;gap:12px;">'
        f'{shared} shared scan{"" if shared == 1 else "s"} &nbsp;&middot;&nbsp;'
        f'{cte} CTE candidate{"" if cte == 1 else "s"} &nbsp;&middot;&nbsp;'
        f'{cache} cache candidate{"" if cache == 1 else "s"}'
        f'</div>'
    )

    return f'<div style="border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;margin-top:8px;">{header}{summary}{findings_html}</div>'