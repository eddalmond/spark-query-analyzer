"""
Python Anti-Pattern Scanner — F-04 of the spark-query-analyzer roadmap.

Scans the full cell content (Python + SQL) for PySpark/Python patterns that bypass
Catalyst optimisation or cause common performance anti-patterns. Uses Python's ast
module to parse without executing the code.
"""

import ast
import re
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PyFinding:
    severity: str  # "critical" | "high" | "medium" | "info"
    code: str
    message: str
    line: int
    suggestion: str
    detail: Optional[str] = None


class PythonAntiPatternScanner(ast.NodeVisitor):
    """
    Traverses the cell's Python AST and records performance anti-patterns.
    Does not execute the code — only inspects the AST for known problematic patterns.
    """

    def __init__(self, source: str):
        self.source = source
        self.findings: list[PyFinding] = []
        self._collect_count = 0

    def scan(self) -> list[PyFinding]:
        """Parse the source and return all findings."""
        try:
            tree = ast.parse(self.source)
            self.visit(tree)
        except SyntaxError:
            self._regex_scan()
        return self.findings

    # ── UDF detection ─────────────────────────────────────────────────

    def visit_FunctionDef(self, node: ast.FunctionDef):
        for decorator in node.decorator_list:
            dec_name = self._get_decorator_name(decorator)
            if dec_name in ("udf", "pandas_udf", "f"):
                self.findings.append(PyFinding(
                    severity="critical",
                    code="PYTHON_UDF",
                    message=f"Python UDF '@{dec_name}' on function '{node.name}'. "
                            "Row-by-row Python execution bypasses Catalyst and Photon — "
                            "expect 10-100x slower than native Spark SQL.",
                    line=node.lineno,
                    suggestion="Replace with F.col expressions, a Pandas UDF, "
                               "or a SQL expression registered via spark.udf.register().",
                    detail=f"Function: {node.name} | Decorator: @{dec_name}",
                ))
                break
        self.generic_visit(node)

    visit_AsyncFunctionDef = visit_FunctionDef

    def _get_decorator_name(self, node: ast.expr) -> str:
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Call):
            return self._get_decorator_name(node.func)
        if isinstance(node, ast.Attribute):
            return node.attr
        return ""

    # ── UDF register and Call-based detections ────────────────────────

    def visit_Call(self, node: ast.Call):
        # Resolve function name
        func_name = ""
        if isinstance(node.func, ast.Attribute):
            func_name = node.func.attr
        elif isinstance(node.func, ast.Name):
            func_name = node.func.id

        # UDF register
        if func_name in ("udf", "pandas_udf", "register") and node.args:
            self.findings.append(PyFinding(
                severity="critical",
                code="PYTHON_UDF_REGISTER",
                message="UDF registered via udf() or spark.udf.register(). "
                        "This wraps a Python function as a row-by-row UDF — "
                        "no Catalyst optimisation, no Photon.",
                line=node.lineno,
                suggestion="Use spark.udf.register() with a SQL expression, "
                           "or F.col-based expressions instead.",
                detail=func_name,
            ))

        # .collect() / .toPandas() — driver pull
        if func_name in ("collect", "toPandas"):
            self._collect_count += 1
            self.findings.append(PyFinding(
                severity="critical",
                code="DRIVER_PULL",
                message=f".{func_name}() pulls all rows to the driver — "
                        "OOM risk on large result sets.",
                line=node.lineno,
                suggestion="Use display() for preview, or write to Delta/Parquet.",
                detail=f"Method: .{func_name}()",
            ))

        # .take() / .first() / .tail() — action triggers
        if func_name in ("take", "first", "tail"):
            self.findings.append(PyFinding(
                severity="medium",
                code="ACTION_TRIGGER",
                message=f".{func_name}() is a Spark action — executes the plan immediately.",
                line=node.lineno,
                suggestion="For repeated access, cache the DataFrame first.",
                detail=f"Action: .{func_name}()",
            ))

        self.generic_visit(node)

    # ── RDD usage ─────────────────────────────────────────────────────

    def visit_Attribute(self, node: ast.Attribute):
        attr = node.attr
        # .rdd access
        if attr == "rdd":
            self.findings.append(PyFinding(
                severity="critical",
                code="RDD_USAGE",
                message=f".rdd accessed — converts DataFrame to RDD, "
                        "losing all Catalyst optimisation and Photon acceleration.",
                line=node.lineno,
                suggestion="Use DataFrame operations: df.filter, df.select, df.withColumn.",
                detail=".rdd",
            ))
        # .map/.filter/.reduce on RDD
        if attr in ("map", "flatMap", "filter", "reduce", "fold", "aggregate"):
            # Check if this is on an RDD (value.attr == 'rdd' means rdd.map etc.)
            if isinstance(node.value, ast.Attribute) and node.value.attr == "rdd":
                self.findings.append(PyFinding(
                    severity="critical",
                    code="RDD_OPERATION",
                    message=f".{attr}() on RDD — bypasses Catalyst completely. "
                            "Runs as Python lambda on JVM, not as optimised plan node.",
                    line=node.lineno,
                    suggestion="Rewrite using DataFrame API: df.filter(), df.select(), "
                               "df.groupBy().agg() instead of rdd.map(lambda ...).",
                    detail=f"RDD.{attr}()",
                ))
        self.generic_visit(node)

    # ── Repeated count() detection via CountCollector ─────────────────

    def _post_scan_count_check(self):
        """Called after scan if we need to check count() repetition."""
        pass

    def _regex_scan(self):
        """Fallback when AST parsing fails on mixed Python/SQL cells."""
        lines = self.source.split("\n")
        count_calls = []

        for i, line in enumerate(lines, 1):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue

            # @udf / @pandas_udf
            if re.match(r"@\w*udf", stripped):
                self.findings.append(PyFinding(
                    severity="critical",
                    code="PYTHON_UDF",
                    message="UDF decorator detected — row-by-row Python execution, no Catalyst.",
                    line=i,
                    suggestion="Replace with F.col expressions or Pandas UDF.",
                    detail=stripped,
                ))

            # .rdd
            if ".rdd" in stripped:
                self.findings.append(PyFinding(
                    severity="critical",
                    code="RDD_USAGE",
                    message=".rdd access — converts DataFrame to RDD, loses all optimisation.",
                    line=i,
                    suggestion="Use DataFrame API.",
                    detail=stripped,
                ))

            # .collect() / .toPandas()
            if re.search(r"\.(collect|toPandas)\s*\(", stripped):
                self.findings.append(PyFinding(
                    severity="critical",
                    code="DRIVER_PULL",
                    message=".collect() or .toPandas() pulls data to driver — OOM risk.",
                    line=i,
                    suggestion="Use display() or write to Delta.",
                    detail=stripped,
                ))

            # .count()
            if re.search(r"\.count\s*\(\)", stripped):
                count_calls.append(i)
                self.findings.append(PyFinding(
                    severity="medium",
                    code="REPEATED_COUNT",
                    message=".count() triggers a Spark job — repeated calls cause repeated full scans.",
                    line=i,
                    suggestion="Cache the DataFrame first: df.cache()",
                    detail=stripped,
                ))

            # pandas import
            if re.match(r"(import|from)\s+pandas", stripped):
                self.findings.append(PyFinding(
                    severity="medium",
                    code="SINGLE_THREADED_PANDAS",
                    message="pandas imported — single-threaded on driver, bypasses Spark parallelism.",
                    line=i,
                    suggestion="Use pyspark.pandas (Koalas) for distributed execution.",
                    detail=stripped,
                ))


def scan_cell(cell_content: str, sql_lines: set[int] = None) -> list[PyFinding]:
    """
    Scan the full cell content for Python performance anti-patterns.
    SQL portions are excluded from analysis.
    """
    if sql_lines is None:
        sql_lines = _detect_sql_lines(cell_content)

    # Remove SQL lines before scanning
    py_only_lines = []
    sql_line_set = set(sql_lines)
    for i, line in enumerate(cell_content.split("\n"), 1):
        if i in sql_line_set:
            continue
        py_only_lines.append(line)

    py_source = "\n".join(py_only_lines)
    if not py_source.strip():
        return []

    scanner = PythonAntiPatternScanner(py_source)
    findings = scanner.scan()

    # Check for repeated .count() via AST
    try:
        tree = ast.parse(py_source)
        count_calls = 0
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Attribute) and node.func.attr == "count":
                    count_calls += 1
        if count_calls >= 3:
            findings.append(PyFinding(
                severity="medium",
                code="REPEATED_COUNT",
                message=f"{count_calls} x .count() calls detected — each triggers a separate Spark job.",
                line=0,
                suggestion="Cache intermediate DataFrames before counting.",
                detail=f"count() calls: {count_calls}",
            ))
    except SyntaxError:
        pass

    return sorted(findings, key=lambda f: f.line)


def _detect_sql_lines(cell_content: str) -> set[int]:
    """Heuristic detection of SQL lines within a mixed Python/SQL cell."""
    lines = cell_content.split("\n")
    sql_lines = set()
    sql_keywords = {
        "SELECT", "FROM", "JOIN", "WHERE", "GROUP", "ORDER", "HAVING",
        "INSERT", "UPDATE", "DELETE", "WITH", "CREATE", "DROP",
        "ALTER", "EXPLAIN", "DESCRIBE", "SHOW", "SET", "USE",
    }
    for i, line in enumerate(lines, 1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        first_word = stripped.split()[0].upper() if stripped else ""
        if first_word in sql_keywords:
            sql_lines.add(i)
    return sql_lines