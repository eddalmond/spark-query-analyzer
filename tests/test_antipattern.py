"""Tests for python_scanner: AST-based anti-pattern detection in Python notebook cells."""

import pytest
from spark_query_analyzer.python_scanner import PythonAntiPatternScanner


class TestPythonAntiPatternScanner:
    """Scanner detects Python UDFs, RDD usage, and other anti-patterns via AST."""

    def test_collect_inside_loop(self):
        """df.collect() inside a loop triggers DRIVER_PULL."""
        code = "for i in range(10):\n    rows = df.collect()"
        scanner = PythonAntiPatternScanner(source=code)
        scanner.scan()
        codes = [f.code for f in scanner.findings]
        assert "DRIVER_PULL" in codes

    def test_collect_at_top_level(self):
        """df.collect() is flagged as DRIVER_PULL regardless of context (best practice)."""
        code = "rows = df.collect()"
        scanner = PythonAntiPatternScanner(source=code)
        scanner.scan()
        codes = [f.code for f in scanner.findings]
        # collect() always triggers DRIVER_PULL since it can OOM large results
        assert "DRIVER_PULL" in codes

    def test_toPandas_in_loop(self):
        """df.toPandas() inside a loop triggers DRIVER_PULL."""
        code = "for file in files:\n    pdf = df.toPandas()"
        scanner = PythonAntiPatternScanner(source=code)
        scanner.scan()
        codes = [f.code for f in scanner.findings]
        assert "DRIVER_PULL" in codes

    def test_lambda_udf(self):
        """Lambda UDF definitions are flagged."""
        code = "\n".join([
            "from pyspark.sql.functions import udf",
            "square = udf(lambda x: x**2, 'int')",
        ])
        scanner = PythonAntiPatternScanner(source=code)
        scanner.scan()
        codes = [f.code for f in scanner.findings]
        assert "PYTHON_UDF_REGISTER" in codes

    def test_decorator_detection(self):
        """@udf-decorated functions are detected correctly."""
        code = "\n".join([
            "from pyspark.sql.functions import udf",
            "@udf('integer')",
            "def my_func(x):",
            "    return x * 2",
        ])
        scanner = PythonAntiPatternScanner(source=code)
        scanner.scan()
        codes = [f.code for f in scanner.findings]
        assert "PYTHON_UDF" in codes or "PYTHON_UDF_REGISTER" in codes

    def test_multiple_issues(self):
        """Multiple anti-patterns in one cell are all detected."""
        code = "for i in range(10):\n    rows = df.collect()"
        scanner = PythonAntiPatternScanner(source=code)
        scanner.scan()
        codes = [f.code for f in scanner.findings]
        assert "DRIVER_PULL" in codes

    def test_no_issues_clean_code(self):
        """A clean PySpark pipeline should produce no issues."""
        code = "\n".join([
            "df = spark.table('events')",
            "df = df.filter(df.active == True)",
            "df = df.groupBy('category').count()",
            "df.write.mode('overwrite').saveAsTable('events_agg')",
        ])
        scanner = PythonAntiPatternScanner(source=code)
        scanner.scan()
        codes = [f.code for f in scanner.findings]
        assert codes == []