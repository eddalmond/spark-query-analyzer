"""
IPython cell magic registration for %analyze.
"""

from IPython.core.magic import register_cell_magic
from spark_query_analyzer.analyzer import run_analysis, Finding


def register_analyze_magic():
    """Call once per notebook session to register the %analyze magic."""
    @register_cell_magic
    def analyze(line, cell):
        """Cell magic: put %analyze on the first line, SQL query below.

        Flags:
          --dry-run   : analyse plan only, no query execution (default)
          --execute   : execute query and include post-execution skew analysis (F-03)
        """
        spark = _get_spark()
        if spark is None:
            raise RuntimeError(
                "Could not acquire SparkSession. "
                "Make sure this notebook is attached to a cluster with Spark >= 3.3."
            )

        # Parse flags
        dry_run = True
        line_stripped = line.strip()
        if line_stripped:
            tokens = line_stripped.split()
            for token in tokens:
                token_lower = token.lower()
                if token_lower == "--dry-run":
                    dry_run = True
                elif token_lower == "--execute":
                    dry_run = False

        return run_analysis(spark, cell.strip(), line.strip(), full_cell=cell, dry_run=dry_run)


def _get_spark():
    """Grab the active SparkSession from the Databricks IPython namespace."""
    try:
        from IPython import get_ipython
        ip = get_ipython()
        if ip is None:
            return None
        ns = ip.user_ns
        # Databricks injects spark as 'spark' in user namespace
        if "spark" in ns:
            return ns["spark"]
        # Fallback: try SparkSession.getActiveSession()
        try:
            from pyspark.sql import SparkSession
            return SparkSession.getActiveSession()
        except Exception:
            return None
    except Exception:
        return None