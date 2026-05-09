"""
IPython cell magic registration for %analyze.
"""

from IPython.core.magic import register_cell_magic
from spark_query_analyzer.analyzer import run_analysis


def register_analyze_magic():
    """Call once per notebook session to register the %analyze magic."""
    @register_cell_magic
    def analyze(line, cell):
        """Cell magic: put %analyze on the first line, SQL query below."""
        spark = _get_spark()
        if spark is None:
            raise RuntimeError(
                "Could not acquire SparkSession. "
                "Make sure this notebook is attached to a cluster with Spark >= 3.3."
            )
        return run_analysis(spark, cell.strip(), line.strip())


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