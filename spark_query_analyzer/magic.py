"""
IPython cell magic registration for %analyze and %%analyze_batch.
"""

from IPython.core.magic import register_cell_magic
from spark_query_analyzer.analyzer import run_analysis, Finding


def register_analyze_magic():
    """Call once per notebook session to register the %analyze magic."""
    @register_cell_magic
    def analyze(line, cell):
        """Cell magic: put %analyze on the first line, SQL query below.

        Flags:
          --dry-run           : analyse plan only, no query execution (default)
          --execute          : execute query and include post-execution skew analysis (F-03)
          --export <path>     : export full HTML report to DBFS/mounted path (F-14)
        """
        spark = _get_spark()
        if spark is None:
            raise RuntimeError(
                "Could not acquire SparkSession. "
                "Make sure this notebook is attached to a cluster with Spark >= 3.3."
            )

        # Parse flags
        dry_run = True
        export_path = ""
        line_stripped = line.strip()
        if line_stripped:
            tokens = line_stripped.split()
            i = 0
            while i < len(tokens):
                token = tokens[i].lower()
                if token == "--dry-run":
                    dry_run = True
                elif token == "--execute":
                    dry_run = False
                elif token == "--export" and i + 1 < len(tokens):
                    export_path = tokens[i + 1]
                    i += 1
                i += 1

        return run_analysis(spark, cell.strip(), line.strip(), full_cell=cell, dry_run=dry_run, export_path=export_path)


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


def register_analyze_batch_magic():
    """Register the %%analyze_batch block magic. Call after register_analyze_magic()."""
    from spark_query_analyzer.cross_query_optimiser import analyse_batch, format_batch_diagnostics

    @register_cell_magic
    def analyze_batch(line, cell):
        """Block magic: put %%analyze_batch at the top of a cell with multiple SQL statements.

        Analyses multiple SQL statements together, finds shared scan opportunities,
        identical filter patterns (CTE candidates), and cache candidates.

        Example:
            %%analyze_batch
            SELECT * FROM fact_sales WHERE date = '2024-01-01';
            SELECT * FROM fact_sales WHERE region = 'UK';
            SELECT * FROM dim_product;
        """
        spark = _get_spark()
        if spark is None:
            raise RuntimeError(
                "Could not acquire SparkSession. "
                "Make sure this notebook is attached to a cluster with Spark >= 3.3."
            )

        result = analyse_batch(spark, cell)
        html = format_batch_diagnostics(result)
        from IPython.display import HTML, display
        display(HTML(html))
        return ""