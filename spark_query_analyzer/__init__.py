"""
Spark Query Analyzer — cell magic for Databricks notebooks.
Run `from spark_query_analyzer import register_analyze_magic; register_analyze_magic()` once per session.
"""

from spark_query_analyzer.magic import register_analyze_magic
from spark_query_analyzer.performance_monitor import monitor_performance

__all__ = ['register_analyze_magic', 'monitor_performance']
__version__ = '0.1.0'
