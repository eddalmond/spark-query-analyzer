"""Pytest fixtures for spark-query-analyzer tests."""

from unittest.mock import MagicMock


def mock_spark():
    """Minimal mock SparkSession for unit tests."""
    spark = MagicMock()
    spark.sparkContext.uiWebUrl = "http://spark-master:4040"
    spark.sparkContext.statusTracker.return_value.getActiveStageIds.return_value = []
    spark.sparkContext.statusTracker.return_value.getJobGroupForStage.return_value = None
    spark.sql.return_value._jdf = MagicMock()
    spark.sql.return_value._jdf.queryExecution.return_value.explainString.return_value = ""
    return spark


def mock_spark_serverless():
    """Mock SparkSession for serverless (no UI available)."""
    spark = mock_spark()
    spark.sparkContext.uiWebUrl = None
    return spark


def load_fixture(name):
    """Load a test fixture file from tests/fixtures/."""
    import pathlib
    return pathlib.Path(__file__).parent.joinpath("fixtures", name).read_text()