"""Tests for bottleneck detection in Physical Plan parsing."""

import pytest
from spark_query_analyzer.analyzer import parse_plan


class TestBottleneckDetector:
    """Test parser detection of physical plan anti-patterns."""

    def test_full_table_scan_top_level_star(self):
        """Top-level scan with * prefix and no filter triggers FULL_TABLE_SCAN."""
        plan = "== Physical Plan ==\n* Scan SparkCatalog events [event_id, user_id, event_type, timestamp]\n"
        result = parse_plan(plan, "SELECT * FROM events")
        codes = [f.code for f in result.findings]
        assert "FULL_TABLE_SCAN" in codes

    def test_full_table_scan_indent(self):
        """Indented scan with +- prefix and no inline filter triggers FULL_TABLE_SCAN."""
        plan = "== Physical Plan ==\n+- Scan SparkCatalog events [event_id, user_id]\n"
        result = parse_plan(plan, "SELECT * FROM events")
        codes = [f.code for f in result.findings]
        assert "FULL_TABLE_SCAN" in codes

    def test_full_table_scan_with_filter_line(self):
        """Scan line with inline [Filter...] does NOT trigger FULL_TABLE_SCAN."""
        plan = "== Physical Plan ==\n* Scan SparkCatalog events [Filter (active = true), event_id, user_id]\n"
        result = parse_plan(plan, "SELECT * FROM events WHERE active = true")
        codes = [f.code for f in result.findings]
        assert "FULL_TABLE_SCAN" not in codes

    def test_pushed_filters_on_separate_line(self):
        """!PushedFilters on separate line triggers FULL_TABLE_SCAN (parser can't connect it to scan)."""
        plan = "== Physical Plan ==\n* Scan SparkCatalog events [event_id, user_id]\n!PushedFilters: [IsNotNull(event_id)]\n"
        result = parse_plan(plan, "SELECT * FROM events")
        codes = [f.code for f in result.findings]
        # Known limitation: parser can't associate pushed filters on separate lines
        # This test documents current behavior (FULL_TABLE_SCAN fires)
        assert "FULL_TABLE_SCAN" in codes

    def test_sort_merge_join_detected(self):
        """Sort + Exchange before join produces SORT_MERGE_JOIN."""
        plan = (
            "== Physical Plan ==\n"
            "* SortMergeJoin [id]\n"
            "+- Exchange\n"
            "|  +- * Sort [id ASC]\n"
            "+- Exchange\n"
            "   +- * Sort [id ASC]\n"
        )
        result = parse_plan(plan, "SELECT * FROM events JOIN users ON events.id = users.id")
        codes = [f.code for f in result.findings]
        assert "SORT_MERGE_JOIN" in codes

    def test_repeated_table_scan(self):
        """Same table scanned 3+ times produces REPEATED_SCAN."""
        plan = (
            "== Physical Plan ==\n"
            "* Project [id]\n"
            "+- * Scan SparkCatalog events [id]\n"
            "+- * Filter (active = true)\n"
            "|  +- * Scan SparkCatalog events [id, active]\n"
            "+- * Sort\n"
            "   +- * Scan SparkCatalog events [id]\n"
        )
        result = parse_plan(plan, "SELECT id FROM events WHERE active = true")
        codes = [f.code for f in result.findings]
        assert "REPEATED_SCAN" in codes

    def test_repeated_table_count_not_triggered_at_two(self):
        """Table scanned twice does NOT trigger REPEATED_SCAN (threshold is 3+)."""
        plan = "== Physical Plan ==\n* Scan SparkCatalog events\n* Scan SparkCatalog events\n"
        result = parse_plan(plan, "SELECT * FROM events")
        codes = [f.code for f in result.findings]
        assert "REPEATED_SCAN" not in codes

    def test_broadcast_exchange_node_not_missing_broadcast(self):
        """When BroadcastExchange appears in the plan, MISSING_BROADCAST is suppressed."""
        plan = (
            "== Physical Plan ==\n"
            "* BroadcastHashJoin [id]\n"
            "+- Exchange (BroadcastCoordination)\n"
            "|  +- * Scan SparkCatalog events\n"
            "+- * Scan SparkCatalog users\n"
        )
        result = parse_plan(plan, "SELECT * FROM events JOIN users ON events.id = users.id")
        codes = [f.code for f in result.findings]
        assert "MISSING_BROADCAST" not in codes

    def test_empty_plan(self):
        """Empty plan should not crash."""
        result = parse_plan("", "SELECT 1")
        assert result.findings == []

    def test_skew_indicator_requires_specific_partition_diversity(self):
        """SKEW_INDICATOR requires >2 exchanges with >3 distinct partition counts."""
        plan = (
            "== Physical Plan ==\n"
            "* SortMergeJoin [id]\n"
            "+- Exchange (partition=8)\n"
            "|  +- * Scan SparkCatalog events\n"
            "+- Exchange (partition=16)\n"
            "   +- * Scan SparkCatalog users\n"
        )
        result = parse_plan(plan, "SELECT * FROM events JOIN users ON events.id = users.id")
        # Only 2 exchanges with 2 distinct partition values — doesn't meet
        # the threshold of >2 exchanges AND >3 distinct partition counts
        codes = [f.code for f in result.findings]
        assert "SKEW_INDICATOR" not in codes


class TestParsePlan:
    """Unit tests for parse_plan with clean fixtures."""

    @pytest.fixture
    def full_scan_plan(self):
        return (
            "== Physical Plan ==\n"
            "* Scan SparkCatalog events [event_id, user_id, event_type, timestamp]\n"
        )

    @pytest.fixture
    def sort_merge_plan(self):
        return (
            "== Physical Plan ==\n"
            "* SortMergeJoin [id]\n"
            "+- Exchange\n"
            "|  +- * Sort [id ASC]\n"
            "+- Exchange\n"
            "   +- * Sort [id ASC]\n"
        )

    @pytest.fixture
    def scan_with_filter(self):
        return (
            "== Physical Plan ==\n"
            "* Scan SparkCatalog events [Filter (active = true), event_id, user_id]\n"
        )

    def test_full_scan_finding(self, full_scan_plan):
        result = parse_plan(full_scan_plan, "SELECT * FROM events")
        assert any(f.code == "FULL_TABLE_SCAN" for f in result.findings)

    def test_scan_with_filter_not_flagged(self, scan_with_filter):
        result = parse_plan(scan_with_filter, "SELECT * FROM events WHERE active = true")
        codes = [f.code for f in result.findings]
        assert "FULL_TABLE_SCAN" not in codes

    def test_sort_merge_finding(self, sort_merge_plan):
        result = parse_plan(sort_merge_plan, "SELECT * FROM events JOIN users ON events.id = users.id")
        assert any(f.code == "SORT_MERGE_JOIN" for f in result.findings)