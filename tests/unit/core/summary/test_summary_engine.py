# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for Base Summary Engine.

Tests verify that the summary engine correctly aggregates raw check results
into a standardized, policy-ready summary format.
"""

import allure

from tests.constants import AllureFeature, AllureSeverity, AllureStory, AllureTag

from neco_dq.core.summary.summary_engine import BaseSummaryEngine


@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.SUMMARY_ENGINE)
class TestBaseSummaryEngineSummarize:
    """Tests for BaseSummaryEngine.summarize() method."""

    @allure.title("Summarize empty results")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_summarize_empty_results(self):
        """Summary should return empty structure for no results."""
        with allure.step("Setup test data"):
            engine = BaseSummaryEngine()
            raw_results = []

        with allure.step("Execute summarize"):
            summary = engine.summarize(raw_results)

        with allure.step("Verify: Summarize empty results"):
            assert summary["status"] == "PASS"
            assert summary["total_checks"] == 0
            assert summary["passed"] == 0
            assert summary["failed"] == 0

    @allure.title("Summarize all passing checks")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_summarize_all_passed(self):
        """Summary should show PASS status when all checks pass."""
        with allure.step("Setup test data"):
            engine = BaseSummaryEngine()
            raw_results = [
                {"engine": "bronze", "table": "t1", "check_name": "not_null", "passed": True},
                {"engine": "bronze", "table": "t1", "check_name": "freshness", "passed": True},
                {"engine": "bronze", "table": "t2", "check_name": "not_null", "passed": True},
            ]

        with allure.step("Execute summarize"):
            summary = engine.summarize(raw_results)

        with allure.step("Verify: Summarize all passing checks"):
            assert summary["status"] == "PASS"
            assert summary["total_checks"] == 3
            assert summary["passed"] == 3
            assert summary["failed"] == 0
            assert summary["tables_processed"] == 2

    @allure.title("Summarize with failures")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_summarize_with_failures(self):
        """Summary should show FAIL status when checks fail."""
        with allure.step("Setup test data"):
            engine = BaseSummaryEngine()
            raw_results = [
                {"engine": "silver", "table": "t1", "check_name": "idempotency", "passed": True},
                {
                    "engine": "silver",
                    "table": "t1",
                    "check_name": "not_null",
                    "column": "id",
                    "passed": False,
                    "severity": "error",
                },
                {
                    "engine": "silver",
                    "table": "t2",
                    "check_name": "freshness",
                    "passed": False,
                    "severity": "warning",
                },
            ]

        with allure.step("Execute summarize"):
            summary = engine.summarize(raw_results)

        with allure.step("Verify: Summarize with failures"):
            assert summary["status"] == "WARN"  # Has warning severity
            assert summary["total_checks"] == 3
            assert summary["passed"] == 1
            assert summary["failed"] == 2
            assert summary["tables_failed"] == 2
            assert summary["columns_with_issues"] == 1
            assert summary["failed_by_severity"]["error"] == 1
            assert summary["failed_by_severity"]["warning"] == 1

    @allure.title("Summarize with blocking failures")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_summarize_blocking_failures(self):
        """Summary should count blocking failures correctly."""
        with allure.step("Setup test data"):
            engine = BaseSummaryEngine()
            raw_results = [
                {"engine": "gold", "table": "t1", "check_name": "uniqueness", "passed": True},
                {
                    "engine": "gold",
                    "table": "t1",
                    "check_name": "completeness",
                    "passed": False,
                    "blocking": True,
                    "severity": "critical",
                },
            ]

        with allure.step("Execute summarize"):
            summary = engine.summarize(raw_results)

        with allure.step("Verify: Summarize with blocking failures"):
            assert summary["status"] == "FAIL"
            assert summary["blocking_failures"] == 1


@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.SUMMARY_ENGINE)
class TestBaseSummaryEngineHelpers:
    """Tests for BaseSummaryEngine helper methods."""

    @allure.title("Split results into passed and failed")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_split_results(self):
        """Should correctly split results into passed and failed lists."""
        with allure.step("Setup test data"):
            engine = BaseSummaryEngine()
            raw_results = [
                {"passed": True, "check_name": "c1"},
                {"passed": False, "check_name": "c2"},
                {"passed": True, "check_name": "c3"},
                {"passed": False, "check_name": "c4"},
            ]

        with allure.step("Execute split results"):
            passed, failed = engine._split_results(raw_results)

        with allure.step("Verify: Split results into passed and failed"):
            assert len(passed) == 2
            assert len(failed) == 2
            assert all(r["passed"] for r in passed)
            assert all(not r["passed"] for r in failed)

    @allure.title("Extract entities from results")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_extract_entities(self):
        """Should extract unique tables and columns from results."""
        with allure.step("Setup test data"):
            engine = BaseSummaryEngine()
            raw_results = [
                {"table": "t1", "column": "c1", "passed": True},
                {"table": "t1", "column": "c2", "passed": False},
                {"table": "t2", "column": "c1", "passed": False},
            ]
            failed = [r for r in raw_results if not r["passed"]]

        with allure.step("Execute extract entities"):
            tables, tables_failed, columns_failed = engine._extract_entities(raw_results, failed)

        with allure.step("Verify: Extract entities from results"):
            assert tables == {"t1", "t2"}
            assert tables_failed == {"t1", "t2"}
            assert columns_failed == {"c2", "c1"}

    @allure.title("Build issues by type")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_build_issues_by_type(self):
        """Should group failures by check type."""
        with allure.step("Setup test data"):
            engine = BaseSummaryEngine()
            failed = [
                {"check_name": "not_null", "column": "id"},
                {"check_name": "not_null", "column": "name"},
                {"check_name": "freshness", "column": "updated_at"},
            ]

        with allure.step("Execute build issues by type"):
            issues = engine._build_issues_by_type(failed)

        with allure.step("Verify: Build issues by type"):
            assert "not_null" in issues
            assert issues["not_null"]["count"] == 2
            assert "id" in issues["not_null"]["columns"]
            assert "name" in issues["not_null"]["columns"]
            assert issues["freshness"]["count"] == 1

    @allure.title("Build failure details preserves check-table pairing")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_build_failure_details(self):
        """Should pair each check with the table it actually failed on (no cartesian product)."""
        with allure.step("Setup test data"):
            engine = BaseSummaryEngine()
            failed = [
                {"check_name": "not_null", "column": "id", "table_fullname": "cat.sch.orders"},
                {"check_name": "not_null", "column": "name", "table_fullname": "cat.sch.orders"},
                {"check_name": "freshness", "column": "updated_at", "table_fullname": "cat.sch.events"},
            ]

        with allure.step("Execute build failure details"):
            details = engine._build_failure_details(failed)

        with allure.step("Verify: each check is linked to its own table only"):
            by_pair = {(d["check"], d["table"]): d for d in details}
            # not_null failed only on orders (2 columns), freshness only on events
            assert set(by_pair) == {("not_null", "orders"), ("freshness", "events")}
            assert by_pair[("not_null", "orders")]["count"] == 2
            assert "id" in by_pair[("not_null", "orders")]["columns"]
            assert "name" in by_pair[("not_null", "orders")]["columns"]
            assert by_pair[("freshness", "events")]["count"] == 1
            # Sorted by count descending
            assert details[0]["count"] >= details[-1]["count"]

    @allure.title("Count failed by severity")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_count_failed_by_severity(self):
        """Should count failures grouped by severity."""
        with allure.step("Setup test data"):
            engine = BaseSummaryEngine()
            failed = [
                {"severity": "critical"},
                {"severity": "error"},
                {"severity": "error"},
                {"severity": "warning"},
            ]

        with allure.step("Execute count failed by severity"):
            counts = engine._count_failed_by_severity(failed)

        with allure.step("Verify: Count failed by severity"):
            assert counts["critical"] == 1
            assert counts["error"] == 2
            assert counts["warning"] == 1

    @allure.title("Detect engine name from results")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_detect_engine_name(self):
        """Should detect engine name from first result."""
        with allure.step("Setup test data"):
            engine = BaseSummaryEngine()
            results = [{"engine": "gold", "check_name": "c1"}]

        with allure.step("Execute detect engine name"):
            name = engine._detect_engine_name(results)

        with allure.step("Verify: Detect engine name from results"):
            assert name == "gold"

    @allure.title("Detect engine name returns unknown for empty")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_detect_engine_name_empty(self):
        """Should return 'unknown' for empty results."""
        with allure.step("Setup test data"):
            engine = BaseSummaryEngine()
            results = []

        with allure.step("Execute detect engine name"):
            name = engine._detect_engine_name(results)

        with allure.step("Verify: Detect engine name returns unknown for empty"):
            assert name == "unknown"

    @allure.title("Resolve status PASS")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_resolve_status_pass(self):
        """Should resolve to PASS when no failures."""
        with allure.step("Setup test data"):
            engine = BaseSummaryEngine()

        with allure.step("Execute resolve status"):
            status = engine._resolve_status(failed=[], blocking_failures=0)

        with allure.step("Verify: Resolve status PASS"):
            assert status == "PASS"

    @allure.title("Resolve status FAIL with blocking")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_resolve_status_fail_blocking(self):
        """Should resolve to FAIL when blocking failures exist."""
        with allure.step("Setup test data"):
            engine = BaseSummaryEngine()

        with allure.step("Execute resolve status"):
            status = engine._resolve_status(failed=[{"passed": False}], blocking_failures=1)

        with allure.step("Verify: Resolve status FAIL with blocking"):
            assert status == "FAIL"

    @allure.title("Resolve status WARN for non-blocking failures")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_resolve_status_warn_non_blocking(self):
        """Should resolve to WARN when failures exist but none are blocking."""
        with allure.step("Setup test data"):
            engine = BaseSummaryEngine()

        with allure.step("Execute resolve status"):
            status = engine._resolve_status(failed=[{"passed": False}], blocking_failures=0)

        with allure.step("Verify: Resolve status WARN for non-blocking failures"):
            assert status == "WARN"
