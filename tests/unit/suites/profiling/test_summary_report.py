# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import allure
import pytest

from tests.constants import AllureFeature, AllureSeverity, AllureStory, AllureTag

pytestmark = pytest.mark.spark

_SUGGEST_RESULTS = [
    {
        "catalog": "c",
        "schema": "s",
        "table": "t1",
        "run_timestamp": "2024-01-01T00:00:00",
        "row_count": 100,
        "suggestions": {
            "table": "c.s.t1",
            "row_count": 100,
            "column_count": 5,
            "bronze": {
                "catalog": "c",
                "schema": "s",
                "table": "t1",
                "checks": {
                    "check1": {"enabled": True, "_confidence": "high"},
                    "check2": {"enabled": True, "_confidence": "medium"},
                },
            },
            "silver": {
                "catalog": "c",
                "schema": "s",
                "table": "t1",
                "checks": {"check3": {"enabled": False, "_confidence": "low"}},
            },
        },
        "column_stats": {
            "order_id": {"type": "string", "null_pct": 0.0, "distinct_count": 100, "distinct_ratio": 1.0},
            "email": {"type": "string", "null_pct": 5.0, "distinct_count": 90, "distinct_ratio": 0.9},
            "shipping_date": {"type": "date", "null_pct": 45.0, "distinct_count": 30, "distinct_ratio": 0.3},
            "status": {"type": "string", "null_pct": 0.0, "distinct_count": 3, "distinct_ratio": 0.03},
            "payload": {
                "type": "string",
                "null_pct": 0.0,
                "distinct_count": 100,
                "distinct_ratio": 1.0,
                "is_json": True,
            },
        },
    },
    {
        "catalog": "c",
        "schema": "s",
        "table": "t2",
        "run_timestamp": "2024-01-01T00:00:00",
        "row_count": 200,
        "suggestions": {
            "table": "c.s.t2",
            "row_count": 200,
            "column_count": 8,
            "gold": {
                "catalog": "c",
                "schema": "s",
                "table": "t2",
                "checks": {"check4": {"enabled": True, "_confidence": "medium"}},
            },
        },
    },
]


@allure.feature(AllureFeature.PROFILING_SUITE)
@allure.story(AllureStory.PROFILING_SUMMARY)
class TestWriteSuggestSummary:
    @allure.title("_write_suggest_summary writes PROFILING_SUMMARY.md with correct content")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_write_suggest_summary_creates_file(self, make_suite, tmp_path):
        """
        Given a list of suggest_results with 2 tables,
        When _write_suggest_summary is called,
        Then PROFILING_SUMMARY.md is written and contains expected content.
        """
        suite = make_suite({})

        with allure.step("Call _write_suggest_summary"):
            result = suite._write_suggest_summary(_SUGGEST_RESULTS, str(tmp_path))

        with allure.step("Verify: PROFILING_SUMMARY.md was written"):
            summary_file = tmp_path / "PROFILING_SUMMARY.md"
            assert summary_file.exists()

        with allure.step("Verify: returned string matches file content"):
            assert result == summary_file.read_text()

        with allure.step("Verify: markdown header is present"):
            assert "# DQ Config Suggestions Summary" in result

        with allure.step("Verify: first table FQ name is present"):
            assert "c.s.t1" in result

        with allure.step("Verify: second table FQ name is present"):
            assert "c.s.t2" in result

        with allure.step("Verify: table count (2) is present in the overview"):
            assert "2" in result

        with allure.step("Verify: Overview shows enabled/disabled totals and layer breakdown"):
            # 3 checks enabled (check1, check2, check4), 1 disabled (check3), 4 total
            assert "4 —" in result
            assert "3 enabled" in result
            assert "1 disabled" in result
            assert "1 bronze" in result
            assert "1 gold" in result

        with allure.step("Verify: Tables by Layer groups by base layer"):
            assert "## Tables by Layer" in result
            # t1 has bronze+silver -> grouped under bronze (first layer in precedence order)
            assert "### Bronze (1 tables)" in result
            assert "### Gold (1 tables)" in result

        with allure.step("Verify: Checks by Suite has Enabled/Disabled columns"):
            assert "| Suite | Tables | Checks | Enabled | Disabled |" in result
            # silver: 1 check, disabled
            assert "| silver | 1 | 1 | 0 | 1 |" in result

        with allure.step("Verify: Disabled Checks section lists the disabled check grouped"):
            assert "## Disabled Checks to Review" in result
            assert "| check3 | silver | 1 |" in result

        with allure.step("Verify: Data Quality Observations flags null/PII"):
            assert "## Data Quality Observations" in result
            assert "High null ratio (45%)" in result  # shipping_date
            assert "shipping_date" in result
            assert "Possible PII" in result  # email
            assert "email" in result

        with allure.step("Verify: Schema Highlights section is present"):
            assert "## Schema Highlights" in result

        with allure.step("Verify: Quick Start and Review Checklist are present"):
            assert "## Quick Start" in result
            assert "from neco_dq.runner import run_dq" in result
            assert "## Review Checklist" in result

    @allure.title("_write_suggest_summary handles empty results gracefully")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.EDGE_CASE, AllureTag.SPARK)
    def test_write_suggest_summary_empty_results(self, make_suite, tmp_path):
        """
        Given an empty suggest_results list,
        When _write_suggest_summary is called,
        Then PROFILING_SUMMARY.md is written without errors and shows 0 tables.
        """
        suite = make_suite({})
        result = suite._write_suggest_summary([], str(tmp_path))
        summary_file = tmp_path / "PROFILING_SUMMARY.md"
        assert summary_file.exists()
        assert "# DQ Config Suggestions Summary" in result

    @allure.title("_write_suggest_summary skips results with error key")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_write_suggest_summary_skips_errors(self, make_suite, tmp_path):
        """
        Given a results list that includes an error entry,
        When _write_suggest_summary is called,
        Then the error result is excluded from the valid table count.
        """
        suite = make_suite({})
        results_with_error = [
            {"error": "table not found", "catalog": "x", "schema": "y", "table": "z"},
            *_SUGGEST_RESULTS,
        ]
        result = suite._write_suggest_summary(results_with_error, str(tmp_path))
        # The errored result must not appear in the tables section
        assert "x.y.z" not in result
        # The 2 valid results must still appear
        assert "c.s.t1" in result
        assert "c.s.t2" in result

    @allure.title("_write_suggest_summary groups repeated observations across tables")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_write_suggest_summary_groups_observations(self, make_suite, tmp_path):
        """
        Given multiple tables with the same column/issue (e.g. backup tables),
        When _write_suggest_summary is called,
        Then the Observations section shows '3 tables' instead of 3 separate rows.
        """
        suite = make_suite({})
        results = [
            {
                "catalog": "c",
                "schema": "s",
                "table": f"backup_{i}",
                "row_count": 10,
                "suggestions": {"bronze": {"checks": {"record_count": {"enabled": True}}}},
                "column_stats": {
                    "always_null": {"type": "string", "null_pct": 100.0, "distinct_count": 0, "distinct_ratio": 0.0},
                },
            }
            for i in range(3)
        ]
        result = suite._write_suggest_summary(results, str(tmp_path))
        assert "3 tables" in result

    @allure.title("_write_suggest_summary groups disabled GE rules as a single summary row")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_write_suggest_summary_groups_ge_disabled(self, make_suite, tmp_path):
        """
        Given tables with disabled GE column rules,
        When _write_suggest_summary is called,
        Then the Disabled Checks section shows a single summary row for GE
        instead of one row per table.
        """
        suite = make_suite({})
        # Real GE column entry structure: a dict of named sub-rules, each with their
        # own enabled flag. A table counts as having disabled rules when at least one
        # sub-rule has enabled: false.
        results = [
            {
                "catalog": "c",
                "schema": "s",
                "table": f"t{i}",
                "row_count": 50,
                "suggestions": {
                    "great_expectations": {
                        "checks": {
                            "col_a": {
                                "not_null": {"enabled": True, "severity": "warning"},
                                "between": {"enabled": False, "min": 0, "max": 100, "severity": "warning"},
                            }
                        }
                    }
                },
            }
            for i in range(3)
        ]
        result = suite._write_suggest_summary(results, str(tmp_path))
        assert "(all column rules)" in result
        # Should appear once, not three times
        assert result.count("(all column rules)") == 1

    @allure.title("_write_suggest_summary omits errors row when there are none")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_write_suggest_summary_no_errors_row(self, make_suite, tmp_path):
        """
        Given results with no errors,
        When _write_suggest_summary is called,
        Then the Overview table does NOT include the 'Tables with errors' row.
        """
        suite = make_suite({})
        result = suite._write_suggest_summary(_SUGGEST_RESULTS, str(tmp_path))
        assert "Tables with errors" not in result


@allure.feature(AllureFeature.PROFILING_SUITE)
@allure.story(AllureStory.PROFILING_SUMMARY)
class TestSummaryReportDispatch:
    @allure.title("suggest_output.summary_report_path triggers PROFILING_SUMMARY.md creation via run()")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_dispatch_summary_report_path_writes_file(self, make_suite, df_mixed, materialize_table, tmp_path):
        """
        Given a profiling suite in suggest mode with summary_report_path configured,
        When run() is called,
        Then PROFILING_SUMMARY.md is written at the configured path and contains
             the fully-qualified table name.
        """
        catalog, schema, table = materialize_table("prof_summary", "t_summary", df_mixed)

        suite = make_suite(
            {
                "mode": "suggest",
                "tables": [{"catalog": catalog, "schema": schema, "table": table}],
                "suggest_output": {"summary_report_path": str(tmp_path)},
            }
        )

        with allure.step("Run suite"):
            suite.run()

        with allure.step("Verify: PROFILING_SUMMARY.md was written"):
            summary_file = tmp_path / "PROFILING_SUMMARY.md"
            assert summary_file.exists()

        with allure.step("Verify: file contains the table's fully-qualified name"):
            content = summary_file.read_text()
            fq_name = f"{catalog}.{schema}.{table}"
            assert fq_name in content
