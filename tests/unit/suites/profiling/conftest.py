# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

import pytest

from neco_dq.test_suites.profiling.profiling_dq_test_suite import SparkProfilingDQTestSuite

pytestmark = pytest.mark.spark


@pytest.fixture
def make_suite(spark):
    """Factory: build a profiling suite with an arbitrary profiling config block."""

    def _make(profiling_cfg: dict | None = None) -> SparkProfilingDQTestSuite:
        return SparkProfilingDQTestSuite(spark, {"profiling": profiling_cfg or {}})

    return _make


@pytest.fixture
def profiling_suite(make_suite):
    """Profiling suite with empty config — for testing pure helper methods."""
    return make_suite({})


@pytest.fixture
def df_mixed(spark):
    """Small DataFrame spanning numeric, string, date, boolean types."""
    from datetime import date

    return spark.createDataFrame(
        [
            (1, "alpha", date(2026, 1, 1), True, 10.5),
            (2, "beta", date(2026, 1, 2), False, 20.0),
            (3, "gamma", date(2026, 1, 3), True, 30.0),
            (4, "alpha", date(2026, 1, 4), False, 40.0),
        ],
        "id INT, name STRING, created DATE, active BOOLEAN, amount DOUBLE",
    )


@pytest.fixture
def materialize_table(spark):
    """Create a real catalog table from rows so _load_table / run() can read it.

    Returns a (catalog, schema, table) tuple. Cleans up the schema afterwards.
    """
    created: list[str] = []

    def _make(schema: str, table: str, df) -> tuple[str, str, str]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        df.write.mode("overwrite").saveAsTable(f"{schema}.{table}")
        created.append(schema)
        return ("spark_catalog", schema, table)

    yield _make

    for schema in set(created):
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
