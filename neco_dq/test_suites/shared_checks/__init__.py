# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Shared check mixins.

Each module provides one focused check mixin. A suite composes exactly the
checks it needs; Silver/Gold use the :class:`SharedChecksMixin` aggregate while
Bronze composes only ``JsonSchemaCheckMixin`` + ``BusinessRulesCheckMixin``.
Adding a new shared check = new module here + one entry in the aggregate.
"""

from neco_dq.test_suites.shared_checks.business_rules_check import BusinessRulesCheckMixin
from neco_dq.test_suites.shared_checks.compare_metric_check import CompareMetricCheckMixin
from neco_dq.test_suites.shared_checks.distinct_count_check import DistinctCountCheckMixin
from neco_dq.test_suites.shared_checks.freshness_check import FreshnessCheckMixin
from neco_dq.test_suites.shared_checks.json_schema_check import JsonSchemaCheckMixin
from neco_dq.test_suites.shared_checks.where_clause import WhereClauseMixin


class SharedChecksMixin(
    DistinctCountCheckMixin,
    JsonSchemaCheckMixin,
    FreshnessCheckMixin,
    BusinessRulesCheckMixin,
    CompareMetricCheckMixin,
):
    """Aggregate of every shared check, composed by the Silver and Gold suites."""


__all__ = [
    "BusinessRulesCheckMixin",
    "CompareMetricCheckMixin",
    "DistinctCountCheckMixin",
    "FreshnessCheckMixin",
    "JsonSchemaCheckMixin",
    "SharedChecksMixin",
    "WhereClauseMixin",
]
