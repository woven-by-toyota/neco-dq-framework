# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Test constants and Allure configuration for dq_framework tests.

This module provides standardized constants for test organization and Allure reporting,
following the Neco DQ Framework testing standards.
"""

from enum import StrEnum


class AllureEpic(StrEnum):
    """Epic names for high-level test organization.

    Usage:
        @allure.epic(AllureEpic.DATA_QUALITY)
    """

    DATA_QUALITY = "Data Quality"


class AllureSeverity(StrEnum):
    """Allure severity levels for test prioritization.

    Usage:
        @allure.severity(AllureSeverity.CRITICAL)
    """

    BLOCKER = "blocker"
    CRITICAL = "critical"
    NORMAL = "normal"
    MINOR = "minor"
    TRIVIAL = "trivial"


class AllureTag(StrEnum):
    """Standard test tags for filtering and categorization.

    Usage:
        @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    """

    # Test type
    POSITIVE = "Positive"
    NEGATIVE = "Negative"
    EDGE_CASE = "EdgeCase"

    # Execution markers
    SMOKE = "Smoke"
    SLOW_TEST = "SlowTest"

    # Environment requirements
    SPARK = "Spark"
    SPARK_REQUIRED = "SparkRequired"

    # Feature areas
    EXPECTED = "Expected"
    OBSERVED = "Observed"
    OPERATORS = "Operators"
    METRICS = "Metrics"
    AGGREGATION = "Aggregation"
    ARRAY = "Array"
    SQL = "SQL"

    # Security
    SECURITY = "Security"


class AllureLinkType(StrEnum):
    """Link types for Allure report references.

    Usage:
        @allure.link(url="...", link_type=AllureLinkType.ISSUE, name="JIRA-123")
    """

    ISSUE = "issue"
    TMS = "tms"
    LINK = "link"


# Feature names for @allure.feature decorator
class AllureFeature(StrEnum):
    """Feature names for test organization.

    Usage:
        @allure.feature(AllureFeature.EXPECTED_RESOLVERS)
    """

    EXPECTED_RESOLVERS = "Expected Resolvers"
    OBSERVED_METRICS = "Observed Metrics"
    OPERATORS = "Comparison Operators"
    SHARED_CHECKS = "Shared Checks"
    ARRAY_PROCESSING = "Array Processing"
    SQL_RESOLUTION = "SQL Resolution"
    AGGREGATION = "Aggregation"

    # Test Suites
    BRONZE_SUITE = "Bronze Suite"
    SILVER_SUITE = "Silver Suite"
    GOLD_SUITE = "Gold Suite"
    GE_SUITE = "Great Expectations Suite"
    PROFILING_SUITE = "Profiling Suite"

    # Core Components
    CORE = "Core Components"
    CONFIG = "Configuration"


# Story names for @allure.story decorator
class AllureStory(StrEnum):
    """Story names for feature breakdown.

    Usage:
        @allure.story(AllureStory.STATIC_RESOLVER)
    """

    # Expected resolvers
    STATIC_RESOLVER = "Static Resolver"
    AGGREGATE_RESOLVER = "Aggregate Resolver"
    COLUMN_VALUE_RESOLVER = "Column Value Resolver"
    SQL_RESOLVER = "SQL Resolver"
    ARRAY_FIELD_RESOLVER = "Array Field Resolver"
    ARRAY_ORDERED_RESOLVER = "Array Ordered Resolver"

    # Observed metrics
    COUNT_METRICS = "Count Metrics"
    SUM_METRICS = "Sum Metrics"
    AVERAGE_METRICS = "Average Metrics"
    MEDIAN_METRICS = "Median Metrics"
    DISTINCT_METRICS = "Distinct Metrics"
    ARRAY_METRICS = "Array Metrics"

    # Operators
    BASIC_OPERATORS = "Basic Operators"
    COLLECTION_OPERATORS = "Collection Operators"
    RANGE_OPERATORS = "Range Operators"
    TOLERANCE_OPERATORS = "Tolerance Operators"
    COMPARE_DISPATCHER = "Compare Dispatcher"
    OPERATORS_REGISTRY = "Operators Registry"

    # Shared Checks
    DISTINCT_COUNT = "Distinct Count Check"
    JSON_SCHEMA = "JSON Schema Check"
    FRESHNESS = "Freshness Check"
    BUSINESS_RULES = "Business Rules Check"
    COMPARE_METRIC = "Compare Metric Check"
    HELPER_METHODS = "Helper Methods"

    # Bronze Suite Checks
    RECORD_COUNT = "Record Count Check"
    RECORD_COUNT_ANOMALY = "Record Count Anomaly Check"
    SCHEMA_DRIFT = "Schema Drift Check"
    MANDATORY_FIELDS = "Mandatory Fields Check"
    BINARY_NOT_NULL = "Binary Not Null Check"
    BINARY_NON_EMPTY = "Binary Non Empty Check"
    BINARY_SIZE_GUARD = "Binary Size Guard Check"
    BINARY_TYPE = "Binary Type Check"
    BINARY_DECODABLE = "Binary Decodable Check"
    BINARY_VERSION_HEADER = "Binary Version Header Check"
    BINARY_SCHEMA_FINGERPRINT = "Binary Schema Fingerprint Check"
    PARTITION_EXISTS = "Partition Exists Check"
    TABLE_FRESHNESS = "Table Freshness Check"

    # Silver Suite Checks
    IDEMPOTENCY = "Idempotency Check"
    STATUS_TRANSITION = "Status Transition Check"
    REFERENTIAL_INTEGRITY = "Referential Integrity Check"
    CARDINALITY = "Cardinality Check"
    TOTALS_CONSISTENCY = "Totals Consistency Check"
    CROSS_TABLE_EQUALITY = "Cross Table Equality Check"
    TEMPORAL_CONSISTENCY = "Temporal Consistency Check"
    ROW_TEMPORAL_CONSISTENCY = "Row Temporal Consistency Check"
    FLOW_COMPLETENESS = "Flow Completeness Check"

    # Gold Suite Checks
    SUITE_IDENTITY = "Suite Identity"
    RECONCILIATION_EXPR = "Reconciliation Expression Check"
    GOLD_REFERENTIAL_INTEGRITY = "Gold Referential Integrity Check"
    UNIQUENESS = "Uniqueness Check"
    COVERAGE = "Coverage Check"
    VOLUME = "Volume Check"
    CROSS_GOLD = "Cross Gold Check"
    COMPLETENESS = "Completeness Check"
    TREND = "Trend Check"

    # Great Expectations Suite
    GE_SUITE_BUILDER = "Suite Builder"
    GE_COLUMN_RULES = "Column Rules"
    GE_DATASET_RULES = "Dataset Rules"
    GE_RESULT_NORMALIZER = "Result Normalizer"

    # Core Components
    TEST_SUITE_REGISTRY = "Test Suite Registry"
    FAILURE_POLICY = "Failure Policy"
    SUMMARY_ENGINE = "Summary Engine"
    ORCHESTRATOR = "Orchestrator"
    RUNNER = "Runner"
    CONFIG_RESOLVER = "Config Resolver"
    TAG_FILTERING = "Tag-Based Filtering"

    # Config - Profiles
    PROFILE_CONTEXT = "Profile Context"
    PROFILE_LOADER = "Profile Loader"
    JINJA_RENDERING = "Jinja Rendering"

    # Profiling Suite
    COLUMN_CLASSIFICATION = "Column Classification"
    PROFILING_COLUMN_STATS = "Column Stats Computation"
    PROFILING_SUGGEST = "Suggestion Builders"
    PROFILING_GE_SUGGEST = "GE Suggestion Builder"
    PROFILING_CROSS_TABLE = "Cross-Table Detection"
    PROFILING_RUN_MODES = "Run Modes"
    PROFILING_MODULAR_OUTPUT = "Modular Config Output"
    PROFILING_SUMMARY = "Summary Report"
