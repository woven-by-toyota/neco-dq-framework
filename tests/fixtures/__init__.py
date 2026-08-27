# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Test fixtures for dq_framework tests.

Fixtures are organized by scope:

Common fixtures (fixtures/dataframes.py):
    - df_empty: Empty DataFrame for edge cases
    - df_simple: Basic 3-row DataFrame
    - df_unique: Unique key-value pairs
    - df_duplicates: DataFrame with duplicate rows
    - df_with_nulls: DataFrame with null values
    - df_basic: Loaded from CSV (comprehensive)
    - df_nulls: Loaded from CSV (null handling)
    - df_arrays: JSON array columns
    - df_rankings: Ranking data for statistical tests

Bronze-specific fixtures (fixtures/bronze.py):
    - bronze_suite: Bronze suite instance
    - df_bronze_simple: Simple bronze data
    - df_bronze_empty: Empty bronze DataFrame
    - df_bronze_nulls: Bronze data with nulls
    - df_timestamp: Recent timestamps
    - df_stale: Stale timestamps
    - df_schema: Specific schema for drift tests
    - df_binary: Binary column data
    - df_binary_with_nulls: Binary with nulls
    - df_binary_with_empty: Binary with empty bytes
    - df_binary_varied_sizes: Varying binary sizes
    - df_binary_with_header: Binary with version headers
    - df_binary_with_fingerprint: Binary with schema fingerprints

Silver-specific fixtures (fixtures/silver.py):
    - silver_suite: Silver suite instance
    - df_transitions: Valid status transitions
    - df_invalid_transitions: Invalid transitions
    - df_temporal: Valid temporal relationships
    - df_temporal_invalid: Invalid temporal data
    - df_cardinality: Cardinality test data
    - df_totals: Totals consistency data
    - df_totals_mismatch: Mismatched totals
    - df_flow: Flow completeness data
    - df_flow_incomplete: High drop rate flow

Gold-specific fixtures (fixtures/gold.py):
    - gold_suite: Gold suite instance
    - df_dimensions: Dimension values
    - df_dimensions_incomplete: Missing dimensions
    - df_metrics: Volume/trend metrics
    - df_metrics_low_volume: Low volume data
    - df_gold_recon: Gold reconciliation data
    - df_silver_recon: Silver reconciliation data
    - df_silver_recon_mismatch: Mismatched silver data

All fixtures are automatically loaded via pytest_plugins in conftest.py.
"""
