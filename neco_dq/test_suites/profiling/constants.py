# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from decimal import Decimal
from typing import Any

from pyspark.sql import types as t


def _json_serializer(obj: Any) -> Any:
    """Custom JSON serializer for types not supported by default encoder."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    if hasattr(obj, "isoformat"):  # date, time
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


# Percentile indices for extracting values from percentile_approx result
_P25_IDX, _P50_IDX, _P75_IDX, _P95_IDX = 0, 1, 2, 3

# FQ name parts indices (catalog.schema.table)
_FQ_CATALOG_IDX, _FQ_SCHEMA_IDX, _FQ_TABLE_IDX = 0, 1, 2

# Thresholds for config suggestion logic
_UNIQUE_RATIO_THRESHOLD = 0.98  # Consider unique if distinct_ratio >= this
_LOW_CARDINALITY_THRESHOLD = 20  # Suggest allowed_values if distinct_count <= this
_NULLABLE_THRESHOLD = 0.0  # Suggest not_null if null_pct == this
_TOP_VALUE_DOMINANCE_THRESHOLD = 0.8  # Suggest allowed_values if top values cover >= 80%
_CARDINALITY_MIN_DISTINCT = 2  # Min distinct for cardinality suggestion
_CARDINALITY_MAX_DISTINCT = 100  # Max distinct for cardinality suggestion
_MIN_DATE_COLS_FOR_PAIR = 2  # Minimum date columns for pair_gte check
_JSON_DETECTION_SAMPLE_SIZE = 50  # Rows to sample for JSON detection
_JSON_DETECTION_MIN_RATIO = 0.8  # Min ratio of JSON-like values to consider column as JSON

# Date-format detection (for GE date_format suggestion on string columns)
_DATE_FORMAT_SAMPLE_SIZE = 50  # Rows to sample for date-format detection
_DATE_FORMAT_MIN_RATIO = 0.9  # Min ratio of sampled values that must match a format
# Candidate strftime formats, tried in order. First whose match ratio meets the
# threshold wins. Ordered most-specific first so a date-time isn't matched as a bare date.
_DATE_FORMAT_CANDIDATES = (
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
)

# Cardinality suggestion thresholds
_CARDINALITY_FK_MIN_DISTINCT = 10  # Min distinct values for FK pattern
_CARDINALITY_FK_MIN_RATIO = 0.01  # Min distinct ratio for FK pattern
_CARDINALITY_FK_MAX_RATIO = 0.5  # Max distinct ratio for FK pattern (not unique)

# Length rule suggestion threshold (max_length - min_length <= this triggers a length rule)
_NARROW_LENGTH_RANGE = 2

# Status transition suggestion thresholds
_STATUS_MIN_DISTINCT = 2  # Min distinct values for status column
_STATUS_MAX_DISTINCT = 10  # Max distinct values for status column

# Distinct count suggestion thresholds
_DISTINCT_COUNT_MAX_DISTINCT = 1000  # Max distinct values for distinct_count suggestion
_DISTINCT_COUNT_MIN_MULTIPLIER = 0.8  # Lower bound = distinct_count * this
_DISTINCT_COUNT_MAX_MULTIPLIER = 1.5  # Upper bound = distinct_count * this

# Trend suggestion thresholds
_TREND_METRIC_MIN_DISTINCT = 10  # Min distinct values for a numeric metric column

HIGH_DISTINCT_RATIO_THRESHOLD = 0.98

# Column type categories for classification
NUMERIC_TYPES = (
    t.ByteType,
    t.ShortType,
    t.IntegerType,
    t.LongType,
    t.FloatType,
    t.DoubleType,
    t.DecimalType,
)
STRING_TYPES = (t.StringType,)
DATE_TYPES = (t.DateType, t.TimestampType, t.TimestampNTZType)
BOOLEAN_TYPES = (t.BooleanType,)
