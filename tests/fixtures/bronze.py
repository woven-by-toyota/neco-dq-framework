# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Bronze suite specific fixtures for dq_framework tests.

This module provides fixtures specific to Bronze layer tests:
- Suite instance
- Binary data fixtures (payload handling, encoding)
- Schema fixtures (schema drift detection)
- Timestamp fixtures (freshness checks)

Note: Common fixtures like df_basic, df_empty, df_with_nulls are in dataframes.py
and should be used as-is or with Bronze-specific variants defined here.
"""

from datetime import UTC, datetime, timedelta

import pytest
from pyspark.sql.types import BinaryType, IntegerType, StringType, StructField, StructType

from neco_dq.test_suites.bronze.bronze_dq_test_suite import SparkBronzeDQTestSuite


# =============================================================================
# Suite Instance
# =============================================================================


@pytest.fixture(scope="module")
def bronze_suite(spark):
    """Create Bronze suite instance with minimal config."""
    config = {"bronze": {"tables": [], "default_checks": {}}}
    return SparkBronzeDQTestSuite(spark, config)


# =============================================================================
# Bronze-specific Basic Fixtures
# These have different schemas than the common fixtures in dataframes.py
# =============================================================================


@pytest.fixture(scope="module")
def df_bronze_simple(spark):
    """Simple DataFrame for Bronze record count tests.

    Schema: id INT, code STRING
    Rows: 3 rows
    """
    data = [(1, "A"), (2, "B"), (3, "C")]
    return spark.createDataFrame(data, ["id", "code"])


@pytest.fixture(scope="module")
def df_bronze_empty(spark):
    """Empty DataFrame for Bronze edge case tests.

    Schema: id INT, code STRING
    Rows: 0
    """
    return spark.createDataFrame([], "id INT, code STRING")


@pytest.fixture(scope="module")
def df_bronze_nulls(spark):
    """DataFrame with null values for Bronze mandatory fields tests.

    Schema: id INT, name STRING, status STRING
    Rows: 3 rows, name is null in row 2, status is null in row 3
    """
    data = [(1, "A", "active"), (2, None, "active"), (3, "C", None)]
    return spark.createDataFrame(data, ["id", "name", "status"])


# =============================================================================
# Timestamp Fixtures - Freshness tests
# =============================================================================


@pytest.fixture(scope="module")
def df_timestamp(spark):
    """DataFrame with recent timestamps for freshness pass tests.

    Schema: id INT, updated_at TIMESTAMP
    Rows: 3 rows with timestamps 1-3 hours ago
    """
    now = datetime.now(UTC)
    data = [
        (1, now - timedelta(hours=1)),
        (2, now - timedelta(hours=2)),
        (3, now - timedelta(hours=3)),
    ]
    return spark.createDataFrame(data, ["id", "updated_at"])


@pytest.fixture(scope="module")
def df_stale(spark):
    """DataFrame with stale timestamps for freshness failure tests.

    Schema: id INT, updated_at TIMESTAMP
    Rows: 2 rows with timestamps 48+ hours ago
    """
    old = datetime.now(UTC) - timedelta(hours=48)
    data = [(1, old), (2, old - timedelta(hours=1))]
    return spark.createDataFrame(data, ["id", "updated_at"])


# =============================================================================
# Schema Fixtures - Schema drift tests
# =============================================================================


@pytest.fixture(scope="module")
def df_schema(spark):
    """DataFrame with specific schema for schema drift tests.

    Schema:
        - id: INT
        - name: STRING
        - count: INT
    """
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("count", IntegerType(), True),
        ]
    )
    data = [(1, "A", 10), (2, "B", 20)]
    return spark.createDataFrame(data, schema)


# =============================================================================
# Binary Fixtures - Binary column tests
# =============================================================================


@pytest.fixture(scope="module")
def df_binary(spark):
    """DataFrame with binary column for binary checks.

    Schema: id INT, payload BINARY
    Rows: 3 rows with JSON-like binary payloads
    """
    data = [
        (1, bytearray(b'{"key": "value1"}')),
        (2, bytearray(b'{"key": "value2"}')),
        (3, bytearray(b'{"key": "value3"}')),
    ]
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("payload", BinaryType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="module")
def df_binary_with_nulls(spark):
    """DataFrame with binary column containing nulls.

    Schema: id INT, payload BINARY
    Rows: 3 rows, one has null payload
    """
    data = [(1, bytearray(b"data")), (2, None), (3, bytearray(b"more"))]
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("payload", BinaryType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="module")
def df_binary_with_empty(spark):
    """DataFrame with binary column containing empty values.

    Schema: id INT, payload BINARY
    Rows: 3 rows, one has empty binary
    """
    data = [(1, bytearray(b"data")), (2, bytearray(b"")), (3, bytearray(b"more"))]
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("payload", BinaryType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="module")
def df_binary_varied_sizes(spark):
    """DataFrame with varying binary sizes for size guard tests.

    Schema: id INT, payload BINARY
    Rows: 3 rows with different payload sizes (5, 16, 26 bytes)
    """
    data = [
        (1, bytearray(b"small")),  # 5 bytes
        (2, bytearray(b"medium_size_data")),  # 16 bytes
        (3, bytearray(b"large_payload_content_here")),  # 26 bytes
    ]
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("payload", BinaryType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="module")
def df_base64_json(spark):
    """DataFrame with base64-encoded JSON payloads (valid for base64_json format).

    Schema: id INT, payload STRING
    Rows: 2 rows, each a base64 string that decodes to a JSON object.
    """
    import base64

    data = [
        (1, base64.b64encode(b'{"key": "value1"}').decode()),
        (2, base64.b64encode(b'{"key": "value2"}').decode()),
    ]
    return spark.createDataFrame(data, ["id", "payload"])


@pytest.fixture(scope="module")
def df_plain_json_string(spark):
    """DataFrame with plain (non-base64) JSON text — malformed for base64_json format.

    Schema: id INT, payload STRING
    Rows: 2 rows holding raw JSON strings. Decoding these as base64 is invalid and,
    under ANSI/Photon strict mode, would raise CONVERSION_INVALID_INPUT unless the
    check tolerates malformed input.
    """
    data = [(1, '{"key": "value1"}'), (2, '{"key": "value2"}')]
    return spark.createDataFrame(data, ["id", "payload"])


@pytest.fixture(scope="module")
def df_non_binary(spark):
    """DataFrame with string column (not binary) for type check tests.

    Schema: id INT, payload STRING
    """
    data = [(1, "not_binary"), (2, "also_string")]
    return spark.createDataFrame(data, ["id", "payload"])


# =============================================================================
# Binary Version Header Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def df_binary_with_header(spark):
    """DataFrame with binary column containing valid version headers.

    Schema: id INT, payload BINARY
    Rows: 3 rows with magic byte 0x00 at start and 5+ byte headers
    Format: [magic_byte][4 bytes header][payload...]
    """
    data = [
        (1, bytearray(b"\x00\x01\x02\x03\x04payload1")),  # magic=0, header=5 bytes
        (2, bytearray(b"\x00\x05\x06\x07\x08payload2")),  # magic=0, header=5 bytes
        (3, bytearray(b"\x00\x09\x0a\x0b\x0cpayload3")),  # magic=0, header=5 bytes
    ]
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("payload", BinaryType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="module")
def df_binary_invalid_magic(spark):
    """DataFrame with binary column containing invalid magic bytes.

    Schema: id INT, payload BINARY
    Rows: 3 rows, one has wrong magic byte (0xFF instead of 0x00)
    """
    data = [
        (1, bytearray(b"\x00\x01\x02\x03\x04payload1")),  # valid magic=0
        (2, bytearray(b"\xff\x05\x06\x07\x08payload2")),  # invalid magic=255
        (3, bytearray(b"\x00\x09\x0a\x0b\x0cpayload3")),  # valid magic=0
    ]
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("payload", BinaryType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="module")
def df_binary_short_header(spark):
    """DataFrame with binary column containing too short headers.

    Schema: id INT, payload BINARY
    Rows: 3 rows, one is too short (less than 5 bytes)
    """
    data = [
        (1, bytearray(b"\x00\x01\x02\x03\x04payload1")),  # 12 bytes - OK
        (2, bytearray(b"\x00\x01\x02")),  # 3 bytes - too short
        (3, bytearray(b"\x00\x09\x0a\x0b\x0cpayload3")),  # 12 bytes - OK
    ]
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("payload", BinaryType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)


# =============================================================================
# Binary Schema Fingerprint Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def df_binary_with_fingerprint(spark):
    """DataFrame with binary column containing valid schema fingerprints.

    Schema: id INT, payload BINARY
    Rows: 3 rows with fingerprint "ABCD1234" at Spark offset 2 (4 bytes)

    Note: Spark substring is 1-indexed. offset=2, length=4 reads bytes at
    positions 2,3,4,5 (1-indexed) = positions 1,2,3,4 (0-indexed).
    So fingerprint must be at 0-indexed position 1.
    Format: [1 byte prefix][4 bytes fingerprint][payload...]
    """
    # Fingerprint "ABCD1234" at 0-indexed position 1 (Spark position 2)
    fp_bytes = bytes.fromhex("ABCD1234")
    data = [
        (1, bytearray(b"\x00" + fp_bytes + b"payload1")),
        (2, bytearray(b"\x00" + fp_bytes + b"payload2")),
        (3, bytearray(b"\x00" + fp_bytes + b"payload3")),
    ]
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("payload", BinaryType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="module")
def df_binary_invalid_fingerprint(spark):
    """DataFrame with binary column containing invalid schema fingerprints.

    Schema: id INT, payload BINARY
    Rows: 3 rows, one has invalid fingerprint at Spark offset 2
    """
    fp_valid = bytes.fromhex("ABCD1234")
    fp_invalid = bytes.fromhex("DEADBEEF")
    data = [
        (1, bytearray(b"\x00" + fp_valid + b"payload1")),
        (2, bytearray(b"\x00" + fp_invalid + b"payload2")),  # invalid
        (3, bytearray(b"\x00" + fp_valid + b"payload3")),
    ]
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("payload", BinaryType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)


# =============================================================================
# Binary Format Fixtures (Avro, Protobuf)
# =============================================================================


@pytest.fixture(scope="module")
def df_binary_avro(spark):
    """DataFrame with binary column containing valid Avro magic bytes.

    Schema: id INT, payload BINARY
    Rows: 3 rows with Avro magic byte 0x00 at start

    Note: Avro binary format starts with magic byte 0x00.
    The binary_decodable check verifies substring(col,1,1) == 0x00.
    """
    data = [
        (1, bytearray(b"\x00\x01\x02\x03avro_payload1")),  # starts with 0x00
        (2, bytearray(b"\x00\x04\x05\x06avro_payload2")),  # starts with 0x00
        (3, bytearray(b"\x00\x07\x08\x09avro_payload3")),  # starts with 0x00
    ]
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("payload", BinaryType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)
