"""Deterministically rendered regression tests."""
from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DateType, DecimalType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType)
from transform import transform

@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder.master("local[1]").appName("neuflow-unit-test").getOrCreate())
    yield session
    session.stop()

def test_001_test_positive_customer_id_filter_and_normalize_email_for_positive_customer_id(spark):
    inputs = {}
    schema_src_customer_contact = StructType([StructField('CUSTOMER_ID', IntegerType(), False), StructField('EMAIL', StringType(), True)])
    rows_src_customer_contact = [(123, 'John.Doe@Example.com'), (456, 'Jane.Smith@Example.com')]
    inputs['SRC_CUSTOMER_CONTACT'] = spark.createDataFrame(rows_src_customer_contact, schema_src_customer_contact)
    actual = transform(inputs)
    assert isinstance(actual, dict)
    assert set(actual) == set(['TGT_CUSTOMER_CONTACT'])
    actual = actual['TGT_CUSTOMER_CONTACT']
    deterministic_columns = ['CUSTOMER_ID', 'EMAIL']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['CUSTOMER_ID']).collect()]
    expected_rows = [(123, 'john.doe@example.com'), (456, 'jane.smith@example.com')]
    assert actual_rows == expected_rows

def test_002_test_negative_customer_id_exclude_records_with_non_positive_customer_id(spark):
    inputs = {}
    schema_src_customer_contact = StructType([StructField('CUSTOMER_ID', IntegerType(), False), StructField('EMAIL', StringType(), True)])
    rows_src_customer_contact = [(-1, 'invalid@example.com'), (0, 'zero@example.com')]
    inputs['SRC_CUSTOMER_CONTACT'] = spark.createDataFrame(rows_src_customer_contact, schema_src_customer_contact)
    actual = transform(inputs)
    assert isinstance(actual, dict)
    assert set(actual) == set(['TGT_CUSTOMER_CONTACT'])
    actual = actual['TGT_CUSTOMER_CONTACT']
    deterministic_columns = ['CUSTOMER_ID', 'EMAIL']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['CUSTOMER_ID']).collect()]
    expected_rows = []
    assert actual_rows == expected_rows

def test_003_test_null_email_handle_null_email_with_positive_customer_id(spark):
    inputs = {}
    schema_src_customer_contact = StructType([StructField('CUSTOMER_ID', IntegerType(), False), StructField('EMAIL', StringType(), True)])
    rows_src_customer_contact = [(789, None)]
    inputs['SRC_CUSTOMER_CONTACT'] = spark.createDataFrame(rows_src_customer_contact, schema_src_customer_contact)
    actual = transform(inputs)
    assert isinstance(actual, dict)
    assert set(actual) == set(['TGT_CUSTOMER_CONTACT'])
    actual = actual['TGT_CUSTOMER_CONTACT']
    deterministic_columns = ['CUSTOMER_ID', 'EMAIL']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['CUSTOMER_ID']).collect()]
    expected_rows = [(789, None)]
    assert actual_rows == expected_rows
