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

def test_001_test_positive_customer_id_filter_and_normalize_with_positive_customer_id(spark):
    inputs = {}
    schema_src_customer_contact = StructType([StructField('CUSTOMER_ID', IntegerType(), False), StructField('EMAIL', StringType(), True)])
    rows_src_customer_contact = [(123, 'Example@Email.com'), (456, 'Another@Example.com')]
    inputs['SRC_CUSTOMER_CONTACT'] = spark.createDataFrame(rows_src_customer_contact, schema_src_customer_contact)
    actual = transform(inputs)
    assert isinstance(actual, dict)
    assert set(actual) == set(['TGT_CUSTOMER_CONTACT'])
    actual = actual['TGT_CUSTOMER_CONTACT']
    deterministic_columns = ['CUSTOMER_ID', 'EMAIL']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['CUSTOMER_ID']).collect()]
    expected_rows = [(123, 'example@email.com'), (456, 'another@example.com')]
    assert actual_rows == expected_rows

def test_002_test_negative_customer_id_filter_out_negative_customer_id(spark):
    inputs = {}
    schema_src_customer_contact = StructType([StructField('CUSTOMER_ID', IntegerType(), False), StructField('EMAIL', StringType(), True)])
    rows_src_customer_contact = [(-1, 'Negative@Email.com'), (0, 'Zero@Email.com')]
    inputs['SRC_CUSTOMER_CONTACT'] = spark.createDataFrame(rows_src_customer_contact, schema_src_customer_contact)
    actual = transform(inputs)
    assert isinstance(actual, dict)
    assert set(actual) == set(['TGT_CUSTOMER_CONTACT'])
    actual = actual['TGT_CUSTOMER_CONTACT']
    deterministic_columns = ['CUSTOMER_ID', 'EMAIL']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['CUSTOMER_ID']).collect()]
    expected_rows = []
    assert actual_rows == expected_rows

def test_003_test_null_email_handle_null_email(spark):
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
