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

def test_001_tc1_filter_and_load_active_accounts_with_valid_data(spark):
    inputs = {}
    schema_src_account = StructType([StructField('ACCOUNT_ID', IntegerType(), False), StructField('ACCOUNT_STATUS', StringType(), True), StructField('ACCOUNT_BALANCE', DecimalType(15, 2), True)])
    rows_src_account = [(1, 'ACTIVE', Decimal('1000.5')), (2, 'INACTIVE', Decimal('200.0')), (3, 'ACTIVE', Decimal('1500.75'))]
    inputs['SRC_ACCOUNT'] = spark.createDataFrame(rows_src_account, schema_src_account)
    actual = transform(inputs)
    deterministic_columns = ['ACCOUNT_ID', 'ACCOUNT_STATUS', 'ACCOUNT_BALANCE']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['ACCOUNT_ID']).collect()]
    expected_rows = [(1, 'ACTIVE', 1000.5), (3, 'ACTIVE', 1500.75)]
    assert actual_rows == expected_rows
    assert 'LOAD_TIMESTAMP' in actual.columns
    assert isinstance(actual.schema['LOAD_TIMESTAMP'].dataType, TimestampType)
    assert actual.filter(F.col('LOAD_TIMESTAMP').isNull()).count() == 0

def test_002_tc2_handle_null_account_status(spark):
    inputs = {}
    schema_src_account = StructType([StructField('ACCOUNT_ID', IntegerType(), False), StructField('ACCOUNT_STATUS', StringType(), True), StructField('ACCOUNT_BALANCE', DecimalType(15, 2), True)])
    rows_src_account = [(4, None, Decimal('500.0')), (5, 'ACTIVE', Decimal('750.0'))]
    inputs['SRC_ACCOUNT'] = spark.createDataFrame(rows_src_account, schema_src_account)
    actual = transform(inputs)
    deterministic_columns = ['ACCOUNT_ID', 'ACCOUNT_STATUS', 'ACCOUNT_BALANCE']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['ACCOUNT_ID']).collect()]
    expected_rows = [(5, 'ACTIVE', 750.0)]
    assert actual_rows == expected_rows
    assert 'LOAD_TIMESTAMP' in actual.columns
    assert isinstance(actual.schema['LOAD_TIMESTAMP'].dataType, TimestampType)
    assert actual.filter(F.col('LOAD_TIMESTAMP').isNull()).count() == 0

def test_003_tc3_no_active_accounts_present(spark):
    inputs = {}
    schema_src_account = StructType([StructField('ACCOUNT_ID', IntegerType(), False), StructField('ACCOUNT_STATUS', StringType(), True), StructField('ACCOUNT_BALANCE', DecimalType(15, 2), True)])
    rows_src_account = [(6, 'INACTIVE', Decimal('300.0')), (7, 'CLOSED', Decimal('0.0'))]
    inputs['SRC_ACCOUNT'] = spark.createDataFrame(rows_src_account, schema_src_account)
    actual = transform(inputs)
    deterministic_columns = ['ACCOUNT_ID', 'ACCOUNT_STATUS', 'ACCOUNT_BALANCE']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['ACCOUNT_ID']).collect()]
    expected_rows = []
    assert actual_rows == expected_rows
    assert 'LOAD_TIMESTAMP' in actual.columns
    assert isinstance(actual.schema['LOAD_TIMESTAMP'].dataType, TimestampType)
    assert actual.filter(F.col('LOAD_TIMESTAMP').isNull()).count() == 0
