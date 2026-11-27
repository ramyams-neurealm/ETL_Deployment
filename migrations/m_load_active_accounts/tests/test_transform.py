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

def test_001_test_active_account_happy_path_happy_path_with_active_account(spark):
    inputs = {}
    schema_src_account = StructType([StructField('ACCOUNT_ID', IntegerType(), False), StructField('ACCOUNT_STATUS', StringType(), True), StructField('ACCOUNT_BALANCE', DecimalType(15, 2), True)])
    rows_src_account = [(1, 'ACTIVE', Decimal('1000.00'))]
    inputs['SRC_ACCOUNT'] = spark.createDataFrame(rows_src_account, schema_src_account)
    actual = transform(inputs, parameters={'$$LOAD_DATE': datetime.fromisoformat('2023-10-01T00:00:00')})
    assert isinstance(actual, dict)
    assert set(actual) == set(['TGT_ACTIVE_ACCOUNT'])
    actual = actual['TGT_ACTIVE_ACCOUNT']
    deterministic_columns = ['ACCOUNT_ID', 'ACCOUNT_STATUS', 'ACCOUNT_BALANCE']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['ACCOUNT_ID']).collect()]
    expected_rows = [(1, 'ACTIVE', Decimal('1000.00'))]
    assert actual_rows == expected_rows
    assert 'LOAD_TIMESTAMP' in actual.columns
    assert isinstance(actual.schema['LOAD_TIMESTAMP'].dataType, TimestampType)
    assert actual.filter(F.col('LOAD_TIMESTAMP').isNull()).count() == 0

def test_002_test_inactive_account_filtered_out_inactive_account_should_be_filtered_out(spark):
    inputs = {}
    schema_src_account = StructType([StructField('ACCOUNT_ID', IntegerType(), False), StructField('ACCOUNT_STATUS', StringType(), True), StructField('ACCOUNT_BALANCE', DecimalType(15, 2), True)])
    rows_src_account = [(2, 'INACTIVE', Decimal('500.00'))]
    inputs['SRC_ACCOUNT'] = spark.createDataFrame(rows_src_account, schema_src_account)
    actual = transform(inputs, parameters={'$$LOAD_DATE': datetime.fromisoformat('2023-10-01T00:00:00')})
    assert isinstance(actual, dict)
    assert set(actual) == set(['TGT_ACTIVE_ACCOUNT'])
    actual = actual['TGT_ACTIVE_ACCOUNT']
    deterministic_columns = ['ACCOUNT_ID', 'ACCOUNT_STATUS', 'ACCOUNT_BALANCE']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['ACCOUNT_ID']).collect()]
    expected_rows = []
    assert actual_rows == expected_rows
    assert 'LOAD_TIMESTAMP' in actual.columns
    assert isinstance(actual.schema['LOAD_TIMESTAMP'].dataType, TimestampType)
    assert actual.filter(F.col('LOAD_TIMESTAMP').isNull()).count() == 0

def test_003_test_null_account_status_null_account_status_should_be_filtered_out(spark):
    inputs = {}
    schema_src_account = StructType([StructField('ACCOUNT_ID', IntegerType(), False), StructField('ACCOUNT_STATUS', StringType(), True), StructField('ACCOUNT_BALANCE', DecimalType(15, 2), True)])
    rows_src_account = [(3, None, Decimal('750.00'))]
    inputs['SRC_ACCOUNT'] = spark.createDataFrame(rows_src_account, schema_src_account)
    actual = transform(inputs, parameters={'$$LOAD_DATE': datetime.fromisoformat('2023-10-01T00:00:00')})
    assert isinstance(actual, dict)
    assert set(actual) == set(['TGT_ACTIVE_ACCOUNT'])
    actual = actual['TGT_ACTIVE_ACCOUNT']
    deterministic_columns = ['ACCOUNT_ID', 'ACCOUNT_STATUS', 'ACCOUNT_BALANCE']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['ACCOUNT_ID']).collect()]
    expected_rows = []
    assert actual_rows == expected_rows
    assert 'LOAD_TIMESTAMP' in actual.columns
    assert isinstance(actual.schema['LOAD_TIMESTAMP'].dataType, TimestampType)
    assert actual.filter(F.col('LOAD_TIMESTAMP').isNull()).count() == 0

def test_004_test_multiple_active_accounts_multiple_active_accounts_processed(spark):
    inputs = {}
    schema_src_account = StructType([StructField('ACCOUNT_ID', IntegerType(), False), StructField('ACCOUNT_STATUS', StringType(), True), StructField('ACCOUNT_BALANCE', DecimalType(15, 2), True)])
    rows_src_account = [(4, 'ACTIVE', Decimal('2000.00')), (5, 'ACTIVE', Decimal('1500.00'))]
    inputs['SRC_ACCOUNT'] = spark.createDataFrame(rows_src_account, schema_src_account)
    actual = transform(inputs, parameters={'$$LOAD_DATE': datetime.fromisoformat('2023-10-01T00:00:00')})
    assert isinstance(actual, dict)
    assert set(actual) == set(['TGT_ACTIVE_ACCOUNT'])
    actual = actual['TGT_ACTIVE_ACCOUNT']
    deterministic_columns = ['ACCOUNT_ID', 'ACCOUNT_STATUS', 'ACCOUNT_BALANCE']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['ACCOUNT_ID']).collect()]
    expected_rows = [(4, 'ACTIVE', Decimal('2000.00')), (5, 'ACTIVE', Decimal('1500.00'))]
    assert actual_rows == expected_rows
    assert 'LOAD_TIMESTAMP' in actual.columns
    assert isinstance(actual.schema['LOAD_TIMESTAMP'].dataType, TimestampType)
    assert actual.filter(F.col('LOAD_TIMESTAMP').isNull()).count() == 0
