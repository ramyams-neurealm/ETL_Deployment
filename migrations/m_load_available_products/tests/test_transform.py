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

def test_001_scenario_1_load_active_products_with_valid_data(spark):
    inputs = {}
    schema_src_product = StructType([StructField('PRODUCT_ID', IntegerType(), False), StructField('PRODUCT_NAME', StringType(), True), StructField('STOCK_QUANTITY', IntegerType(), True), StructField('STATUS', StringType(), True)])
    rows_src_product = [(101, 'Widget A', 50, 'ACTIVE'), (102, 'Widget B', 0, 'ACTIVE')]
    inputs['SRC_PRODUCT'] = spark.createDataFrame(rows_src_product, schema_src_product)
    actual = transform(inputs, parameters={'$$LOAD_DATE': datetime.fromisoformat('2023-10-01T00:00:00')})
    deterministic_columns = ['PRODUCT_ID', 'PRODUCT_NAME', 'STOCK_QUANTITY']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['PRODUCT_ID']).collect()]
    expected_rows = [(101, 'Widget A', 50), (102, 'Widget B', 0)]
    assert actual_rows == expected_rows
    assert 'LOAD_DATE' in actual.columns
    assert isinstance(actual.schema['LOAD_DATE'].dataType, TimestampType)
    assert actual.filter(F.col('LOAD_DATE').isNull()).count() == 0

def test_002_scenario_2_exclude_discontinued_products(spark):
    inputs = {}
    schema_src_product = StructType([StructField('PRODUCT_ID', IntegerType(), False), StructField('PRODUCT_NAME', StringType(), True), StructField('STOCK_QUANTITY', IntegerType(), True), StructField('STATUS', StringType(), True)])
    rows_src_product = [(103, 'Widget C', 20, 'DISCONTINUED'), (104, 'Widget D', 15, 'ACTIVE')]
    inputs['SRC_PRODUCT'] = spark.createDataFrame(rows_src_product, schema_src_product)
    actual = transform(inputs, parameters={'$$LOAD_DATE': datetime.fromisoformat('2023-10-01T00:00:00')})
    deterministic_columns = ['PRODUCT_ID', 'PRODUCT_NAME', 'STOCK_QUANTITY']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['PRODUCT_ID']).collect()]
    expected_rows = [(104, 'Widget D', 15)]
    assert actual_rows == expected_rows
    assert 'LOAD_DATE' in actual.columns
    assert isinstance(actual.schema['LOAD_DATE'].dataType, TimestampType)
    assert actual.filter(F.col('LOAD_DATE').isNull()).count() == 0

def test_003_scenario_3_handle_null_product_name(spark):
    inputs = {}
    schema_src_product = StructType([StructField('PRODUCT_ID', IntegerType(), False), StructField('PRODUCT_NAME', StringType(), True), StructField('STOCK_QUANTITY', IntegerType(), True), StructField('STATUS', StringType(), True)])
    rows_src_product = [(105, None, 30, 'ACTIVE')]
    inputs['SRC_PRODUCT'] = spark.createDataFrame(rows_src_product, schema_src_product)
    actual = transform(inputs, parameters={'$$LOAD_DATE': datetime.fromisoformat('2023-10-01T00:00:00')})
    deterministic_columns = ['PRODUCT_ID', 'PRODUCT_NAME', 'STOCK_QUANTITY']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['PRODUCT_ID']).collect()]
    expected_rows = [(105, None, 30)]
    assert actual_rows == expected_rows
    assert 'LOAD_DATE' in actual.columns
    assert isinstance(actual.schema['LOAD_DATE'].dataType, TimestampType)
    assert actual.filter(F.col('LOAD_DATE').isNull()).count() == 0
