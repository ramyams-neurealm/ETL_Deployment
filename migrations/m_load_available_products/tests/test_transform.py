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

def test_001_scenario_1_happy_path_with_active_products(spark):
    inputs = {}
    schema_src_product = StructType([StructField('PRODUCT_ID', IntegerType(), False), StructField('PRODUCT_NAME', StringType(), True), StructField('STOCK_QUANTITY', IntegerType(), True), StructField('STATUS', StringType(), True)])
    rows_src_product = [(1, 'Widget A', 100, 'ACTIVE'), (2, 'Widget B', 200, 'ACTIVE')]
    inputs['SRC_PRODUCT'] = spark.createDataFrame(rows_src_product, schema_src_product)
    actual = transform(inputs, parameters={'$$LOAD_DATE': datetime.fromisoformat('2023-10-01T00:00:00')})
    deterministic_columns = ['PRODUCT_ID', 'PRODUCT_NAME', 'STOCK_QUANTITY']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['PRODUCT_ID']).collect()]
    expected_rows = [(1, 'Widget A', 100), (2, 'Widget B', 200)]
    assert actual_rows == expected_rows
    assert 'LOAD_DATE' in actual.columns
    assert isinstance(actual.schema['LOAD_DATE'].dataType, TimestampType)
    assert actual.filter(F.col('LOAD_DATE').isNull()).count() == 0

def test_002_scenario_2_filter_out_discontinued_products(spark):
    inputs = {}
    schema_src_product = StructType([StructField('PRODUCT_ID', IntegerType(), False), StructField('PRODUCT_NAME', StringType(), True), StructField('STOCK_QUANTITY', IntegerType(), True), StructField('STATUS', StringType(), True)])
    rows_src_product = [(3, 'Widget C', 150, 'DISCONTINUED'), (4, 'Widget D', 50, 'ACTIVE')]
    inputs['SRC_PRODUCT'] = spark.createDataFrame(rows_src_product, schema_src_product)
    actual = transform(inputs, parameters={'$$LOAD_DATE': datetime.fromisoformat('2023-10-01T00:00:00')})
    deterministic_columns = ['PRODUCT_ID', 'PRODUCT_NAME', 'STOCK_QUANTITY']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['PRODUCT_ID']).collect()]
    expected_rows = [(4, 'Widget D', 50)]
    assert actual_rows == expected_rows
    assert 'LOAD_DATE' in actual.columns
    assert isinstance(actual.schema['LOAD_DATE'].dataType, TimestampType)
    assert actual.filter(F.col('LOAD_DATE').isNull()).count() == 0

def test_003_scenario_3_null_handling_in_product_name(spark):
    inputs = {}
    schema_src_product = StructType([StructField('PRODUCT_ID', IntegerType(), False), StructField('PRODUCT_NAME', StringType(), True), StructField('STOCK_QUANTITY', IntegerType(), True), StructField('STATUS', StringType(), True)])
    rows_src_product = [(5, None, 300, 'ACTIVE')]
    inputs['SRC_PRODUCT'] = spark.createDataFrame(rows_src_product, schema_src_product)
    actual = transform(inputs, parameters={'$$LOAD_DATE': datetime.fromisoformat('2023-10-01T00:00:00')})
    deterministic_columns = ['PRODUCT_ID', 'PRODUCT_NAME', 'STOCK_QUANTITY']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['PRODUCT_ID']).collect()]
    expected_rows = [(5, None, 300)]
    assert actual_rows == expected_rows
    assert 'LOAD_DATE' in actual.columns
    assert isinstance(actual.schema['LOAD_DATE'].dataType, TimestampType)
    assert actual.filter(F.col('LOAD_DATE').isNull()).count() == 0
