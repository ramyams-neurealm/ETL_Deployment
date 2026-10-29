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

def test_001_uts_001_happy_path_items_below_reorder_level(spark):
    inputs = {}
    schema_src_inventory = StructType([StructField('ITEM_ID', StringType(), False), StructField('WAREHOUSE_ID', StringType(), False), StructField('ON_HAND_QTY', IntegerType(), False), StructField('REORDER_LEVEL', IntegerType(), False)])
    rows_src_inventory = [('ITEM001', 'WH001', 50, 100), ('ITEM002', 'WH002', 20, 50)]
    inputs['SRC_INVENTORY'] = spark.createDataFrame(rows_src_inventory, schema_src_inventory)
    actual = transform(inputs)
    deterministic_columns = ['ITEM_ID', 'WAREHOUSE_ID', 'ON_HAND_QTY', 'REORDER_QTY']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['ITEM_ID', 'WAREHOUSE_ID']).collect()]
    expected_rows = [('ITEM001', 'WH001', 50, 50), ('ITEM002', 'WH002', 20, 30)]
    assert actual_rows == expected_rows

def test_002_uts_002_boundary_case_on_hand_quantity_equals_reorder_level(spark):
    inputs = {}
    schema_src_inventory = StructType([StructField('ITEM_ID', StringType(), False), StructField('WAREHOUSE_ID', StringType(), False), StructField('ON_HAND_QTY', IntegerType(), False), StructField('REORDER_LEVEL', IntegerType(), False)])
    rows_src_inventory = [('ITEM003', 'WH003', 100, 100)]
    inputs['SRC_INVENTORY'] = spark.createDataFrame(rows_src_inventory, schema_src_inventory)
    actual = transform(inputs)
    deterministic_columns = ['ITEM_ID', 'WAREHOUSE_ID', 'ON_HAND_QTY', 'REORDER_QTY']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['ITEM_ID', 'WAREHOUSE_ID']).collect()]
    expected_rows = []
    assert actual_rows == expected_rows

def test_003_uts_003_negative_case_on_hand_quantity_greater_than_reorder_level(spark):
    inputs = {}
    schema_src_inventory = StructType([StructField('ITEM_ID', StringType(), False), StructField('WAREHOUSE_ID', StringType(), False), StructField('ON_HAND_QTY', IntegerType(), False), StructField('REORDER_LEVEL', IntegerType(), False)])
    rows_src_inventory = [('ITEM004', 'WH004', 150, 100)]
    inputs['SRC_INVENTORY'] = spark.createDataFrame(rows_src_inventory, schema_src_inventory)
    actual = transform(inputs)
    deterministic_columns = ['ITEM_ID', 'WAREHOUSE_ID', 'ON_HAND_QTY', 'REORDER_QTY']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['ITEM_ID', 'WAREHOUSE_ID']).collect()]
    expected_rows = []
    assert actual_rows == expected_rows

def test_004_uts_004_null_handling_valid_values_in_non_key_fields(spark):
    inputs = {}
    schema_src_inventory = StructType([StructField('ITEM_ID', StringType(), False), StructField('WAREHOUSE_ID', StringType(), False), StructField('ON_HAND_QTY', IntegerType(), False), StructField('REORDER_LEVEL', IntegerType(), False)])
    rows_src_inventory = [('ITEM005', 'WH005', 0, 100), ('ITEM006', 'WH006', 50, 50)]
    inputs['SRC_INVENTORY'] = spark.createDataFrame(rows_src_inventory, schema_src_inventory)
    actual = transform(inputs)
    deterministic_columns = ['ITEM_ID', 'WAREHOUSE_ID', 'ON_HAND_QTY', 'REORDER_QTY']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['ITEM_ID', 'WAREHOUSE_ID']).collect()]
    expected_rows = [('ITEM005', 'WH005', 0, 100)]
    assert actual_rows == expected_rows
