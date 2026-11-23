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

def test_001_scenario_1_happy_path_single_completed_order(spark):
    inputs = {}
    schema_src_customers = StructType([StructField('CUSTOMER_ID', IntegerType(), False), StructField('CUST_NAME', StringType(), False), StructField('CUST_REGION', StringType(), False)])
    rows_src_customers = [(1, 'John Doe', 'North')]
    inputs['SRC_CUSTOMERS'] = spark.createDataFrame(rows_src_customers, schema_src_customers)
    schema_src_orders = StructType([StructField('ORDER_ID', IntegerType(), False), StructField('CUSTOMER_ID', IntegerType(), False), StructField('ORDER_STATUS', StringType(), True), StructField('ORDER_AMOUNT', DecimalType(12, 2), False)])
    rows_src_orders = [(101, 1, 'COMPLETED', Decimal('100.00'))]
    inputs['SRC_ORDERS'] = spark.createDataFrame(rows_src_orders, schema_src_orders)
    actual = transform(inputs, parameters={'RunDate': date.fromisoformat('2026-01-01'), 'SourceSystem': 'CRM'})
    assert isinstance(actual, dict)
    assert set(actual) == set(['TGT_CUSTOMER_SALES_SUMMARY'])
    actual = actual['TGT_CUSTOMER_SALES_SUMMARY']
    deterministic_columns = ['CUSTOMER_ID', 'CUSTOMER_NAME', 'REGION', 'TOTAL_ORDER_AMOUNT', 'ORDER_COUNT']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['CUSTOMER_ID']).collect()]
    expected_rows = [(1, 'JOHN DOE', 'NORTH', Decimal('100.00'), 1)]
    assert actual_rows == expected_rows

def test_002_scenario_2_boundary_no_completed_orders(spark):
    inputs = {}
    schema_src_customers = StructType([StructField('CUSTOMER_ID', IntegerType(), False), StructField('CUST_NAME', StringType(), False), StructField('CUST_REGION', StringType(), False)])
    rows_src_customers = [(2, 'Jane Smith', 'South')]
    inputs['SRC_CUSTOMERS'] = spark.createDataFrame(rows_src_customers, schema_src_customers)
    schema_src_orders = StructType([StructField('ORDER_ID', IntegerType(), False), StructField('CUSTOMER_ID', IntegerType(), False), StructField('ORDER_STATUS', StringType(), True), StructField('ORDER_AMOUNT', DecimalType(12, 2), False)])
    rows_src_orders = [(102, 2, 'PENDING', Decimal('200.00'))]
    inputs['SRC_ORDERS'] = spark.createDataFrame(rows_src_orders, schema_src_orders)
    actual = transform(inputs, parameters={'RunDate': date.fromisoformat('2026-01-01'), 'SourceSystem': 'CRM'})
    assert isinstance(actual, dict)
    assert set(actual) == set(['TGT_CUSTOMER_SALES_SUMMARY'])
    actual = actual['TGT_CUSTOMER_SALES_SUMMARY']
    deterministic_columns = ['CUSTOMER_ID', 'CUSTOMER_NAME', 'REGION', 'TOTAL_ORDER_AMOUNT', 'ORDER_COUNT']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['CUSTOMER_ID']).collect()]
    expected_rows = []
    assert actual_rows == expected_rows

def test_003_scenario_3_multiple_completed_orders_for_a_single_customer(spark):
    inputs = {}
    schema_src_customers = StructType([StructField('CUSTOMER_ID', IntegerType(), False), StructField('CUST_NAME', StringType(), False), StructField('CUST_REGION', StringType(), False)])
    rows_src_customers = [(3, 'Alice Johnson', 'East')]
    inputs['SRC_CUSTOMERS'] = spark.createDataFrame(rows_src_customers, schema_src_customers)
    schema_src_orders = StructType([StructField('ORDER_ID', IntegerType(), False), StructField('CUSTOMER_ID', IntegerType(), False), StructField('ORDER_STATUS', StringType(), True), StructField('ORDER_AMOUNT', DecimalType(12, 2), False)])
    rows_src_orders = [(103, 3, 'COMPLETED', Decimal('150.00')), (104, 3, 'COMPLETED', Decimal('250.00'))]
    inputs['SRC_ORDERS'] = spark.createDataFrame(rows_src_orders, schema_src_orders)
    actual = transform(inputs, parameters={'RunDate': date.fromisoformat('2026-01-01'), 'SourceSystem': 'CRM'})
    assert isinstance(actual, dict)
    assert set(actual) == set(['TGT_CUSTOMER_SALES_SUMMARY'])
    actual = actual['TGT_CUSTOMER_SALES_SUMMARY']
    deterministic_columns = ['CUSTOMER_ID', 'CUSTOMER_NAME', 'REGION', 'TOTAL_ORDER_AMOUNT', 'ORDER_COUNT']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['CUSTOMER_ID']).collect()]
    expected_rows = [(3, 'ALICE JOHNSON', 'EAST', Decimal('400.00'), 2)]
    assert actual_rows == expected_rows

def test_004_scenario_4_null_handling_null_order_status(spark):
    inputs = {}
    schema_src_customers = StructType([StructField('CUSTOMER_ID', IntegerType(), False), StructField('CUST_NAME', StringType(), False), StructField('CUST_REGION', StringType(), False)])
    rows_src_customers = [(4, 'Bob Brown', 'West')]
    inputs['SRC_CUSTOMERS'] = spark.createDataFrame(rows_src_customers, schema_src_customers)
    schema_src_orders = StructType([StructField('ORDER_ID', IntegerType(), False), StructField('CUSTOMER_ID', IntegerType(), False), StructField('ORDER_STATUS', StringType(), True), StructField('ORDER_AMOUNT', DecimalType(12, 2), False)])
    rows_src_orders = [(105, 4, None, Decimal('300.00'))]
    inputs['SRC_ORDERS'] = spark.createDataFrame(rows_src_orders, schema_src_orders)
    actual = transform(inputs, parameters={'RunDate': date.fromisoformat('2026-01-01'), 'SourceSystem': 'CRM'})
    assert isinstance(actual, dict)
    assert set(actual) == set(['TGT_CUSTOMER_SALES_SUMMARY'])
    actual = actual['TGT_CUSTOMER_SALES_SUMMARY']
    deterministic_columns = ['CUSTOMER_ID', 'CUSTOMER_NAME', 'REGION', 'TOTAL_ORDER_AMOUNT', 'ORDER_COUNT']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['CUSTOMER_ID']).collect()]
    expected_rows = []
    assert actual_rows == expected_rows
