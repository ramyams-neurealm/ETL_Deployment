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

def test_001_tc1_happy_path_single_customer_with_completed_orders(spark):
    inputs = {}
    schema_src_customers = StructType([StructField('CUSTOMER_ID', IntegerType(), False), StructField('CUST_NAME', StringType(), False), StructField('CUST_REGION', StringType(), False)])
    rows_src_customers = [(1, 'John Doe', 'North')]
    inputs['SRC_CUSTOMERS'] = spark.createDataFrame(rows_src_customers, schema_src_customers)
    schema_src_orders = StructType([StructField('ORDER_ID', IntegerType(), False), StructField('CUSTOMER_ID', IntegerType(), False), StructField('ORDER_STATUS', StringType(), True), StructField('ORDER_AMOUNT', DecimalType(12, 2), False)])
    rows_src_orders = [(101, 1, 'COMPLETED', Decimal('150.0')), (102, 1, 'COMPLETED', Decimal('200.0'))]
    inputs['SRC_ORDERS'] = spark.createDataFrame(rows_src_orders, schema_src_orders)
    actual = transform(inputs)
    deterministic_columns = ['CUSTOMER_ID', 'CUSTOMER_NAME', 'REGION', 'TOTAL_ORDER_AMOUNT', 'ORDER_COUNT']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['CUSTOMER_ID']).collect()]
    expected_rows = [(1, 'JOHN DOE', 'NORTH', 350.0, 2)]
    assert actual_rows == expected_rows

def test_002_tc2_no_completed_orders(spark):
    inputs = {}
    schema_src_customers = StructType([StructField('CUSTOMER_ID', IntegerType(), False), StructField('CUST_NAME', StringType(), False), StructField('CUST_REGION', StringType(), False)])
    rows_src_customers = [(2, 'Jane Smith', 'South')]
    inputs['SRC_CUSTOMERS'] = spark.createDataFrame(rows_src_customers, schema_src_customers)
    schema_src_orders = StructType([StructField('ORDER_ID', IntegerType(), False), StructField('CUSTOMER_ID', IntegerType(), False), StructField('ORDER_STATUS', StringType(), True), StructField('ORDER_AMOUNT', DecimalType(12, 2), False)])
    rows_src_orders = [(103, 2, 'PENDING', Decimal('300.0'))]
    inputs['SRC_ORDERS'] = spark.createDataFrame(rows_src_orders, schema_src_orders)
    actual = transform(inputs)
    deterministic_columns = ['CUSTOMER_ID', 'CUSTOMER_NAME', 'REGION', 'TOTAL_ORDER_AMOUNT', 'ORDER_COUNT']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['CUSTOMER_ID']).collect()]
    expected_rows = []
    assert actual_rows == expected_rows

def test_003_tc3_multiple_customers_with_mixed_order_status(spark):
    inputs = {}
    schema_src_customers = StructType([StructField('CUSTOMER_ID', IntegerType(), False), StructField('CUST_NAME', StringType(), False), StructField('CUST_REGION', StringType(), False)])
    rows_src_customers = [(3, 'Alice Brown', 'East'), (4, 'Bob White', 'West')]
    inputs['SRC_CUSTOMERS'] = spark.createDataFrame(rows_src_customers, schema_src_customers)
    schema_src_orders = StructType([StructField('ORDER_ID', IntegerType(), False), StructField('CUSTOMER_ID', IntegerType(), False), StructField('ORDER_STATUS', StringType(), True), StructField('ORDER_AMOUNT', DecimalType(12, 2), False)])
    rows_src_orders = [(104, 3, 'COMPLETED', Decimal('400.0')), (105, 3, 'PENDING', Decimal('500.0')), (106, 4, 'COMPLETED', Decimal('600.0'))]
    inputs['SRC_ORDERS'] = spark.createDataFrame(rows_src_orders, schema_src_orders)
    actual = transform(inputs)
    deterministic_columns = ['CUSTOMER_ID', 'CUSTOMER_NAME', 'REGION', 'TOTAL_ORDER_AMOUNT', 'ORDER_COUNT']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['CUSTOMER_ID']).collect()]
    expected_rows = [(3, 'ALICE BROWN', 'EAST', 400.0, 1), (4, 'BOB WHITE', 'WEST', 600.0, 1)]
    assert actual_rows == expected_rows

def test_004_tc4_null_order_status_handling(spark):
    inputs = {}
    schema_src_customers = StructType([StructField('CUSTOMER_ID', IntegerType(), False), StructField('CUST_NAME', StringType(), False), StructField('CUST_REGION', StringType(), False)])
    rows_src_customers = [(5, 'Charlie Green', 'Central')]
    inputs['SRC_CUSTOMERS'] = spark.createDataFrame(rows_src_customers, schema_src_customers)
    schema_src_orders = StructType([StructField('ORDER_ID', IntegerType(), False), StructField('CUSTOMER_ID', IntegerType(), False), StructField('ORDER_STATUS', StringType(), True), StructField('ORDER_AMOUNT', DecimalType(12, 2), False)])
    rows_src_orders = [(107, 5, None, Decimal('700.0'))]
    inputs['SRC_ORDERS'] = spark.createDataFrame(rows_src_orders, schema_src_orders)
    actual = transform(inputs)
    deterministic_columns = ['CUSTOMER_ID', 'CUSTOMER_NAME', 'REGION', 'TOTAL_ORDER_AMOUNT', 'ORDER_COUNT']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['CUSTOMER_ID']).collect()]
    expected_rows = []
    assert actual_rows == expected_rows
