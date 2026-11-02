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

def test_001_tc1_filter_and_export_active_customers(spark):
    inputs = {}
    schema_src_customers = StructType([StructField('CUSTOMER_ID', IntegerType(), False), StructField('EMAIL', StringType(), True), StructField('STATUS', StringType(), False)])
    rows_src_customers = [(1, 'customer1@example.com', 'ACTIVE'), (2, 'customer2@example.com', 'INACTIVE'), (3, 'customer3@example.com', 'ACTIVE')]
    inputs['SRC_CUSTOMERS'] = spark.createDataFrame(rows_src_customers, schema_src_customers)
    actual = transform(inputs)
    deterministic_columns = ['CUSTOMER_ID', 'EMAIL', 'CUSTOMER_STATUS']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['CUSTOMER_ID']).collect()]
    expected_rows = [(1, 'customer1@example.com', 'ACTIVE'), (3, 'customer3@example.com', 'ACTIVE')]
    assert actual_rows == expected_rows

def test_002_tc2_handle_null_email_for_active_customers(spark):
    inputs = {}
    schema_src_customers = StructType([StructField('CUSTOMER_ID', IntegerType(), False), StructField('EMAIL', StringType(), True), StructField('STATUS', StringType(), False)])
    rows_src_customers = [(4, None, 'ACTIVE'), (5, 'customer5@example.com', 'ACTIVE')]
    inputs['SRC_CUSTOMERS'] = spark.createDataFrame(rows_src_customers, schema_src_customers)
    actual = transform(inputs)
    deterministic_columns = ['CUSTOMER_ID', 'EMAIL', 'CUSTOMER_STATUS']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['CUSTOMER_ID']).collect()]
    expected_rows = [(4, None, 'ACTIVE'), (5, 'customer5@example.com', 'ACTIVE')]
    assert actual_rows == expected_rows

def test_003_tc3_no_active_customers(spark):
    inputs = {}
    schema_src_customers = StructType([StructField('CUSTOMER_ID', IntegerType(), False), StructField('EMAIL', StringType(), True), StructField('STATUS', StringType(), False)])
    rows_src_customers = [(6, 'customer6@example.com', 'INACTIVE'), (7, 'customer7@example.com', 'INACTIVE')]
    inputs['SRC_CUSTOMERS'] = spark.createDataFrame(rows_src_customers, schema_src_customers)
    actual = transform(inputs)
    deterministic_columns = ['CUSTOMER_ID', 'EMAIL', 'CUSTOMER_STATUS']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['CUSTOMER_ID']).collect()]
    expected_rows = []
    assert actual_rows == expected_rows
