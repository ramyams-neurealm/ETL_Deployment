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

def test_001_tc_1_happy_path_all_fields_populated(spark):
    inputs = {}
    schema_src_active_customers = StructType([StructField('CUSTOMER_ID', IntegerType(), False), StructField('EMAIL', StringType(), True), StructField('CUSTOMER_STATUS', StringType(), False)])
    rows_src_active_customers = [(1, 'customer1@example.com', 'Active'), (2, 'customer2@example.com', 'Inactive')]
    inputs['SRC_ACTIVE_CUSTOMERS'] = spark.createDataFrame(rows_src_active_customers, schema_src_active_customers)
    actual = transform(inputs)
    deterministic_columns = ['CUSTOMER_ID', 'EMAIL', 'CUSTOMER_STATUS']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['CUSTOMER_ID']).collect()]
    expected_rows = [(1, 'customer1@example.com', 'Active'), (2, 'customer2@example.com', 'Inactive')]
    assert actual_rows == expected_rows

def test_002_tc_2_null_handling_nullable_email_field(spark):
    inputs = {}
    schema_src_active_customers = StructType([StructField('CUSTOMER_ID', IntegerType(), False), StructField('EMAIL', StringType(), True), StructField('CUSTOMER_STATUS', StringType(), False)])
    rows_src_active_customers = [(3, None, 'Active')]
    inputs['SRC_ACTIVE_CUSTOMERS'] = spark.createDataFrame(rows_src_active_customers, schema_src_active_customers)
    actual = transform(inputs)
    deterministic_columns = ['CUSTOMER_ID', 'EMAIL', 'CUSTOMER_STATUS']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['CUSTOMER_ID']).collect()]
    expected_rows = [(3, None, 'Active')]
    assert actual_rows == expected_rows

def test_003_tc_3_boundary_case_minimum_customer_id(spark):
    inputs = {}
    schema_src_active_customers = StructType([StructField('CUSTOMER_ID', IntegerType(), False), StructField('EMAIL', StringType(), True), StructField('CUSTOMER_STATUS', StringType(), False)])
    rows_src_active_customers = [(0, 'boundary@example.com', 'Active')]
    inputs['SRC_ACTIVE_CUSTOMERS'] = spark.createDataFrame(rows_src_active_customers, schema_src_active_customers)
    actual = transform(inputs)
    deterministic_columns = ['CUSTOMER_ID', 'EMAIL', 'CUSTOMER_STATUS']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['CUSTOMER_ID']).collect()]
    expected_rows = [(0, 'boundary@example.com', 'Active')]
    assert actual_rows == expected_rows

def test_004_tc_4_negative_case_invalid_customer_status(spark):
    inputs = {}
    schema_src_active_customers = StructType([StructField('CUSTOMER_ID', IntegerType(), False), StructField('EMAIL', StringType(), True), StructField('CUSTOMER_STATUS', StringType(), False)])
    rows_src_active_customers = [(4, 'invalid@example.com', 'Unknown')]
    inputs['SRC_ACTIVE_CUSTOMERS'] = spark.createDataFrame(rows_src_active_customers, schema_src_active_customers)
    actual = transform(inputs)
    deterministic_columns = ['CUSTOMER_ID', 'EMAIL', 'CUSTOMER_STATUS']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['CUSTOMER_ID']).collect()]
    expected_rows = [(4, 'invalid@example.com', 'Unknown')]
    assert actual_rows == expected_rows
