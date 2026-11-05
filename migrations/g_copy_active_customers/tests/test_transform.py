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

def test_001_scenario_1_happy_path_with_all_fields_populated(spark):
    inputs = {}
    schema_src_active_customers = StructType([StructField('CUSTOMER_ID', IntegerType(), False), StructField('EMAIL', StringType(), True), StructField('CUSTOMER_STATUS', StringType(), False)])
    rows_src_active_customers = [(1, 'customer1@example.com', 'ACTIVE'), (2, 'customer2@example.com', 'INACTIVE')]
    inputs['SRC_ACTIVE_CUSTOMERS'] = spark.createDataFrame(rows_src_active_customers, schema_src_active_customers)
    actual = transform(inputs)
    assert isinstance(actual, dict)
    assert set(actual) == set(['TGT_ACTIVE_CUSTOMERS'])
    actual = actual['TGT_ACTIVE_CUSTOMERS']
    deterministic_columns = ['CUSTOMER_ID', 'EMAIL', 'CUSTOMER_STATUS']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['CUSTOMER_ID']).collect()]
    expected_rows = [(1, 'customer1@example.com', 'ACTIVE'), (2, 'customer2@example.com', 'INACTIVE')]
    assert actual_rows == expected_rows

def test_002_scenario_2_handling_null_email_field(spark):
    inputs = {}
    schema_src_active_customers = StructType([StructField('CUSTOMER_ID', IntegerType(), False), StructField('EMAIL', StringType(), True), StructField('CUSTOMER_STATUS', StringType(), False)])
    rows_src_active_customers = [(3, None, 'ACTIVE')]
    inputs['SRC_ACTIVE_CUSTOMERS'] = spark.createDataFrame(rows_src_active_customers, schema_src_active_customers)
    actual = transform(inputs)
    assert isinstance(actual, dict)
    assert set(actual) == set(['TGT_ACTIVE_CUSTOMERS'])
    actual = actual['TGT_ACTIVE_CUSTOMERS']
    deterministic_columns = ['CUSTOMER_ID', 'EMAIL', 'CUSTOMER_STATUS']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['CUSTOMER_ID']).collect()]
    expected_rows = [(3, None, 'ACTIVE')]
    assert actual_rows == expected_rows

def test_003_scenario_3_boundary_test_with_minimum_customer_id(spark):
    inputs = {}
    schema_src_active_customers = StructType([StructField('CUSTOMER_ID', IntegerType(), False), StructField('EMAIL', StringType(), True), StructField('CUSTOMER_STATUS', StringType(), False)])
    rows_src_active_customers = [(0, 'boundary@example.com', 'ACTIVE')]
    inputs['SRC_ACTIVE_CUSTOMERS'] = spark.createDataFrame(rows_src_active_customers, schema_src_active_customers)
    actual = transform(inputs)
    assert isinstance(actual, dict)
    assert set(actual) == set(['TGT_ACTIVE_CUSTOMERS'])
    actual = actual['TGT_ACTIVE_CUSTOMERS']
    deterministic_columns = ['CUSTOMER_ID', 'EMAIL', 'CUSTOMER_STATUS']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['CUSTOMER_ID']).collect()]
    expected_rows = [(0, 'boundary@example.com', 'ACTIVE')]
    assert actual_rows == expected_rows
