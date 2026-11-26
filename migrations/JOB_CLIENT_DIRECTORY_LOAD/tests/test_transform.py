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

def test_001_test_happy_path_happy_path_all_fields_populated_and_transformed_correctly(spark):
    inputs = {}
    schema_src_client = StructType([StructField('CLIENT_CODE', StringType(), False), StructField('FIRST_NAME', StringType(), True), StructField('LAST_NAME', StringType(), True), StructField('ORGANIZATION_NAME', StringType(), True), StructField('DEPARTMENT_NAME', StringType(), True), StructField('JOB_TITLE', StringType(), True), StructField('OFFICE_LOCATION', StringType(), True), StructField('STATUS_CODE', StringType(), False)])
    rows_src_client = [('C123', 'John', 'Doe', 'OpenAI', 'Research', 'Engineer', 'San Francisco', 'Active')]
    inputs['SRC_CLIENT'] = spark.createDataFrame(rows_src_client, schema_src_client)
    actual = transform(inputs, parameters={'ENV': 'DEV'})
    assert isinstance(actual, dict)
    assert set(actual) == set(['TGT_CLIENT'])
    actual = actual['TGT_CLIENT']
    deterministic_columns = ['CLIENT_CODE', 'FIRST_NAME', 'LAST_NAME', 'ORGANIZATION_NAME', 'DEPARTMENT_NAME', 'JOB_TITLE', 'OFFICE_LOCATION', 'STATUS_CODE']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['CLIENT_CODE']).collect()]
    expected_rows = [('C123', 'JOHN', 'DOE', 'OPENAI', 'RESEARCH', 'ENGINEER', 'SAN FRANCISCO', 'ACTIVE')]
    assert actual_rows == expected_rows

def test_002_test_null_handling_null_handling_optional_fields_are_null(spark):
    inputs = {}
    schema_src_client = StructType([StructField('CLIENT_CODE', StringType(), False), StructField('FIRST_NAME', StringType(), True), StructField('LAST_NAME', StringType(), True), StructField('ORGANIZATION_NAME', StringType(), True), StructField('DEPARTMENT_NAME', StringType(), True), StructField('JOB_TITLE', StringType(), True), StructField('OFFICE_LOCATION', StringType(), True), StructField('STATUS_CODE', StringType(), False)])
    rows_src_client = [('C124', None, None, None, None, None, None, 'Inactive')]
    inputs['SRC_CLIENT'] = spark.createDataFrame(rows_src_client, schema_src_client)
    actual = transform(inputs, parameters={'ENV': 'DEV'})
    assert isinstance(actual, dict)
    assert set(actual) == set(['TGT_CLIENT'])
    actual = actual['TGT_CLIENT']
    deterministic_columns = ['CLIENT_CODE', 'FIRST_NAME', 'LAST_NAME', 'ORGANIZATION_NAME', 'DEPARTMENT_NAME', 'JOB_TITLE', 'OFFICE_LOCATION', 'STATUS_CODE']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['CLIENT_CODE']).collect()]
    expected_rows = [('C124', None, None, None, None, None, None, 'INACTIVE')]
    assert actual_rows == expected_rows

def test_003_test_boundary_status_code_boundary_case_status_code_with_leading_and_trailing_spaces(spark):
    inputs = {}
    schema_src_client = StructType([StructField('CLIENT_CODE', StringType(), False), StructField('FIRST_NAME', StringType(), True), StructField('LAST_NAME', StringType(), True), StructField('ORGANIZATION_NAME', StringType(), True), StructField('DEPARTMENT_NAME', StringType(), True), StructField('JOB_TITLE', StringType(), True), StructField('OFFICE_LOCATION', StringType(), True), StructField('STATUS_CODE', StringType(), False)])
    rows_src_client = [('C125', 'Alice', 'Smith', 'AI Labs', 'Development', 'Data Scientist', 'New York', '  Pending  '), ('C126', 'Bob', 'Brown', 'Tech Corp', 'HR', 'Manager', 'Los Angeles', '  Active')]
    inputs['SRC_CLIENT'] = spark.createDataFrame(rows_src_client, schema_src_client)
    actual = transform(inputs, parameters={'ENV': 'DEV'})
    assert isinstance(actual, dict)
    assert set(actual) == set(['TGT_CLIENT'])
    actual = actual['TGT_CLIENT']
    deterministic_columns = ['CLIENT_CODE', 'FIRST_NAME', 'LAST_NAME', 'ORGANIZATION_NAME', 'DEPARTMENT_NAME', 'JOB_TITLE', 'OFFICE_LOCATION', 'STATUS_CODE']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['CLIENT_CODE']).collect()]
    expected_rows = [('C125', 'ALICE', 'SMITH', 'AI LABS', 'DEVELOPMENT', 'DATA SCIENTIST', 'NEW YORK', 'PENDING'), ('C126', 'BOB', 'BROWN', 'TECH CORP', 'HR', 'MANAGER', 'LOS ANGELES', 'ACTIVE')]
    assert actual_rows == expected_rows
