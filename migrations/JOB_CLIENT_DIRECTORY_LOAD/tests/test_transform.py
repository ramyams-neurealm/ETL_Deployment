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
    schema_src_client = StructType([StructField('CLIENT_CODE', StringType(), False), StructField('FIRST_NAME', StringType(), True), StructField('LAST_NAME', StringType(), True), StructField('ORGANIZATION_NAME', StringType(), True), StructField('DEPARTMENT_NAME', StringType(), True), StructField('JOB_TITLE', StringType(), True), StructField('OFFICE_LOCATION', StringType(), True), StructField('STATUS_CODE', StringType(), False)])
    rows_src_client = [('C001', 'John', 'Doe', 'OpenAI', 'Research', 'Scientist', 'San Francisco', 'Active')]
    inputs['SRC_CLIENT'] = spark.createDataFrame(rows_src_client, schema_src_client)
    actual = transform(inputs, parameters={'ENV': 'DEV'})
    deterministic_columns = ['CLIENT_CODE', 'FIRST_NAME', 'LAST_NAME', 'ORGANIZATION_NAME', 'DEPARTMENT_NAME', 'JOB_TITLE', 'OFFICE_LOCATION', 'STATUS_CODE']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['CLIENT_CODE']).collect()]
    expected_rows = [('C001', 'JOHN', 'DOE', 'OPENAI', 'RESEARCH', 'SCIENTIST', 'SAN FRANCISCO', 'ACTIVE')]
    assert actual_rows == expected_rows

def test_002_scenario_2_null_handling_for_optional_fields(spark):
    inputs = {}
    schema_src_client = StructType([StructField('CLIENT_CODE', StringType(), False), StructField('FIRST_NAME', StringType(), True), StructField('LAST_NAME', StringType(), True), StructField('ORGANIZATION_NAME', StringType(), True), StructField('DEPARTMENT_NAME', StringType(), True), StructField('JOB_TITLE', StringType(), True), StructField('OFFICE_LOCATION', StringType(), True), StructField('STATUS_CODE', StringType(), False)])
    rows_src_client = [('C002', None, None, None, None, None, None, 'Inactive')]
    inputs['SRC_CLIENT'] = spark.createDataFrame(rows_src_client, schema_src_client)
    actual = transform(inputs, parameters={'ENV': 'DEV'})
    deterministic_columns = ['CLIENT_CODE', 'FIRST_NAME', 'LAST_NAME', 'ORGANIZATION_NAME', 'DEPARTMENT_NAME', 'JOB_TITLE', 'OFFICE_LOCATION', 'STATUS_CODE']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['CLIENT_CODE']).collect()]
    expected_rows = [('C002', None, None, None, None, None, None, 'INACTIVE')]
    assert actual_rows == expected_rows

def test_003_scenario_3_boundary_test_for_maximum_field_lengths(spark):
    inputs = {}
    schema_src_client = StructType([StructField('CLIENT_CODE', StringType(), False), StructField('FIRST_NAME', StringType(), True), StructField('LAST_NAME', StringType(), True), StructField('ORGANIZATION_NAME', StringType(), True), StructField('DEPARTMENT_NAME', StringType(), True), StructField('JOB_TITLE', StringType(), True), StructField('OFFICE_LOCATION', StringType(), True), StructField('STATUS_CODE', StringType(), False)])
    rows_src_client = [('C003', 'Averylongfirstnameexceedingfiftycharacters', 'Averylonglastnameexceedingfiftycharacters', 'Averylongorganizationnameexceedingonehundredcharactersaverylongorganizationnameexceedingonehundredcharacters', 'Averylongdepartmentnameexceedingfiftycharacters', 'Averylongjobtitleexceedingfiftycharacters', 'Averylongofficelocationexceedingfiftycharacters', 'Averylongstatuscodeexceedingtwentycharacters')]
    inputs['SRC_CLIENT'] = spark.createDataFrame(rows_src_client, schema_src_client)
    actual = transform(inputs, parameters={'ENV': 'DEV'})
    deterministic_columns = ['CLIENT_CODE', 'FIRST_NAME', 'LAST_NAME', 'ORGANIZATION_NAME', 'DEPARTMENT_NAME', 'JOB_TITLE', 'OFFICE_LOCATION', 'STATUS_CODE']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['CLIENT_CODE']).collect()]
    expected_rows = [('C003', 'AVERYLONGFIRSTNAMEEXCEEDINGFIFTYCHARACTERS', 'AVERYLONGLASTNAMEEXCEEDINGFIFTYCHARACTERS', 'AVERYLONGORGANIZATIONNAMEEXCEEDINGONEHUNDREDCHARACTERSAVERYLONGORGANIZATIONNAMEEXCEEDINGONEHUNDREDCHARACTERS', 'AVERYLONGDEPARTMENTNAMEEXCEEDINGFIFTYCHARACTERS', 'AVERYLONGJOBTITLEEXCEEDINGFIFTYCHARACTERS', 'AVERYLONGOFFICELOCATIONEXCEEDINGFIFTYCHARACTERS', 'AVERYLONGSTATUSCODEEXCEEDINGTWENTYCHARACTERS')]
    assert actual_rows == expected_rows
