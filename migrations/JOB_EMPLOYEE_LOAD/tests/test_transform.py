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

def test_001_employee_load_01_happy_path_with_valid_employee_data(spark):
    inputs = {}
    schema_src_employee = StructType([StructField('EMP_ID', IntegerType(), False), StructField('EMP_NAME', StringType(), True), StructField('SALARY', DecimalType(12, 2), True)])
    rows_src_employee = [(1, 'John Doe', Decimal('50000.0')), (2, 'Jane Smith', Decimal('60000.0'))]
    inputs['SRC_EMPLOYEE'] = spark.createDataFrame(rows_src_employee, schema_src_employee)
    actual = transform(inputs, parameters={'ENV': 'DEV'})
    assert isinstance(actual, dict)
    assert set(actual) == set(['TGT_EMPLOYEE'])
    actual = actual['TGT_EMPLOYEE']
    deterministic_columns = ['EMP_ID', 'EMP_NAME', 'SALARY']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['EMP_ID']).collect()]
    expected_rows = [(1, 'JOHN DOE', Decimal('50000.0')), (2, 'JANE SMITH', Decimal('60000.0'))]
    assert actual_rows == expected_rows

def test_002_employee_load_02_null_handling_for_emp_name(spark):
    inputs = {}
    schema_src_employee = StructType([StructField('EMP_ID', IntegerType(), False), StructField('EMP_NAME', StringType(), True), StructField('SALARY', DecimalType(12, 2), True)])
    rows_src_employee = [(3, None, Decimal('45000.0'))]
    inputs['SRC_EMPLOYEE'] = spark.createDataFrame(rows_src_employee, schema_src_employee)
    actual = transform(inputs, parameters={'ENV': 'DEV'})
    assert isinstance(actual, dict)
    assert set(actual) == set(['TGT_EMPLOYEE'])
    actual = actual['TGT_EMPLOYEE']
    deterministic_columns = ['EMP_ID', 'EMP_NAME', 'SALARY']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['EMP_ID']).collect()]
    expected_rows = [(3, None, Decimal('45000.0'))]
    assert actual_rows == expected_rows

def test_003_employee_load_03_boundary_test_for_salary_with_zero_value(spark):
    inputs = {}
    schema_src_employee = StructType([StructField('EMP_ID', IntegerType(), False), StructField('EMP_NAME', StringType(), True), StructField('SALARY', DecimalType(12, 2), True)])
    rows_src_employee = [(4, 'Alice Brown', Decimal('0.0'))]
    inputs['SRC_EMPLOYEE'] = spark.createDataFrame(rows_src_employee, schema_src_employee)
    actual = transform(inputs, parameters={'ENV': 'DEV'})
    assert isinstance(actual, dict)
    assert set(actual) == set(['TGT_EMPLOYEE'])
    actual = actual['TGT_EMPLOYEE']
    deterministic_columns = ['EMP_ID', 'EMP_NAME', 'SALARY']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['EMP_ID']).collect()]
    expected_rows = [(4, 'ALICE BROWN', Decimal('0.0'))]
    assert actual_rows == expected_rows

def test_004_employee_load_04_negative_test_with_missing_emp_id(spark):
    inputs = {}
    schema_src_employee = StructType([StructField('EMP_ID', IntegerType(), False), StructField('EMP_NAME', StringType(), True), StructField('SALARY', DecimalType(12, 2), True)])
    rows_src_employee = [(5, 'Charlie Black', Decimal('55000.0'))]
    inputs['SRC_EMPLOYEE'] = spark.createDataFrame(rows_src_employee, schema_src_employee)
    actual = transform(inputs, parameters={'ENV': 'DEV'})
    assert isinstance(actual, dict)
    assert set(actual) == set(['TGT_EMPLOYEE'])
    actual = actual['TGT_EMPLOYEE']
    deterministic_columns = ['EMP_ID', 'EMP_NAME', 'SALARY']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['EMP_ID']).collect()]
    expected_rows = [(5, 'CHARLIE BLACK', Decimal('55000.0'))]
    assert actual_rows == expected_rows
