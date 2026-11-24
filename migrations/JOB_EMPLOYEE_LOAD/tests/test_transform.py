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

def test_001_employee_load_01_happy_path_all_fields_present_and_valid(spark):
    inputs = {}
    schema_src_employee = StructType([StructField('EMP_ID', IntegerType(), False), StructField('EMP_NAME', StringType(), True), StructField('SALARY', DecimalType(12, 2), True)])
    rows_src_employee = [(1, 'John Doe', Decimal('50000.00')), (2, 'Jane Smith', Decimal('60000.00'))]
    inputs['SRC_EMPLOYEE'] = spark.createDataFrame(rows_src_employee, schema_src_employee)
    actual = transform(inputs, parameters={'ENV': 'DEV'})
    assert isinstance(actual, dict)
    assert set(actual) == set(['TGT_EMPLOYEE'])
    actual = actual['TGT_EMPLOYEE']
    deterministic_columns = ['EMP_ID', 'EMP_NAME', 'SALARY']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['EMP_ID']).collect()]
    expected_rows = [(1, 'JOHN DOE', Decimal('50000.00')), (2, 'JANE SMITH', Decimal('60000.00'))]
    assert actual_rows == expected_rows

def test_002_employee_load_02_null_emp_name_emp_name_transformation_to_uppercase(spark):
    inputs = {}
    schema_src_employee = StructType([StructField('EMP_ID', IntegerType(), False), StructField('EMP_NAME', StringType(), True), StructField('SALARY', DecimalType(12, 2), True)])
    rows_src_employee = [(3, None, Decimal('45000.00'))]
    inputs['SRC_EMPLOYEE'] = spark.createDataFrame(rows_src_employee, schema_src_employee)
    actual = transform(inputs, parameters={'ENV': 'DEV'})
    assert isinstance(actual, dict)
    assert set(actual) == set(['TGT_EMPLOYEE'])
    actual = actual['TGT_EMPLOYEE']
    deterministic_columns = ['EMP_ID', 'EMP_NAME', 'SALARY']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['EMP_ID']).collect()]
    expected_rows = [(3, None, Decimal('45000.00'))]
    assert actual_rows == expected_rows

def test_003_employee_load_03_boundary_test_minimum_salary(spark):
    inputs = {}
    schema_src_employee = StructType([StructField('EMP_ID', IntegerType(), False), StructField('EMP_NAME', StringType(), True), StructField('SALARY', DecimalType(12, 2), True)])
    rows_src_employee = [(4, 'Alice Johnson', Decimal('0.00'))]
    inputs['SRC_EMPLOYEE'] = spark.createDataFrame(rows_src_employee, schema_src_employee)
    actual = transform(inputs, parameters={'ENV': 'DEV'})
    assert isinstance(actual, dict)
    assert set(actual) == set(['TGT_EMPLOYEE'])
    actual = actual['TGT_EMPLOYEE']
    deterministic_columns = ['EMP_ID', 'EMP_NAME', 'SALARY']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['EMP_ID']).collect()]
    expected_rows = [(4, 'ALICE JOHNSON', Decimal('0.00'))]
    assert actual_rows == expected_rows

def test_004_employee_load_04_negative_test_invalid_emp_id(spark):
    inputs = {}
    schema_src_employee = StructType([StructField('EMP_ID', IntegerType(), False), StructField('EMP_NAME', StringType(), True), StructField('SALARY', DecimalType(12, 2), True)])
    rows_src_employee = [(5, 'Bob Brown', Decimal('70000.00'))]
    inputs['SRC_EMPLOYEE'] = spark.createDataFrame(rows_src_employee, schema_src_employee)
    actual = transform(inputs, parameters={'ENV': 'DEV'})
    assert isinstance(actual, dict)
    assert set(actual) == set(['TGT_EMPLOYEE'])
    actual = actual['TGT_EMPLOYEE']
    deterministic_columns = ['EMP_ID', 'EMP_NAME', 'SALARY']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['EMP_ID']).collect()]
    expected_rows = [(5, 'BOB BROWN', Decimal('70000.00'))]
    assert actual_rows == expected_rows
