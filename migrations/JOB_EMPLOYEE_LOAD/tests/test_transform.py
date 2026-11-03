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

def test_001_scenario_1_basic_transformation_with_all_fields_populated(spark):
    inputs = {}
    schema_src_employee = StructType([StructField('EMP_ID', IntegerType(), False), StructField('EMP_NAME', StringType(), True), StructField('SALARY', DecimalType(12, 2), True)])
    rows_src_employee = [(1, 'John Doe', Decimal('50000.0')), (2, 'Jane Smith', Decimal('60000.0'))]
    inputs['SRC_EMPLOYEE'] = spark.createDataFrame(rows_src_employee, schema_src_employee)
    actual = transform(inputs, parameters={'ENV': 'DEV'})
    deterministic_columns = ['EMP_ID', 'EMP_NAME', 'SALARY']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['EMP_ID']).collect()]
    expected_rows = [(1, 'JOHN DOE', Decimal('50000.0')), (2, 'JANE SMITH', Decimal('60000.0'))]
    assert actual_rows == expected_rows

def test_002_scenario_2_transformation_with_null_emp_name(spark):
    inputs = {}
    schema_src_employee = StructType([StructField('EMP_ID', IntegerType(), False), StructField('EMP_NAME', StringType(), True), StructField('SALARY', DecimalType(12, 2), True)])
    rows_src_employee = [(3, None, Decimal('70000.0'))]
    inputs['SRC_EMPLOYEE'] = spark.createDataFrame(rows_src_employee, schema_src_employee)
    actual = transform(inputs, parameters={'ENV': 'DEV'})
    deterministic_columns = ['EMP_ID', 'EMP_NAME', 'SALARY']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['EMP_ID']).collect()]
    expected_rows = [(3, None, Decimal('70000.0'))]
    assert actual_rows == expected_rows

def test_003_scenario_3_transformation_with_null_salary(spark):
    inputs = {}
    schema_src_employee = StructType([StructField('EMP_ID', IntegerType(), False), StructField('EMP_NAME', StringType(), True), StructField('SALARY', DecimalType(12, 2), True)])
    rows_src_employee = [(4, 'Alice Johnson', None)]
    inputs['SRC_EMPLOYEE'] = spark.createDataFrame(rows_src_employee, schema_src_employee)
    actual = transform(inputs, parameters={'ENV': 'DEV'})
    deterministic_columns = ['EMP_ID', 'EMP_NAME', 'SALARY']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['EMP_ID']).collect()]
    expected_rows = [(4, 'ALICE JOHNSON', None)]
    assert actual_rows == expected_rows

def test_004_scenario_4_boundary_test_with_maximum_emp_id(spark):
    inputs = {}
    schema_src_employee = StructType([StructField('EMP_ID', IntegerType(), False), StructField('EMP_NAME', StringType(), True), StructField('SALARY', DecimalType(12, 2), True)])
    rows_src_employee = [(2147483647, 'Max Int', Decimal('80000.0'))]
    inputs['SRC_EMPLOYEE'] = spark.createDataFrame(rows_src_employee, schema_src_employee)
    actual = transform(inputs, parameters={'ENV': 'DEV'})
    deterministic_columns = ['EMP_ID', 'EMP_NAME', 'SALARY']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['EMP_ID']).collect()]
    expected_rows = [(2147483647, 'MAX INT', Decimal('80000.0'))]
    assert actual_rows == expected_rows
