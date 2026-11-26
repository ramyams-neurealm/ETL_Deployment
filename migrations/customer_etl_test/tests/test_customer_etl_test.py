"""Deterministically rendered regression tests."""
from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DateType, DecimalType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType)
from customer_etl_test import transform

@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder.master("local[1]").appName("neuflow-unit-test").getOrCreate())
    yield session
    session.stop()

def test_001_scenario_1_filter_and_reformat_us_customers(spark):
    inputs = {}
    schema_input_customer_table = StructType([StructField('customer_id', DecimalType(18, 0), False), StructField('customer_name', StringType(), False), StructField('country', StringType(), False), StructField('signup_date', DateType(), False)])
    rows_input_customer_table = [(Decimal('1'), 'John Doe', 'US', date.fromisoformat('2020-05-15')), (Decimal('2'), 'Jane Smith', 'CA', date.fromisoformat('2019-07-22')), (Decimal('3'), 'Alice Johnson', 'US', date.fromisoformat('2021-01-10'))]
    inputs['Input_Customer_Table'] = spark.createDataFrame(rows_input_customer_table, schema_input_customer_table)
    actual = transform(inputs, parameters={'TARGET_COUNTRY': 'US'})
    assert isinstance(actual, dict)
    assert set(actual) == set(['Output_US_Customers'])
    actual = actual['Output_US_Customers']
    deterministic_columns = ['customer_id', 'customer_name_upper', 'country', 'signup_year']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['customer_id']).collect()]
    expected_rows = [(Decimal('1'), 'JOHN DOE', 'US', Decimal('2020')), (Decimal('3'), 'ALICE JOHNSON', 'US', Decimal('2021'))]
    assert actual_rows == expected_rows

def test_002_scenario_2_no_us_customers(spark):
    inputs = {}
    schema_input_customer_table = StructType([StructField('customer_id', DecimalType(18, 0), False), StructField('customer_name', StringType(), False), StructField('country', StringType(), False), StructField('signup_date', DateType(), False)])
    rows_input_customer_table = [(Decimal('4'), 'Bob Brown', 'CA', date.fromisoformat('2018-11-30')), (Decimal('5'), 'Charlie Black', 'UK', date.fromisoformat('2020-03-15'))]
    inputs['Input_Customer_Table'] = spark.createDataFrame(rows_input_customer_table, schema_input_customer_table)
    actual = transform(inputs, parameters={'TARGET_COUNTRY': 'US'})
    assert isinstance(actual, dict)
    assert set(actual) == set(['Output_US_Customers'])
    actual = actual['Output_US_Customers']
    deterministic_columns = ['customer_id', 'customer_name_upper', 'country', 'signup_year']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['customer_id']).collect()]
    expected_rows = []
    assert actual_rows == expected_rows

def test_003_scenario_3_boundary_case_empty_customer_name(spark):
    inputs = {}
    schema_input_customer_table = StructType([StructField('customer_id', DecimalType(18, 0), False), StructField('customer_name', StringType(), False), StructField('country', StringType(), False), StructField('signup_date', DateType(), False)])
    rows_input_customer_table = [(Decimal('6'), ' ', 'US', date.fromisoformat('2022-06-01'))]
    inputs['Input_Customer_Table'] = spark.createDataFrame(rows_input_customer_table, schema_input_customer_table)
    actual = transform(inputs, parameters={'TARGET_COUNTRY': 'US'})
    assert isinstance(actual, dict)
    assert set(actual) == set(['Output_US_Customers'])
    actual = actual['Output_US_Customers']
    deterministic_columns = ['customer_id', 'customer_name_upper', 'country', 'signup_year']
    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*['customer_id']).collect()]
    expected_rows = [(Decimal('6'), '', 'US', Decimal('2022'))]
    assert actual_rows == expected_rows
