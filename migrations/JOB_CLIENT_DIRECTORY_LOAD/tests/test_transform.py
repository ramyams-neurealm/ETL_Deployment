import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from transform import transform
from decimal import Decimal

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local").appName("test").getOrCreate()
    yield spark
    spark.stop()

def test_transform(spark):
    input_data = [
        Row(CLIENT_CODE="C001", FIRST_NAME=" John ", LAST_NAME="Doe ", ORGANIZATION_NAME=" Acme Corp ", DEPARTMENT_NAME=" Sales ", JOB_TITLE=" Manager ", OFFICE_LOCATION=" HQ ", STATUS_CODE=" Active "),
        Row(CLIENT_CODE="C002", FIRST_NAME=None, LAST_NAME=None, ORGANIZATION_NAME=None, DEPARTMENT_NAME=None, JOB_TITLE=None, OFFICE_LOCATION=None, STATUS_CODE=" Inactive ")
    ]
    expected_data = [
        Row(CLIENT_CODE="C001", FIRST_NAME="JOHN", LAST_NAME="DOE", ORGANIZATION_NAME="ACME CORP", DEPARTMENT_NAME="SALES", JOB_TITLE="MANAGER", OFFICE_LOCATION="HQ", STATUS_CODE="ACTIVE"),
        Row(CLIENT_CODE="C002", FIRST_NAME=None, LAST_NAME=None, ORGANIZATION_NAME=None, DEPARTMENT_NAME=None, JOB_TITLE=None, OFFICE_LOCATION=None, STATUS_CODE="INACTIVE")
    ]

    src_client_df = spark.createDataFrame(input_data)
    inputs = {"SRC_CLIENT": src_client_df}
    result_df = transform(inputs)
    expected_df = spark.createDataFrame(expected_data)

    assert result_df.collect() == expected_df.collect()
