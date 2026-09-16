import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType
from transform import transform
from decimal import Decimal

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local").appName("pytest").getOrCreate()
    yield spark
    spark.stop()

def test_transform(spark):
    schema = StructType([
        StructField("ACCOUNT_ID", IntegerType(), False),
        StructField("ACCOUNT_STATUS", StringType(), True),
        StructField("ACCOUNT_BALANCE", DecimalType(15, 2), True)
    ])

    data = [
        (1, "ACTIVE", Decimal("1000.00")),
        (2, "INACTIVE", Decimal("2000.00")),
        (3, "ACTIVE", Decimal("3000.00")),
        (4, None, Decimal("4000.00"))
    ]

    stg_account_df = spark.createDataFrame(data, schema)
    inputs = {"STG_ACCOUNT": stg_account_df}

    result_df = transform(inputs)

    expected_data = [
        (1, Decimal("1000.00")),
        (3, Decimal("3000.00"))
    ]

    expected_schema = StructType([
        StructField("ACCOUNT_ID", IntegerType(), False),
        StructField("ACCOUNT_BALANCE", DecimalType(15, 2), True)
    ])

    expected_df = spark.createDataFrame(expected_data, expected_schema)

    assert result_df.collect() == expected_df.collect()
