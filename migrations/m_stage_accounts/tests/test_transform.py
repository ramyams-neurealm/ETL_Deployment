import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType
from decimal import Decimal
from transform import transform

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

    input_data = [
        (1, "active", Decimal("1000.00")),
        (2, "inactive", Decimal("200.50")),
        (3, None, Decimal("300.00"))
    ]

    expected_data = [
        (1, "ACTIVE", Decimal("1000.00")),
        (2, "INACTIVE", Decimal("200.50")),
        (3, None, Decimal("300.00"))
    ]

    raw_account_df = spark.createDataFrame(input_data, schema)
    inputs = {"RAW_ACCOUNT": raw_account_df}

    result_df = transform(inputs)
    expected_df = spark.createDataFrame(expected_data, schema)

    assert result_df.collect() == expected_df.collect()
