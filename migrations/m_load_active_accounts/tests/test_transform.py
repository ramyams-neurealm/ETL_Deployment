import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType
from decimal import Decimal
from transform import transform

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .appName("pytest") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_transform(spark):
    schema = StructType([
        StructField("ACCOUNT_ID", IntegerType(), False),
        StructField("ACCOUNT_STATUS", StringType(), True),
        StructField("ACCOUNT_BALANCE", DecimalType(15, 2), True)
    ])

    data = [
        (1, 'ACTIVE', Decimal('100.00')),
        (2, 'INACTIVE', Decimal('200.00')),
        (3, 'ACTIVE', Decimal('300.00')),
        (4, None, Decimal('400.00'))
    ]

    input_df = spark.createDataFrame(data, schema)
    result_df = transform(input_df)

    expected_data = [
        (1, Decimal('100.00')),
        (3, Decimal('300.00'))
    ]

    expected_schema = StructType([
        StructField("ACCOUNT_ID", IntegerType(), False),
        StructField("ACCOUNT_BALANCE", DecimalType(15, 2), True)
    ])

    expected_df = spark.createDataFrame(expected_data, expected_schema)

    assert result_df.collect() == expected_df.collect()
