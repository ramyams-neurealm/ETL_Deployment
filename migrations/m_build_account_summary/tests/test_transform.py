import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DecimalType
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
        StructField("ACCOUNT_BALANCE", DecimalType(15, 2), True)
    ])
    data = [
        (1, Decimal('1000.00')),
        (2, None),
        (3, Decimal('250.50'))
    ]
    input_df = spark.createDataFrame(data, schema)

    result_df = transform(input_df)
    result_data = [
        (1, Decimal('1000.00')),
        (2, None),
        (3, Decimal('250.50'))
    ]
    expected_df = spark.createDataFrame(result_data, schema)

    assert result_df.collect() == expected_df.collect()
