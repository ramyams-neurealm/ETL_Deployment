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
    # Define schema for TGT_ACTIVE_ACCOUNT
    schema = StructType([
        StructField("ACCOUNT_ID", IntegerType(), False),
        StructField("ACCOUNT_BALANCE", DecimalType(15, 2), True)
    ])

    # Create input DataFrame
    data = [
        (1, Decimal("1000.00")),
        (2, None),
        (3, Decimal("250.50"))
    ]
    tgt_active_account_df = spark.createDataFrame(data, schema)

    # Prepare inputs dictionary
    inputs = {"TGT_ACTIVE_ACCOUNT": tgt_active_account_df}

    # Execute the transformation
    result_df = transform(inputs)

    # Collect results
    result = {row["ACCOUNT_ID"]: row["ACCOUNT_BALANCE"] for row in result_df.collect()}

    # Define expected results
    expected = {
        1: Decimal("1000.00"),
        2: None,
        3: Decimal("250.50")
    }

    # Assert results
    assert result == expected
