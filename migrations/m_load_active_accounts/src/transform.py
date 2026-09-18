from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from datetime import datetime

# Define the transform function

def transform(inputs: dict[str, DataFrame]) -> DataFrame:
    # Extract the source DataFrame
    src_account_df = inputs["SRC_ACCOUNT"]

    # Filter active accounts
    active_accounts_df = src_account_df.filter(F.col("ACCOUNT_STATUS") == "ACTIVE")

    # Add load timestamp
    result_df = active_accounts_df.withColumn("LOAD_TIMESTAMP", F.current_timestamp())

    # Select and return the final DataFrame
    return result_df.select(
        F.col("ACCOUNT_ID"),
        F.col("ACCOUNT_STATUS"),
        F.col("ACCOUNT_BALANCE"),
        F.col("LOAD_TIMESTAMP")
    )
