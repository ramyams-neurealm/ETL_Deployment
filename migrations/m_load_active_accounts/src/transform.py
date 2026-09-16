from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp

def transform(inputs: dict[str, DataFrame]) -> DataFrame:
    # Resolve source DataFrame
    src_account_df = inputs["SRC_ACCOUNT"]
    
    # Filter active accounts
    active_accounts_df = src_account_df.filter(col("ACCOUNT_STATUS") == "ACTIVE")
    
    # Add load timestamp
    result_df = active_accounts_df.withColumn("LOAD_TIMESTAMP", current_timestamp())
    
    # Select target columns in order
    return result_df.select(
        col("ACCOUNT_ID"),
        col("ACCOUNT_STATUS"),
        col("ACCOUNT_BALANCE"),
        col("LOAD_TIMESTAMP")
    )
