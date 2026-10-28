from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp

def transform(inputs: dict[str, DataFrame], *, parameters: dict[str, any] | None = None) -> DataFrame:
    parameters = parameters or {}
    
    # Extract the source DataFrame
    src_account_df = inputs["SRC_ACCOUNT"]
    
    # Apply the filter transformation to retain only active accounts
    fil_active_accounts_df = src_account_df.filter(col("ACCOUNT_STATUS") == "ACTIVE")
    
    # Add the load timestamp
    exp_add_load_timestamp_df = fil_active_accounts_df.withColumn("LOAD_TIMESTAMP", current_timestamp())
    
    # Select the target columns in the specified order
    tgt_active_account_df = exp_add_load_timestamp_df.select(
        col("ACCOUNT_ID"),
        col("ACCOUNT_STATUS"),
        col("ACCOUNT_BALANCE"),
        col("LOAD_TIMESTAMP")
    )
    
    return tgt_active_account_df
