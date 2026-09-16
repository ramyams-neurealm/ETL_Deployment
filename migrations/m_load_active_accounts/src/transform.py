from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def transform(inputs: dict[str, DataFrame]) -> DataFrame:
    stg_account_df = inputs["STG_ACCOUNT"]
    
    # Filter active accounts
    fil_active_accounts_df = stg_account_df.filter(col("ACCOUNT_STATUS") == "ACTIVE")
    
    # Select and rename columns for the target
    tgt_active_account_df = fil_active_accounts_df.select(
        col("ACCOUNT_ID").alias("ACCOUNT_ID"),
        col("ACCOUNT_BALANCE").alias("ACCOUNT_BALANCE")
    )
    
    return tgt_active_account_df
