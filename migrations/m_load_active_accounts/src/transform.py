from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Any

def transform(inputs: dict[str, DataFrame], *, parameters: dict[str, Any] | None = None) -> dict[str, DataFrame]:
    parameters = parameters or {}
    
    # Extract the source DataFrame
    src_account_df = inputs["SRC_ACCOUNT"]
    
    # Filter active accounts
    active_accounts_df = src_account_df.filter(F.col("ACCOUNT_STATUS") == "ACTIVE")
    
    # Add load timestamp
    result_df = active_accounts_df.withColumn("LOAD_TIMESTAMP", F.current_timestamp())
    
    # Select and rename columns for the target schema
    result_df = result_df.select(
        F.col("ACCOUNT_ID"),
        F.col("ACCOUNT_STATUS"),
        F.col("ACCOUNT_BALANCE"),
        F.col("LOAD_TIMESTAMP")
    )
    
    return {"TGT_ACTIVE_ACCOUNT": result_df}
