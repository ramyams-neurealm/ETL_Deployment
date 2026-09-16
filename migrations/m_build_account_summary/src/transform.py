from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def transform(inputs: dict[str, DataFrame]) -> DataFrame:
    # Resolve the source DataFrame
    tgt_active_account_df = inputs["TGT_ACTIVE_ACCOUNT"]
    
    # Select and rename the necessary columns
    account_summary_df = tgt_active_account_df.select(
        col("ACCOUNT_ID"),
        col("ACCOUNT_BALANCE")
    )
    
    return account_summary_df
