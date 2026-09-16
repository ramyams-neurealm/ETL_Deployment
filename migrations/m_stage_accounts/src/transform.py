from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper

def transform(inputs: dict[str, DataFrame]) -> DataFrame:
    raw_account_df = inputs["RAW_ACCOUNT"]
    
    standardized_df = raw_account_df.select(
        col("ACCOUNT_ID"),
        upper(col("ACCOUNT_STATUS")).alias("ACCOUNT_STATUS"),
        col("ACCOUNT_BALANCE")
    )
    
    return standardized_df
