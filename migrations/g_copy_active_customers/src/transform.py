from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def transform(inputs: dict[str, DataFrame]) -> DataFrame:
    # Extract the source DataFrame
    src_active_customers_df = inputs["SRC_ACTIVE_CUSTOMERS"]
    
    # Select and rename the necessary columns
    result_df = src_active_customers_df.select(
        col("CUSTOMER_ID"),
        col("EMAIL"),
        col("CUSTOMER_STATUS")
    )
    
    return result_df
