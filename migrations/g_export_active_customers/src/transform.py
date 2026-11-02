from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def transform(inputs: dict[str, DataFrame]) -> DataFrame:
    # Resolve source DataFrame
    customers_df = inputs["SRC_CUSTOMERS"]
    
    # Apply filter to select active customers
    active_customers_df = customers_df.filter(col("STATUS") == "ACTIVE")
    
    # Reformat and select the required columns
    result_df = active_customers_df.select(
        col("CUSTOMER_ID"),
        col("EMAIL"),
        col("STATUS").alias("CUSTOMER_STATUS")
    )
    
    return result_df
