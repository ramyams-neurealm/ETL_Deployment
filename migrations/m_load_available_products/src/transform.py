from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp

def transform(inputs: dict[str, DataFrame], *, parameters: dict[str, any] = None) -> DataFrame:
    parameters = parameters or {}
    
    # Extract the source DataFrame
    src_product_df = inputs["SRC_PRODUCT"]
    
    # Apply filter to exclude discontinued products
    active_products_df = src_product_df.filter(col("STATUS") != "DISCONTINUED")
    
    # Add load date
    result_df = active_products_df.withColumn("LOAD_DATE", current_timestamp())
    
    # Select and order the final columns
    return result_df.select(
        col("PRODUCT_ID"),
        col("PRODUCT_NAME"),
        col("STOCK_QUANTITY"),
        col("LOAD_DATE")
    )
