from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp

def transform(inputs: dict[str, DataFrame], *, parameters: dict[str, any] | None = None) -> DataFrame:
    parameters = parameters or {}
    
    # Resolve source DataFrame
    src_product_df = inputs["SRC_PRODUCT"]
    
    # Filter out discontinued products
    active_products_df = src_product_df.filter(col("STATUS") != "DISCONTINUED")
    
    # Add load date
    result_df = active_products_df.select(
        col("PRODUCT_ID"),
        col("PRODUCT_NAME"),
        col("STOCK_QUANTITY"),
        current_timestamp().alias("LOAD_DATE")
    )
    
    return result_df
