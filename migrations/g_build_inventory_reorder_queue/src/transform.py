from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def transform(inputs: dict[str, DataFrame]) -> DataFrame:
    # Resolve source DataFrame
    inventory_df = inputs["SRC_INVENTORY"]
    
    # Apply filter to select items below reorder level
    filtered_df = inventory_df.filter(col("ON_HAND_QTY") < col("REORDER_LEVEL"))
    
    # Calculate reorder quantity and select target columns
    result_df = filtered_df.select(
        col("ITEM_ID"),
        col("WAREHOUSE_ID"),
        col("ON_HAND_QTY"),
        (col("REORDER_LEVEL") - col("ON_HAND_QTY")).alias("REORDER_QTY")
    )
    
    return result_df
