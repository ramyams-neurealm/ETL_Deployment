from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def transform(inputs: dict[str, DataFrame]) -> DataFrame:
    # Resolve source DataFrame
    inventory_df = inputs["SRC_INVENTORY"]
    
    # Apply filter to select items below reorder level
    filtered_df = inventory_df.filter(
        F.col("ON_HAND_QTY") < F.col("REORDER_LEVEL")
    )
    
    # Calculate reorder quantity and select target columns
    result_df = filtered_df.select(
        F.col("ITEM_ID"),
        F.col("WAREHOUSE_ID"),
        F.col("ON_HAND_QTY"),
        (F.col("REORDER_LEVEL") - F.col("ON_HAND_QTY")).alias("REORDER_QTY")
    )
    
    return result_df
