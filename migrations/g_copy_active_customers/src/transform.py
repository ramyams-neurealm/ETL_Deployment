from typing import Any, Dict
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def transform(inputs: Dict[str, DataFrame], *, parameters: Dict[str, Any] | None = None) -> Dict[str, DataFrame]:
    # Resolve the source DataFrame
    src_active_customers_df = inputs["SRC_ACTIVE_CUSTOMERS"]
    
    # Select and rename the necessary columns
    tgt_active_customers_df = src_active_customers_df.select(
        F.col("CUSTOMER_ID"),
        F.col("EMAIL"),
        F.col("CUSTOMER_STATUS")
    )
    
    # Return the transformed DataFrame
    return {"TGT_ACTIVE_CUSTOMERS": tgt_active_customers_df}
