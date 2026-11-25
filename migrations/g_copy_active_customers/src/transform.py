from pyspark.sql import DataFrame
from typing import Any, Dict

def transform(inputs: Dict[str, DataFrame], *, parameters: Dict[str, Any] | None = None) -> Dict[str, DataFrame]:
    # Extract the source DataFrame
    src_active_customers_df = inputs["SRC_ACTIVE_CUSTOMERS"]
    
    # Select the required columns for the target
    tgt_active_customers_df = src_active_customers_df.select(
        src_active_customers_df["CUSTOMER_ID"],
        src_active_customers_df["EMAIL"],
        src_active_customers_df["CUSTOMER_STATUS"]
    )
    
    # Return the result as a dictionary with the target dataset name
    return {"TGT_ACTIVE_CUSTOMERS": tgt_active_customers_df}
