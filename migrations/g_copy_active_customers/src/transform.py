from pyspark.sql import DataFrame
from typing import Any, Dict


def transform(inputs: Dict[str, DataFrame], *, parameters: Dict[str, Any] | None = None) -> Dict[str, DataFrame]:
    # Resolve the source DataFrame
    src_active_customers_df = inputs["SRC_ACTIVE_CUSTOMERS"]

    # Select and rename the necessary columns
    tgt_active_customers_df = src_active_customers_df.select(
        src_active_customers_df["CUSTOMER_ID"].alias("CUSTOMER_ID"),
        src_active_customers_df["EMAIL"].alias("EMAIL"),
        src_active_customers_df["CUSTOMER_STATUS"].alias("CUSTOMER_STATUS")
    )

    # Return the transformed DataFrame
    return {"TGT_ACTIVE_CUSTOMERS": tgt_active_customers_df}
