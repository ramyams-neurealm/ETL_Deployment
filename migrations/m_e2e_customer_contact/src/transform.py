from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Any

def transform(inputs: dict[str, DataFrame], *, parameters: dict[str, Any] | None = None) -> dict[str, DataFrame]:
    # Extract the source DataFrame
    src_customer_contact_df = inputs["SRC_CUSTOMER_CONTACT"]
    
    # Apply filter to keep records with a positive CUSTOMER_ID
    filtered_df = src_customer_contact_df.filter(F.col("CUSTOMER_ID") > 0)
    
    # Normalize EMAIL to lowercase
    normalized_df = filtered_df.withColumn("EMAIL", F.lower(F.col("EMAIL")))
    
    # Select the target columns in the specified order
    tgt_customer_contact_df = normalized_df.select(
        F.col("CUSTOMER_ID"),
        F.col("EMAIL")
    )
    
    return {"TGT_CUSTOMER_CONTACT": tgt_customer_contact_df}
