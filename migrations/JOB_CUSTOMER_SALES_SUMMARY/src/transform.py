from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Any

def transform(inputs: dict[str, DataFrame], *, parameters: dict[str, Any] | None = None) -> dict[str, DataFrame]:
    parameters = parameters or {}
    
    # Load source DataFrames
    customers_df = inputs["SRC_CUSTOMERS"]
    orders_df = inputs["SRC_ORDERS"]

    # Join customers and orders on CUSTOMER_ID
    joined_df = customers_df.alias("customers").join(
        orders_df.alias("orders"),
        F.col("customers.CUSTOMER_ID") == F.col("orders.CUSTOMER_ID"),
        "inner"
    ).select(
        F.col("customers.CUSTOMER_ID"),
        F.col("customers.CUST_NAME"),
        F.col("customers.CUST_REGION"),
        F.col("orders.ORDER_ID"),
        F.col("orders.ORDER_STATUS"),
        F.col("orders.ORDER_AMOUNT")
    )

    # Filter for completed orders
    completed_orders_df = joined_df.filter(F.col("ORDER_STATUS") == "COMPLETED")

    # Standardize customer name and region to upper case
    standardized_df = completed_orders_df.select(
        F.col("CUSTOMER_ID"),
        F.upper(F.col("CUST_NAME")).alias("CUSTOMER_NAME"),
        F.upper(F.col("CUST_REGION")).alias("REGION"),
        F.col("ORDER_ID"),
        F.col("ORDER_AMOUNT")
    )

    # Aggregate total order amount and order count per customer
    aggregated_df = standardized_df.groupBy(
        "CUSTOMER_ID", "CUSTOMER_NAME", "REGION"
    ).agg(
        F.sum("ORDER_AMOUNT").alias("TOTAL_ORDER_AMOUNT"),
        F.count("ORDER_ID").alias("ORDER_COUNT")
    )

    # Return the final DataFrame
    return {"TGT_CUSTOMER_SALES_SUMMARY": aggregated_df}
