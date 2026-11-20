from typing import Any, Dict
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from decimal import Decimal


def transform(inputs: Dict[str, DataFrame], *, parameters: Dict[str, Any] | None = None) -> Dict[str, DataFrame]:
    parameters = parameters or {}
    
    # Load source DataFrames
    customers_df = inputs["SRC_CUSTOMERS"]
    orders_df = inputs["SRC_ORDERS"]

    # Join customers and orders on CUSTOMER_ID
    joined_df = customers_df.alias("customers").join(
        orders_df.alias("orders"),
        customers_df["CUSTOMER_ID"] == orders_df["CUSTOMER_ID"],
        "inner"
    ).select(
        customers_df["CUSTOMER_ID"],
        customers_df["CUST_NAME"],
        customers_df["CUST_REGION"],
        orders_df["ORDER_ID"],
        orders_df["ORDER_STATUS"],
        orders_df["ORDER_AMOUNT"]
    )

    # Filter completed orders
    completed_orders_df = joined_df.filter(F.col("ORDER_STATUS") == "COMPLETED")

    # Standardize customer name and region to upper case
    standardized_df = completed_orders_df.select(
        F.col("CUSTOMER_ID"),
        F.upper(F.col("CUST_NAME")).alias("CUSTOMER_NAME"),
        F.upper(F.col("CUST_REGION")).alias("REGION"),
        F.col("ORDER_ID"),
        F.col("ORDER_AMOUNT")
    )

    # Aggregate total order amount and count per customer
    aggregated_df = standardized_df.groupBy(
        "CUSTOMER_ID", "CUSTOMER_NAME", "REGION"
    ).agg(
        F.sum("ORDER_AMOUNT").alias("TOTAL_ORDER_AMOUNT"),
        F.count("ORDER_ID").alias("ORDER_COUNT")
    )

    # Return the result as a dictionary
    return {"TGT_CUSTOMER_SALES_SUMMARY": aggregated_df}
