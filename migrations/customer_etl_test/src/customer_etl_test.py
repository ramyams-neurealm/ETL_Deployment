from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from typing import Any

def transform(inputs: dict[str, DataFrame], *, parameters: dict[str, Any] | None = None) -> dict[str, DataFrame]:
    parameters = parameters or {}
    target_country = parameters.get("TARGET_COUNTRY", "US")
    
    # Extract the input DataFrame
    customers_df = inputs["Input_Customer_Table"]
    
    # Filter for active US customers
    filtered_df = customers_df.filter(F.col("country") == F.lit(target_country))
    
    # Reformat the DataFrame
    reformatted_df = filtered_df.select(
        F.col("customer_id").alias("customer_id"),
        F.upper(F.trim(F.col("customer_name"))).alias("customer_name_upper"),
        F.col("country").alias("country"),
        F.year(F.col("signup_date")).cast(DecimalType(38, 0)).alias("signup_year")
    )
    
    # Return the result as a dictionary
    return {"Output_US_Customers": reformatted_df}
