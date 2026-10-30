from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def transform(inputs: dict[str, DataFrame], *, parameters: dict[str, any] | None = None) -> DataFrame:
    parameters = parameters or {}
    
    # Extract the source DataFrame
    src_client_df = inputs["SRC_CLIENT"]

    # Apply transformations
    transformed_df = src_client_df.select(
        F.col("CLIENT_CODE"),
        F.upper(F.trim(F.col("FIRST_NAME"))).alias("FIRST_NAME"),
        F.upper(F.trim(F.col("LAST_NAME"))).alias("LAST_NAME"),
        F.upper(F.trim(F.col("ORGANIZATION_NAME"))).alias("ORGANIZATION_NAME"),
        F.upper(F.trim(F.col("DEPARTMENT_NAME"))).alias("DEPARTMENT_NAME"),
        F.upper(F.trim(F.col("JOB_TITLE"))).alias("JOB_TITLE"),
        F.upper(F.trim(F.col("OFFICE_LOCATION"))).alias("OFFICE_LOCATION"),
        F.upper(F.trim(F.col("STATUS_CODE"))).alias("STATUS_CODE")
    )

    return transformed_df
