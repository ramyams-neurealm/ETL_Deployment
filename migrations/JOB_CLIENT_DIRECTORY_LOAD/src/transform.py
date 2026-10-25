from pyspark.sql import DataFrame
from pyspark.sql.functions import trim, upper, col

def transform(inputs: dict[str, DataFrame]) -> DataFrame:
    src_client_df = inputs["SRC_CLIENT"]
    
    transformed_df = src_client_df.select(
        col("CLIENT_CODE"),
        upper(trim(col("FIRST_NAME"))).alias("FIRST_NAME"),
        upper(trim(col("LAST_NAME"))).alias("LAST_NAME"),
        upper(trim(col("ORGANIZATION_NAME"))).alias("ORGANIZATION_NAME"),
        upper(trim(col("DEPARTMENT_NAME"))).alias("DEPARTMENT_NAME"),
        upper(trim(col("JOB_TITLE"))).alias("JOB_TITLE"),
        upper(trim(col("OFFICE_LOCATION"))).alias("OFFICE_LOCATION"),
        upper(trim(col("STATUS_CODE"))).alias("STATUS_CODE")
    )
    
    return transformed_df
