from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper
from typing import Any

def transform(inputs: dict[str, DataFrame], *, parameters: dict[str, Any] | None = None) -> DataFrame:
    parameters = parameters or {}
    
    # Extract the source DataFrame
    src_employee_df = inputs["SRC_EMPLOYEE"]
    
    # Apply transformations
    transformed_df = src_employee_df.select(
        col("EMP_ID"),
        upper(col("EMP_NAME")).alias("EMP_NAME"),
        col("SALARY")
    )
    
    return transformed_df
