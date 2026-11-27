from typing import Any, Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper

def transform(inputs: Dict[str, DataFrame], *, parameters: Dict[str, Any] | None = None) -> Dict[str, DataFrame]:
    parameters = parameters or {}
    
    # Extract the source DataFrame
    src_employee_df = inputs["SRC_EMPLOYEE"]
    
    # Apply transformations
    transformed_df = src_employee_df.select(
        col("EMP_ID"),
        upper(col("EMP_NAME")).alias("EMP_NAME"),
        col("SALARY")
    )
    
    # Return the transformed DataFrame as the target
    return {"TGT_EMPLOYEE": transformed_df}
