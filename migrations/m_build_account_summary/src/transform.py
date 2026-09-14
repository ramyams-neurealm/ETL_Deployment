from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def transform(input_df: DataFrame) -> DataFrame:
    return input_df.select(
        col('ACCOUNT_ID').cast('int'),
        col('ACCOUNT_BALANCE').cast('decimal(15,2)')
    )
