from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, struct, current_timestamp

def add_file_metadata_column(df: DataFrame) -> DataFrame:
    """
    Add a column to a DataFrame that captures audit information about the file data was
    ingested from

    Parameters:
    df (DataFrame): The input DataFrame

    Returns:
    DataFrame: The DataFrame with added file audit column
    """
    return df.withColumn("file", struct(
            current_timestamp().alias("ingested_at"),
            expr("_metadata.file_path").alias("path"),
            expr("_metadata.file_name").alias("name")
        ))