from pyspark.sql import DataFrame
from pyspark.sql.functions import col, min, max, avg, stddev_pop, expr

def summarise_daily(df: DataFrame) -> DataFrame:
    """
    Summarise the turbine power telemetry by day highlighting anomalies

    Parameters:
    df (DataFrame): A DataFrame containing turbine telemetry data

    Returns:
    DataFrame: A DataFrame with telemetry summarised by day
    """
    return (df.withColumn("date", col("timestamp").cast("date"))
            .groupBy("date", "turbine_id")
            .agg(
                min("power_output").alias("min_power_output"),
                max("power_output").alias("max_power_output"),
                avg("power_output").alias("avg_power_output"),
                stddev_pop("power_output").alias("stddev_power_output")
            )
            .withColumn("has_anomaly", (
                expr("min_power_output < avg_power_output - (2 * stddev_power_output)")
                | expr("max_power_output > avg_power_output + (2 * stddev_power_output)"))
            )
            .drop("stddev_power_output")
    )
    