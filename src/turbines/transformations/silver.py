from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, col, expr, create_map, array_contains, map_values
from pyspark.sql.types import StringType, BooleanType, MapType

def drop_audit_columns(df: DataFrame) -> DataFrame:
    """
    Drop file audit column from a DataFrame

    Parameters:
    df (DataFrame): The input DataFrame

    Returns:
    DataFrame: The DataFrame without the file audit column
    """
    return df.drop("file", "_rescued_data")

def quarantine(df: DataFrame, rules: dict) -> DataFrame:
    """
    Filter on data that has been quarantined and add a _quarantine_reasons column to the
    DataFrame based on the provided rules

    Parameters:
    df (DataFrame): The input DataFrame
    rules (dict): A dictionary where the keys are the quanrantine reason names and values
    are the expressions to calculate them

    Returns:
    DataFrame: The DataFrame with quarantined data and _quarantine_reasons column added
    """

    # Create a list of expressions for the map
    map_expr = []
    for rule_name, rule_expr in rules.items():
        map_expr.append(lit(rule_name))
        map_expr.append(expr(f"NOT({rule_expr})"))

    # Add the reasons column
    df_with_reasons = (df.withColumn("_quarantine_reasons", create_map(*map_expr).cast(MapType(StringType(), BooleanType()))))

    return df_with_reasons.filter(array_contains(map_values(col("_quarantine_reasons")), True))
            
