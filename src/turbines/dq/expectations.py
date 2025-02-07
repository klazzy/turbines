from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def get_rules_for_table(rules_df: DataFrame, target_table_name: str, failure_action: str) -> dict:
    """
    Load data quality rules from the expectations table

    Parameters:
    rules_df (DataFrame): A DataFrame containing all configured rules
    target_table_name (str): The target table to which to apply the rules
    failure_action (str): The failure action to load the rules for

    Returns:
    dict: A dictionary containing the rule names and expressions
    """
    expectations = {}
    filtered_rules_df = (rules_df
          .filter(
              (col("target_table") == target_table_name) &
              (col("failure_action") == failure_action))
    )
    for row in filtered_rules_df.collect():
        expectations[row["name"]] = row["expr"]
    return expectations

def get_quarantine_drop_rule_expr(rules_df: DataFrame, target_table: str) -> str:
    """
    Get the inverted expression of all the drop rules for a table for
    quarantine purposes

    Parameters:
    rules_df (DataFrame): A DataFrame containing all configured rules
    target_table (str): The target table to which the rules apply

    Returns:
    str: The expression that can be used to filter on the quarantine
    table
    """
    drop_rules = get_rules_for_table(rules_df, target_table, "drop")
    return "NOT({0})".format(" AND ".join(drop_rules.values()))
    