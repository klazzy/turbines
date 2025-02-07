import pytest
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual

from turbines.dq.expectations import get_rules_for_table

# Arrange
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()

class TestExpectations:

    def test_get_rules_for_table(self, spark):
        
        # Arrange
        input_failure_action = "failureAction1"
        input_target_table_name = "table1"
        input_rules_schema = "target_table STRING, name STRING, expr STRING, failure_action STRING"
        input_rules_data = [
            ("table1", "rule1", "expr1", "failureAction1"),
            ("table1", "rule2", "expr2", "failureAction2"),
            ("table2", "rule3", "expr3", "failureAction1")
        ]
        input_rules_df = spark.createDataFrame(input_rules_data, input_rules_schema)

        expected_dict = {
            "rule1": "expr1"
        }

        # Act
        result_dict = get_rules_for_table(input_rules_df, input_target_table_name, input_failure_action)

        # Assert
        assert result_dict == expected_dict