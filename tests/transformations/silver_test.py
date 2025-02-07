import pytest
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual

from turbines.transformations.silver import drop_audit_columns

# Arrange
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()

class TestSilverTransformations:

    def test_drop_audit_columns(self, spark):

        # Arrange
        input_schema = "id INT, name STRING, file STRUCT<name: STRING>, _rescued_data STRING"
        input_data = [
            (1, "Alice", { "name": "value1"}, "data1"),
            (2, "Bob", { "name": "value2"}, "data2")
        ]
        input_df = spark.createDataFrame(input_data, input_schema)

        expected_schema = "id INT, name STRING"
        expected_data = [
            (1, "Alice"),
            (2, "Bob")
        ]

        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        result_df = drop_audit_columns(input_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)