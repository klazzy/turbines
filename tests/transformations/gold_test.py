import pytest
from datetime import date, datetime
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual

from turbines.transformations.gold import summarise_daily

# Arrange
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()

class TestGoldTransformations:

    def test_summarise_daily(self, spark):

        # Arrange
        input_schema = "timestamp TIMESTAMP, turbine_id INT, wind_speed DOUBLE, wind_direction INT, power_output DOUBLE"
        input_data = [
            (datetime(2022, 3, 1, 0, 0, 0), 1, 10.0, 10, 1.5),
            (datetime(2022, 3, 1, 0, 0, 0), 2, 10.0, 10, 10.0),
            (datetime(2022, 3, 1, 1, 0, 0), 1, 10.0, 10, 2.5),
            (datetime(2022, 3, 1, 1, 0, 0), 2, 10.0, 10, 20.0),
            (datetime(2022, 3, 2, 0, 0, 0), 1, 10.0, 10, 1.0),
            (datetime(2022, 3, 2, 1, 0, 0), 1, 10.0, 10, 1.0),
            (datetime(2022, 3, 2, 2, 0, 0), 1, 10.0, 10, 1.0),
            (datetime(2022, 3, 2, 3, 0, 0), 1, 10.0, 10, 1.0),
            (datetime(2022, 3, 2, 4, 0, 0), 1, 10.0, 10, 1.0),
            (datetime(2022, 3, 2, 5, 0, 0), 1, 10.0, 10, 1.0),
            (datetime(2022, 3, 2, 6, 0, 0), 1, 10.0, 10, 1.0),
            (datetime(2022, 3, 2, 7, 0, 0), 1, 10.0, 10, 1.0),
            (datetime(2022, 3, 2, 8, 0, 0), 1, 10.0, 10, 1.0),
            (datetime(2022, 3, 2, 9, 0, 0), 1, 10.0, 10, 1.0),
            (datetime(2022, 3, 2, 10, 0, 0), 1, 10.0, 10, 100.0)
        ]
        input_df = spark.createDataFrame(input_data, input_schema)

        expected_schema = "date DATE, turbine_id INT, min_power_output DOUBLE, max_power_output DOUBLE, avg_power_output DOUBLE, has_anomaly BOOLEAN"
        expected_data = [
            (date(2022, 3, 1), 1, 1.5, 2.5, 2.0, False),
            (date(2022, 3, 1), 2, 10.0, 20.0, 15.0, False),
            (date(2022, 3, 2), 1, 1.0, 100.0, 10.0, True)
        ]

        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        result_df = summarise_daily(input_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)