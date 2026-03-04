import pytest
from pyspark.sql import SparkSession, Row
from pyspark.testing import assertDataFrameEqual
from product.Testing.src.statAnalysis import get_summary_stats
@pytest.fixture(scope="session")
def spark():
    spark_session = SparkSession.builder \
        .master("local[1]") \
        .appName("PytestSpark") \
        .getOrCreate()
    yield spark_session
    spark_session.stop()

def test_summary_stats(spark):
    data = [(10.0,), (20.0,), (30.0,), (40.0,), (50.0,)]
    df = spark.createDataFrame(data, ["TEMP"])
    actual_df = get_summary_stats(df)
    
    expected_data = [Row(count=5, min=10.0, max=50.0, median=30.0)]
    expected_df = spark.createDataFrame(expected_data)
    assertDataFrameEqual(actual_df, expected_df)