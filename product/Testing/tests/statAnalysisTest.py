import pytest
from pyspark.sql import SparkSession, Row
from pyspark.testing import assertDataFrameEqual
from product.Testing.src.statAnalysis import get_summary_stats, get_frequency_distribution

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

def test_frequency_distribution_total_count(spark):
    data = [(10.0,), (20.0,), (30.0,), (40.0,), (50.0,)]
    df = spark.createDataFrame(data, ["TEMP"])

    freq_df, _ = get_frequency_distribution(df)
    total = freq_df.agg({"count": "sum"}).collect()[0][0]

    assert total == 5

def test_frequency_distribution_num_buckets(spark):
    data = [(10.0,), (20.0,), (30.0,), (40.0,), (50.0,)]
    df = spark.createDataFrame(data, ["TEMP"])

    freq_df, splits = get_frequency_distribution(df)
    
    assert len(splits) == 11
    assert freq_df.count() <= 10