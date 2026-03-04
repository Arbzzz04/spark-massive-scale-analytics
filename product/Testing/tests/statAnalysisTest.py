import pytest
from pyspark.sql import SparkSession, Row
from pyspark.testing import assertDataFrameEqual

@pytest.fixture(scope="session")
def spark():
    spark_session = SparkSession.builder \
        .master("local[1]") \
        .appName("PytestSpark") \
        .getOrCreate()
    yield spark_session
    spark_session.stop()