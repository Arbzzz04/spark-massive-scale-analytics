import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))) # add src to path for imports
from kMeans import ClimateClusterer

class TestClimateClusterer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # start local session for testing
        cls.spark = SparkSession.builder.master("local[1]").appName("KMeans").getOrCreate()
        cls.clusterer = ClimateClusterer(spark_session=cls.spark)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_clean_data(self):
        # setup: create fake raw data based on noaa schema
        schema = self.clusterer.noaa_schema
        mock_raw_data = [
            ("STAT1", "20230101", "TMAX", 250.0, "", "", "", ""), # valid (25.0c)
            ("STAT1", "20230102", "TMIN", 100.0, "", "", "", ""), # invalid element (tmin)
            ("STAT2", "20230101", "TMAX", None, "", "", "", ""),  # invalid (null)
            ("STAT3", "20230101", "TMAX", 9999.0, "", "", "", "") # invalid (extreme temp)
        ]
        raw_df = self.spark.createDataFrame(mock_raw_data, schema)

        # execute: run the cleaning method
        clean_df = self.clusterer.clean_data(raw_df)
        results = clean_df.collect()

        # assert: only the first row should survive, and 2500 should become 25.0
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["STATION"], "STAT1")
        self.assertEqual(results[0]["TEMP"], 25.0)

if __name__ == "__main__":
    unittest.main()