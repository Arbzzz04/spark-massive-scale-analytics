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
            ("STAT1", "20230101", "TMAX", 250.0, "", "", "", ""), # valid temp (25.0c)
            ("STAT1", "20230102", "PRCP", 50.0, "", "", "", ""),  # valid rain (5.0mm)
            ("STAT1", "20230103", "TMIN", 100.0, "", "", "", ""), # invalid element (tmin)
            ("STAT2", "20230101", "TMAX", None, "", "", "", "")   # invalid (null)
        ]
        raw_df = self.spark.createDataFrame(mock_raw_data, schema)

        # execute: run the cleaning method
        clean_df = self.clusterer.clean_data(raw_df)
        results = clean_df.collect()

        # assert: only the first row should survive, and 2500 should become 25.0
        self.assertEqual(len(results), 2)
        
        # check the values for TMAX and PRCP
        temp_row = [r for r in results if r["ELEMENT"] == "TMAX"][0]
        prcp_row = [r for r in results if r["ELEMENT"] == "PRCP"][0]
        
        # check the cleaned values
        self.assertEqual(temp_row["VALUE"], 25.0)
        self.assertEqual(prcp_row["VALUE"], 5.0)

    def test_extract_features(self):
        # setup: create fake clean data
        schema = StructType([
            StructField("STATION", StringType(), True),
            StructField("ELEMENT", StringType(), True),
            StructField("VALUE", DoubleType(), True)
        ])
        mock_clean_data = [
            ("STAT1", "TMAX", 10.0), ("STAT1", "TMAX", 30.0), # avg 20, min 10, max 30
            ("STAT1", "PRCP", 0.0),  ("STAT1", "PRCP", 10.0), # total rain 10
            ("STAT2", "TMAX", -5.0), ("STAT2", "TMAX", 5.0)   # avg 0, min -5, max 5
        ]
        clean_df = self.spark.createDataFrame(mock_clean_data, schema)

        # execute: run the feature extraction
        result_df = self.clusterer.extract_features(clean_df)
        
        # convert results to a dictionary
        results = {row["STATION"]: row for row in result_df.collect()}

        # assert: check the math for STAT1 and STAT2
        self.assertEqual(results["STAT1"]["max_temp"], 30.0)
        self.assertEqual(results["STAT1"]["min_temp"], 10.0)
        self.assertEqual(results["STAT1"]["avg_temp"], 20.0)
        self.assertEqual(results["STAT1"]["total_prcp"], 10.0)
        
        self.assertEqual(results["STAT2"]["max_temp"], 5.0)
if __name__ == "__main__":
    unittest.main()