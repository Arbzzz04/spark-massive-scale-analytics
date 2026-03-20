from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

class ClimateClusterer:
    def __init__(self, app_name="KMeans-Climate", spark_session=None):
        if spark_session:
            self.spark = spark_session
        else:
            self.spark = SparkSession.builder.appName(app_name).getOrCreate()
        
        # define standard noaa schema
        self.noaa_schema = StructType([
            StructField("STATION", StringType(), True),
            StructField("DATE", StringType(), True),
            StructField("ELEMENT", StringType(), True),
            StructField("DATA_VALUE", DoubleType(), True),
            StructField("M_FLAG", StringType(), True),
            StructField("Q_FLAG", StringType(), True),
            StructField("S_FLAG", StringType(), True),
            StructField("OBS_TIME", StringType(), True)
        ])

    def clean_data(self, raw_df):
        # filter for valid max temperatures and convert to celsius
        return (
            raw_df.filter(F.col("ELEMENT") == "TMAX")
            .withColumn("TEMP", F.col("DATA_VALUE") / 10.0)
            .filter(F.col("TEMP").isNotNull())
            .filter(F.col("TEMP").between(-100.0, 60.0))
            .select("STATION", "TEMP")
        )