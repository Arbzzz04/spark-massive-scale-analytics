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
        # filter for valid temperatures and precipitation, convert to standard units
        return (
            raw_df.filter(F.col("ELEMENT").isin("TMAX", "PRCP"))
            .withColumn("VALUE", F.col("DATA_VALUE") / 10.0)
            .filter(F.col("VALUE").isNotNull())
            .filter(
                (F.col("ELEMENT") == "PRCP") | 
                (F.col("VALUE").between(-100.0, 60.0))
            )
            .select("STATION", "ELEMENT", "VALUE")
        )
    
    def extract_features(self, clean_df):
        # separate temp and rain data
        temp_df = clean_df.filter(F.col("ELEMENT") == "TMAX")
        prcp_df = clean_df.filter(F.col("ELEMENT") == "PRCP")

        # aggregate temperature behavior
        temp_stats = temp_df.groupBy("STATION").agg(
            F.avg("VALUE").alias("avg_temp"),
            F.max("VALUE").alias("max_temp"),
            F.min("VALUE").alias("min_temp")
        )

        # aggregate total precipitation
        prcp_stats = prcp_df.groupBy("STATION").agg(
            F.sum("VALUE").alias("total_prcp")
        )

        # join them together and drop stations with missing metrics
        combined_stats = temp_stats.join(prcp_stats, on="STATION", how="left")
        
        # fill missing precipitation with 0 (no rain) before dropping any rows
        combined_stats = combined_stats.fillna({"total_prcp": 0.0}) 
        return combined_stats.dropna()