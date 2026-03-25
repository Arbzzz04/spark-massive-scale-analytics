from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

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
        # filter for valid temperatures, convert to standard units
        return (
            raw_df.filter(F.col("ELEMENT") == "TMAX")
            .withColumn("VALUE", F.col("DATA_VALUE") / 10.0)
            .filter(F.col("VALUE").isNotNull())
            .filter(F.col("VALUE").between(-100.0, 60.0))
            .select("STATION", "ELEMENT", "VALUE")
        )
    
    def extract_features(self, clean_df):
        # separate temp data
        temp_df = clean_df.filter(F.col("ELEMENT") == "TMAX")

        # aggregate temperature behavior
        temp_stats = temp_df.groupBy("STATION").agg(
            F.avg("VALUE").alias("avg_temp"),
            F.max("VALUE").alias("max_temp"),
            F.min("VALUE").alias("min_temp")
        ).withColumn("temp_range", F.col("max_temp") - F.col("min_temp"))

        # join them together and drop stations with missing metrics
        combined_stats = temp_stats

        return combined_stats.dropna()

    def apply_kmeans(self, feature_df, k=5):
        # assemble the 2 metrics into a single mathematical vector
        assembler = VectorAssembler(
            inputCols=["avg_temp", "temp_range"], 
            outputCol="raw_features"
        )
        assembled_df = assembler.transform(feature_df)

        # scale the features so temperature values are comparable
        scaler = StandardScaler(
            inputCol="raw_features", 
            outputCol="scaled_features", 
            withStd=True, 
            withMean=False
        )
        scaler_model = scaler.fit(assembled_df)
        scaled_df = scaler_model.transform(assembled_df)

        # train the k-means model
        kmeans = KMeans(k=k, seed=42, featuresCol="scaled_features", predictionCol="cluster_id")
        model = kmeans.fit(scaled_df)

        # make predictions and return the final clean dataframe
        predictions = model.transform(scaled_df)
        return predictions.select(
            "STATION", "avg_temp", "max_temp", "min_temp", "temp_range", "cluster_id"
        )
    
    def stop(self):
        self.spark.stop()