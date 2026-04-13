import os
import sys
from pyspark.sql import SparkSession

# add src to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "Testing", "src")))
from kMeans import ClimateClusterer

if __name__ == "__main__":
    # define input and output paths
    input_path = "hdfs:///user/zlac302/datasets/weather/"
    output_path = "hdfs:///user/zlac302/outputs/kmeans_temperature_zones"
    k = 5
    
    # start spark session
    spark = (
        SparkSession.builder
        .appName("BigData-KMeans-Temperature-Zones")
        .getOrCreate()
    )

    # reduce log noise for cleaner output
    spark.sparkContext.setLogLevel("WARN")

    # create clusterer instance with existing spark session
    clusterer = ClimateClusterer(spark_session=spark)

    try:
        # read full weather dataset from HDFS using the defined noaa schema
        print("loading weather data")
        raw_df = spark.read.csv(
            input_path,
            header=False,
            schema=clusterer.noaa_schema
        )

        # filter to only TMAX records for clustering
        print("cleaning data")
        clean_df = clusterer.clean_data(raw_df)

        # aggregate temperature state for each station
        print("extracting features")
        features_df = clusterer.extract_features(clean_df)

        # run kmeans clustering algorithm to find climate zones
        print("applying kmeans")
        final_df = clusterer.apply_kmeans(features_df, k=k)

        # print("showing sample output")
        # final_df.show(20, truncate=False)

        # write the clustered results back to HDFS for later visualization
        print("writing results to hdfs")
        (
            final_df.coalesce(1)
            .write
            .mode("overwrite")
            .csv(output_path, header=True)
        )

        print("job complete")
        print(f"output path: {output_path}")

    finally:
        clusterer.stop()
