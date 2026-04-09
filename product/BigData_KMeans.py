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

    spark = (
        SparkSession.builder
        .appName("BigData-KMeans-Temperature-Zones")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    clusterer = ClimateClusterer(spark_session=spark)

    try:
        print("loading weather data")
        raw_df = spark.read.csv(
            input_path,
            header=False,
            schema=clusterer.noaa_schema
        )

        print("cleaning data")
        clean_df = clusterer.clean_data(raw_df)

        print("extracting features")
        features_df = clusterer.extract_features(clean_df)

        print("applying kmeans")
        final_df = clusterer.apply_kmeans(features_df, k=k)

        # print("showing sample output")
        # final_df.show(20, truncate=False)

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
