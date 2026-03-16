from pyspark.sql import functions as F
from pyspark.ml.feature import Bucketizer
import matplotlib.pyplot as plt

def get_summary_stats(df, col_name="TEMP"):
    stats = df.agg(
        F.count(col_name).alias("count"),
        F.min(col_name).alias("min"),
        F.max(col_name).alias("max"),
        F.expr(f"percentile_approx({col_name},0.5)").alias("median")
    )
    return stats

def get_frequency_distribution(df, col_name="TEMP", num_buckets=10):
    bounds = df.agg(F.min(col_name).alias("min"), F.max(col_name).alias("max")).collect()[0]
    min_val, max_val = bounds["min"], bounds["max"]
    
    step = (max_val - min_val) / num_buckets
    
    splits = [min_val + i * step for i in range(num_buckets)]
    splits.append(max_val + 0.001)

    bucketizer = Bucketizer(splits=splits, inputCol=col_name, outputCol="bucket")
    bucketed_df = bucketizer.transform(df)

    freq_df = (
        bucketed_df
            .groupBy("bucket") 
            .count() 
            .orderBy("bucket") 
    )
    
    return freq_df, splits

def visualize_distribution(freq_pdf, splits):
    labels = [f"{round(splits[i], 1)} to {round(splits[i+1], 1)}" for i in range(len(splits)-1)]
    
    plt.figure(figsize=(10, 6))
    
    plt.bar(freq_pdf['bucket'], freq_pdf['count'], tick_label=labels, color='skyblue', edgecolor='black')
    
    plt.title('Frequency Distribution of Temperatures')
    plt.xlabel('Temperature Ranges')
    plt.ylabel('Frequency (Count)')
    plt.xticks(rotation=45, ha='right') 
    plt.tight_layout() 

    plt.show()

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    from pyspark.sql import functions as F

    # start up spark session for cluster execution
    spark = (
        SparkSession.builder
        .appName("WeatherAnalysis-2023-Parquet")
        .getOrCreate()
    )

    # variable to define the noaa ghcn daily data schema
    noaa_schema = StructType([
        StructField("STATION", StringType(), True),
        StructField("DATE", StringType(), True),
        StructField("ELEMENT", StringType(), True),
        StructField("DATA_VALUE", DoubleType(), True),
        StructField("M_FLAG", StringType(), True),
        StructField("Q_FLAG", StringType(), True),
        StructField("S_FLAG", StringType(), True),
        StructField("OBS_TIME", StringType(), True)
    ])

    # read only the 2023 dataset
    df = (
        spark.read.csv(
            "hdfs:///user/zlac302/datasets/weather/2023.csv",
            header=False,
            schema=noaa_schema
        )
    )

    # filter down the dataset to select only max temperatures and clean garbage data
    weather_df = (
        df.filter(F.col("ELEMENT") == "TMAX") 
        .withColumn("TEMP", F.col("DATA_VALUE") / 10.0) 
        .select("TEMP")
        .filter(F.col("TEMP").isNotNull()) 
        .filter(F.col("TEMP").between(-100.0, 60.0)) 
    )

    # calculate summary stats and output to terminal
    summary_stats = get_summary_stats(weather_df)
    summary_stats.show()

    # save summary stats to Parquet on HDFS
    print("Saving summary stats to Parquet...")
    summary_path = "hdfs:///user/zlac302/outputs/2023_summary.parquet"
    summary_stats.coalesce(1).write.parquet(summary_path, mode="overwrite")

    # get frequency distribution with 10 buckets
    freq_df, splits = get_frequency_distribution(weather_df, num_buckets=10)

    # create human-readable temperature ranges for power bi
    range_labels = []
    for i in range(len(splits) - 1):
        label = f"{round(splits[i], 1)}C to {round(splits[i+1], 1)}C"
        range_labels.append((float(i), label))

    # join labels to the frequency dataframe
    labels_df = spark.createDataFrame(range_labels, ["bucket", "temperature_range"])
    final_export_df = freq_df.join(labels_df, on="bucket", how="inner").orderBy("bucket")
    final_export_df.show(truncate=False)

    # save frequency distribution to Parquet on HDFS
    print("Saving distribution data to Parquet...")
    dist_path = "hdfs:///user/zlac302/outputs/2023_distribution.parquet"
    final_export_df.coalesce(1).write.parquet(dist_path, mode="overwrite")

    print("All requirements saved to HDFS as Parquet.")

    spark.stop()
