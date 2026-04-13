from pyspark.sql import functions as F
from pyspark.ml.feature import Bucketizer
import matplotlib.pyplot as plt

def get_summary_stats(df, col_name="TEMP"):
    # calculate count, min, max and median for the specified column
    stats = df.agg(
        F.count(col_name).alias("count"),
        F.min(col_name).alias("min"),
        F.max(col_name).alias("max"),
        F.expr(f"percentile_approx({col_name},0.5)").alias("median")
    )
    return stats

def get_frequency_distribution(df, col_name="TEMP", num_buckets=10):
    # calculate min and max to define bucket splits
    bounds = df.agg(F.min(col_name).alias("min"), F.max(col_name).alias("max")).collect()[0]
    min_val, max_val = bounds["min"], bounds["max"]
    
    # calculate the step size for the buckets and define the splits
    step = (max_val - min_val) / num_buckets
    
    # build the list of split points for the bucketizer
    splits = [min_val + i * step for i in range(num_buckets)]
    splits.append(max_val + 0.001)

    # apply bucketizer to assign each temperature to a bucket
    bucketizer = Bucketizer(splits=splits, inputCol=col_name, outputCol="bucket")
    bucketed_df = bucketizer.transform(df)

    # count the number of rows in each bucket to get the frequency distribution
    freq_df = (
        bucketed_df
            .groupBy("bucket") 
            .count() 
            .orderBy("bucket") 
    )
    
    return freq_df, splits

def visualize_distribution(freq_pdf, splits):
    # create readable labels for the buckets using its range
    labels = [f"{round(splits[i], 1)} to {round(splits[i+1], 1)}" for i in range(len(splits)-1)]
    
    plt.figure(figsize=(10, 6))
    
    plt.bar(freq_pdf['bucket'], freq_pdf['count'], tick_label=labels, color='skyblue', edgecolor='black')
    
    # add labels and title to the plot
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
        .appName("WeatherAnalysis-100GB-Production")
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

    # read the entire weather dataset
    df = (
        spark.read.csv(
            "hdfs:///user/zlac302/datasets/weather/",
            header=False,
            schema=noaa_schema
        )
    )

    # filter dataset to select max temperatures and convert to celsius
    weather_df = (
        df.filter(F.col("ELEMENT") == "TMAX") 
        .withColumn("TEMP", F.col("DATA_VALUE") / 10.0) 
        .select("TEMP")
        .filter(F.col("TEMP").isNotNull()) # filter out nulls
        .filter(F.col("TEMP").between(-100.0, 60.0)) # filter out unrealistic temperatures
    )

    # calculate and output summary stats
    summary_stats = get_summary_stats(weather_df)
    summary_stats.show()

    # export summary stats to csv on HDFS
    print("exporting 100gb summary stats to csv...")
    summary_path_csv = "hdfs:///user/zlac302/outputs/100gb_summary_csv"
    summary_stats.coalesce(1).write.csv(summary_path_csv, header=True, mode="overwrite")

    # get frequency distribution with 10 buckets
    freq_df, splits = get_frequency_distribution(weather_df, num_buckets=10)

    # format ordered range labels
    range_labels = []
    for i in range(len(splits) - 1):
        label = f"{i}: {round(splits[i], 1)}C to {round(splits[i+1], 1)}C"
        range_labels.append((float(i), label))

    # join labels to the frequency dataframe
    labels_df = spark.createDataFrame(range_labels, ["bucket", "temperature_range"])
    final_export_df = freq_df.join(labels_df, on="bucket", how="inner").orderBy("bucket")
    final_export_df.show(truncate=False)

    # export distribution data to csv
    print("exporting 100gb distribution data to csv...")
    dist_path_csv = "hdfs:///user/zlac302/outputs/100gb_distribution_csv"
    (final_export_df.coalesce(1)
        .write
        .csv(dist_path_csv, header=True, mode="overwrite"))

    print("success 100gb data exports completed")

    spark.stop()
