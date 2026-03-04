from pyspark.sql import functions as F
from pyspark.ml.feature import Bucketizer
import matplotlib.pyplot as plt

def get_summary_stats(df, col_name="TEMP"):
    stats = df.agg(
        F.count(col_name).alias("count"),
        F.min(col_name).alias("min"),
        F.max(col_name).alias("max"),
        F.percentile_approx(col_name, 0.5).alias("median")
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

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("WeatherAnalysis")
        .getOrCreate()
    )

    df = (
        spark.read.csv(
            "product/data/01001099999.csv",
            header=True,
            inferSchema=True
        )
    )

    weather_df = df.select("TEMP").na.drop()

    summary_stats = get_summary_stats(weather_df)
    summary_stats.show()

    freq_df, splits = get_frequency_distribution(weather_df, num_buckets=10)
    
    freq_pdf = freq_df.toPandas() 

    parquet_path = "product/data/temperature_distribution.parquet"
    
    # cant run locally as no HDFS present
    """
    (
        freq_df.write
        .mode("overwrite")
        .parquet(parquet_path)
    )
    """
    visualize_distribution(freq_pdf, splits)
    
    spark.stop()