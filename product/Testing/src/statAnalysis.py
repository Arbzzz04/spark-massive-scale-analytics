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
    splits.append(max_val + 0.001) # ensure the absolute maximum value is included

    bucketizer = Bucketizer(splits=splits, inputCol=col_name, outputCol="bucket")
    bucketed_df = bucketizer.transform(df)

    freq_df = (
        bucketed_df
            .groupBy("bucket") 
            .count() 
            .orderBy("bucket") 
    )
    
    return freq_df, splits