from pyspark.sql import functions as F

def get_summary_stats(df, col_name="TEMP"):
    stats = df.agg(
        F.count(col_name).alias("count"),
        F.min(col_name).alias("min"),
        F.max(col_name).alias("max"),
        F.percentile_approx(col_name, 0.5).alias("median")
    )
    return stats