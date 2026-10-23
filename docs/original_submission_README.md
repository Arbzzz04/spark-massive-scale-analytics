# Final Year Project

## Massive Scale Data Analytics using Spark

This project implements four distributed data processing programs using PySpark on a Big Data cluster.

1. Word Count — counts word frequencies across a 100 GB Wikipedia XML dataset.
2. Distributed Grep — searches for a keyword across the same Wikipedia dataset.
3. Statistical Analysis — computes summary statistics and a frequency distribution over the NOAA weather dataset.
4. K-Means Clustering — groups weather stations into temperature zones using Spark MLlib.

### Interim Instructions

#### Running the Spark Applications on the Big Data Cluster

To run these programs as intended you must:

- Have access to the Big Data Cluster.
- Then SSH onto the server and git clone the project code onto the server.
- Note: if the server is down for maintenance, you must wait until it is back online as access will not be possible.

#### Spark submit parameters

In the commands below:

- `X` is the number of Spark executors to allocate.
- `Y` is the number of cores allocated per executor.
- `ZG` is the memory per executor (in gigabytes e.g., 4G is 4 gigabytes per executor).
- `--driver-memory 4G` sets the Spark drivers memory to 4GB (fixed parameter).

#### Package Used

The job required:

- `--package com.databricks:spark-xml_2.11:0.12.0`

This was used to read and parse the XML dataset within PySpark.

#### Word Count

Recommended values for each parameter:

- `X` = 6
- `Y` = 4
- `ZG` = 6G

Use this command to run the Word Count job over the cluster:

```cmd
time spark-submit \
  --packages com.databricks:spark-xml_2.11:0.12.0 \
  --num-executors X \
  --executor-cores Y \
  --executor-memory ZG \
  --driver-memory 4G \
  BigData_WikiWordCount.py
```

#### Distributed Grep

Recommended values for each parameter:

- `X` = 6
- `Y` = 4
- `ZG` = 4G

Use this command to run the Distributed Grep job over the cluster:

```cmd
time spark-submit \
  --packages com.databricks:spark-xml_2.11:0.12.0 \
  --num-executors X \
  --executor-cores Y \
  --executor-memory ZG \
  --driver-memory 4G \
  BigData_WikiDistributedGrep.py
```

### Plotting Results

The plots are provided in the Performance Testing folder, as Jupyter Notebook files and do not need to be executed to view the graphs or break downs.

### Final Instructions

#### Statistical Analysis

Recommended values for each parameter:

- `X` = 6
- `Y` = 4
- `ZG` = 4G
- shuffle partitions = 128

```cmd
time spark-submit \
  --num-executors X \
  --executor-cores Y \
  --executor-memory ZG \
  --driver-memory 4G \
  --conf spark.sql.shuffle.partitions=128 \
  BigData_StatAnalysis.py
```

#### K-Means Clustering

Applies K-Means clustering to group weather stations into temperature zones.

Recommended values for each parameter:

- `X` = 6
- `Y` = 4
- `ZG` = 4G
- shuffle partitions = 800

```cmd
time spark-submit \
  --num-executors X \
  --executor-cores Y \
  --executor-memory ZG \
  --driver-memory 4G \
  --conf spark.sql.shuffle.partitions=800 \
  BigData_KMeans.py
```

### Visualisations

The K-Means cluster output can be visualised locally without rerunning the Spark job.

1. Download the clustered CSV output from HDFS on the Big Data cluster.
2. Download the station metadata file from `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt`.
3. Place both files in the `product/data/` folder.

```cmd
python BigData_KMeans_Map.py
python BigData_KMeans_Scatter.py
```

The map script produces an interactive HTML file and the scatter script produces a PNG file, both saved to the `product/data/` folder.
