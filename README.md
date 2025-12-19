# Final Year Project

## Interim Instructions

### Running the Spark Applications on the Big Data Cluster

To run these programs as intended you must:

- Have access to the Big Data Cluster.
- Then SSH onto the server and git clone the project code onto the server.
- Note: if the server is down for maintenance, you must wait until it is back online as access will not be possible.

### Spark submit parameters

In the commands below:

- `X` is the number of Spark executors to allocate.
- `Y` is the number of cores allocated per executor.
- `ZG` is the memory per executor (in gigabytes e.g., 4G is 4 gigabytes per executor).
- `--driver-memory 4G` sets the Spark drivers memory to 4GB (fixed parameter).

### Package Used

The job required:

- `--package com.databricks:spark-xml_2.11:0.12.0`

This was used to read and parse the XML dataset within PySpark.

### Word Count

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

### Distributed Grep

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