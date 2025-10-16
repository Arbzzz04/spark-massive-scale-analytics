# Project diary

## Date: October 16th 2025

Today, I made a small dataset to verify that the PySpark is functioning properly on my local environment. Using the `NameAgeData.csv` file, I implemented a simple Word Count program as an initial test, which I aim to later upscale with a larger dataset. The program successfully read the data, then using the groupBy function I was able to collect the same items in the 'Name' column, and produce the correct output.

Additionally, I researched into potential datasets available online for the Word Count Program. I found sources suggested by my supervisor on Reddit and Wikipedia for data dumps. I decided to work with Wikipedia dumps as they are text heavy and can get quite large in size, suitable for later data processing as well.

## Date: October 1st 2025

In my first meeting with my supervisor, Dr. O'Keeffe, we discussed the technologies to be used for the project. Hadoop was the initial framework that was decided on in the project description. However, my supervisor advised using Apache Spark, as it's a more modern framework that offers additional capabilities like Spark SQL, MLlib for machine learning and built-in graph processing functionalities.

Additionally, I raised a few key questions I had:

- Datasets: my supervisor suggested starting with datasets found on kaggle, and later possibly generating synthetic datasets.
- Defining **Large** Datasets: this will be decided in a later meeting as the main focus is to learn the Pyspark library and implement the word count program.
- Why Spark: spark provides added flexibility, faster in-memory computation.
- Gaining access to cluster: Using university cluster, supervisor will ask the CIM team for access, either through SSH tunneling or using machine on campus.

The planned technology stack:

- Processing - Pyspark

- Resource Management - YARN (conceptual)

- Storage - HDFS or AWS S3 (paid - could get access through supervisor AWS educate)
