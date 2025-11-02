# Project diary

## Date: October 29th 2025

I had my second meeting with my supervisor, I was able to sort out a bug in my code that did not allow me to clean the text from the Wikipedia data I was using. Now that the results of the cleaning data worked I have decided to continue with the XML file. This is due to an issue caused by Visual Studio Code, my supervisor helped me to fix this issue during the meeting by performing the 'cleaning' process in one code block.

During the meeting I was told to further research into these concepts:

- Actions - cause something to happen in the cluster

- Transformations - these are lazy and don't affect the cluster

- Lazy Evaluation

I found these terms mentioned in the Learning Spark book and will read more into them. These terms have to do with dataframes and how to process the data. Another factor I had discussed during the meeting was the early deliverables, which were related more towards the project being carried out with Hadoop MapReduce. My supervisor was able to change them for me to suit the project being carried out in Spark instead which set out a clearer frame for me to work with.

I was additionally provided with access to the Cluster via CIM help desk, who sent me an email stating how to access the cluster using an SSH command.

## Date: October 27th 2025

The focus for today is attempting to produce clean data, and pick up where I left of with yesterday's issue of the resulting data remaining seemingly unaffected by the methods of processing I had attempted.

I found that I should probably try and do this word count on a dataset from Kaggle first as this would allow me to practise on a more manageable format instead of XML, I will be able to find a CSV file to perform the word count program on. Looking back at the dataset I chose it seemed quite difficult to perform the operations I desired and I have chosen to take a step back and look for another dataset to allow myself to gain more experience before going onto more complex tasks.

## Date: October 26th 2025

I am going to continue trying to clean the wiki data I have got from the raw XML file. I have looked into regex functions that I can use to potentially clean the data in order to be able to count the words in the file. Although, I am running into a road block as the functions don't seem to be affecting the output in the cleanText column I am generating. I will now look further into the documenation and attempt to find out how I can overcome this dilema.

## Date: October 23nd 2025

I found a file to attempt to process from `https://dumps.wikimedia.org/simplewiki/latest/` in order to find a manageable dataset that I planned to use. The aim with the data I process would be to attempt the word count program in order to continue to scale up with the size of data I am able to process.

Moreover, I have used `https://spark.apache.org/docs/latest/sql-programming-guide.html` in order to understand and learn specific function I feel would allow me to achieve my aim. I have also read parts of chapter 2 from Learning Spark suggested by my supervisor, Dr. O'Keeffe, in order to further my understanding on how DataFrames work.

In terms of getting performance analysis, this will be done once I am successfully able to process this dataset and get a result on my local machine as it is comparatively small. Once I have done this task, I will look to discuss when and how I can gain access to the cluster from the university. At that point, I will look to make more in depth performance analysis based on my findings, as well as report on any problem I run into and how I was able to solve them.

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
