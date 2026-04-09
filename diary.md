# Project diary

## Date: April 4th 2026

Today I completed the executor tests, I moved to partition testing using a fixed executor and core setup so that I can isolate the effects that changing the number of partitions has. I ran the program with a range of partition counts and compared the outcomes to see the outcome.

I also repeated some of the runs to improve confidence in the consistency of the results and extended the partiton tests further by trying larger values. This stage helped me understand the performance tuning in spark is not only about throwing resources at the problem, but also about matching the partitioning strategy to the workload so that the available resources are used more efficiently.

My next step is to review the collected results and compare the best performing configurations.

## Date: April 3rd 2026

I continued the performance testing stage by varying the executors number in the spark-submit command while keeping other factors the same allowing for a controlled experiments. I tested several executor configurations and compared the runtimes to see how well the program would scale as more cluster resources were assigned.

This helped to begin identifying whether adding more executors would improve performance in a meaningful way or whether the gains were starting to level off. I also used this stage to keep a more organised record of outcomes.

My next step was to move on to partition testing so that I could examine how task partitions affect the runtime alongside the executor choices.

## Date: April 1st 2026

I began the performance testing stage for the statistical analysis program. At this stage my focus was to create a more structured and repeatable testing process so that I could compare configurations more reliably.

I used repeated runs for the setup to begin establishing a baseline and recorded information like the runtime and application details from the cluster. This stage allowed me to see how consistent the program was between each of the runs. My next step was to test the programs behaviour under different spark-submit configurations.

## Date: March 30th 2026

I thought about how the clustered output should move between the BigData cluster and my local machine. I realised it would be inefficient to rerun the entire clustering job to adjust the visual output or presentation of the results.

To improve this workflow, I treated the cluster like a place to perform the main workload and use the saved output files for the map and scatter visuals later. The made the project easier to manage and gave me a cleaner seperation between the BigData computation stage and the later visualisation stage of the clustered results.

My next step was to begin the performance testing stage for the statistical analysis program.

## Date: March 27th 2026

After finalising the temperature based K-Means representation I made some more adjustments to the weather dataset used in the main clustering program by adding more data to the total dataset from NOAA. Once the run had completed I worked on handling the output files more carefully. I pulled the results from HDFS onto the cluster machine and moved it into my project data folder so that I could manage them with more ease.

My next step was to make the workflow between the cluster output and the later visulisation stage alot more clear and organised.

## Date: March 26th 2026

After generating the first visualisations of the K-Means results, I reviewed them and made sure they were sensible outcomes. This helped make sure the program ran and also that the climate grouping made sense.

One main issue I spotted was that including precipitation in the main BigData cluster, it resulted in a skewed outcome and producing a less sensible result when viewed on the map as there was no clear pattern or meaningful groups forming. Then I took the precipitation out of the clustering and the representation and proceeded to focus on the temperature features for the final output.

This stage helped to understand the sucess of the K-Means implementation depending not only on the code but also the choice of weather feature representation that made sense when the result was visualised.

## Date: March 19th 2026

Today I started developing the K-Means implementation and spent some time thinking about how the choice of features would affect the clustering results. Therefore my main focus was getting the clustering workflow in place and thinking about which of the statistics in the dataset would provide the most meaningful groups of stations.

I spent time exploring how the clustering output could be interpreted once plotted visually, as I wanted the map and scatter plot outputs after to make sense geographically. This helped to begin connecting the machine learning part of the project with the presentation of the final clustered results.

The next steps would be to generate clustering outputs along with visual representations.

## Date: March 17th 2026

I had a meeting with my supervisor where we discussed a few final project points. We spoke about the Professional Issues report and what would be included in that part of the project. We also discussed the possibility of incorporating AWS Academy in some way, depending if time permitted and it would be realistic for the later stage of the project.

We also went through a few final decisions around the technical direction the project would take. The main outcome was confirming that I would implement the K-Means data mining algorithm, which gave me a good direction to take for the next stage of development.

My next step was to continue with the implementation work for the statistical analysis files while also looking to begin to plan the K-Means program.

## Date: March 10th 2026

Today I bought the statistical analysis functions together and implemented an iteration of the program with the larger set of NOAA weather data. I then implemented a run on the cluster and confirmed a successful run was completed.

My next step was to set a meeting with the supervisor to confirm the algorithm I would be using for the final problem in the project.

## Date: March 4th 2026

I worked on bringing the functions that I had created together into a main function that reads the real NOAA weather dataset, runs the stat analysis, outputs the results to a Parquet file and generates a graph of the frequency distribution. I did hit a roadblock when running the script locally as I dont have HDFS on my local machine, I would have to wait to save my Parquet file on the BigData cluster. For now, I commented out the section of code that was resulting in the error.

My next step is to look into how to get more data on the university HDFS to perform the analysis on a larger scale of data.

## Date: Febuary 28th 2026

I attempted to tackle the percentile-based buckets and frequency distribution today. Initially, I thought of implementing a for-loop to filter and count the rows for each temperature interval. However, applying what I learnt about Spark execution plans, I understood that this would trigger multiple Spark actions and scan the massive dataset from ground zero each time. To improve this and take a better approach, I implemented PySpark MLlib's Bucketizer. This was a great way to tackle the problem as it allowed me to put the data into fixed percentiles in a single distributed action.

I also added pytest functions to verify the bucket boundaries and total counts using mock data.

## Date: Febuary 22nd 2026

I began writing code for the core stats module. I ran into an interesting issue:

Calculating an exact median in Spark requires a massive shuffle of data across the cluster to sort it, which is not good for performance. After reading through the PySpark documentation, I solved this issue by using F.percentile_approx, which estimates the median much faster without moving all the data around the cluster which would be much more efficient over a large amount of data.

I also successfully set the first mock data test for the function to verify the logic was correct.

## Date: Febuary 18th 2026

I spent time researching TDD and how unit testing works specifically in a distributed PySpark environment. I learnt that using real massive CSV files for testing is not feasible as unit tests are made to be fast and deterministic. Instead, the standard approach is to use a pytest fixture to create a local Spark session, then create small mock DataFrames to test the logic. I also found out about PySpark's assertDataFrameEqual function which allows me to compare distributed dataframes.

## Date: Febuary 12th 2026

Today I looked into potential large numerically heavy datasets to use for the statistical analysis. I looked into NYC taxi data and flight delay datasets, which are quite popular for these type of tasks. In the end, I decided to use weather data from NOAA (National Oceanic and Atmospheric Administration). It has a massive amount of historical temperature recordings in a clean format, which aligns well with my goal to calculate core stats and find a frequency distribution.

## Date: Febuary 2nd 2026

I had a meeting with my supervisor where we discussed the next stage of the project. We spoke about focusing on the statistical analysis problem and selecting a dataset, before choosing an algorithm for the last datamining problem. I will being with core stats then percentile-based buckets as instructed by the project problem.

We looked into the potential use of AWS Academy for accessing cloud resources if needed later in the project. We also discussed automating outputs by saving results as Parquet files and following a structured PySpark unit testing approach.

For the advanced datamining problem later in the project, I may explore Spark MLlib and potentially implement my own variation of an advanced datamining algorithm.

## Date: December 13th 2025

Updated some of my graphs for word count and added some of the new graphs for my distributed grep program along with some breakdowns for them to provide some insight into what is being displayed.

## Date: December 10th 2025

I have recommented my code and removed some unnecessary comments/code that was not doing anything. I have left the main code as I used it for my performance testing. I sent a ticket to the CIM help desk as I needed to access the server for some further testing but was delayed by a maintenance update on the hardware meaning the server was down for a couple days.

Next steps are to complete the distributed grep performance graphs using matplotlib and breakdowns using any theoretical findings within my research to explain the results I ended up with.

## Date: December 5th 2025

I have extracted more of the data I gathered from my experimental spark jobs and did some research as to the shape that was forming to provide more insight on why they were occuring in that pattern. I also performed more tests on the Distributed Grep program on the Big Data cluster and recorded the results.

Going forward I want to sort out any commenting and redundant code within my program files.

## Date: December 3rd 2025

I have completed the tests for my Word Count program and will plot them using matplotlib and start testing my Distributed Grep program in the same way that I have been performance testing.

My next step will be to successfully generate plots for each of the tests I have run and make conclusions about each of them.

## Date: December 2nd 2025

I continued implemenations of performance tests on the BigData cluster on my Word Count program. I am running a variety of configurations and using a variety of dataset sizes. My goal will be to performing the same tests on my Distributed Grep program but was delayed due to the hardware issue the server was having.

I have begun the process of creating matplotlib plots and will create a plot for each experiment I ran on the BigData cluster.

## Date: December 1st 2025

I had my fourth meeting with my supervisor, where we discussed a few topics: the expectation for my upcoming presentation, video and reports as well as the continuation of the performance testing stage and the matplotlib plots.

My supervisor told me to structure the performance tests more systematically by beginning with a minimal setup: 1 executor, 1 core, and 4G memory. By doing this I can measure throughput with no parallelism. Once I have this baseline measure, I can examine how scaling with more cores, executors and memory effect the implementation as well as trying it out on different datasizes.

He explained throughput being:

Throughput = Dataset Size (MB)/ Runtime (s)

Finally, I found out about using tmux, which would allow me to run my spark jobs while not being connected to the BigData server, and was advised that if the server goes down for maintenance or crashes, this would 'exit' all the sessions I was runnning and to be wary of that fact.

## Date: November 27th 2025

Today I focused on the performance testing stage for the Word Count program as I setup an excel sheet with the list of different configurations I wanted to test out on the 100GB dataset. I had run some tests but ran into a roadblock due to the BigData server repeatedly crashing during a particular configurations execution. Whilst running this configuration the job would run for around 15-20 minutes then proceed to disconnect me from the BigData server. I would then try to restart my connection but the server would be unreachable.

I opened up a ticket in order to understand the issue and was told there was a hardware issue with the server and that the CIM help desk would respond to me once the issue was resolved. My next step will be to continue trying out lower configurations to the one that failed once the server comes back online. I will attempt to incrementally scale and record the execution times for each of the successful experiments and document the ones that failed.

## Date: November 25th 2025

I was to successfully run a full distributed grep job on the 100GB Wikipedia dataset on the BigData cluster. This execution confirmed that the implementation scales correctly over the larger version of the dataset. The job completed without an error message although I did have to use a package to interpret the XML file as a spark dataframe and was able to find the command in the github page: <https://github.com/databricks/spark-xml>.

With the XML reading working, I was able to focus on what my next goal would be which would be to performance test both of my programs with different configurations of executors, cores and memory over the cluster.

## Date: November 23rd 2025

I was able to start progress on my 2nd program, the distributed grep problem, and since I was able to perform the implementation on the same Wikipedia data as both the problems I have been tasked to program are reliant on text-based datasets. During the early stages, I was able to reuse the cleanText I had developed to be used over the word count program in the distributed grep problem to allow for easier access to the words. I was able to again split the words but this time I was matching them to the keyword variable I had defined within the code. I used an sql function, rlike, as the core of my program to find the matching variables with the cleaned text.

I was able to run the code on a 1GB version of the entire Wikipedia dataset to test if the code would run as intended and plan to run the program on the full dataset at a later date when I am sure it is able to find the correct words over the smaller dataset.

## Date: November 21st 2025

I got feedback from the CIM help desk ticket I sent explaining how the spark-submit command works as well as what the theoretical maximum resources I could use on the BigData server. There are 10 executors with 4 cores and 24G of memory each, which are representations of the 10 nodes within the cluster. This being the theoretical max values meant I would not run my program at these values as other users would also need resources for their own programs. I was able to get an insight into the software stack on the cluster through the ticket as well and found that my code had to be compatible with the following versions of each language:

- Python 3.6.9

- Spark 2.3.2

- Hadoop 3.1.1

## Date: November 20th 2025

I had my third meeting with my supervisor, in which I showed my progress on how the program was able to run on the BigData server and that I was able to produce a table to show the word counts. I was able to understand more about the second part of my early deliverables and how to implement a version of it in code. That will be my next goal.

Also sent a ticket to clarify the default modes for the spark-submit command that allows me to run the python script on the BigData server.

## Date: November 19th 2025

I was able to program part of the word count program on the BigData server and implement tests in terms of running my code on a smaller sample dataset, around 1GB, to be able to access that the word count was happening appropriately.

Then I ran the program on the larger set of data, around 100GB. This took a while and I will start using a parameter in my terminal command that will allow me to view the amount of time process the dataset took.

## Date: November 17th 2025

I spend the day learning how to interact with HDFS and how to correctly connect to the rhul BigData cluster. This involved practising commands like listing directories, copying data into HDFS for processing, as well as downloading the Wiki data onto the BigData server via a wget command.

I reviewed how Spark interacts with HDFS internally, which helped me gain an understanding for why storing data in HDFS is necessary for efficient distributed processing.

A key part of the day was understanding how spark-submit works in the cluster's environment. I learned about the different modes (client and cluster), how resources such as executor, cores and memory are allocated and how to incorparate my python script to link to the command. By the end, I had a better idae of how to run my PySpark application on the BigData cluster and how to use the spark-submit command for later performance analysis.

## Date: November 12th 2025

I was not able to use the Google Cloud storage as the one I have access to will not be approriate for the parsing of large datasets and running PySpark. The actual Google Cloud has a free trial and I will look into that for the final deliverables of my project as I need to learn about HDFS as a starter.

## Date: November 7th 2025

I was looking into using Google Cloud as a potential way in which to store my data. Since I have access to around 2TB of storage space available, I will need to look further into how I will be able to use that space and use it to for the purpose of testing and processing the dataset I choose.

## Date: November 4th 2025

Today, I focused on understanding transformations, actions and lazy evaluations in Spark.

- Transformations are operations that create a new DataGrame from an existing one. Examples include funtions like map(), filter() and flatMap(). They are lazy, meaning they do not execute immediately but build up a so called logical plan for computation to occur.

- Actions are operations that trigger the actual computation and return a value to the driver or write data to storage, having a real impact that is executed. Examples include count(), collect() and saveAsTextFile().

- Lazy Evaluation means Spark delays execution of transformations until an action is called that sets things into motion. This allows Spark to optimize the execution plan through techinques like pipelining and DAG - Directed Acyclic Graph optimisation leading to overall better performance as a result.

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
