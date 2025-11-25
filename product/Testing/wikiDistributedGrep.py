from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# using the 1GB sample dataset for testing
dataPath = "hdfs:///user/zlac302/datasets/wikipedia_sample/wiki_1gb_sample.xml"


# variable to define regex pattern to perform grep on
word = "elemental"

# start up spark session with config for xml file
spark = (
    SparkSession.builder
	.appName("DistributedGrep")
	.config("spark.jars.package", "com.databricks:spark-xml_2:11:0.12.0")
	.getOrCreate()
)

# read the xml file from the hdfs
dataFrame = (
    spark.read.format("xml")
	.option("rowTag", "page")
	.option("samplingRatio", 0.1)
	.load(dataPath)
)

pages = (
    dataFrame
	.filter(F.col("ns") == 0) # keep only the article content
	.filter(F.col("redirect").isNull()) # remove any redirect pages
	.select(
	    F.col("title"), # the title of the article 
	    F.col("revision.text._VALUE").alias("text") # main content from the page
	)
	.filter(F.col("text").isNotNull()) # remove any 'text' field with nothing in them
)

cleanText = (
    pages
        # lowercase
        .withColumn("cleanText", F.lower(F.col("text")))
        # remove wiki links [[...]] and templates {{...}}
        .withColumn("cleanText", F.regexp_replace(F.col("cleanText"), r"\[\[.*?\]\]", " "))
        .withColumn("cleanText", F.regexp_replace(F.col("cleanText"), r"\{\{.*?\}\}", " "))
        # remove <ref>...</ref> and generic HTML tags
        .withColumn("cleanText", F.regexp_replace(F.col("cleanText"), r"<ref[^>]*>.*?</ref>", " "))
        .withColumn("cleanText", F.regexp_replace(F.col("cleanText"), r"<[^>]+>", " "))
        # remove URLs
        .withColumn("cleanText", F.regexp_replace(F.col("cleanText"), r"http\S+", " "))
        # turn anything that isn't a–z or whitespace into space
        .withColumn("cleanText", F.regexp_replace(F.col("cleanText"), r"[^a-z\s]", " "))
	# remove multiple spaces
        .withColumn("cleanText", F.regexp_replace(F.col("cleanText"), r"\s+", " "))
	# remove all single letter's apart from 'a' and 'i'
	.withColumn("cleanText", F.regexp_replace(F.col("cleanText"), r"\b[b-hj-z]\b", " "))
        # trim
        .withColumn("cleanText", F.trim(F.col("cleanText")))
        # drop empty rows
        .filter(F.length(F.col("cleanText")) > 0)
)

# print(cleanText.printSchema())

matches = (
    cleanText
        .select(
            "title", # select title to use as location
            F.explode(F.split(F.col("cleanText"), r"\s+")).alias("word") # split the cleanText into seperate words
        )
        .filter(F.col("word") != "") # filter any empty strings
        .filter(F.col("word").rlike(word)) # keep any words that match the 'word' variable
        .select( # select the fields to output
            F.col("word").alias("matched_string"),
            F.col("title").alias("location")
        )
)

# display a sample of the matches and location
matches.show(20, truncate=False)
