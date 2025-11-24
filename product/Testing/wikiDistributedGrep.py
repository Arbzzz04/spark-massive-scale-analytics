from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Using the 1GB sample dataset for testing
dataPath = "hdfs:///user/zlac302/datasets/wikipedia_sample/wiki_1gb_sample.xml"


# Variable to define regex pattern to perform grep on
word = "elemental"

spark = (
    SparkSession.builder
	.appName("DistributedGrep")
	.config("spark.jars.package", "com.databricks:spark-xml_2:11:0.12.0")
	.getOrCreate()
)

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
	    F.col("title"),
	    F.col("revision.text._VALUE").alias("text")
	)
	.filter(F.col("text").isNotNull())
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
            "title",
            F.explode(F.split(F.col("cleanText"), r"\s+")).alias("word")
        )
        .filter(F.col("word") != "")
        .filter(F.col("word").rlike(word))
        .select(
            F.col("word").alias("matched_string"),
            F.col("title").alias("location")
        )
)

matches.show(20, truncate=False)
