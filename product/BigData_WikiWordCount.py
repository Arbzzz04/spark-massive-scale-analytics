from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# the HDFS path for the wikipedia dataset
dataPath = "hdfs:///user/zlac302/datasets/wikipedia/wiki_100gb.xml"

# creating Spark session with config for xml file parsing
spark = (
    SparkSession.builder
    .appName("BigDataWikiWordCount")
    .config("spark.jars.packages", "com.databricks:spark-xml_2.11:0.12.0")
    .getOrCreate()
)

# read wikipedia data dump into DataFrame
df = (
    spark.read.format("xml")
    .option("rowTag", "page") # each <page> element is a record
    .option("samplingRatio", 0.1) # used for schema interface (not limiting data read)
    .load(dataPath)
)

# filter down the dataset using sql to select the title and text columns
pages = (
    df
    .filter(F.col("ns") == 0)
    .filter(F.col("redirect").isNull())
    .select(
        F.col("title"),
        F.col("revision.text._VALUE").alias("text")
    )
    .filter(F.col("text").isNotNull())
)

# transform the text column into clean usable data
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

# custom junk words mostly coming from tables / markup
junkWords = [
    "infobox", "wikitable", "bgcolor", "style", "rowspan", 
    "colspan", "br", "ref", "nbsp", "thumb", "px",
    "border", "font", "color", "valign", "http", "url"
]

# split the pages into seperate words and filter any unnecessary data
words = (
    cleanText
      .select(F.explode(F.split(F.col("cleanText"), r"\s+")).alias("word"))
      .filter(F.col("word") != "")
      # drop all-digit tokens
      .filter(~F.col("word").rlike(r"^\d+$"))
      # drop custom junk tokens
      .filter(~F.col("word").isin(junkWords))
)

# word count variable
counts = (
    words
      .groupBy("word")
      .count()
      .orderBy(F.desc("count"))
)

# show the table of words and count in terminal
counts.show(20, truncate=False)