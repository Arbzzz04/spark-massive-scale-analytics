from pyspark.sql import SparkSession
from pyspark.sql import functions as F

dataPath = "hdfs:///user/zlac302/datasets/wikipedia/wiki_100gb.xml"

spark = (
    SparkSession.builder
    .appName("BigDataWikiWordCount")
    .config("spark.jars.packages", "com.databricks:spark-xml_2.11:0.12.0")
    .getOrCreate()
)

df = (
    spark.read.format("xml")
    .option("rowTag", "page")
    .option("samplingRatio", 0.1)
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
    "infobox", "wikitable", "bgcolor", "style", "align", "center",
    "rowspan", "colspan", "small", "br", "ref", "name",
    "url", "website", "archive", "access", "date"
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

# limit the data saved to the output
# top20 = counts.limit(20)

# load the results into HDFS
# outputPath = "hdfs:///user/zlac302/bigdata_wiki_wordcount_output"

# save the wordcount into HDFS
# top20.write.mode("overwrite").parquet(outputPath)


