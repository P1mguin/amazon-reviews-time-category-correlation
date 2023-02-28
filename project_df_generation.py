from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

sc = SparkContext(appName="Amazon")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

# Requires metadata_df_generation and review_df_generation to be run first
metadata_df = spark.read.json("./project/metadata_df")
review_df = spark.read.json("./project/review_df")

# Join the categories to the review (if any)
project_df = review_df.join(metadata_df, review_df.asin == metadata_df.asin)\
    .select(review_df.asin,
            review_df.time,
            review_df.overall,
            explode(flatten(metadata_df.categories)).alias('categories')
            )

project_df.write.format("json").mode("overwrite").option("header", "true").save("./project/project_df")
