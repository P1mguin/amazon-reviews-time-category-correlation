from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

sc = SparkContext(appName="Amazon")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

# Create the dataframe with the products
# Note, this also contains products without category
metadata_files = [
    "/data/doina/UCSD-Amazon-Data/meta_Home_and_Kitchen.json.gz",
    "/data/doina/UCSD-Amazon-Data/meta_Kindle_Store.json.gz",
    "/data/doina/UCSD-Amazon-Data/meta_Movies_and_TV.json.gz",
    "/data/doina/UCSD-Amazon-Data/meta_Musical_Instruments.json.gz",
    "/data/doina/UCSD-Amazon-Data/meta_Office_Products.json.gz",
    "/data/doina/UCSD-Amazon-Data/meta_Sports_and_Outdoors.json.gz",
    "/data/doina/UCSD-Amazon-Data/meta_Tools_and_Home_Improvement.json.gz",
    "/data/doina/UCSD-Amazon-Data/meta_Toys_and_Games.json.gz",
    "/data/doina/UCSD-Amazon-Data/meta_Video_Games.json.gz",
    "/data/doina/UCSD-Amazon-Data/metadata.json.gz"
]

metadata_df = spark.read.json(metadata_files)\
    .filter(col("asin").isNotNull() & col("_corrupt_record").isNull())\
    .select('asin', 'categories')\
    .dropDuplicates()

metadata_df.write.format("json").mode("overwrite").option("header", "true").save("./project/metadata_df")
