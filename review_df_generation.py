import datetime

from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *

sc = SparkContext(appName="Amazon")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

review_files = [
    "/data/doina/UCSD-Amazon-Data/reviews_Amazon_Instant_Video.json.gz",
    "/data/doina/UCSD-Amazon-Data/reviews_Apps_for_Android.json.gz",
    "/data/doina/UCSD-Amazon-Data/reviews_Automotive.json.gz",
    "/data/doina/UCSD-Amazon-Data/reviews_Books.json.gz",
    "/data/doina/UCSD-Amazon-Data/reviews_CDs_and_Vinyl.json.gz",
    "/data/doina/UCSD-Amazon-Data/reviews_Cell_Phones_and_Accessories.json.gz",
    "/data/doina/UCSD-Amazon-Data/reviews_Clothing_Shoes_and_Jewelry.json.gz",
    "/data/doina/UCSD-Amazon-Data/reviews_Digital_Music.json.gz",
    "/data/doina/UCSD-Amazon-Data/reviews_Electronics.json.gz",
    "/data/doina/UCSD-Amazon-Data/reviews_Health_and_Personal_Care.json.gz",
    "/data/doina/UCSD-Amazon-Data/reviews_Home_and_Kitchen.json.gz",
    "/data/doina/UCSD-Amazon-Data/reviews_Kindle_Store.json.gz",
    "/data/doina/UCSD-Amazon-Data/reviews_Movies_and_TV.json.gz",
    "/data/doina/UCSD-Amazon-Data/reviews_Musical_Instruments.json.gz",
    "/data/doina/UCSD-Amazon-Data/reviews_Office_Products.json.gz",
    "/data/doina/UCSD-Amazon-Data/reviews_Sports_and_Outdoors.json.gz",
    "/data/doina/UCSD-Amazon-Data/reviews_Tools_and_Home_Improvement.json.gz",
    "/data/doina/UCSD-Amazon-Data/reviews_Toys_and_Games.json.gz",
    "/data/doina/UCSD-Amazon-Data/reviews_Video_Games.json.gz"
]


# Get the necessary info for each data frame
def unix_time_to_int(timestamp):
    time = datetime.datetime.fromtimestamp(timestamp)
    return float(time.hour) + (float(time.minute) / 60)


# For working without category based information
unix_time_to_int_udf = udf(lambda x: unix_time_to_int(x), FloatType())

schema = StructType([
    StructField("asin", StringType(), True),
    StructField("time", FloatType(), True),
    StructField("overall", DoubleType(), True),
    StructField("category", StringType(), True)
])

review_df = spark.read.json(review_files)\
    .filter(col("asin").isNotNull() & col("unixReviewTime").isNotNull())\
    .select('asin', unix_time_to_int_udf(col('unixReviewTime')).alias('time'), 'overall')

review_df.write.format("json").mode("overwrite").option("header", "true").save("./project/review_df")
