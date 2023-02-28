from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

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

for review_file in review_files:
    category = review_file.split("/")[-1].replace("reviews_", "").replace(".json.gz", "").replace("_", " ")
    review_df = spark.read.json("./project/" + category + "_Summary")
    print("\n\n\n\n" + category + ":")
    review_df.show()

for time in ["1.0", "2.0"]:
    time_df = spark.read.json("./project/" + time + "_Summary")
    print("\n\n\n\n" + time + ":")
    time_df.show()