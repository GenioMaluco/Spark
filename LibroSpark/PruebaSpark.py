import pyspark
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("PruebaSpark") \
    .getOrCreate()
# Create a DataFrame with a range of numbers
myrange = spark.range(1000).toDF("number")
myrange.show(1000)
spark.stop()