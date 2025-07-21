import pyspark
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("PruebaCSV") \
    .getOrCreate()

# leer CSV 
flights_spark = spark\
    .read\
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("C:/Users/Dev-Adrian/Documents/Spark/LibroSpark/src/flight-summary.csv")

# Show the DataFrame
flights_spark.sort("count").explain()
flights_spark.take(3)
spark.stop()