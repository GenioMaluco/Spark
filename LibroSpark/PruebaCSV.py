import pyspark
from pyspark.sql import SparkSession
from time import sleep


# Create a Spark session
spark = SparkSession.builder \
    .appName("PruebaCSV") \
    .config("spark.sql.shuffle.partitions", "5") \
    .getOrCreate()
    

# leer CSV 
flights_spark = spark\
    .read\
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("C:/Users/Dev-Adrian/Documents/Spark/LibroSpark/src/flight-summary.csv")

# Show the DataFrame

flights_spark.sort("count").show(2)
input("Presiona Enter para terminar...")
spark.stop()