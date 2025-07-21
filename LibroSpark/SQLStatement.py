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

#Registrar tabla temporal
flights_spark.createOrReplaceTempView("flights_spark")

# Show the DataFrame

sqlway=spark.sql("SELECT dest_city,count(1) AS Catidad_Viajes FROM flights_spark GROUP BY dest_city")

dataframeway = flights_spark \
    .groupBy("dest_city") \
    .count()
sqlway.show(1000)
dataframeway.show(10000)

input("Presiona Enter para terminar...")
spark.stop()