import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import max

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

sqlway=spark.sql("SELECT dest_city,sum(count) AS total_vuelos FROM flights_spark GROUP BY dest_city ORDER BY total_vuelos DESC LIMIT 5")

# Show the DataFrame

sqlway.show()
input("Presiona Enter para terminar...")
spark.stop()
