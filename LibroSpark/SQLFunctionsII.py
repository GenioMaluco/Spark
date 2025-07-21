import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

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

flights_spark\
    .groupBy("dest_city") \
    .sum("count") \
    .withColumnRenamed("sum(count)", "total_vuelos") \
    .sort(desc("total_vuelos")) \
    .limit(5) \
    .show()

# Show the DataFrame


input("Presiona Enter para terminar...")
spark.stop()
