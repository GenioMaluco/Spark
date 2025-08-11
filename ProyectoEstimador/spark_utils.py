from pyspark.sql import SparkSession
from py4j.java_gateway import java_import

# Configuración de rutas para Windows
jdbc_path = "C:\\spark\\jars\\mssql-jdbc-12.4.2.jre11.jar"  # Usar doble barra o raw string r"C:\spark\jars\..."

def get_spark_session():   

    spark = SparkSession.builder \
        .appName("Spark SQL Server Connector") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "16g") \
        .config("spark.driver.memory", "8g") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3") \
        .config("spark.driver.extraClassPath", jdbc_path) \
        .config("spark.executor.extraClassPath", jdbc_path) \
        .config("spark.jars", jdbc_path) \
        .config("spark.ui.enabled", "true") \
        .getOrCreate()
    
    # Cargar manualmente el driver en la JVM
    jvm = spark._jvm
    java_import(jvm, "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    
    return spark
    