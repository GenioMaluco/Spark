from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, when, lit, udf, lag, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType
from datetime import datetime, timedelta
import time
import os
from py4j.java_gateway import java_import

# Configuración de rutas para Windows
jdbc_path = "C:\\spark\\jars\\mssql-jdbc-12.4.2.jre11.jar"  # Usar doble barra o raw string r"C:\spark\jars\..."

def get_spark_session():
    # Configurar entorno Hadoop para Windows (necesario para Spark en Windows)
    #os.environ['HADOOP_HOME'] = 'C:\\spark\\hadoop'
    #os.environ['PATH'] = f"{os.environ['PATH']};C:\\spark\\hadoop\\bin"
    
    

    spark = SparkSession.builder \
        .appName("Spark SQL Server Connector") \
        .config("spark.sql.debug.maxToStringFields", "1000") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "16g") \
        .config("spark.driver.memory", "8g") \
        .config("spark.jars", jdbc_path) \
        .config("spark.driver.extraClassPath", jdbc_path) \
        .config("spark.executor.extraClassPath", jdbc_path) \
        .getOrCreate()
    
    # Cargar manualmente el driver en la JVM
    jvm = spark._jvm
    java_import(jvm, "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    
    return spark
  
    try:
        # Construir URL JDBC con parámetros adicionales
        jdbc_url = (f"jdbc:sqlserver://{server}:{port};"
                   f"databaseName={database};"
                   "encrypt=true;"
                   "trustServerCertificate=true;"
                   "loginTimeout=30;")
        
        print(f"🔗 Conectando a: {jdbc_url}")
        start_time = time.time()

        # Cargar datos usando la sintaxis más robusta
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("query", query) \
            .option("user", username) \
            .option("password", password) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .load()
        
        elapsed = time.time() - start_time
        print(f"✅ Datos cargados correctamente ({elapsed:.2f}s)")
        return df, elapsed
        
    except Exception as e:
        print(f"❌ Error en execute_spark_query: {str(e)}")
        from pyspark.sql.types import StructType
        return spark.createDataFrame([], StructType([])), 0