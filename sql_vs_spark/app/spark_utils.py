from pyspark.sql import SparkSession
import time
def get_spark_session():
    return SparkSession.builder \
        .appName("SQLvsSpark") \
        .config("spark.jars","/opt/bitnami/spark/jars/mssql-jdbc12.2.0.jre8.jar") \
        .getOrCreate()
def execute_spark_query(spark, server, database, username, password, query):
    jdbc_url = f"jdbc:sqlserver://{server};databaseName={database}"
    properties = {
        "user": username,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    start_time = time.time()
    df = spark.read.jdbc(url=jdbc_url, table=f"({query}) as query", properties=properties)
    execution_time = time.time() - start_time
    return df, execution_time