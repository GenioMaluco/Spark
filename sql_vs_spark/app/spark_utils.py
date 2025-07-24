from pyspark.sql import SparkSession
import time

def get_spark_session():
    return SparkSession.builder \
        .appName("SQLvsSpark") \
        .config("spark.jars", "/opt/bitnami/spark/jars/mssql-jdbc-12.4.2.jre11.jar") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.network.timeout", "600s") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

def execute_spark_query(spark, server="192.168.5.136", port="1433", database="master", 
                      username="Adrian.Araya", password="Soporte1990%", query="SELECT TOP 5 name FROM sys.databases"):
    try:
        # Construcción correcta de la URL JDBC
        jdbc_url = f"jdbc:sqlserver://{server}:{port};databaseName={database}"
        
        properties = {
            "user": username,
            "password": password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "encrypt": "false",
            "trustServerCertificate": "true",
            "loginTimeout": "30",
            "socketTimeout": "600",
            "applicationIntent": "ReadWrite"
        }
        
        print(f"🔗 Intentando conexión Spark a: {jdbc_url}")
        
        start_time = time.time()
        
        # Usar siempre el formato de subconsulta para mayor compatibilidad
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"({query}) as custom_query") \
            .options(**properties) \
            .load()
        
        execution_time = time.time() - start_time
        print(f"✅ Spark: Consulta ejecutada en {execution_time:.2f} segundos")
        return df, execution_time
        
    except Exception as e:
        print(f"❌ Error en execute_spark_query: {str(e)}")
        from pyspark.sql.types import StructType
        return spark.createDataFrame([], StructType([])), 0