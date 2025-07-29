from spark_utils import get_spark_session, execute_spark_query
import time
from pyspark.sql import SparkSession

def test_spark_internal():
    """Prueba interna de Spark sin conexión externa"""
    spark = None
    try:
        spark = get_spark_session()
        test_df = spark.range(100).cache()
        count = test_df.count()
        assert count == 100, f"Error: Expected 100 rows, got {count}"
        print("✅ Prueba interna de Spark exitosa")
        return True
    except Exception as e:
        print(f"❌ Error en prueba interna de Spark: {str(e)}")
        return False
    finally:
        if spark:
            spark.stop()

def test_sql_connection(server, port, database, username, password, query="SELECT TOP 5 name FROM sys.databases"):
    """Prueba de conexión a SQL Server"""
    spark = None
    try:
        spark = get_spark_session()
        
        # Primera prueba: Consulta simple
        start_time = time.time()
        df, exec_time = execute_spark_query(
            spark,
            server=server,
            port=port,
            database=database,
            username=username,
            password=password,
            query=query
        )
        
        # Mostrar resultados
        print(f"\n🔍 Resultados de la consulta (ejecutada en {exec_time:.2f}s):")
        df.show(truncate=False)
        
        # Segunda prueba: Contar registros
        count = df.count()
        print(f"\n📊 Total de registros obtenidos: {count}")
        
        return True
    except Exception as e:
        print(f"\n❌ Error en conexión a SQL Server: {str(e)}")
        print("\nPosibles soluciones:")
        print("1. Verifica que SQL Server esté corriendo y aceptando conexiones")
        print("2. Confirma que el puerto 18698 está abierto en el firewall")
        print("3. Asegúrate que las credenciales son correctas")
        print("4. Verifica que el JDBC driver esté en /opt/bitnami/spark/jars/")
        return False
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    print("🚀 Iniciando pruebas de conexión Spark...")
    
    # Configuración
    SERVER = "192.168.5.136"
    PORT = "18698"  # Puerto específico
    DATABASE = "master"
    USERNAME = "Adrian.Araya"
    PASSWORD = "Soporte1990%"
    TEST_QUERY = "SELECT name, create_date FROM sys.databases"
    
    # Paso 1: Probar Spark internamente
    print("\n🔧 Probando configuración básica de Spark...")
    if not test_spark_internal():
        exit(1)
    
    # Paso 2: Probar conexión a SQL
    print(f"\n🔌 Probando conexión a SQL Server: {SERVER}:{PORT}")
    success = test_sql_connection(
        server=SERVER,
        port=PORT,
        database=DATABASE,
        username=USERNAME,
        password=PASSWORD,
        query=TEST_QUERY
    )
    
    # Resultado final
    if success:
        print("\n🎉 ¡Todas las pruebas pasaron exitosamente!")
    else:
        print("\n🔴 Hubo problemas en las pruebas. Verifica los mensajes de error.")