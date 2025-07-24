from spark_utils import get_spark_session, execute_spark_query

def main():
    spark = None
    try:
        spark = get_spark_session()
        print("✅ SparkSession creada correctamente")
                       
        # Consulta real
        real_query = "SELECT TOP (1000) identificacion, concat(nombre,' ',apellido1,' ', apellido2) as NombreCompleto FROM [ReferenciasComerciales].[dbo].DatoReferencia"
        df_real, time_elapsed = execute_spark_query(spark, query=real_query)
        
        print("\n📊 Resultados de la consulta:")
        df_real.show(truncate=False)
        print(f"\n⏱ Tiempo de ejecución: {time_elapsed:.2f} segundos")
        
    except Exception as e:
        print(f"\n❌ Error en main: {str(e)}")
    finally:
        if spark:
            spark.stop()
            print("\n🛑 SparkSession detenida")

if __name__ == "__main__":
    main()
