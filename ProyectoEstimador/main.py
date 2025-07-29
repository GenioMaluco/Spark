from spark_utils import get_spark_session
from pyspark.sql.functions import col,lit,current_date
from datetime import datetime
from dateutil import relativedelta
import time
from transformaciones import transformar_dataframe

def main():
    spark = None
    try:
        start_time_total = time.time()
        spark = get_spark_session()
        print("✅ SparkSession creada correctamente")
        #Conexiones JDBC
        props = {
            "user": "Adrian.Araya",
            "password": "Soporte1990%",
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "encrypt": "true", 
            "trustServerCertificate":"true"
            
        }

        jdbc_estimador= "jdbc:sqlserver://192.168.5.136:18698;databaseName=ReferenciasComerciales"
        start_load = time.time()
        #Cargar Tablas necesarias
        df_DatoReferencia=spark.read.jdbc(jdbc_estimador,"dbo.DatoReferencia",properties=props)\
            .select("Id",
                    "fuente_informacion_id",
                    "tipo_credito_id",
                    "codigo_estado_cuenta_id",
                    "identidicacion",
                    "tipo_deudor_id",
                    "tipo_informacion_id",
                    "fecha_otorgamiento_credito",
                    "fecha_vencimiento",
                    "saldo_mora",
                    "tipo_moneda_id",
                    "cuotas_vencidas",
                    "fecha_informacion",
                    "tipo_deudor_id",
                    "fecha_ultimo_pago",
                    "dias_mora",
                    "Estado"
                    )

        df_ClienteFuente=spark.read.jdbc(jdbc_estimador,"dbo.ClienteFuente",properties=props)\
            .select("Id",
                    "Cliente"
                    )
        
        load_time = time.time() - start_load
        print(f"⏱ Tiempo de carga de datos: {load_time:.2f} segundos")

        #Definicion de fechas
        fecha_tope_incio = datetime.now().date()
        fecha_tope_final = fecha_tope_incio - relativedelta.relativedelta(months=24)

         # Medimos tiempo de la consulta
        start_query = time.time()
        
        # Realizar el Inner Join entre ambas tablas
        inner_join_df = df_DatoReferencia.alias("d").join(
            df_ClienteFuente.alias("c"),
            col("d.fuente_informacion_id") == col("c.Id"),
            "inner"
        ).filter(
            (col("d.fecha_informacion")>= fecha_tope_final) & 
            (col("d.dias_mora") > 0) &
            (col("d.estado") == 1)
            )

        print(f"Muestra total{inner_join_df.columns}Resultado")

        result_df = transformar_dataframe(inner_join_df)
        

        # Forzar ejecución y contar registros (para asegurar que la consulta se ejecute)
        count = result_df.count()

        query_time = time.time() - start_query
        print(f"⏱ Tiempo de ejecución de la consulta: {query_time:.2f} segundos")
        print(f"📊 Número de registros obtenidos: {count}")

        # Guardar en una carpeta 'data' en el directorio actual
        #output_path = "data/referencias_comerciales"  # Ruta relativa
        #inner_join_df.take(100)

        #if count > 0:
        #    print("\n📊 Muestra de resultados (primeras 10 filas):")
        #    inner_join_df.write \
        #        .mode("overwrite") \
        #        .csv(output_path)
        #print(f"✅ Datos guardados en: {output_path}")
        
        # Tiempo total de ejecución
        total_time = time.time() - start_time_total
        print(f"\n⏱ Tiempo total de ejecución: {total_time:.2f} segundos")
        
                
    except Exception as e:
        print(f"\n❌ Error en main: {str(e)}")
    finally:
        if spark:
            spark.stop()
            print("\n🛑 SparkSession detenida")

if __name__ == "__main__":
    main()

    
