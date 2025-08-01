from spark_utils import get_spark_session
from pyspark.sql.functions import col,lit,current_date
from pyspark.sql import DataFrame
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
        jdbc_Historico= "jdbc:sqlserver://192.168.5.136:18698;databaseName=DatosDavivienda"        

        start_load = time.time()
        #Cargar Tablas necesarias
        df_DatoReferencia=spark.read.jdbc(jdbc_estimador,"dbo.DatoReferencia",properties=props)\
            .select("Id",
                    "fuente_informacion_id",
                    "tipo_credito_id",
                    "codigo_estado_cuenta_id",
                    "identificacion",
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
                    ).filter(
                        (col("identificacion")== "502720062")
                    )

        df_ClienteFuente=spark.read.jdbc(jdbc_estimador,"dbo.ClienteFuente",properties=props)\
            .select("Id",
                    "Cliente"
                    ).filter(
                        (col("Id") == "162")
                    )
        load_time = time.time() - start_load
        print(f"⏱ Tiempo de carga de datos: {load_time:.2f} segundos")

        #Definicion de fechas
        fecha_tope_incio = datetime.now().date()
        fecha_tope_final = fecha_tope_incio - relativedelta.relativedelta(months=24)

         # Medimos tiempo de la consulta
        start_query = time.time()

        # Antes del join, renombrar las columnas Id
        df_DatoReferencia = df_DatoReferencia.withColumnRenamed("fuente_informacion_id", "Id_referencia")
        df_ClienteFuente = df_ClienteFuente.withColumnRenamed("Id", "Id_cliente")
        
        # Realizar el Inner Join entre ambas tablas
        inner_join_df = df_DatoReferencia.join(
            df_ClienteFuente,
            col("Id_referencia") == col("Id_cliente"),
            "inner"
            ).filter(
                (col("fecha_informacion")>= fecha_tope_final) & 
                (col("dias_mora") > 0) &
                (col("estado") == 1)
            )

        result_df = transformar_dataframe(inner_join_df, 
            jdbc_url=jdbc_Historico,
            props={
            "user": "Adrian.Araya",
            "password": "Soporte1990%",
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "encrypt": "true", 
            "trustServerCertificate":"true"
            }
        )

        result_df.select(
            
        )

        print("\n📋 Columnas del DataFrame resultante:")
        for i, col_name in enumerate(result_df.columns, 1):
            print(f"{i}. {col_name}")

        
        # Forzar ejecución y contar registros (para asegurar que la consulta se ejecute)
        count = result_df.count()

        query_time = time.time() - start_query
        print(f"⏱ Tiempo de ejecución de la consulta: {query_time:.2f} segundos")
        print(f"📊 Número de registros obtenidos: {count}")

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

    
