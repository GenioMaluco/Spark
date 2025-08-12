from spark_utils import get_spark_session
from pyspark.sql.functions import col
from datetime import datetime
from dateutil import relativedelta
import time
from transformaciones import transformar_dataframe
from config import Config

def main():
    start_query = time.time()
    spark = None
    
    try:
        spark = get_spark_session()
        print("✅ SparkSession creada correctamente")
        
        # Cargar Tablas necesarias usando la configuración
        df_DatoReferencia = spark.read.jdbc(
            url=Config.JDBC_URL_ESTIMADOR,
            table=Config.TABLA_REFERENCIAS,
            properties=Config.DB_PROPERTIES
        ).select(
            "Id", "fuente_informacion_id", "tipo_credito_id", "codigo_estado_cuenta_id",
            "identificacion", "tipo_deudor_id", "tipo_informacion_id", "fecha_otorgamiento_credito",
            "fecha_vencimiento", "saldo_mora", "tipo_moneda_id", "cuotas_vencidas", 
            "fecha_informacion", "tipo_deudor_id", "fecha_ultimo_pago", "dias_mora", "Estado"
        ).filter(
            (col("Identificacion") == '206850212') &
            (col("dias_mora") > 0) & (col("estado") == 1)
        )

        df_ClienteFuente = spark.read.jdbc(
            url=Config.JDBC_URL_ESTIMADOR,
            table=Config.TABLA_CLIENTES,
            properties=Config.DB_PROPERTIES
        ).select("Id", "Cliente", "VersionDatos")

        # Definición de fechas usando configuración
        fecha_tope_final = datetime.now().date() - relativedelta.relativedelta(months=Config.MESES_HISTORICO)

        # Renombrar columnas
        df_DatoReferencia = df_DatoReferencia.withColumnRenamed("fuente_informacion_id", "Id_referencia")
        df_ClienteFuente = df_ClienteFuente.withColumnRenamed("Id", "Id_cliente")

        # Realizar el join
        inner_join_df = df_DatoReferencia.join(
            df_ClienteFuente,
            col("Id_referencia") == col("Id_cliente"),
            "inner"
        ).filter(col("fecha_informacion") >= fecha_tope_final)

        # Transformar el DataFrame
        result_df = transformar_dataframe(
            inner_join_df, 
            jdbc_url=Config.JDBC_URL_HISTORICO,
            props=Config.DB_PROPERTIES
        )

        # Escribir el DataFrame a la base de datos
        result_df.write.jdbc(
            url=Config.JDBC_URL_HISTORICO,
            table=Config.TABLA_DESTINO,
            mode="overwrite",
            properties=Config.DB_PROPERTIES
        )

        # Mostrar tiempos de ejecución
        query_time = time.time() - start_query
        print(f"⏱ Tiempo de ejecución de la consulta: {query_time:.2f} segundos")
        print(f"\n⏱ Tiempo total de ejecución: {query_time:.2f} segundos")
                
    except Exception as e:
        print(f"\n❌ Error en main: {str(e)}")
    finally:
        if spark:
            spark.stop()
            print("\n🛑 SparkSession detenida")

if __name__ == "__main__":
    main()