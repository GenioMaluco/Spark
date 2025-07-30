from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
from dateutil.relativedelta import relativedelta

def fnc_obtener_campo_historico(spark, df_referencias, cedula: str, fuente_referencia: int):
    """
    Versión completamente funcional con manejo correcto de fechas y optimizaciones
    """
    try:
        # 1. Filtrar datos para la cédula y fuente específica
        df_filtrado = df_referencias.filter(
            (F.col("Identificacion") == cedula) & 
            (F.col("fuente_informacion_id") == fuente_referencia) &
            (F.col("dias_mora") >= 1)
        )
        
        # 2. Filtrar últimos 4 años
        df_filtrado = df_filtrado.filter(
            F.col("fecha_informacion") >= F.add_months(F.current_date(), -48)
        )       
        
        if df_filtrado.isEmpty():
            return spark.createDataFrame(
                [(1, cedula, "", "")],
                ["Id", "CedulaReferencia", "Historico", "HistoricoMes"]
            )
        
        # 3. Obtener fechas extremas como objetos Python
        fecha_data = df_filtrado.agg(
            F.max("fecha_informacion").alias("max_fecha"),
            F.min("fecha_informacion").alias("min_fecha")
        ).first()
        
        fecha_final = fecha_data["max_fecha"]
        fecha_inicio = fecha_final - relativedelta(months=23)

        
        
        # 4. Generar secuencia de meses (versión robusta)
        df_meses = spark.sql(f"""
            SELECT explode(sequence(
                to_date('{fecha_inicio.strftime("%Y-%m-%d")}'),
                to_date('{fecha_final.strftime("%Y-%m-%d")}'),
                interval 1 month
            )) as FechaHistorico
        """)

        # 1. Primero agregamos las columnas de periodo y mes_str
        df_filtrado = df_filtrado.withColumn(
            "periodo", 
            F.date_format("fecha_informacion", "yyyyMM")
        ).withColumn(
            "mes_str",
            F.date_format("fecha_informacion", "MMM-yy")
        )

        # 2. Calculamos los totales por periodo
        df_totales = df_filtrado.groupBy("periodo").agg(
            F.sum("dias_mora").alias("dias_mora_total"),
            F.first("mes_str").alias("mes_str"),
            F.first("fecha_informacion").alias("fecha")
        )

        # 3. Hacemos join para mantener todas las columnas
        df_proceso = df_filtrado.join(
            df_totales,
            "periodo",
            "left"
        ).dropDuplicates(["periodo"])  # Nos quedamos con un registro por periodo

        # 5. Procesamiento de datos y cálculo de calificaciones
        df_proceso = df_filtrado.groupBy(
            F.date_format("fecha_informacion", "yyyyMM").alias("periodo"),
            F.date_format("fecha_informacion", "MMM-yy").alias("mes_str")
        ).agg(
            F.first("fecha_informacion").alias("fecha"),
            F.sum("dias_mora").alias("dias_mora_total")
        )
        
        
        # 6. Calcular calificación
        df_calificado = df_proceso.withColumn(
            "calificacion",
            F.when(F.col("dias_mora_total").between(1, 30), F.lit(1))
            .when(F.col("dias_mora_total").between(31, 60), F.lit(2))
            .when(F.col("dias_mora_total").between(61, 90), F.lit(3))
            .when(F.col("dias_mora_total").between(91, 120), F.lit(4))
            .when(F.col("dias_mora_total").between(121, 150), F.lit(5))
            .otherwise(F.lit(6))
        )

        # 7. Join con todos los meses
        df_completo = df_meses.join(
            df_calificado,
            F.date_format("FechaHistorico", "yyyyMM") == F.col("periodo"),
            "left_outer"
        ).orderBy("FechaHistorico")
        
        # 8. Generar strings históricos
        # Solución directa sin ordenar (más rápida)
        
        historico_rows = df_completo.collect()

        # Generar histórico de calificaciones
        historico = "-".join(
        str(row["calificacion"]) if row["calificacion"] is not None else "X" 
        for row in historico_rows
)

        print(df_completo)
        print(historico)
        
        historico_mes = "/".join(
            row["mes_str"] if row["mes_str"] is not None 
            else row["FechaHistorico"].strftime("%b-%y") 
            for row in historico_rows
        )
        
        print("\n " , str(historico_mes.columns))
        print(historico_mes)

        # 9. Retornar resultado
        return spark.createDataFrame(
            [(1, cedula, historico, historico_mes)],
            ["Id", "CedulaReferencia", "Historico", "HistoricoMes"]
        )
        
    except Exception as e:
        print(f"\n❌ Error en fnc_obtener_campo_historico: {str(e)}")
        raise