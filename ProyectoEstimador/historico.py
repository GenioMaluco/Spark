from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
from dateutil.relativedelta import relativedelta

def fnc_obtener_campo_historico(spark, df_referencias, cedula: str, fuente_referencia: int):
    """
    Devuelve el dataset completo con columnas históricas adicionales
    """
    
    try:
        # 1. Filtrar datos para la cédula y fuente específica
        df_filtrado = df_referencias.filter(
            (F.col("Identificacion") == cedula) & 
            (F.col("Id_referencia") == fuente_referencia) &
            (F.col("dias_mora") >= 1)
        )
        
        # 2. Filtrar últimos 4 años
        df_filtrado = df_filtrado.filter(
            F.col("fecha_informacion") >= F.add_months(F.current_date(), -48)
        )       

        if df_filtrado.isEmpty():
            return spark.createDataFrame([], schema=df_referencias.schema
                ).withColumn("Historico", F.lit("")
                ).withColumn("HistoricoMes", F.lit(""))
        
        # 3. Obtener fechas extremas
        fecha_data = df_filtrado.agg(
            F.max("fecha_informacion").alias("max_fecha"),
            F.min("fecha_informacion").alias("min_fecha")
        ).first()
        
        fecha_final = fecha_data["max_fecha"]
        fecha_inicio = fecha_final - relativedelta(months=23)

        # 4. Generar secuencia de meses
        df_meses = spark.sql(f"""
            SELECT explode(sequence(
                to_date('{fecha_inicio.strftime("%Y-%m-%d")}'),
                to_date('{fecha_final.strftime("%Y-%m-%d")}'),
                interval 1 month
            )) as FechaHistorico
        """)
        # 5. Procesamiento de datos
        df_proceso = df_filtrado.groupBy(
            F.date_format("fecha_informacion", "yyyyMM").alias("periodo"),
            F.date_format("fecha_informacion", "MMM-yy").alias("mes_str"),
        ).agg(
            F.first("fecha_informacion").alias("fecha"),
            F.sum("dias_mora").alias("dias_mora_total"),
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
        )

        # 8. Ordenar por fecha para el histórico
        window = Window.orderBy("FechaHistorico")

        df_resultado = df_completo.withColumn(
            "Historico",
            F.array_join(
                F.collect_list(
                    F.coalesce(F.col("calificacion").cast("string"), F.lit("X"))
                ).over(window),
                "-"
            )
        ).withColumn(
            "HistoricoMes",
            F.array_join(
                F.collect_list(
                    F.coalesce(F.col("mes_str"), 
                    F.date_format("FechaHistorico", "MMM-yy"))
                ).over(window),
                "/"
            )
        ).filter(
            F.col("FechaHistorico") == fecha_final  # Solo la fila más reciente
        ).withColumn(
            "Identificacion", F.lit(cedula)
        ).withColumn(
            "Id_referencia", F.lit(fuente_referencia)
        )

        df_resultado = df_referencias.alias("c").join(
            df_resultado.alias("d"),
            (df_referencias["Identificacion"] == df_resultado["Identificacion"]) & 
            (df_referencias["Id_referencia"] == df_resultado["Id_referencia"]),
            "left"
        )
        columnas_finales = [
            'c.Id',
            'c.tipo_credito_id',
            'c.codigo_estado_cuenta_id',
            'c.Identificacion',  # Usamos la versión con mayúscula
            'c.tipo_deudor_id',
            'c.tipo_informacion_id',
            'c.fecha_otorgamiento_credito',
            'c.fecha_vencimiento',
            'c.saldo_mora',
            'c.tipo_moneda_id',
            'c.cuotas_vencidas',
            'c.fecha_informacion',
            'c.fecha_ultimo_pago',
            'c.dias_mora',
            'c.Cliente',
            'd.FechaHistorico',
            'd.Historico',
            'd.HistoricoMes'
        ]

        # Seleccionar solo las columnas únicas
        df_resultado = df_resultado.select(columnas_finales)
        return df_resultado

    except Exception as e:
        print(f"\n❌ Error en fnc_obtener_campo_historico: {str(e)}")
        raise