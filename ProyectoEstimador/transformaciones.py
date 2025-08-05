from functools import reduce
from pyspark.sql.functions import col, when, date_format, lit, udf, explode
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType, StructField, StructType,IntegerType,StringType
from spark_utils import get_spark_session
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import time


def agregar_tipo_credito(df: DataFrame) -> DataFrame:
    try:
        col_name = "tipo_credito_id"
        
        # Crea la nueva columna con un nombre diferente
        return df.withColumn(
            col_name,  # Nuevo nombre para evitar conflictos
            when(col(col_name) == 1, "Tarjeta de Crédito comercial")
            .when(col(col_name) == 2, "Crédito a Plazo")
            .when(col(col_name) == 3, "Crédito Rotativo")
            .when(col(col_name) == 4, "Tarjeta de crédito internacional")
            .when(col(col_name) == 5, "Tarjeta de Crédito local")
            .when(col(col_name) == 6, "Hipotecario")
            .when(col(col_name) == 7, "Efectivo a 30 días Plazo")
            .when(col(col_name) == 8, "Prendario")
            .when(col(col_name) == 9, "Refinamiento")
            .when(col(col_name) == 10, "Otros")
            .when(col(col_name) == 11, "Pago bienes inmuebles")
            .when(col(col_name) == 12, "Pago patentes")
            .when(col(col_name) == 13, "LineaBlanca")
            .when(col(col_name) == 14, "Prestamo Comercial")
            .when(col(col_name) == 15, "Empresarial")
            .when(col(col_name) == 16, "Cuota nivelada")
            .when(col(col_name) == 17, "Linea Crédito")
            .when(col(col_name) == 18, "Fiduciario")
            .when(col(col_name) == 19, "Automovil Prendario")
            .when(col(col_name) == 20, "Capital Social")
            .when(col(col_name) == 21, "CPH-3 Fiduciario")
            .when(col(col_name) == 22, "CPH-3 Hipotecario")
            .when(col(col_name) == 23, "CPH-3 Sin Fiador")
            .when(col(col_name) == 24, "Hipotecario Consumo")
            .when(col(col_name) == 25, "Hipotecario Cuota Tradicional (Cerrado)")
            .when(col(col_name) == 26, "MULTIPLUSCRÉDI")
            .when(col(col_name) == 27, "Premium")
            .when(col(col_name) == 28, "Sin Fiador")
            .when(col(col_name) == 29, "Uso Multiple")
            .when(col(col_name) == 30, "Vivienda")
            .otherwise(None)
        )
    except Exception as e:
        print(f"\n❌ Error en TipoCredito: {str(e)}")
        raise

def agregar_estado_operacion(df: DataFrame) -> DataFrame:
    try:
        """Agrega la columna EstadoOperacion con descripciones"""
        return df.withColumn(
            "codigo_estado_cuenta_id",
            when(col("codigo_estado_cuenta_id") == 1, "Cuenta Nueva")
            .when(col("codigo_estado_cuenta_id") == 2, "Cuenta cancelada por el cliente")
            .when(col("codigo_estado_cuenta_id") == 3, "Cuenta cancelada por el Acreedor")
            .when(col("codigo_estado_cuenta_id") == 4, "Cuenta en Mora")
            .when(col("codigo_estado_cuenta_id") == 5, "Cuenta en Cobro judicial")
            .when(col("codigo_estado_cuenta_id") == 6, "Cuenta Incobrable")
            .when(col("codigo_estado_cuenta_id") == 7, "Cuenta con arreglo de pago")
            .when(col("codigo_estado_cuenta_id") == 8, "Cuenta en Cobro Administrativo")
            .when(col("codigo_estado_cuenta_id") == 9, "Cuenta en estudio")
            .when(col("codigo_estado_cuenta_id") == 10, "Cuenta al día")
            .when(col("codigo_estado_cuenta_id") == 11, "Cancelado con atraso")
            .when(col("codigo_estado_cuenta_id") == 12, "Cobro especial")
            .when(col("codigo_estado_cuenta_id") == 13, "Cartera Separada")
            .when(col("codigo_estado_cuenta_id") == 14, "Cancelado")
            .when(col("codigo_estado_cuenta_id") == 15, "Excluída")
            .when(col("codigo_estado_cuenta_id") == 16, "No indica")
            .when(col("codigo_estado_cuenta_id") == 17, "Deuda declarada prescrita o incobrable por el juzgado de cobro de Heredia el 26 de setiembre del 2017")
            .when(col("codigo_estado_cuenta_id") == 18, "Cuenta con Problemas")
            .when(col("codigo_estado_cuenta_id") == 19, "Cuenta de Baja")
            .when(col("codigo_estado_cuenta_id") == 20, "Cuenta Normal")
            .when(col("codigo_estado_cuenta_id") == 21, "Cuenta en Pre-Cobro Judicial")
            .when(col("codigo_estado_cuenta_id") == 22, "Cuenta Vencida")
            .when(col("codigo_estado_cuenta_id") == 23, "Cobro Pre-Legal")
            .when(col("codigo_estado_cuenta_id") == 24, "Cancelado En Arreglo Extrajudicial")
            .when(col("codigo_estado_cuenta_id") == 25, "Traslado a Cobro")
            .when(col("codigo_estado_cuenta_id") == 26, "Crédito cerrado")
            .when(col("codigo_estado_cuenta_id") == 27, "Cobro Legal")
            .when(col("codigo_estado_cuenta_id") == 28, "Cobro Ordinario")
            .when(col("codigo_estado_cuenta_id") == 29, "Rechazado")
            .when(col("codigo_estado_cuenta_id") == 30, "Aplicación Parcial")
            .when(col("codigo_estado_cuenta_id") == 31, "Activo")
            .when(col("codigo_estado_cuenta_id") == 32, "Reestructuracion")
            .when(col("codigo_estado_cuenta_id") == 33, "Desembolsado")
            .otherwise("Cuenta en Mora").alias("EstadoOperacion"),
            )
    except Exception as e:
        print(f"\n❌ Error en CodigoEstado: {str(e)}")

def agregar_tipo_deudor(df: DataFrame) -> DataFrame:
    try:
        """Agrega la columna TipoDeudor con descripciones"""
        return df.withColumn(
            "tipo_deudor_id",
            when(col("tipo_deudor_id") == 1, "Principal")
            .when(col("tipo_deudor_id") == 2, "Codeudor")
            .when(col("tipo_deudor_id") == 3, "Fiador")
            .when(col("tipo_deudor_id") == 4, "Fideicomitente")
            .otherwise(None)
        )
    except Exception as e:
        print(f"\n❌ Error en TipoDeudor: {str(e)}")

def agregar_sector_credito(df: DataFrame) -> DataFrame:
    try:
        """Agrega la columna SectorCredito con descripciones"""
        return df.withColumn(
            "tipo_informacion_id",
            when(col("tipo_informacion_id") == 1, "Banco Estatal")
            .when(col("tipo_informacion_id") == 2, "Banca Privada")
            .when(col("tipo_informacion_id") == 3, "Financiera Regulada")
            .when(col("tipo_informacion_id") == 4, "Financiera No Regulada")
            .when(col("tipo_informacion_id") == 5, "Cooperativa")
            .when(col("tipo_informacion_id") == 6, "Mutual")
            .when(col("tipo_informacion_id") == 7, "Empresa Administradora de Tarjetas de Crédito")
            .when(col("tipo_informacion_id") == 8, "Venta de Catalogos")
            .when(col("tipo_informacion_id") == 9, "Distribuidora de Electródomesticos")
            .when(col("tipo_informacion_id") == 10, "Industria")
            .when(col("tipo_informacion_id") == 11, "Construcción")
            .when(col("tipo_informacion_id") == 12, "Telecomunicaciones")
            .when(col("tipo_informacion_id") == 13, "Servicios Públicos")
            .when(col("tipo_informacion_id") == 14, "IMF")
            .when(col("tipo_informacion_id") == 15, "Agencias de Viaje")
            .when(col("tipo_informacion_id") == 16, "Distribuidoras de Vehículos")
            .when(col("tipo_informacion_id") == 17, "Comercio")
            .when(col("tipo_informacion_id") == 18, "Otros")
            .when(col("tipo_informacion_id") == 19, "Colegios Profesionales")
            .when(col("tipo_informacion_id") == 20, "Municipalidades")
            .when(col("tipo_informacion_id") == 21, "Tienda por departamentos")
            .when(col("tipo_informacion_id") == 22, "Cooperativas de Ahorro y Crédito")
            .when(col("tipo_informacion_id") == 23, "Título personal")
            .when(col("tipo_informacion_id") == 24, "Universidades")
            .when(col("tipo_informacion_id") == 25, "Asociación Solidarista")
            .when(col("tipo_informacion_id") == 26, "Laboratorio Dental")
            .when(col("tipo_informacion_id") == 27, "Servicios de seguridad privada")
            .when(col("tipo_informacion_id") == 28, "Farmacia")
            .when(col("tipo_informacion_id") == 29, "Gobierno")
            .when(col("tipo_informacion_id") == 30, "Aseguradora")
            .otherwise(None).alias("SectorCredito"),
        )
    except Exception as e:
        print(f"\n❌ Error en SectorCredito: {str(e)}")

def agregar_campos_financieros(df: DataFrame) -> DataFrame:
    try:    
        """Agrega campos relacionados con saldos y categorías"""
                
        # Saldos por tipo de moneda
        df = df.withColumn("SaldoLocalColones", when(col("tipo_moneda_id") == 1, col("saldo_mora")))
        df = df.withColumn("SaldoLocalDolares", when(col("tipo_moneda_id") == 2, col("saldo_mora")))
        
        # Categorías SUGEFF - Separar en operaciones individuales
        df = df.withColumn(
            "Cat_Sugef_Colones",
            when(
                (col("tipo_moneda_id") == 1) &
                (col("dias_mora") <= 90) & 
                (col("codigo_estado_cuenta_id") != 5), 2)
            .when(
                (col("tipo_moneda_id") == 1) &
                (col("dias_mora") > 90) & 
                (col("dias_mora") <= 180) & 
                (col("codigo_estado_cuenta_id") != 5), 3)
            .when(
                (col("tipo_moneda_id") == 1) &
                (col("dias_mora") > 180) & 
                (col("codigo_estado_cuenta_id") != 5), 4)
            .when(
                (col("tipo_moneda_id") == 1) &
                (col("codigo_estado_cuenta_id") == 5), 5)
            .otherwise(0)
        )
        
        df = df.withColumn("Dias_Atraso_Dolares", 
            when(col("tipo_moneda_id") == 2, col("dias_mora")))
        
        df = df.withColumn(
            "Cat_Sugef_Dolares",
            when(
                (col("tipo_moneda_id") == 2) &
                (col("dias_mora") <= 90) & 
                (col("codigo_estado_cuenta_id") != 5), 2)
            .when(
                (col("tipo_moneda_id") == 2) &
                (col("dias_mora") > 90) & 
                (col("dias_mora") <= 180) & 
                (col("codigo_estado_cuenta_id") != 5), 3)
            .when(
                (col("tipo_moneda_id") == 2) &
                (col("dias_mora") > 180) & 
                (col("codigo_estado_cuenta_id") != 5), 4)
            .when(
                (col("tipo_moneda_id") == 2) &
                (col("codigo_estado_cuenta_id") == 5), 5)
            .otherwise(0)
        )
        
        return df
    
    except Exception as e:
        print(f"\n❌ Error en Saldos y Categorias: {str(e)}")

def formatear_fechas(df: DataFrame) -> DataFrame:
    try:
        """Formatea las columnas de fecha"""
        df = df.withColumn("FechaOtorgado", date_format(col("fecha_otorgamiento_credito"), "dd/MM/yyyy"))
        df = df.withColumn("fecha_informacion", date_format(col("fecha_informacion"), "dd/MM/yyyy"))
        df = df.withColumn("Fecha_Ultimo_Pago", date_format(col("fecha_ultimo_pago"), "dd/MM/yyyy"))
        return df
    except Exception as e:
        print(f"\n❌ Error en Formateo de fechas: {str(e)}")

def obtener_datos_historicos(df: DataFrame, jdbc_url: str, props: dict) -> DataFrame:

    spark = get_spark_session()
    
    try:
        

        # Obtener valores únicos de parámetros
        start_time_total= time.time()

        parametros = df.select("identificacion", "Id_referencia").distinct().collect()
        timeCollect= time.time() - start_time_total
        print(f"⏱ Tiempo de recolección de parámetros: {timeCollect:.2f} segundos")

        results = []
        for row in parametros:
            identificacion = row["identificacion"]
            fuente_id = row["Id_referencia"]
            
            # Usar la implementación Spark en lugar de llamar a la función SQL
            df_temp = fnc_obtener_campo_historico(
                spark,
                df,  # DataFrame completo de referencias
                identificacion,
                fuente_id
            )

            results.append(df_temp)

        # Combinar todos los resultados
        if not results:
            return spark.createDataFrame([], StructType([
                StructField("Id", IntegerType()),
                StructField("CedulaReferencia", StringType()),
                StructField("Historico", StringType()),
                StructField("HistoricoMes", StringType())
            ]))
                
        return reduce(lambda a, b: a.unionByName(b), results)

    except Exception as e:
        print(f"\n❌ Error en obtener_datos_historicos: {str(e)}")
        raise

def renombrarColumnas(df: DataFrame) -> DataFrame:
    try:
        df = df.withColumnRenamed("Id", "Num_Referencia") \
                    .withColumnRenamed("Identificacion", "NumCedula_Cliente") \
                    .withColumnRenamed("Cliente", "Entidad") \
                    .withColumnRenamed("tipo_credito_id", "TipoOperacion") \
                    .withColumnRenamed("codigo_estado_cuenta_id", "EstadoOperacion") \
                    .withColumnRenamed("tipo_deudor_id", "Tipo_Deudor") \
                    .withColumnRenamed("tipo_informacion_id", "Sector_Credito") \
                    .withColumnRenamed("fecha_otorgamiento_credito", "Fecha_Otorgado") \
                    .withColumnRenamed("fecha_vencimiento", "Fecha_Vencimiento") \
                    .withColumnRenamed("saldo_mora", "Principal") \
                    .withColumnRenamed("tipo_moneda_id", "TipoMoneda") \
                    .withColumnRenamed("cuotas_vencidas", "CuotasVencidas") \
                    .withColumnRenamed("fecha_informacion", "Fec_Actualizacion") \
                    .withColumnRenamed("fecha_ultimo_pago", "Fecha_Ultimo_Pago") \
                    .withColumnRenamed("dias_mora", "DiasAtraso") \
                    .withColumnRenamed("FechaHistorico", "FechaHistorico") \
                    .withColumnRenamed("Historico", "Historico") \
                    .withColumnRenamed("HistoricoMes", "Historico_Mes")
        
        return df
    except Exception as e:
        print(f"\n❌ Error en Renombrando: {str(e)}")

def OrdenarColumnas(df: DataFrame) -> DataFrame:
    try:
        df = df.alias("d").select("Num_Referencia",
                       "NumCedula_Cliente",
                       "Entidad",
                       "TipoOperacion",
                       "EstadoOperacion",
                       col("Tipo_Deudor").alias("TipoDeudor"),
                       lit(None).cast("string").alias("TipoGarantia"),
                       col("Sector_Credito").alias("SectorCredito"),
                       col("Fecha_Otorgado").alias("FechaOtorgado"),
                       col("Fecha_Vencimiento").alias("FechaVencimiento"),
                       "Principal",
                       "SaldoLocalColones",
                       "SaldoLocalDolares",
                       lit(None).cast("double").alias("SaldoMoraColones"),
                       lit(None).cast("double").alias("SaldoMoraDolares"),
                       col("CuotasVencidas").alias("Cuota"),
                       "DiasAtraso",
                       "CuotasVencidas",
                       "Historico",
                       "Fec_Actualizacion",
                       col("Tipo_Deudor").alias("Responsabilidad"),
                       "Fecha_Ultimo_Pago",
                       "Cat_Sugef_Colones",
                       "Dias_Atraso_Dolares",
                       "Cat_Sugef_Dolares",
                       col("Num_Referencia").alias("Codigo_Unico"),
                       "Historico_Mes"
                       ) 
                    
        return df
    except Exception as e:
        print(f"\n❌ Error en Ordenado Columnas: {str(e)}")

def transformar_dataframe(df: DataFrame, jdbc_url: str, props: dict) -> DataFrame:

    try:
        """Aplica todas las transformaciones al DataFrame"""
        #df = obtener_datos_historicos(df,jdbc_url,props)
        df = procesamiento_historico_masivo(df)
        df = agregar_tipo_credito(df)
        df = agregar_estado_operacion(df)
        df = agregar_tipo_deudor(df)
        df = agregar_sector_credito(df)
        df = agregar_campos_financieros(df)
        df = formatear_fechas(df)
        df = renombrarColumnas(df)
        df = OrdenarColumnas(df)
        #df.show()

        return df
        
    except Exception as e:
        print(f"\n❌ Error en Transformado: {str(e)}")
       
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
        
        # Reparticionar para mejorar el rendimiento
        df_filtrado = df_filtrado.repartition(200, "Identificacion", "Id_referencia")
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


def procesamiento_historico_masivo(df_referencias):
    try:
        # 1. Filtrar solo registros con mora (1 paso para todos los datos)
        df_filtrado = df_referencias.filter(F.col("dias_mora") >= 1)
        
        # 2. Filtrar últimos 4 años (aplicado a todo el dataset)
        df_filtrado = df_filtrado.filter(
            F.col("fecha_informacion") >= F.add_months(F.current_date(), -48)
        )
        
        # 3. Obtener fecha máxima por Identificacion/Fuente (en una sola operación)
        window_spec = Window.partitionBy("Identificacion", "Id_referencia")

        df_filtrado = df_filtrado.withColumn(
            "max_fecha", 
            F.max("fecha_informacion").over(window_spec)
        )
        
        # 4. Calcular fecha_inicio (23 meses antes de max_fecha)
        df_filtrado = df_filtrado.withColumn(
            "fecha_inicio",
            F.expr("add_months(max_fecha, -23)")
        )
        
        # 5. Generar secuencia de meses para cada grupo (usando join lateral)
        df_meses = df_filtrado.select(
            "Identificacion", 
            "Id_referencia",
            F.expr("""
                explode(sequence(
                    date_trunc('month', fecha_inicio), 
                    date_trunc('month', max_fecha), 
                    interval 1 month
                )) as FechaHistorico
            """)
        ).distinct()
        
        # 6. Agregación por periodo (todos los grupos a la vez)
        df_proceso = df_filtrado.groupBy(
            "Identificacion",
            "Id_referencia",
            F.date_format("fecha_informacion", "yyyyMM").alias("periodo"),
            F.date_format("fecha_informacion", "MMM-yy").alias("mes_str")
        ).agg(
            F.first("fecha_informacion").alias("fecha"),
            F.sum("dias_mora").alias("dias_mora_total")
        )

        
        
        # 7. Calificación (aplicada a todos los registros)
        df_calificado = df_proceso.withColumn(
            "calificacion",
            F.when(F.col("dias_mora_total").between(1, 30), 1)
             .when(F.col("dias_mora_total").between(31, 60), 2)
             .when(F.col("dias_mora_total").between(61, 90), 3)
             .when(F.col("dias_mora_total").between(91, 120), 4)
             .when(F.col("dias_mora_total").between(121, 150), 5)
             .otherwise(6)
        )
        

        # 8. Join con meses (todos los grupos)
        df_completo = df_meses.join(
        df_calificado,
        (df_meses["Identificacion"] == df_calificado["Identificacion"]) &
        (df_meses["Id_referencia"] == df_calificado["Id_referencia"]) &
        (F.date_format(df_meses["FechaHistorico"], "yyyyMM") == df_calificado["periodo"]),
        "left_outer"
        )
        
        # 9. Ventana por grupo para construir histórico
        window_historico = Window.partitionBy("Identificacion", "Id_referencia").orderBy("FechaHistorico")
        
        print(df_completo.printSchema())
        print("Procesamiento de datos completado, ahora calculando calificaciones...")
        exit(0)

        df_resultado = df_completo.groupBy("Identificacion", "Id_referencia", "FechaHistorico").agg(
            F.first("calificacion").alias("calificacion"),
            F.first("mes_str").alias("mes_str")
        ).withColumn(
            "Historico",
            F.array_join(
                F.collect_list(
                    F.coalesce(F.col("calificacion").cast("string"), F.lit("X"))
                ).over(window_historico),
                "-"
            )
        ).withColumn(
            "HistoricoMes",
            F.array_join(
                F.collect_list(
                    F.coalesce(F.col("mes_str"), F.date_format("FechaHistorico", "MMM-yy"))
                ).over(window_historico),
                "/"
            )
        ).filter(
            F.col("FechaHistorico") == F.col("max_fecha")
        )
        
        

        # 10. Join final con datos originales
        df_final = df_referencias.join(
            df_resultado,
            ["Identificacion", "Id_referencia"],
            "left"
        ).select(
            'Id', 'tipo_credito_id', 'codigo_estado_cuenta_id', 'Identificacion',
            'tipo_deudor_id', 'tipo_informacion_id', 'fecha_otorgamiento_credito',
            'fecha_vencimiento', 'saldo_mora', 'tipo_moneda_id', 'cuotas_vencidas',
            'fecha_informacion', 'fecha_ultimo_pago', 'dias_mora', 'Cliente',
            'FechaHistorico', 'Historico', 'HistoricoMes'
        )

        
        
        return df_final
        
    except Exception as e:
        print(f"\n❌ Error en procesamiento masivo: {str(e)}")
        raise
    
    
   