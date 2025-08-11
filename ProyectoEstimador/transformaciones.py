
from pyspark.sql.functions import col, when, date_format, lit
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


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
                       col("SaldoLocalColones").alias("SaldoMoraColones"),
                       col("SaldoLocalDolares").alias("SaldoMoraDolares"),
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
       
def procesamiento_historico_masivo(df_referencias):

    try:
        df_filtrado = df_referencias.filter(
            F.col("fecha_informacion") >= F.add_months(F.current_date(), -24)
        )
        

        # 2. Obtener fecha máxima por Identificacion/Id_referencia
        window_spec = Window.partitionBy("Identificacion", "Id_referencia")
        
        df_con_max = df_filtrado.withColumn(
            "max_fecha", 
            F.max("fecha_informacion").over(window_spec)
        )
        
        # 3. Calcular fecha_inicio (23 meses antes de max_fecha para tener 24 meses total)
        df_con_fechas = df_con_max.withColumn(
            "fecha_inicio",
            F.expr("add_months(max_fecha, -24)")  # CAMBIO: -23 en lugar de -48
        )
        
        # 4. Obtener todas las combinaciones únicas de Identificacion/Id_referencia que tienen datos
        df_combinaciones = df_con_fechas.select(
            "Identificacion", 
            "Id_referencia",
            "fecha_inicio",
            "max_fecha"
        ).distinct()

        # 5. Generar secuencia completa de 24 meses para cada combinación
        df_meses_completo = df_combinaciones.select(
            "Identificacion", 
            "Id_referencia",
            "max_fecha",
            F.expr("""
                explode(sequence(
                    date_trunc('month', fecha_inicio), 
                    date_trunc('month', max_fecha), 
                    interval 1 month
                )) as FechaHistorico
            """)
        )

        # 6. Procesar datos reales - agregación por periodo
        df_proceso = df_filtrado.groupBy(
            "Identificacion",
            "Id_referencia",
            F.date_format("fecha_informacion", "yyyyMM").alias("periodo")
        ).agg(
            F.first("fecha_informacion").alias("fecha_original"),
            F.sum("dias_mora").alias("dias_mora_total"),  # CAMBIO: usar suma total
            F.first(F.date_format("fecha_informacion", "MMM-yy")).alias("mes_str_original")
        )
        
        # 7. Aplicar calificación a datos reales
        df_calificado = df_proceso.withColumn(
            "calificacion",
            F.when(F.col("dias_mora_total") == 0, 0)           # CAMBIO: usar dias_mora_total
             .when(F.col("dias_mora_total").between(1, 30), 1)
             .when(F.col("dias_mora_total").between(31, 60), 2)
             .when(F.col("dias_mora_total").between(61, 90), 3)
             .when(F.col("dias_mora_total").between(91, 120), 4)
             .when(F.col("dias_mora_total").between(121, 150), 5)
             .when(F.col("dias_mora_total") > 150, 6)
             .otherwise(6)
        )

        # 8. JOIN: Combinar todos los meses con datos reales (LEFT JOIN)
        df_historico_completo = df_meses_completo.alias("meses").join(
            df_calificado.alias("datos"),
            (F.col("meses.Identificacion") == F.col("datos.Identificacion")) &
            (F.col("meses.Id_referencia") == F.col("datos.Id_referencia")) &
            (F.date_format(F.col("meses.FechaHistorico"), "yyyyMM") == F.col("datos.periodo")),
            "left"
        )
        
        # 9. Preparar datos para el histórico con X para meses sin datos
        df_preparado = df_historico_completo.select(
            F.col("meses.Identificacion").alias("Identificacion"),
            F.col("meses.Id_referencia").alias("Id_referencia"), 
            F.col("meses.FechaHistorico").alias("FechaHistorico"),
            F.col("meses.max_fecha").alias("max_fecha"),
            F.coalesce(F.col("datos.calificacion").cast("string"), F.lit("X")).alias("calificacion_str"),
            F.coalesce(
                F.col("datos.mes_str_original"), 
                F.date_format(F.col("meses.FechaHistorico"), "MMM-yy")
            ).alias("mes_str"),
            F.col("datos.dias_mora_total").alias("dias_mora_total"),
            F.col("datos.fecha_original").alias("fecha_original")
        )
        # 10. Crear ventana ordenada por fecha para construir histórico
        #window_historico = Window.partitionBy("Identificacion", "Id_referencia").orderBy("FechaHistorico")

        # 11. Construir el histórico usando solo groupBy y collect_list
        # Primero crear un struct para mantener el orden
        df_con_orden = df_preparado.withColumn(
            "fecha_orden", F.col("FechaHistorico")
        ).withColumn(
            "calificacion_con_fecha", F.struct(F.col("fecha_orden"), F.col("calificacion_str"))
        ).withColumn(
            "mes_con_fecha", F.struct(F.col("fecha_orden"), F.col("mes_str"))
        )

        # 12. Agrupar y construir histórico ordenado
        df_historico_completo = df_con_orden.groupBy("Identificacion", "Id_referencia").agg(
            F.max("max_fecha").alias("max_fecha"),
            F.array_join(
                F.expr("transform(array_sort(collect_list(calificacion_con_fecha)), x -> x.calificacion_str)"),
                "-"
            ).alias("Historico"),
            F.array_join(
                F.expr("transform(array_sort(collect_list(mes_con_fecha)), x -> x.mes_str)"),
                "/"
            ).alias("HistoricoMes")
        )
                
        # 13. Calcular totales por Identificacion/Id_referencia
        df_totales = df_calificado.groupBy("Identificacion", "Id_referencia").agg(
            F.sum("dias_mora_total").alias("DiasMoraTotal"),
            F.max("fecha_original").alias("fecha_informacion_mas_reciente")
        )
        
        # 14. Combinar con totales
        df_resultado_final = df_historico_completo.join(
            df_totales,
            ["Identificacion", "Id_referencia"],
            "INNER"
        ).select(
            "Identificacion",
            "Id_referencia", 
            F.coalesce("fecha_informacion_mas_reciente", "max_fecha").alias("fecha_informacion_mas_reciente"),
            F.coalesce("DiasMoraTotal", F.lit(0)).alias("DiasMoraTotal"),
            "Historico",
            "HistoricoMes"
        )

        # 15. JOIN final con datos originales
        df_final = df_referencias.join(
            df_resultado_final,
            ["Identificacion", "Id_referencia"],
            "LEFT"
        ).filter(
            F.col("fecha_informacion") == F.col("fecha_informacion_mas_reciente")
        ).select(
            'Id', 'tipo_credito_id', 'codigo_estado_cuenta_id', 'Identificacion',
            'Id_referencia', 'tipo_deudor_id', 'tipo_informacion_id', 
            'fecha_otorgamiento_credito', 'fecha_vencimiento', 'saldo_mora', 
            'tipo_moneda_id', 'cuotas_vencidas', 'fecha_informacion', 
            'fecha_ultimo_pago', 'dias_mora', 'Cliente',
            'fecha_informacion_mas_reciente', 'DiasMoraTotal', 'Historico', 'HistoricoMes'
        )
        
        return df_final
        
    except Exception as e:
        print(f"\n❌ Error en procesamiento masivo: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

def transformar_dataframe(df: DataFrame, jdbc_url: str, props: dict) -> DataFrame:

    try:
        """Aplica todas las transformaciones al DataFrame"""
        df = procesamiento_historico_masivo(df)
        df = agregar_tipo_credito(df)
        df = agregar_tipo_deudor(df)
        df = agregar_sector_credito(df)
        df = agregar_campos_financieros(df)
        df = agregar_estado_operacion(df)
        df = formatear_fechas(df)
        df = renombrarColumnas(df)
        df = OrdenarColumnas(df)

        return df
        
    except Exception as e:
        print(f"\n❌ Error en Transformado: {str(e)}")