from functools import reduce
from pyspark.sql.functions import col, when, date_format, lit, udf, explode
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType, StructField, StructType,IntegerType,StringType
from spark_utils import get_spark_session
from historico import fnc_obtener_campo_historico


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
            "TipoDeudor",
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
            "SectorCredito",
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
        
        parametros = df.select("identificacion", "Id_referencia").distinct().collect()

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
                       "Tipo_Deudor",
                       #AQUI DEBE IR LA GARANTIA VACIA
                       "Sector_Credito",
                       "Fecha_Otorgado",
                       "Fecha_Vencimiento",
                       "Principal",
                       "SaldoLocalColones",
                       "SaldoLocalDolares",
                       "DiasAtraso",
                       "CuotasVencidas",
                       "Historico",
                       "Fec_Actualizacion",
                       "Fecha_Ultimo_Pago",
                       "Cat_Sugef_Colones",
                       "Dias_Atraso_Dolares",
                       "Cat_Sugef_Dolares",
                       "Num_Referencia",
                       "Historico_Mes"
                       ) 
                    
        return df
    except Exception as e:
        print(f"\n❌ Error en Ordenado Columnas: {str(e)}")

def transformar_dataframe(df: DataFrame, jdbc_url: str, props: dict) -> DataFrame:
    try:
        """Aplica todas las transformaciones al DataFrame"""
        df = obtener_datos_historicos(df,jdbc_url,props)
        df = agregar_tipo_credito(df)
        df = agregar_estado_operacion(df)
        df = agregar_tipo_deudor(df)
        df = agregar_sector_credito(df)
        df = agregar_campos_financieros(df)
        df = formatear_fechas(df)
        df = renombrarColumnas(df)
        df = OrdenarColumnas(df)
        df.show()

        return df
        
    except Exception as e:
        print(f"\n❌ Error en Transformado: {str(e)}")
    
    


    
    
   