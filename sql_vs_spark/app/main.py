import time
import streamlit as st
import pandas as pd
from sql_utils import get_sql_connection, execute_sql_query
from spark_utils import get_spark_session, execute_spark_query

# Configuración de la página
st.set_page_config(page_title="SQL vs Spark Benchmark", layout="wide")
st.title("🚀 Comparación de Rendimiento: SQL Server vs Spark")

# Sidebar - Configuración de conexión
st.sidebar.header("🔧 Configuración de Conexión")
server = st.sidebar.text_input("Servidor SQL", "192.168.5.136")
database = st.sidebar.text_input("Base de datos", "ReferenciasComerciales")
username = st.sidebar.text_input("Usuario", "Adrian.Araya")
password = st.sidebar.text_input("Contraseña", "Soporte1990%", type="password")

# Consulta SQL de ejemplo
default_query = """SELECT TOP (100000) 
      [Identificacion],
      [Nombre]
FROM [ReferenciasComerciales].[dbo].[ReferenciasCCSS]"""

query = st.text_area("📝 Consulta SQL", default_query, height=150)

# Opción para habilitar/deshabilitar Spark
use_spark = st.sidebar.checkbox("Habilitar comparación con Spark", value=True)

# Sección de ejecución
if st.button("⚡ Ejecutar Comparación", type="primary"):
    if not query:
        st.error("⚠️ Por favor ingrese una consulta SQL válida")
    
    else:
        # Contenedores para resultados
        sql_col, spark_col = st.columns(2)
        
        try:
            # Conexión y ejecución SQL
            with sql_col:
                st.subheader("🔵 SQL Server")
                with st.spinner("Conectando a SQL Server..."):
                    conn = get_sql_connection(server=server, database=database, username=username, password=password)
                
                with st.spinner("Ejecutando consulta SQL..."):
                    sql_df, sql_time = execute_sql_query(conn, query)
                
                st.success(f"✅ Tiempo SQL: {sql_time:.2f} segundos")
                st.metric("Registros recuperados", f"{len(sql_df):,}")
                
                # Mostrar resultados
                st.dataframe(sql_df.head(), use_container_width=True)
                
                # Opción para descargar resultados
                csv = sql_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "📤 Descargar resultados SQL",
                    csv,
                    "resultados_sql.csv",
                    "text/csv"
                )
            
            # Conexión y ejecución Spark (si está habilitado)
            if use_spark:
                with spark_col:
                    st.subheader("🟠 Apache Spark")
                    with st.spinner("Inicializando Spark..."):
                        spark = get_spark_session()
                    
                    with st.spinner("Ejecutando consulta en Spark..."):
                        spark_df, spark_time = execute_spark_query(spark, server="192.168.5.136", port="18698", database="ReferenciasComerciales", 
                                                                   username="Adrian.Araya", password="Soporte1990%", query=query)
                    
                    st.success(f"✅ Tiempo Spark: {spark_time:.2f} segundos")
                    st.metric("Registros procesados", f"{spark_df.count():,}")
                    
                    # Mostrar resultados
                    st.dataframe(spark_df.limit(5).toPandas(), use_container_width=True)
            
            # Sección de comparación
            st.subheader("📊 Comparación de Rendimiento")
            
            if use_spark:
                comparison_data = {
                    'Sistema': ['SQL Server', 'Apache Spark'],
                    'Tiempo (segundos)': [sql_time, spark_time],
                    'Registros': [len(sql_df), spark_df.count()]
                }
                
                col1, col2 = st.columns(2)
                with col1:
                    st.bar_chart(data=comparison_data, x='Sistema', y='Tiempo (segundos)')
                with col2:
                    st.write(pd.DataFrame(comparison_data))
                
                ratio = sql_time / spark_time
                st.metric(
                    "Relación de rendimiento (Spark/SQL)", 
                    f"{ratio:.2f}x más {'rápido' if ratio > 1 else 'lento'}"
                )
            else:
                st.info("Comparación con Spark deshabilitada en la configuración")
        
        except Exception as e:
            st.error(f"❌ Error durante la ejecución: {str(e)}")
            st.exception(e)
        
        finally:
            # Limpieza de recursos
            if 'conn' in locals():
                conn.close()
            if use_spark and 'spark' in locals():
                spark.stop()
                st.cache_resource.clear()

# Sección informativa
st.sidebar.markdown("""
### ℹ️ Información
Esta aplicación compara el rendimiento de:
- **SQL Server**: Ejecución directa de consultas
- **Apache Spark**: Procesamiento distribuido

**Configuración recomendada:**
- Servidor: `localhost` o nombre del contenedor SQL
- Base de datos: `performance_db` (creada automáticamente)
""")

# Notas adicionales
st.markdown("""
---
### 📝 Notas:
1. La primera conexión puede ser más lenta debido a la inicialización
2. Para consultas complejas, Spark puede mostrar mejor rendimiento
3. Los tiempos incluyen transferencia de datos
""")