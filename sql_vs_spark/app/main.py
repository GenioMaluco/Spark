import streamlit as st
from sql_utils import ObtenerVentas

st.title("Reporte de Ventas")

# Conexión con autenticación Windows (SSPI)
df_ventas = ObtenerVentas()  # Usará SSPI por defecto

# O con autenticación SQL estándar
# df_ventas = ObtenerVentas(username='usuario_prueba', password='Admin1234!')

if not df_ventas.empty:
    st.dataframe(df_ventas)
else:
    st.error("No se pudieron obtener los datos de ventas")