import pyodbc
from functools import wraps
import time
import streamlit as st

def handle_connection_errors(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        max_retries = 3
        delay = 1
        
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except pyodbc.Error as e:
                error_msg = str(e)
                st.warning(f"Intento {attempt + 1} de {max_retries}: {error_msg}")
                
                if "SSPI" in error_msg and attempt == 0:
                    st.info("Intentando con autenticación alternativa...")
                    kwargs['use_fallback'] = True
                elif attempt == max_retries - 1:
                    st.error("Error de conexión persistente. Verifica la configuración del servidor.")
                    raise
                else:
                    time.sleep(delay * (attempt + 1))
    return wrapper

@handle_connection_errors
def get_sql_connection(server, database, use_fallback=False):
    if use_fallback:
        # Autenticación por usuario/contraseña (fallback)
        connection_string = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server};"
            f"DATABASE={database};"
            "UID=sa;"
            "PWD=YourStrong!Passw0rd;"
            "TrustServerCertificate=yes;"
            "Connection Timeout=15;"
        )
    else:
        # Tu conexión original con SSPI
        connection_string = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server};"
            f"DATABASE={database};"
            "TrustServerCertificate=yes;"
            "Integrated Security=SSPI;"
            "Connection Timeout=15;"
        )
    
    try:
        conn = pyodbc.connect(connection_string)
        st.success("Conexión establecida correctamente")
        return conn
    except pyodbc.Error as e:
        st.error(f"Error de conexión: {e}")
        raise

def execute_sql_query(conn, query):
    start_time = time.time()
    try:
        df = pd.read_sql(query, conn)
        execution_time = time.time() - start_time
        return df, execution_time
    except Exception as e:
        st.error(f"Error al ejecutar consulta: {e}")
        raise