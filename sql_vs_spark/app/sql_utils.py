import pyodbc as odbc
import pandas as pd
import time  # Importación faltante

def get_sql_connection(server="192.168.5.136, 18698", database="ReferenciasComerciales", username="Adrian.Araya", password="Soporte1990%"):
    try:
        # Opción 1: Conexión directa a LocalDB
        connection_string = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"UID={username};"
            f"PWD={password};"
            f"SERVER={server};"
            f"DATABASE={database};"
            "Connection Timeout=30;"
        )
        
        
        print(f"Intentando conectar con: {connection_string}")
        conn = odbc.connect(connection_string)
        print("✅ Conexión exitosa a LocalDB!")
        return conn
        
    except Exception as e:
        print(f"Error de conexión: {str(e)}")
        print(f"aqui esta el error: {str(e)}")


def execute_sql_query(conn, query):
    try:
        start_time = time.time()
        df = pd.read_sql(query, conn)
        execution_time = time.time() - start_time
        return df, execution_time
            
    except Exception as e:
        print(f"Error en execute_sql_query: {str(e)}")
        return pd.DataFrame(), 0  # Devuelve DataFrame vacío y tiempo 0