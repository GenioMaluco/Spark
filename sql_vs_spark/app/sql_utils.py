import pyodbc as odbc
import pandas as pd
import time  # Importación faltante

def get_sql_connection(server='(localdb)\\Luffy', database='master', username=None, password=None):
    try:
        # Opción 1: Conexión directa a LocalDB
        connection_string = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server};"
            f"DATABASE={database};"
            "Trusted_Connection=yes;"
            "Connection Timeout=5;"
        )
        
        
        print(f"Intentando conectar con: {connection_string}")
        conn = odbc.connect(connection_string)
        print("✅ Conexión exitosa a LocalDB!")
        return conn
        
    except Exception as e:
        print(f"Error de conexión: {str(e)}")
        
        # Intento alternativo con conexión compartida
        try:
            shared_connection = f"""
                DRIVER={{ODBC Driver 17 for SQL Server}};
                SERVER=(localdb)\.\\Luffy;
                DATABASE={database};
                "Trusted_Connection=yes;"
            "Connection Timeout=5;"
            """
            print("Intentando con conexión compartida...")
            return odbc.connect(shared_connection)
        except Exception as fallback_error:
            print(f"Error en conexión alternativa: {str(fallback_error)}")
            raise RuntimeError(f"No se pudo conectar a SQL Server. Verifica: \n"
                           f"1. Que LocalDB esté instalado y funcionando\n"
                           f"2. Que el servicio 'SQL Server (Luffy)' esté ejecutándose\n"
                           f"3. Que el ODBC Driver 17 esté instalado")

def execute_sql_query(conn, query):
    try:
        start_time = time.time()
        df = pd.read_sql(query, conn)
        execution_time = time.time() - start_time
        return df, execution_time
            
    except Exception as e:
        print(f"Error en execute_sql_query: {str(e)}")
        return pd.DataFrame(), 0  # Devuelve DataFrame vacío y tiempo 0