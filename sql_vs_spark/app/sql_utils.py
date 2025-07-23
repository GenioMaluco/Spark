import pyodbc as odbc
import pandas as pd

def ObtenerVentas(server='(localdb)\\Luffy', database='StreamlitDemo', 
                 username=None, password=None):
    try:
        if username and password:
            # Autenticación SQL estándar
            cadena_conexion = (
                "DRIVER={ODBC Driver 17 for SQL Server};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"UID={username};"
                f"PWD={password};"
                "TrustServerCertificate=yes;"
                "Connection Timeout=15;"
            )
        else:
            # Autenticación Windows (SSPI)
            cadena_conexion = (
                "DRIVER={ODBC Driver 17 for SQL Server};"
                f"SERVER={server};"
                f"DATABASE={database};"
                "TrustServerCertificate=yes;"
                "Integrated Security=SSPI;"
                "Connection Timeout=15;"
            )
        
        with odbc.connect(cadena_conexion) as conexion:
            print("¡Conexión exitosa!")
            consulta = "SELECT * FROM Sales"
            return pd.read_sql(consulta, conexion)
            
    except Exception as e:
        print(f"Error: {str(e)}")
        return pd.DataFrame()