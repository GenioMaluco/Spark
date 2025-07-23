import pyodbc

def get_sql_connection(server, database):
    connection_string = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "TrustServerCertificate=yes;"
        "Integrated Security=SSPI;"
        "Connection Timeout=15;"
    )
    try:
        return pyodbc.connect(connection_string)
    except pyodbc.Error as e:
        print(f"Error con SSPI: {e}")
        # Fallback a autenticación por usuario/contraseña
        connection_string = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server};"
            f"DATABASE={database};"
            "UID=sa;"
            f"PWD=YourStrong!Passw0rd;"
            "TrustServerCertificate=yes;"
        )
        return pyodbc.connect(connection_string)