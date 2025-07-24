import pyodbc

def test_sql_connection(server='(localdb)\\Luffy', database='master', username=None, password=None):
    try:
        # Conexión para LocalDB
        connection_string = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server};"
            f"DATABASE={database};"
            "Trusted_Connection=yes;"
            "Connection Timeout=5;"
        )
        
        print(f"Intentando conectar con: {connection_string}")
        conn = pyodbc.connect(connection_string)
        print("✅ Conexión exitosa a LocalDB!")
        
        # Prueba simple
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sys.databases")
        print("Bases de datos disponibles:")
        for row in cursor:
            print(f"- {row[0]}")
            
        conn.close()
        return True
        
    except pyodbc.Error as e:
        print(f"❌ Error de conexión (LocalDB): {e}")
# Llamada correcta a la función (sin el prefijo 'prueba.')
if __name__ == "__main__":
    test_sql_connection()  # Así se llama correctamente a la función