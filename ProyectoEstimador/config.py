class Config:
    # Configuración de conexión JDBC
    JDBC_URL_ESTIMADOR = "jdbc:sqlserver://192.168.5.136:18698;databaseName=ReferenciasComerciales"
    JDBC_URL_HISTORICO = "jdbc:sqlserver://192.168.5.136:18698;databaseName=DatosDavivienda"
    
    # Credenciales de base de datos
    DB_PROPERTIES = {
        "user": "Adrian.Araya",
        "password": "Soporte1990%",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "encrypt": "true", 
        "trustServerCertificate": "true"
    }
    
    # Configuración de tablas
    TABLA_REFERENCIAS = "dbo.DatoReferencia"
    TABLA_CLIENTES = "dbo.ClienteFuente"
    TABLA_DESTINO = "SPK.CLI_REFERENCIASCREDITICIAS_Backup"
    
    # Configuración de fechas
    MESES_HISTORICO =24