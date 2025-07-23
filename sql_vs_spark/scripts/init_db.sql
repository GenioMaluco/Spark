USE master;
GO

-- Crear base de datos de prueba
CREATE DATABASE performance_db;
GO

USE performance_db;
GO

-- Crear tabla con 10 millones de registros
CREATE TABLE large_table (
    id INT IDENTITY(1,1) PRIMARY KEY,
    random_data FLOAT,
    timestamp DATETIME DEFAULT GETDATE(),
    description VARCHAR(255)
);
GO

-- Insertar datos de prueba (ejemplo simplificado)
-- En la práctica usarías un procedimiento para generar 10M registros
INSERT INTO large_table (random_data, description)
SELECT 
    RAND() * 1000,
    'Descripción ' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS VARCHAR)
FROM sys.objects a
CROSS JOIN sys.objects b
CROSS JOIN sys.objects c;
GO

-- Crear índices para mejorar rendimiento
CREATE INDEX idx_random_data ON large_table(random_data);
CREATE INDEX idx_timestamp ON large_table(timestamp);
GO