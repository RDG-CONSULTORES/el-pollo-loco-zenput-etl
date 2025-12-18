-- INSERCIÓN DE DATOS MAESTROS - SUCURSALES
-- Generado: 2025-12-17 15:19:55

-- Limpiar datos existentes (opcional)
-- TRUNCATE TABLE sucursales_master RESTART IDENTITY CASCADE;

-- Insertar sucursales normalizadas
INSERT INTO sucursales_master 
    (sucursal_numero, nombre_actual, nombres_historicos, tipo_sucursal, ciudad, estado, activa) 
VALUES 
    (1, '1 - Pino Suarez', '{"1 - Pino Suarez"}', 'FORANEA', 'Por determinar', 'Nuevo León', True);
INSERT INTO sucursales_master 
    (sucursal_numero, nombre_actual, nombres_historicos, tipo_sucursal, ciudad, estado, activa) 
VALUES 
    (2, '2 - Madero', '{"2 - Madero"}', 'LOCAL', 'Por determinar', 'Nuevo León', True);
INSERT INTO sucursales_master 
    (sucursal_numero, nombre_actual, nombres_historicos, tipo_sucursal, ciudad, estado, activa) 
VALUES 
    (5, '5 - Felix U. Gomez', '{"5 - Felix U. Gomez"}', 'FORANEA', 'Por determinar', 'Nuevo León', True);
INSERT INTO sucursales_master 
    (sucursal_numero, nombre_actual, nombres_historicos, tipo_sucursal, ciudad, estado, activa) 
VALUES 
    (9, '9 - Anahuac', '{"9 - Anahuac"}', 'LOCAL', 'Por determinar', 'Nuevo León', True);
INSERT INTO sucursales_master 
    (sucursal_numero, nombre_actual, nombres_historicos, tipo_sucursal, ciudad, estado, activa) 
VALUES 
    (10, '10 - Barragan', '{"10 - Barragan"}', 'LOCAL', 'Por determinar', 'Nuevo León', True);
INSERT INTO sucursales_master 
    (sucursal_numero, nombre_actual, nombres_historicos, tipo_sucursal, ciudad, estado, activa) 
VALUES 
    (12, '12 - Concordia', '{"12 - Concordia"}', 'LOCAL', 'Por determinar', 'Nuevo León', True);
INSERT INTO sucursales_master 
    (sucursal_numero, nombre_actual, nombres_historicos, tipo_sucursal, ciudad, estado, activa) 
VALUES 
    (13, '13 - Escobedo', '{"13 - Escobedo"}', 'LOCAL', 'Monterrey (Área Metropolitana)', 'Nuevo León', True);
INSERT INTO sucursales_master 
    (sucursal_numero, nombre_actual, nombres_historicos, tipo_sucursal, ciudad, estado, activa) 
VALUES 
    (22, '22 - Satelite', '{"22 - Satelite"}', 'LOCAL', 'Por determinar', 'Nuevo León', True);
INSERT INTO sucursales_master 
    (sucursal_numero, nombre_actual, nombres_historicos, tipo_sucursal, ciudad, estado, activa) 
VALUES 
    (27, '27 - Santiago', '{"27 - Santiago"}', 'FORANEA', 'Por determinar', 'Nuevo León', True);
INSERT INTO sucursales_master 
    (sucursal_numero, nombre_actual, nombres_historicos, tipo_sucursal, ciudad, estado, activa) 
VALUES 
    (36, '36 - Apodaca Centro', '{"36 - Apodaca Centro"}', 'LOCAL', 'Monterrey (Área Metropolitana)', 'Nuevo León', True);
INSERT INTO sucursales_master 
    (sucursal_numero, nombre_actual, nombres_historicos, tipo_sucursal, ciudad, estado, activa) 
VALUES 
    (37, '37 - Stiva', '{"37 - Stiva"}', 'FORANEA', 'Por determinar', 'Nuevo León', True);
INSERT INTO sucursales_master 
    (sucursal_numero, nombre_actual, nombres_historicos, tipo_sucursal, ciudad, estado, activa) 
VALUES 
    (39, '39 - Lazaro Cardenas', '{"39 - Lazaro Cardenas"}', 'FORANEA', 'Por determinar', 'Nuevo León', True);
INSERT INTO sucursales_master 
    (sucursal_numero, nombre_actual, nombres_historicos, tipo_sucursal, ciudad, estado, activa) 
VALUES 
    (40, '40 - Plaza 1500', '{"40 - Plaza 1500"}', 'FORANEA', 'Por determinar', 'Nuevo León', True);
INSERT INTO sucursales_master 
    (sucursal_numero, nombre_actual, nombres_historicos, tipo_sucursal, ciudad, estado, activa) 
VALUES 
    (41, '41 - Vasconcelos', '{"41 - Vasconcelos"}', 'FORANEA', 'Por determinar', 'Nuevo León', True);
INSERT INTO sucursales_master 
    (sucursal_numero, nombre_actual, nombres_historicos, tipo_sucursal, ciudad, estado, activa) 
VALUES 
    (52, '52 - Venustiano Carranza', '{"52 - Venustiano Carranza"}', 'FORANEA', 'Por determinar', 'Nuevo León', True);
INSERT INTO sucursales_master 
    (sucursal_numero, nombre_actual, nombres_historicos, tipo_sucursal, ciudad, estado, activa) 
VALUES 
    (53, '53 - Lienzo Charro', '{"53 - Lienzo Charro"}', 'FORANEA', 'Por determinar', 'Nuevo León', True);
INSERT INTO sucursales_master 
    (sucursal_numero, nombre_actual, nombres_historicos, tipo_sucursal, ciudad, estado, activa) 
VALUES 
    (54, '54 - Ramos Arizpe', '{"54 - Ramos Arizpe"}', 'FORANEA', 'Por determinar', 'Nuevo León', True);
INSERT INTO sucursales_master 
    (sucursal_numero, nombre_actual, nombres_historicos, tipo_sucursal, ciudad, estado, activa) 
VALUES 
    (55, '55 - Eulalio Gutierrez', '{"55 - Eulalio Gutierrez"}', 'FORANEA', 'Por determinar', 'Nuevo León', True);
INSERT INTO sucursales_master 
    (sucursal_numero, nombre_actual, nombres_historicos, tipo_sucursal, ciudad, estado, activa) 
VALUES 
    (56, '56 - Luis Echeverria', '{"56 - Luis Echeverria"}', 'FORANEA', 'Por determinar', 'Nuevo León', True);
INSERT INTO sucursales_master 
    (sucursal_numero, nombre_actual, nombres_historicos, tipo_sucursal, ciudad, estado, activa) 
VALUES 
    (72, '72 - Sabinas Hidalgo', '{"72 - Sabinas Hidalgo"}', 'FORANEA', 'Sabinas Hidalgo', 'Nuevo León', True);

-- Estadísticas de inserción
-- Total sucursales: 20
-- Locales: 7
-- Foráneas: 13
