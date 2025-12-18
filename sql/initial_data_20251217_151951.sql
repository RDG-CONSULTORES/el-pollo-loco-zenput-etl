
    -- ================================================
    -- DATOS INICIALES DE CONFIGURACIÓN
    -- ================================================
    
    -- Periodos oficiales 2026 (EJEMPLO - Roberto debe ajustar fechas)
    INSERT INTO periodos_supervision (periodo_codigo, año, tipo_sucursal, fecha_inicio, fecha_fin, fecha_limite_supervision, formularios_requeridos, descripcion) VALUES
    ('T1', 2026, 'LOCAL', '2026-01-01', '2026-03-31', '2026-04-05', ARRAY['877138', '877139'], 'Primer trimestre 2026 - Sucursales locales'),
    ('T1', 2026, 'FORANEA', '2026-01-01', '2026-03-31', '2026-04-05', ARRAY['877138', '877139'], 'Primer trimestre 2026 - Sucursales foráneas'),
    ('T2', 2026, 'LOCAL', '2026-04-01', '2026-06-30', '2026-07-05', ARRAY['877138', '877139'], 'Segundo trimestre 2026 - Sucursales locales'),
    ('T2', 2026, 'FORANEA', '2026-04-01', '2026-06-30', '2026-07-05', ARRAY['877138', '877139'], 'Segundo trimestre 2026 - Sucursales foráneas'),
    ('T3', 2026, 'LOCAL', '2026-07-01', '2026-09-30', '2026-10-05', ARRAY['877138', '877139'], 'Tercer trimestre 2026 - Sucursales locales'),
    ('T3', 2026, 'FORANEA', '2026-07-01', '2026-09-30', '2026-10-05', ARRAY['877138', '877139'], 'Tercer trimestre 2026 - Sucursales foráneas'),
    ('T4', 2026, 'LOCAL', '2026-10-01', '2026-12-31', '2027-01-05', ARRAY['877138', '877139'], 'Cuarto trimestre 2026 - Sucursales locales'),
    ('T4', 2026, 'FORANEA', '2026-10-01', '2026-12-31', '2027-01-05', ARRAY['877138', '877139'], 'Cuarto trimestre 2026 - Sucursales foráneas');
    
    -- Configuración de áreas estándar
    INSERT INTO supervision_areas (supervision_id, area_codigo, area_nombre, area_orden) VALUES
    (0, 'HEADER', 'DATOS GENERALES', 0),
    (0, 'COMEDOR', 'I. AREA COMEDOR', 1),
    (0, 'ASADORES', 'II. AREA ASADORES', 2),
    (0, 'MARINADO', 'III. AREA DE MARINADO', 3),
    (0, 'BODEGA', 'IV. AREA DE BODEGA', 4),
    (0, 'HORNO', 'V. AREA DE HORNO', 5),
    (0, 'FREIDORAS', 'VI. AREA FREIDORAS', 6),
    (0, 'CENTRO_CARGA', 'VII. CENTRO DE CARGA', 7),
    (0, 'AZOTEA', 'VIII. AREA AZOTEA', 8),
    (0, 'EXTERIOR', 'IX. AREA EXTERIOR', 9),
    (0, 'PROTECCION_CIVIL', 'X. PROGRAMA INTERNO PROTECCION CIVIL', 10),
    (0, 'BITACORAS', 'XI. BITACORAS', 11),
    (0, 'FIRMAS', 'XII. NOMBRES Y FIRMAS', 12);
    
    -- Eliminar registro temporal
    DELETE FROM supervision_areas WHERE supervision_id = 0;
    