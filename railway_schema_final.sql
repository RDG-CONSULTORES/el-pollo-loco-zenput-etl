-- ============================================================
-- EL POLLO LOCO ZENPUT ETL - RAILWAY POSTGRESQL SCHEMA
-- Fecha: 17 Diciembre 2025
-- Status: PRODUCTION READY
-- ============================================================

-- Extensiones necesarias
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- 1. TABLA GRUPOS OPERATIVOS
-- ============================================================
CREATE TABLE grupos_operativos (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) UNIQUE NOT NULL,
    tipo_supervision VARCHAR(20) NOT NULL CHECK (tipo_supervision IN ('LOCAL', 'FORANEA', 'MIXTO')),
    director_nombre VARCHAR(100),
    director_email VARCHAR(100),
    director_telefono VARCHAR(20),
    team_id INTEGER,
    team_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- 2. TABLA SUCURSALES
-- ============================================================
CREATE TABLE sucursales (
    id INTEGER PRIMARY KEY,
    nombre VARCHAR(200) NOT NULL,
    nombre_completo VARCHAR(300),
    direccion TEXT,
    ciudad VARCHAR(100),
    estado VARCHAR(100),
    codigo_postal VARCHAR(10),
    latitud DECIMAL(10,8),
    longitud DECIMAL(11,8),
    grupo_operativo_id INTEGER,
    grupo_operativo_nombre VARCHAR(100),
    tipo_ubicacion VARCHAR(20) CHECK (tipo_ubicacion IN ('LOCAL', 'FORANEA')),
    external_key VARCHAR(10),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (grupo_operativo_id) REFERENCES grupos_operativos(id)
);

-- ============================================================
-- 3. TABLA PERIODOS SUPERVISION 2025
-- ============================================================
CREATE TABLE periodos_supervision (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(10) NOT NULL, -- T1, T2, T3, T4, S1, S2
    descripcion VARCHAR(100),
    fecha_inicio DATE NOT NULL,
    fecha_fin DATE NOT NULL,
    tipo VARCHAR(20) NOT NULL CHECK (tipo IN ('TRIMESTRAL', 'SEMESTRAL')),
    aplicable_a VARCHAR(20) NOT NULL CHECK (aplicable_a IN ('LOCAL', 'FORANEA')),
    year INTEGER DEFAULT 2025,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- 4. TABLA SUPERVISIONS (UNIFICADA)
-- ============================================================
CREATE TABLE supervisions (
    id SERIAL PRIMARY KEY,
    submission_id VARCHAR(50) UNIQUE NOT NULL,
    form_id VARCHAR(10) NOT NULL,
    form_type VARCHAR(20) NOT NULL CHECK (form_type IN ('OPERATIVA', 'SEGURIDAD')),
    
    -- Información básica
    sucursal_id INTEGER NOT NULL,
    sucursal_nombre VARCHAR(200),
    grupo_operativo_id INTEGER,
    auditor_nombre VARCHAR(100),
    auditor_email VARCHAR(100),
    fecha_supervision DATE NOT NULL,
    fecha_submission TIMESTAMP NOT NULL,
    
    -- Periodo de supervision
    periodo_id INTEGER,
    periodo_nombre VARCHAR(10),
    
    -- Calificación general
    puntos_max INTEGER NOT NULL,
    puntos_obtenidos INTEGER NOT NULL,
    calificacion_porcentaje DECIMAL(5,2) NOT NULL,
    
    -- Score original de Zenput
    score_zenput DECIMAL(5,2),
    
    -- Metadatos
    latitude DECIMAL(10,8),
    longitude DECIMAL(11,8),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    -- Foreign Keys
    FOREIGN KEY (sucursal_id) REFERENCES sucursales(id),
    FOREIGN KEY (grupo_operativo_id) REFERENCES grupos_operativos(id),
    FOREIGN KEY (periodo_id) REFERENCES periodos_supervision(id)
);

-- ============================================================
-- 5. TABLA SUPERVISIONS_OPERATIVA (Detalles form 877138)
-- ============================================================
CREATE TABLE supervisions_operativa (
    id SERIAL PRIMARY KEY,
    supervision_id INTEGER UNIQUE NOT NULL,
    submission_id VARCHAR(50) UNIQUE NOT NULL,
    
    -- Desglose por áreas (basado en análisis previo)
    area_limpieza_puntos INTEGER DEFAULT 0,
    area_limpieza_max INTEGER DEFAULT 0,
    area_limpieza_porcentaje DECIMAL(5,2) DEFAULT 0,
    
    area_equipos_puntos INTEGER DEFAULT 0,
    area_equipos_max INTEGER DEFAULT 0, 
    area_equipos_porcentaje DECIMAL(5,2) DEFAULT 0,
    
    area_procesos_puntos INTEGER DEFAULT 0,
    area_procesos_max INTEGER DEFAULT 0,
    area_procesos_porcentaje DECIMAL(5,2) DEFAULT 0,
    
    area_inventario_puntos INTEGER DEFAULT 0,
    area_inventario_max INTEGER DEFAULT 0,
    area_inventario_porcentaje DECIMAL(5,2) DEFAULT 0,
    
    area_personal_puntos INTEGER DEFAULT 0,
    area_personal_max INTEGER DEFAULT 0,
    area_personal_porcentaje DECIMAL(5,2) DEFAULT 0,
    
    -- Información adicional
    notas TEXT,
    observaciones TEXT,
    acciones_correctivas TEXT,
    
    created_at TIMESTAMP DEFAULT NOW(),
    
    FOREIGN KEY (supervision_id) REFERENCES supervisions(id) ON DELETE CASCADE
);

-- ============================================================
-- 6. TABLA SUPERVISIONS_SEGURIDAD (Detalles form 877139)
-- ============================================================
CREATE TABLE supervisions_seguridad (
    id SERIAL PRIMARY KEY,
    supervision_id INTEGER UNIQUE NOT NULL,
    submission_id VARCHAR(50) UNIQUE NOT NULL,
    
    -- 11 áreas de seguridad (validadas)
    area_comedor_puntos INTEGER DEFAULT 0,
    area_comedor_max INTEGER DEFAULT 0,
    area_comedor_porcentaje DECIMAL(5,2) DEFAULT 0,
    
    area_asadores_puntos INTEGER DEFAULT 0,
    area_asadores_max INTEGER DEFAULT 0,
    area_asadores_porcentaje DECIMAL(5,2) DEFAULT 0,
    
    area_marinado_puntos INTEGER DEFAULT 0,
    area_marinado_max INTEGER DEFAULT 0,
    area_marinado_porcentaje DECIMAL(5,2) DEFAULT 0,
    
    area_bodega_puntos INTEGER DEFAULT 0,
    area_bodega_max INTEGER DEFAULT 0,
    area_bodega_porcentaje DECIMAL(5,2) DEFAULT 0,
    
    area_horno_puntos INTEGER DEFAULT 0,
    area_horno_max INTEGER DEFAULT 0,
    area_horno_porcentaje DECIMAL(5,2) DEFAULT 0,
    
    area_freidoras_puntos INTEGER DEFAULT 0,
    area_freidoras_max INTEGER DEFAULT 0,
    area_freidoras_porcentaje DECIMAL(5,2) DEFAULT 0,
    
    area_centro_carga_puntos INTEGER DEFAULT 0,
    area_centro_carga_max INTEGER DEFAULT 0,
    area_centro_carga_porcentaje DECIMAL(5,2) DEFAULT 0,
    
    area_azotea_puntos INTEGER DEFAULT 0,
    area_azotea_max INTEGER DEFAULT 0,
    area_azotea_porcentaje DECIMAL(5,2) DEFAULT 0,
    
    area_exterior_puntos INTEGER DEFAULT 0,
    area_exterior_max INTEGER DEFAULT 0,
    area_exterior_porcentaje DECIMAL(5,2) DEFAULT 0,
    
    area_proteccion_civil_puntos INTEGER DEFAULT 0,
    area_proteccion_civil_max INTEGER DEFAULT 0,
    area_proteccion_civil_porcentaje DECIMAL(5,2) DEFAULT 0,
    
    area_bitacoras_puntos INTEGER DEFAULT 0,
    area_bitacoras_max INTEGER DEFAULT 0,
    area_bitacoras_porcentaje DECIMAL(5,2) DEFAULT 0,
    
    -- Información adicional
    gerente_turno VARCHAR(100),
    observaciones_seguridad TEXT,
    
    created_at TIMESTAMP DEFAULT NOW(),
    
    FOREIGN KEY (supervision_id) REFERENCES supervisions(id) ON DELETE CASCADE
);

-- ============================================================
-- 7. VISTAS PARA DASHBOARD HEATMAP
-- ============================================================

-- Vista: Supervisiones Locales (T1-T4)
CREATE VIEW dashboard_heatmap_locales AS
SELECT 
    s.id,
    s.sucursal_id,
    s.sucursal_nombre,
    s.grupo_operativo_id,
    go.nombre as grupo_nombre,
    go.director_nombre,
    go.director_email,
    s.form_type,
    s.fecha_supervision,
    s.periodo_nombre,
    s.puntos_obtenidos,
    s.puntos_max,
    s.calificacion_porcentaje,
    suc.tipo_ubicacion,
    ps.tipo as periodo_tipo
FROM supervisions s
JOIN sucursales suc ON s.sucursal_id = suc.id
JOIN grupos_operativos go ON s.grupo_operativo_id = go.id
LEFT JOIN periodos_supervision ps ON s.periodo_id = ps.id
WHERE suc.tipo_ubicacion = 'LOCAL' 
   OR (go.tipo_supervision = 'MIXTO' AND suc.tipo_ubicacion = 'LOCAL')
ORDER BY s.fecha_supervision DESC;

-- Vista: Supervisiones Foráneas (S1-S2)
CREATE VIEW dashboard_heatmap_foraneas AS
SELECT 
    s.id,
    s.sucursal_id,
    s.sucursal_nombre,
    s.grupo_operativo_id,
    go.nombre as grupo_nombre,
    go.director_nombre,
    go.director_email,
    s.form_type,
    s.fecha_supervision,
    s.periodo_nombre,
    s.puntos_obtenidos,
    s.puntos_max,
    s.calificacion_porcentaje,
    suc.tipo_ubicacion,
    ps.tipo as periodo_tipo
FROM supervisions s
JOIN sucursales suc ON s.sucursal_id = suc.id
JOIN grupos_operativos go ON s.grupo_operativo_id = go.id
LEFT JOIN periodos_supervision ps ON s.periodo_id = ps.id
WHERE suc.tipo_ubicacion = 'FORANEA'
   OR (go.tipo_supervision = 'MIXTO' AND suc.tipo_ubicacion = 'FORANEA')
ORDER BY s.fecha_supervision DESC;

-- Vista: Resumen Dashboard General
CREATE VIEW dashboard_resumen_general AS
SELECT 
    go.id as grupo_id,
    go.nombre as grupo_nombre,
    go.tipo_supervision,
    go.director_nombre,
    go.director_email,
    COUNT(DISTINCT s.sucursal_id) as total_sucursales,
    COUNT(DISTINCT CASE WHEN suc.tipo_ubicacion = 'LOCAL' THEN s.sucursal_id END) as sucursales_locales,
    COUNT(DISTINCT CASE WHEN suc.tipo_ubicacion = 'FORANEA' THEN s.sucursal_id END) as sucursales_foraneas,
    COUNT(s.id) as total_supervisiones,
    COUNT(CASE WHEN s.form_type = 'OPERATIVA' THEN 1 END) as supervisiones_operativa,
    COUNT(CASE WHEN s.form_type = 'SEGURIDAD' THEN 1 END) as supervisiones_seguridad,
    ROUND(AVG(s.calificacion_porcentaje), 2) as promedio_calificacion,
    COUNT(CASE WHEN s.fecha_supervision >= DATE_TRUNC('month', CURRENT_DATE) THEN 1 END) as supervisiones_mes_actual
FROM grupos_operativos go
LEFT JOIN sucursales suc ON go.id = suc.grupo_operativo_id
LEFT JOIN supervisions s ON suc.id = s.sucursal_id AND EXTRACT(YEAR FROM s.fecha_supervision) = 2025
GROUP BY go.id, go.nombre, go.tipo_supervision, go.director_nombre, go.director_email
ORDER BY go.nombre;

-- ============================================================
-- 8. TRIGGERS Y FUNCIONES
-- ============================================================

-- Función para auto-asignar tipo_ubicacion basado en coordenadas GPS
CREATE OR REPLACE FUNCTION auto_assign_tipo_ubicacion()
RETURNS TRIGGER AS $$
BEGIN
    -- Clasificación GPS: LOCAL = Nuevo León + Saltillo
    IF NEW.latitud IS NOT NULL AND NEW.longitud IS NOT NULL THEN
        -- Nuevo León: Aprox 24.5-27.0 lat, -101.5 a -98.5 lon
        -- Saltillo: Aprox 25.3-25.5 lat, -101.1 a -100.8 lon
        IF (NEW.latitud BETWEEN 24.5 AND 27.0 AND NEW.longitud BETWEEN -101.5 AND -98.5) OR
           (NEW.latitud BETWEEN 25.3 AND 25.5 AND NEW.longitud BETWEEN -101.1 AND -100.8) THEN
            NEW.tipo_ubicacion = 'LOCAL';
        ELSE
            NEW.tipo_ubicacion = 'FORANEA';
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger para sucursales
CREATE TRIGGER trigger_auto_tipo_ubicacion 
    BEFORE INSERT OR UPDATE ON sucursales
    FOR EACH ROW EXECUTE FUNCTION auto_assign_tipo_ubicacion();

-- Función para actualizar updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Triggers updated_at
CREATE TRIGGER update_grupos_operativos_updated_at 
    BEFORE UPDATE ON grupos_operativos 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_sucursales_updated_at 
    BEFORE UPDATE ON sucursales 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_supervisions_updated_at 
    BEFORE UPDATE ON supervisions 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================
-- 9. INDICES PARA PERFORMANCE
-- ============================================================

-- Indices principales
CREATE INDEX idx_supervisions_sucursal_fecha ON supervisions(sucursal_id, fecha_supervision);
CREATE INDEX idx_supervisions_grupo_fecha ON supervisions(grupo_operativo_id, fecha_supervision);
CREATE INDEX idx_supervisions_form_type ON supervisions(form_type);
CREATE INDEX idx_supervisions_periodo ON supervisions(periodo_id);
CREATE INDEX idx_supervisions_calificacion ON supervisions(calificacion_porcentaje);
CREATE INDEX idx_supervisions_year ON supervisions(fecha_supervision) WHERE EXTRACT(YEAR FROM fecha_supervision) = 2025;

CREATE INDEX idx_sucursales_grupo ON sucursales(grupo_operativo_id);
CREATE INDEX idx_sucursales_tipo_ubicacion ON sucursales(tipo_ubicacion);
CREATE INDEX idx_sucursales_gps ON sucursales(latitud, longitud);

-- Indices para búsquedas por submission_id
CREATE INDEX idx_supervisions_submission ON supervisions(submission_id);
CREATE INDEX idx_supervisions_operativa_submission ON supervisions_operativa(submission_id);
CREATE INDEX idx_supervisions_seguridad_submission ON supervisions_seguridad(submission_id);

-- ============================================================
-- SCHEMA COMPLETADO - LISTO PARA ETL
-- ============================================================

-- Comentarios de tablas
COMMENT ON TABLE grupos_operativos IS 'Grupos operativos de El Pollo Loco con directores asignados';
COMMENT ON TABLE sucursales IS 'Sucursales con clasificación LOCAL/FORANEA por GPS';
COMMENT ON TABLE periodos_supervision IS 'Periodos 2025: T1-T4 (locales) y S1-S2 (foráneas)';
COMMENT ON TABLE supervisions IS 'Supervisiones unificadas (Operativa + Seguridad)';
COMMENT ON TABLE supervisions_operativa IS 'Detalles específicos de supervisiones operativas';
COMMENT ON TABLE supervisions_seguridad IS 'Detalles específicos de supervisiones de seguridad (11 áreas)';

-- Información del schema
SELECT 
    'EL POLLO LOCO ZENPUT ETL - RAILWAY POSTGRESQL SCHEMA' as proyecto,
    'PRODUCTION READY' as status,
    '17 Diciembre 2025' as fecha,
    'Claude Code SuperClaude ETL Framework' as framework;