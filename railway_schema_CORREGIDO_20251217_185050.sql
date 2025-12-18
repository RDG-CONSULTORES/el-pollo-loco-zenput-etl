
-- ============================================================================
-- ESQUEMA CORREGIDO EL POLLO LOCO MÉXICO - RAILWAY POSTGRESQL
-- Períodos 2025 reales + manejo grupos mixtos para dashboard
-- ============================================================================

-- 1. GRUPOS OPERATIVOS (20 grupos con clasificación)
CREATE TABLE grupos_operativos (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL UNIQUE,
    total_sucursales INTEGER NOT NULL DEFAULT 0,
    sucursales_locales INTEGER DEFAULT 0,
    sucursales_foraneas INTEGER DEFAULT 0,
    tipo_grupo VARCHAR(20) NOT NULL CHECK (tipo_grupo IN ('LOCAL', 'FORANEO', 'MIXTO')),
    estados_cobertura TEXT[],
    ciudades_cobertura INTEGER DEFAULT 0,
    coordenadas_centro POINT,
    director_principal VARCHAR(100),
    director_email VARCHAR(100),
    api_team_id INTEGER,
    activo BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. SUCURSALES MASTER (86 sucursales con clasificación LOCAL/FORANEA)
CREATE TABLE sucursales_master (
    numero INTEGER PRIMARY KEY CHECK (numero BETWEEN 1 AND 100),
    nombre_oficial VARCHAR(100) NOT NULL,
    nombre_zenput VARCHAR(100) NOT NULL,
    grupo_operativo_id INTEGER REFERENCES grupos_operativos(id),
    ciudad VARCHAR(50),
    estado VARCHAR(50),
    direccion TEXT,
    coordenadas POINT,
    location_code VARCHAR(20),
    zenput_location_id INTEGER,
    tiempo_zona VARCHAR(50),
    activa BOOLEAN DEFAULT TRUE,
    clasificacion VARCHAR(20) NOT NULL CHECK (clasificacion IN ('LOCAL', 'FORANEA')),
    patron_supervision VARCHAR(20) NOT NULL CHECK (patron_supervision IN ('TRIMESTRAL', 'SEMESTRAL')),
    observaciones TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. PERIODOS SUPERVISIÓN 2025 (Fechas reales ajustadas por entrega marzo)
CREATE TABLE periodos_supervision (
    id SERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    tipo VARCHAR(10) NOT NULL CHECK (tipo IN ('TRIMESTRE', 'SEMESTRE')),
    numero INTEGER NOT NULL,
    codigo VARCHAR(10) NOT NULL UNIQUE,  -- T1, T2, T3, T4, S1, S2
    fecha_inicio DATE NOT NULL,
    fecha_fin DATE NOT NULL,
    descripcion VARCHAR(100),
    aplica_a VARCHAR(20) NOT NULL CHECK (aplica_a IN ('LOCALES', 'FORANEAS')),
    activo BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE (year, tipo, numero)
);

-- 4. SUPERVISIONES (877138 Operativa + 877139 Seguridad)
CREATE TABLE supervisiones (
    id SERIAL PRIMARY KEY,
    sucursal_numero INTEGER REFERENCES sucursales_master(numero),
    form_id INTEGER NOT NULL CHECK (form_id IN (877138, 877139)),
    form_name VARCHAR(50) NOT NULL,
    submission_id BIGINT NOT NULL UNIQUE,
    fecha_supervision DATE NOT NULL,
    periodo_id INTEGER REFERENCES periodos_supervision(id),
    supervisor_nombre VARCHAR(100),
    supervisor_email VARCHAR(100),
    
    -- Calificaciones generales (automáticas desde Zenput)
    puntos_maximos INTEGER,
    puntos_totales INTEGER,
    porcentaje_general NUMERIC(5,2),
    
    -- Metadatos
    submission_date TIMESTAMP,
    location_zenput_id INTEGER,
    raw_data JSONB,  -- Datos completos desde API
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. AREAS SUPERVISION (43 areas: 31 operativas + 12 seguridad)
CREATE TABLE supervision_areas (
    id SERIAL PRIMARY KEY,
    form_id INTEGER NOT NULL CHECK (form_id IN (877138, 877139)),
    area_nombre VARCHAR(100) NOT NULL,
    area_orden INTEGER,
    descripcion TEXT,
    activa BOOLEAN DEFAULT TRUE,
    
    UNIQUE (form_id, area_nombre)
);

-- 6. RESPUESTAS POR AREA (calificaciones específicas)
CREATE TABLE supervision_respuestas (
    id SERIAL PRIMARY KEY,
    supervision_id INTEGER REFERENCES supervisiones(id) ON DELETE CASCADE,
    area_id INTEGER REFERENCES supervision_areas(id),
    
    -- Calificación del área
    puntos_area INTEGER,
    porcentaje_area NUMERIC(5,2),
    
    -- Respuestas específicas
    respuestas_detalle JSONB,  -- Respuestas completas del área
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 7. DIRECTORES Y USUARIOS (desde API /users)
CREATE TABLE usuarios_zenput (
    id INTEGER PRIMARY KEY,  -- ID desde API
    username VARCHAR(100),
    nombre_completo VARCHAR(100),
    email VARCHAR(100) UNIQUE,
    telefono VARCHAR(20),
    rol_principal VARCHAR(50),
    grupos JSONB,  -- Grupos desde API
    teams JSONB,   -- Teams asignados
    sucursales_asignadas JSONB,  -- owned_locations
    default_team_id INTEGER,
    default_team_name VARCHAR(100),
    activo BOOLEAN DEFAULT TRUE,
    fecha_invitacion TIMESTAMP,
    zona_tiempo VARCHAR(50),
    raw_user_data JSONB,  -- Datos completos usuario
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- ÍNDICES PARA PERFORMANCE
-- ============================================================================

-- Supervisiones
CREATE INDEX idx_supervisiones_sucursal ON supervisiones(sucursal_numero);
CREATE INDEX idx_supervisiones_fecha ON supervisiones(fecha_supervision);
CREATE INDEX idx_supervisiones_periodo ON supervisiones(periodo_id);
CREATE INDEX idx_supervisiones_form ON supervisiones(form_id);
CREATE INDEX idx_supervisiones_supervisor ON supervisiones(supervisor_nombre);

-- Sucursales
CREATE INDEX idx_sucursales_grupo ON sucursales_master(grupo_operativo_id);
CREATE INDEX idx_sucursales_estado ON sucursales_master(estado);
CREATE INDEX idx_sucursales_clasificacion ON sucursales_master(clasificacion);
CREATE INDEX idx_sucursales_patron ON sucursales_master(patron_supervision);
CREATE INDEX idx_sucursales_coordenadas ON sucursales_master USING GIST(coordenadas);
CREATE INDEX idx_sucursales_zenput_id ON sucursales_master(zenput_location_id);

-- Grupos
CREATE INDEX idx_grupos_tipo ON grupos_operativos(tipo_grupo);

-- Respuestas
CREATE INDEX idx_respuestas_supervision ON supervision_respuestas(supervision_id);
CREATE INDEX idx_respuestas_area ON supervision_respuestas(area_id);

-- Usuarios
CREATE INDEX idx_usuarios_email ON usuarios_zenput(email);
CREATE INDEX idx_usuarios_team ON usuarios_zenput(default_team_id);

-- ============================================================================
-- VISTAS ESPECIALIZADAS PARA DASHBOARD HEATMAP
-- ============================================================================

-- Vista grupos LOCALES para heatmap trimestral
CREATE VIEW vista_dashboard_locales AS
SELECT 
    go.id,
    go.nombre,
    CASE 
        WHEN go.tipo_grupo = 'LOCAL' THEN go.total_sucursales
        ELSE go.sucursales_locales
    END as sucursales_aplicables,
    go.tipo_grupo,
    COUNT(CASE WHEN s.form_id = 877138 THEN s.id END) as supervisiones_operativas,
    COUNT(CASE WHEN s.form_id = 877139 THEN s.id END) as supervisiones_seguridad,
    AVG(CASE WHEN s.form_id = 877138 THEN s.porcentaje_general END) as promedio_operativa,
    AVG(CASE WHEN s.form_id = 877139 THEN s.porcentaje_general END) as promedio_seguridad,
    COUNT(DISTINCT p.codigo) as periodos_supervisados,
    STRING_AGG(DISTINCT p.codigo, ', ' ORDER BY p.codigo) as periodos_lista
FROM grupos_operativos go
LEFT JOIN sucursales_master sm ON go.id = sm.grupo_operativo_id AND sm.clasificacion = 'LOCAL'
LEFT JOIN supervisiones s ON sm.numero = s.sucursal_numero
LEFT JOIN periodos_supervision p ON s.periodo_id = p.id AND p.aplica_a = 'LOCALES'
WHERE go.tipo_grupo IN ('LOCAL', 'MIXTO')
GROUP BY go.id, go.nombre, go.total_sucursales, go.sucursales_locales, go.tipo_grupo;

-- Vista grupos FORÁNEAS para heatmap semestral
CREATE VIEW vista_dashboard_foraneas AS
SELECT 
    go.id,
    go.nombre,
    CASE 
        WHEN go.tipo_grupo = 'FORANEO' THEN go.total_sucursales
        ELSE go.sucursales_foraneas
    END as sucursales_aplicables,
    go.tipo_grupo,
    COUNT(CASE WHEN s.form_id = 877138 THEN s.id END) as supervisiones_operativas,
    COUNT(CASE WHEN s.form_id = 877139 THEN s.id END) as supervisiones_seguridad,
    AVG(CASE WHEN s.form_id = 877138 THEN s.porcentaje_general END) as promedio_operativa,
    AVG(CASE WHEN s.form_id = 877139 THEN s.porcentaje_general END) as promedio_seguridad,
    COUNT(DISTINCT p.codigo) as periodos_supervisados,
    STRING_AGG(DISTINCT p.codigo, ', ' ORDER BY p.codigo) as periodos_lista
FROM grupos_operativos go
LEFT JOIN sucursales_master sm ON go.id = sm.grupo_operativo_id AND sm.clasificacion = 'FORANEA'
LEFT JOIN supervisiones s ON sm.numero = s.sucursal_numero
LEFT JOIN periodos_supervision p ON s.periodo_id = p.id AND p.aplica_a = 'FORANEAS'
WHERE go.tipo_grupo IN ('FORANEO', 'MIXTO')
GROUP BY go.id, go.nombre, go.total_sucursales, go.sucursales_foraneas, go.tipo_grupo;

-- Vista heatmap por período y grupo (LOCALES)
CREATE VIEW vista_heatmap_trimestral AS
SELECT 
    p.codigo as periodo,
    p.descripcion,
    go.nombre as grupo,
    go.tipo_grupo,
    COUNT(DISTINCT sm.numero) as sucursales_supervisadas,
    COUNT(s.id) as total_supervisiones,
    AVG(s.porcentaje_general) as promedio_general,
    AVG(CASE WHEN s.form_id = 877138 THEN s.porcentaje_general END) as promedio_operativa,
    AVG(CASE WHEN s.form_id = 877139 THEN s.porcentaje_general END) as promedio_seguridad,
    MIN(s.porcentaje_general) as minimo_general,
    MAX(s.porcentaje_general) as maximo_general
FROM periodos_supervision p
LEFT JOIN supervisiones s ON p.id = s.periodo_id
LEFT JOIN sucursales_master sm ON s.sucursal_numero = sm.numero AND sm.clasificacion = 'LOCAL'
LEFT JOIN grupos_operativos go ON sm.grupo_operativo_id = go.id
WHERE p.aplica_a = 'LOCALES' AND go.tipo_grupo IN ('LOCAL', 'MIXTO')
GROUP BY p.codigo, p.descripcion, go.nombre, go.tipo_grupo
ORDER BY p.fecha_inicio, go.nombre;

-- Vista heatmap por período y grupo (FORÁNEAS)
CREATE VIEW vista_heatmap_semestral AS
SELECT 
    p.codigo as periodo,
    p.descripcion,
    go.nombre as grupo,
    go.tipo_grupo,
    COUNT(DISTINCT sm.numero) as sucursales_supervisadas,
    COUNT(s.id) as total_supervisiones,
    AVG(s.porcentaje_general) as promedio_general,
    AVG(CASE WHEN s.form_id = 877138 THEN s.porcentaje_general END) as promedio_operativa,
    AVG(CASE WHEN s.form_id = 877139 THEN s.porcentaje_general END) as promedio_seguridad,
    MIN(s.porcentaje_general) as minimo_general,
    MAX(s.porcentaje_general) as maximo_general
FROM periodos_supervision p
LEFT JOIN supervisiones s ON p.id = s.periodo_id
LEFT JOIN sucursales_master sm ON s.sucursal_numero = sm.numero AND sm.clasificacion = 'FORANEA'
LEFT JOIN grupos_operativos go ON sm.grupo_operativo_id = go.id
WHERE p.aplica_a = 'FORANEAS' AND go.tipo_grupo IN ('FORANEO', 'MIXTO')
GROUP BY p.codigo, p.descripcion, go.nombre, go.tipo_grupo
ORDER BY p.fecha_inicio, go.nombre;

-- Vista resumen ejecutivo general
CREATE VIEW vista_resumen_ejecutivo AS
SELECT 
    go.nombre as grupo,
    go.tipo_grupo,
    go.total_sucursales,
    go.sucursales_locales,
    go.sucursales_foraneas,
    go.director_principal,
    COUNT(s.id) as total_supervisiones_2025,
    AVG(s.porcentaje_general) as promedio_general_2025,
    COUNT(CASE WHEN sm.clasificacion = 'LOCAL' THEN s.id END) as supervisiones_locales,
    COUNT(CASE WHEN sm.clasificacion = 'FORANEA' THEN s.id END) as supervisiones_foraneas,
    COUNT(DISTINCT s.sucursal_numero) as sucursales_con_supervision,
    MAX(s.fecha_supervision) as ultima_supervision
FROM grupos_operativos go
LEFT JOIN sucursales_master sm ON go.id = sm.grupo_operativo_id
LEFT JOIN supervisiones s ON sm.numero = s.sucursal_numero
GROUP BY go.nombre, go.tipo_grupo, go.total_sucursales, go.sucursales_locales, go.sucursales_foraneas, go.director_principal
ORDER BY go.total_sucursales DESC;

-- ============================================================================
-- TRIGGERS PARA MANTENIMIENTO AUTOMÁTICO
-- ============================================================================

-- Trigger para actualizar updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Aplicar a todas las tablas principales
CREATE TRIGGER update_grupos_operativos_updated_at BEFORE UPDATE ON grupos_operativos FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_sucursales_master_updated_at BEFORE UPDATE ON sucursales_master FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_supervisiones_updated_at BEFORE UPDATE ON supervisiones FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_usuarios_zenput_updated_at BEFORE UPDATE ON usuarios_zenput FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Trigger para calcular clasificación y patrón supervisión
CREATE OR REPLACE FUNCTION set_sucursal_clasificacion_y_patron()
RETURNS TRIGGER AS $$
BEGIN
    -- Clasificación: LOCAL = Nuevo León + Saltillo, FORANEA = resto
    -- Si estado está vacío, usar coordenadas GPS
    IF NEW.estado = 'Nuevo León' OR (NEW.estado = 'Coahuila' AND NEW.ciudad LIKE '%Saltillo%') THEN
        NEW.clasificacion = 'LOCAL';
        NEW.patron_supervision = 'TRIMESTRAL';
    ELSIF NEW.estado IS NULL OR NEW.estado = '' THEN
        -- Usar coordenadas para área metropolitana Monterrey
        IF NEW.coordenadas IS NOT NULL THEN
            -- Extraer latitud y longitud del POINT
            IF ST_Y(NEW.coordenadas) BETWEEN 25.4 AND 26.0 
               AND ST_X(NEW.coordenadas) BETWEEN -100.7 AND -99.9 THEN
                NEW.clasificacion = 'LOCAL';
                NEW.patron_supervision = 'TRIMESTRAL';
            ELSE
                NEW.clasificacion = 'FORANEA';
                NEW.patron_supervision = 'SEMESTRAL';
            END IF;
        ELSE
            -- Sin coordenadas, asumir foránea
            NEW.clasificacion = 'FORANEA';
            NEW.patron_supervision = 'SEMESTRAL';
        END IF;
    ELSE
        NEW.clasificacion = 'FORANEA';
        NEW.patron_supervision = 'SEMESTRAL';
    END IF;
    
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER set_sucursal_clasificacion_trigger 
    BEFORE INSERT OR UPDATE ON sucursales_master 
    FOR EACH ROW EXECUTE FUNCTION set_sucursal_clasificacion_y_patron();

-- ============================================================================
-- COMENTARIOS Y DOCUMENTACIÓN
-- ============================================================================

COMMENT ON TABLE grupos_operativos IS 'Grupos operativos con clasificación LOCAL/FORANEO/MIXTO para dashboard separado';
COMMENT ON TABLE sucursales_master IS 'Sucursales con clasificación y patrón supervisión (TRIMESTRAL/SEMESTRAL)';
COMMENT ON TABLE periodos_supervision IS 'Períodos 2025 reales ajustados por entrega marzo Zenput';
COMMENT ON VIEW vista_dashboard_locales IS 'Dashboard heatmap sucursales LOCALES (patrón trimestral)';
COMMENT ON VIEW vista_dashboard_foraneas IS 'Dashboard heatmap sucursales FORÁNEAS (patrón semestral)';
COMMENT ON VIEW vista_heatmap_trimestral IS 'Heatmap períodos T1-T4 para sucursales locales';
COMMENT ON VIEW vista_heatmap_semestral IS 'Heatmap períodos S1-S2 para sucursales foráneas';
