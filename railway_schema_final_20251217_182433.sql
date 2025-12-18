
-- ============================================================================
-- ESQUEMA COMPLETO EL POLLO LOCO MÉXICO - RAILWAY POSTGRESQL
-- Estructura organizacional con 20 grupos operativos + supervisiones
-- ============================================================================

-- 1. GRUPOS OPERATIVOS (20 grupos desde Excel Roberto)
CREATE TABLE grupos_operativos (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL UNIQUE,
    total_sucursales INTEGER NOT NULL DEFAULT 0,
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

-- 2. SUCURSALES MASTER (86 sucursales con GPS desde Roberto)
CREATE TABLE sucursales_master (
    numero INTEGER PRIMARY KEY CHECK (numero BETWEEN 1 AND 100),
    nombre_oficial VARCHAR(100) NOT NULL,
    nombre_zenput VARCHAR(100) NOT NULL,
    grupo_operativo_id INTEGER REFERENCES grupos_operativos(id),
    ciudad VARCHAR(50) NOT NULL,
    estado VARCHAR(50) NOT NULL,
    direccion TEXT,
    coordenadas POINT,
    location_code VARCHAR(20),
    zenput_location_id INTEGER,
    tiempo_zona VARCHAR(50),
    activa BOOLEAN DEFAULT TRUE,
    clasificacion VARCHAR(20) CHECK (clasificacion IN ('LOCAL', 'FORANEA')),
    observaciones TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. PERIODOS SUPERVISIÓN 2025 (T1-T4 + S1-S2)
CREATE TABLE periodos_supervision (
    id SERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    tipo VARCHAR(10) NOT NULL CHECK (tipo IN ('TRIMESTRE', 'SEMESTRE')),
    numero INTEGER NOT NULL,
    codigo VARCHAR(10) NOT NULL UNIQUE,  -- T1, T2, T3, T4, S1, S2
    fecha_inicio DATE NOT NULL,
    fecha_fin DATE NOT NULL,
    descripcion VARCHAR(100),
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
CREATE INDEX idx_sucursales_coordenadas ON sucursales_master USING GIST(coordenadas);
CREATE INDEX idx_sucursales_zenput_id ON sucursales_master(zenput_location_id);

-- Respuestas
CREATE INDEX idx_respuestas_supervision ON supervision_respuestas(supervision_id);
CREATE INDEX idx_respuestas_area ON supervision_respuestas(area_id);

-- Usuarios
CREATE INDEX idx_usuarios_email ON usuarios_zenput(email);
CREATE INDEX idx_usuarios_team ON usuarios_zenput(default_team_id);

-- ============================================================================
-- VISTAS PARA DASHBOARD
-- ============================================================================

-- Vista resumen por grupo operativo
CREATE VIEW vista_grupos_resumen AS
SELECT 
    go.id,
    go.nombre,
    go.total_sucursales,
    go.estados_cobertura,
    go.director_principal,
    COUNT(s.id) as total_supervisiones,
    AVG(s.porcentaje_general) as promedio_calificacion,
    MAX(s.fecha_supervision) as ultima_supervision
FROM grupos_operativos go
LEFT JOIN sucursales_master sm ON go.id = sm.grupo_operativo_id
LEFT JOIN supervisiones s ON sm.numero = s.sucursal_numero
GROUP BY go.id, go.nombre, go.total_sucursales, go.estados_cobertura, go.director_principal;

-- Vista supervisiones por periodo
CREATE VIEW vista_supervisiones_periodo AS
SELECT 
    p.codigo as periodo,
    p.descripcion,
    s.form_name,
    COUNT(s.id) as total_supervisiones,
    AVG(s.porcentaje_general) as promedio_general,
    COUNT(DISTINCT s.sucursal_numero) as sucursales_supervisadas
FROM periodos_supervision p
LEFT JOIN supervisiones s ON p.id = s.periodo_id
GROUP BY p.codigo, p.descripcion, s.form_name
ORDER BY p.fecha_inicio;

-- Vista performance por sucursal
CREATE VIEW vista_sucursales_performance AS
SELECT 
    sm.numero,
    sm.nombre_oficial,
    sm.ciudad,
    sm.estado,
    go.nombre as grupo_operativo,
    COUNT(s.id) as total_supervisiones,
    AVG(CASE WHEN s.form_id = 877138 THEN s.porcentaje_general END) as promedio_operativa,
    AVG(CASE WHEN s.form_id = 877139 THEN s.porcentaje_general END) as promedio_seguridad,
    AVG(s.porcentaje_general) as promedio_general,
    MAX(s.fecha_supervision) as ultima_supervision
FROM sucursales_master sm
LEFT JOIN grupos_operativos go ON sm.grupo_operativo_id = go.id
LEFT JOIN supervisiones s ON sm.numero = s.sucursal_numero
GROUP BY sm.numero, sm.nombre_oficial, sm.ciudad, sm.estado, go.nombre;

-- Vista areas más problemáticas
CREATE VIEW vista_areas_problematicas AS
SELECT 
    sa.area_nombre,
    sa.form_id,
    CASE WHEN sa.form_id = 877138 THEN 'Operativa' ELSE 'Seguridad' END as tipo_supervision,
    COUNT(sr.id) as total_evaluaciones,
    AVG(sr.porcentaje_area) as promedio_area,
    MIN(sr.porcentaje_area) as minimo_area,
    STDDEV(sr.porcentaje_area) as desviacion_area
FROM supervision_areas sa
LEFT JOIN supervision_respuestas sr ON sa.id = sr.area_id
GROUP BY sa.area_nombre, sa.form_id
ORDER BY promedio_area ASC;

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

-- Trigger para calcular clasificación sucursal
CREATE OR REPLACE FUNCTION set_sucursal_clasificacion()
RETURNS TRIGGER AS $$
BEGIN
    -- LOCAL = Nuevo León + Saltillo (Coahuila)
    -- FORANEA = Otros estados
    IF NEW.estado = 'Nuevo León' OR (NEW.estado = 'Coahuila' AND NEW.ciudad = 'Saltillo') THEN
        NEW.clasificacion = 'LOCAL';
    ELSE
        NEW.clasificacion = 'FORANEA';
    END IF;
    
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER set_sucursal_clasificacion_trigger 
    BEFORE INSERT OR UPDATE ON sucursales_master 
    FOR EACH ROW EXECUTE FUNCTION set_sucursal_clasificacion();

-- ============================================================================
-- COMENTARIOS Y DOCUMENTACIÓN
-- ============================================================================

COMMENT ON TABLE grupos_operativos IS 'Grupos operativos de El Pollo Loco México según estructura organizacional';
COMMENT ON TABLE sucursales_master IS 'Catálogo maestro de 86 sucursales con coordenadas GPS y asignación de grupos';
COMMENT ON TABLE periodos_supervision IS 'Períodos de supervisión 2025: T1-T4 trimestrales, S1-S2 semestrales';
COMMENT ON TABLE supervisiones IS 'Supervisiones operativas (877138) y de seguridad (877139) con calificaciones automáticas';
COMMENT ON TABLE supervision_areas IS 'Catálogo de 43 áreas: 31 operativas + 12 seguridad';
COMMENT ON TABLE supervision_respuestas IS 'Respuestas detalladas por área con calificaciones específicas';
COMMENT ON TABLE usuarios_zenput IS 'Usuarios y directores desde API Zenput con asignaciones de teams y sucursales';

-- ============================================================================
-- DATOS INICIALES REQUERIDOS
-- ============================================================================

-- Los siguientes datos se cargarán desde los scripts Python:
-- 1. grupos_operativos: 20 grupos desde Excel Roberto
-- 2. sucursales_master: 86 sucursales con GPS desde Excel Roberto  
-- 3. periodos_supervision: Períodos 2025 T1-T4, S1-S2
-- 4. supervision_areas: 43 áreas desde análisis de forms 877138 y 877139
-- 5. usuarios_zenput: 20 usuarios desde API /users
    