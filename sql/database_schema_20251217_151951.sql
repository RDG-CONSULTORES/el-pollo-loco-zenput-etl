-- ESTRUCTURA DE BASE DE DATOS - EL POLLO LOCO SUPERVISIONES
-- Generado: 2025-12-17 15:19:51
-- Archivo: sql/database_schema_20251217_151951.sql


    -- ================================================
    -- TABLA MAESTRO DE SUCURSALES
    -- ================================================
    CREATE TABLE IF NOT EXISTS sucursales_master (
        id SERIAL PRIMARY KEY,
        sucursal_numero INTEGER UNIQUE NOT NULL,  -- Número oficial (1-86)
        sucursal_id_zenput VARCHAR(20) UNIQUE,    -- ID en Zenput API
        nombre_actual VARCHAR(100) NOT NULL,      -- Nombre actual
        nombres_historicos TEXT[],                -- Nombres anteriores (array)
        tipo_sucursal VARCHAR(10) NOT NULL,       -- 'LOCAL' o 'FORANEA'
        
        -- Ubicación
        ciudad VARCHAR(50),
        estado VARCHAR(50),
        direccion TEXT,
        coordenadas_lat DECIMAL(10,8),
        coordenadas_lon DECIMAL(11,8),
        
        -- Clasificación
        zona_operativa VARCHAR(50),
        grupo_operativo VARCHAR(50),
        director_responsable VARCHAR(100),
        
        -- Control
        activa BOOLEAN DEFAULT TRUE,
        fecha_apertura DATE,
        fecha_creacion TIMESTAMP DEFAULT NOW(),
        fecha_actualizacion TIMESTAMP DEFAULT NOW()
    );
    
    -- ================================================
    -- TABLA DE PERIODOS OFICIALES T1-T4
    -- ================================================
    CREATE TABLE IF NOT EXISTS periodos_supervision (
        id SERIAL PRIMARY KEY,
        periodo_codigo VARCHAR(10) NOT NULL,      -- 'T1', 'T2', 'T3', 'T4'
        año INTEGER NOT NULL,
        tipo_sucursal VARCHAR(10) NOT NULL,       -- 'LOCAL', 'FORANEA', 'TODAS'
        
        -- Fechas del periodo
        fecha_inicio DATE NOT NULL,
        fecha_fin DATE NOT NULL,
        fecha_limite_supervision DATE,            -- Fecha límite para completar
        
        -- Configuración
        supervisiones_requeridas INTEGER DEFAULT 1,
        formularios_requeridos TEXT[],           -- ['877138', '877139']
        activo BOOLEAN DEFAULT TRUE,
        
        -- Metadatos
        descripcion TEXT,
        creado_por VARCHAR(100),
        fecha_creacion TIMESTAMP DEFAULT NOW(),
        
        UNIQUE(periodo_codigo, año, tipo_sucursal)
    );
    
    -- ================================================
    -- TABLA PRINCIPAL DE SUPERVISIONES
    -- ================================================
    CREATE TABLE IF NOT EXISTS supervisiones (
        id SERIAL PRIMARY KEY,
        
        -- Identificación Zenput
        submission_id VARCHAR(50) UNIQUE NOT NULL,
        form_id VARCHAR(10) NOT NULL,
        form_name VARCHAR(150),
        
        -- Sucursal (normalizada)
        sucursal_numero INTEGER NOT NULL REFERENCES sucursales_master(sucursal_numero),
        sucursal_nombre_zenput VARCHAR(100),      -- Como aparece en Zenput
        sucursal_nombre_normalizado VARCHAR(100), -- Nombre oficial normalizado
        
        -- Supervisor
        supervisor_id VARCHAR(20),
        supervisor_nombre VARCHAR(100),
        supervisor_rol VARCHAR(50),
        
        -- Fechas y tiempos
        fecha_supervision TIMESTAMP NOT NULL,
        fecha_completada TIMESTAMP,
        fecha_enviada TIMESTAMP,
        tiempo_supervision_minutos INTEGER,
        
        -- Calificaciones globales
        calificacion_general DECIMAL(5,2),       -- Calificación % principal
        puntos_obtenidos INTEGER,
        puntos_maximos INTEGER,
        
        -- Completitud general
        total_preguntas INTEGER,
        total_respondidas INTEGER,
        completitud_porcentaje DECIMAL(5,2),
        
        -- Evidencia
        total_imagenes INTEGER DEFAULT 0,
        respuestas_si INTEGER DEFAULT 0,
        respuestas_no INTEGER DEFAULT 0,
        
        -- Ubicación GPS
        coordenadas_lat DECIMAL(10,8),
        coordenadas_lon DECIMAL(11,8),
        distancia_sucursal_km DECIMAL(8,3),
        
        -- Periodo de supervisión
        periodo_id INTEGER REFERENCES periodos_supervision(id),
        
        -- Metadatos técnicos
        plataforma VARCHAR(20),                   -- 'ios', 'android', 'web'
        ambiente VARCHAR(20),                     -- 'app', 'web'
        zona_horaria VARCHAR(50),
        
        -- Control
        procesada BOOLEAN DEFAULT FALSE,
        fecha_procesamiento TIMESTAMP,
        fecha_creacion TIMESTAMP DEFAULT NOW()
    );
    
    -- ================================================
    -- TABLA DETALLE POR ÁREAS
    -- ================================================
    CREATE TABLE IF NOT EXISTS supervision_areas (
        id SERIAL PRIMARY KEY,
        supervision_id INTEGER NOT NULL REFERENCES supervisiones(id) ON DELETE CASCADE,
        
        -- Área operativa
        area_codigo VARCHAR(30) NOT NULL,         -- 'COMEDOR', 'ASADORES', etc.
        area_nombre VARCHAR(100) NOT NULL,        -- 'I. AREA COMEDOR'
        area_orden INTEGER,                       -- Orden en formulario (1,2,3...)
        
        -- KPIs del área
        elementos_evaluados INTEGER DEFAULT 0,
        elementos_conformes INTEGER DEFAULT 0,
        elementos_no_conformes INTEGER DEFAULT 0,
        conformidad_porcentaje DECIMAL(5,2),
        
        completitud_porcentaje DECIMAL(5,2),
        campos_completados INTEGER DEFAULT 0,
        total_campos INTEGER DEFAULT 0,
        
        -- Evidencia por área
        evidencia_fotografica INTEGER DEFAULT 0,
        respuestas_si INTEGER DEFAULT 0,
        respuestas_no INTEGER DEFAULT 0,
        
        -- Elementos críticos fallidos
        elementos_criticos_fallidos TEXT[],      -- JSON array de problemas
        
        -- Control de tiempo
        tiempo_area_estimado INTEGER,            -- minutos estimados en área
        
        fecha_creacion TIMESTAMP DEFAULT NOW()
    );
    
    -- ================================================
    -- TABLA DE RESPUESTAS DETALLADAS (OPCIONAL)
    -- ================================================
    CREATE TABLE IF NOT EXISTS supervision_respuestas (
        id SERIAL PRIMARY KEY,
        supervision_id INTEGER NOT NULL REFERENCES supervisiones(id) ON DELETE CASCADE,
        area_id INTEGER REFERENCES supervision_areas(id) ON DELETE CASCADE,
        
        -- Campo específico
        field_id INTEGER NOT NULL,
        field_version_id INTEGER,
        field_title TEXT NOT NULL,
        field_type VARCHAR(20) NOT NULL,         -- 'yesno', 'text', 'image', etc.
        
        -- Respuesta
        field_value TEXT,                        -- Valor como texto
        is_answered BOOLEAN DEFAULT FALSE,
        yesno_value BOOLEAN,                     -- Para campos SI/NO
        numeric_value DECIMAL(10,3),            -- Para campos numéricos
        
        -- Evidencia
        image_count INTEGER DEFAULT 0,          -- Número de imágenes
        image_keys TEXT[],                       -- S3 keys de imágenes
        
        -- Criticidad
        es_critico BOOLEAN DEFAULT FALSE,
        fallo_critico BOOLEAN DEFAULT FALSE,
        
        fecha_creacion TIMESTAMP DEFAULT NOW()
    );
    
    -- ================================================
    -- TABLA DE LOG DE CAMBIOS EN ESTRUCTURA
    -- ================================================
    CREATE TABLE IF NOT EXISTS estructura_cambios (
        id SERIAL PRIMARY KEY,
        
        tipo_cambio VARCHAR(20) NOT NULL,        -- 'NUEVA_AREA', 'NUEVO_CAMPO', 'AREA_ELIMINADA'
        form_id VARCHAR(10),
        submission_id VARCHAR(50),
        
        -- Detalle del cambio
        area_anterior VARCHAR(100),
        area_nueva VARCHAR(100),
        campo_anterior TEXT,
        campo_nuevo TEXT,
        
        -- Impacto
        impacto_estimado VARCHAR(20),            -- 'ALTO', 'MEDIO', 'BAJO'
        accion_requerida TEXT,
        procesado BOOLEAN DEFAULT FALSE,
        
        fecha_deteccion TIMESTAMP DEFAULT NOW()
    );
    
    -- ================================================
    -- ÍNDICES PARA PERFORMANCE
    -- ================================================
    CREATE INDEX IF NOT EXISTS idx_supervisiones_sucursal ON supervisiones(sucursal_numero);
    CREATE INDEX IF NOT EXISTS idx_supervisiones_fecha ON supervisiones(fecha_supervision);
    CREATE INDEX IF NOT EXISTS idx_supervisiones_periodo ON supervisiones(periodo_id);
    CREATE INDEX IF NOT EXISTS idx_supervisiones_calificacion ON supervisiones(calificacion_general);
    
    CREATE INDEX IF NOT EXISTS idx_areas_supervision ON supervision_areas(supervision_id);
    CREATE INDEX IF NOT EXISTS idx_areas_codigo ON supervision_areas(area_codigo);
    CREATE INDEX IF NOT EXISTS idx_areas_conformidad ON supervision_areas(conformidad_porcentaje);
    
    CREATE INDEX IF NOT EXISTS idx_respuestas_supervision ON supervision_respuestas(supervision_id);
    CREATE INDEX IF NOT EXISTS idx_respuestas_area ON supervision_respuestas(area_id);
    CREATE INDEX IF NOT EXISTS idx_respuestas_critico ON supervision_respuestas(es_critico, fallo_critico);
    
    -- ================================================
    -- VISTAS PARA DASHBOARD
    -- ================================================
    
    -- Vista: Resumen por sucursal
    CREATE OR REPLACE VIEW v_sucursales_dashboard AS
    SELECT 
        sm.sucursal_numero,
        sm.nombre_actual,
        sm.tipo_sucursal,
        sm.zona_operativa,
        COUNT(s.id) as total_supervisiones,
        AVG(s.calificacion_general) as calificacion_promedio,
        MAX(s.fecha_supervision) as ultima_supervision,
        COUNT(CASE WHEN s.calificacion_general < 70 THEN 1 END) as supervisiones_criticas,
        COUNT(CASE WHEN s.calificacion_general >= 90 THEN 1 END) as supervisiones_excelentes
    FROM sucursales_master sm
    LEFT JOIN supervisiones s ON sm.sucursal_numero = s.sucursal_numero
    WHERE sm.activa = TRUE
    GROUP BY sm.sucursal_numero, sm.nombre_actual, sm.tipo_sucursal, sm.zona_operativa;
    
    -- Vista: KPIs por área
    CREATE OR REPLACE VIEW v_areas_dashboard AS
    SELECT 
        sa.area_codigo,
        sa.area_nombre,
        COUNT(sa.id) as total_supervisiones,
        AVG(sa.conformidad_porcentaje) as conformidad_promedio,
        AVG(sa.completitud_porcentaje) as completitud_promedio,
        SUM(sa.elementos_no_conformes) as total_elementos_fallidos,
        SUM(sa.evidencia_fotografica) as total_evidencia
    FROM supervision_areas sa
    JOIN supervisiones s ON sa.supervision_id = s.id
    WHERE s.procesada = TRUE
    GROUP BY sa.area_codigo, sa.area_nombre;
    
    -- Vista: Alertas activas
    CREATE OR REPLACE VIEW v_alertas_dashboard AS
    SELECT 
        sa.area_codigo,
        sa.area_nombre,
        sm.nombre_actual as sucursal,
        sm.sucursal_numero,
        sa.conformidad_porcentaje,
        s.supervisor_nombre,
        s.fecha_supervision,
        CASE 
            WHEN sa.conformidad_porcentaje < 70 THEN 'CRITICA'
            WHEN sa.conformidad_porcentaje < 80 THEN 'ADVERTENCIA'
            ELSE 'OK'
        END as nivel_alerta
    FROM supervision_areas sa
    JOIN supervisiones s ON sa.supervision_id = s.id
    JOIN sucursales_master sm ON s.sucursal_numero = sm.sucursal_numero
    WHERE sa.conformidad_porcentaje < 80
    ORDER BY sa.conformidad_porcentaje ASC;
    