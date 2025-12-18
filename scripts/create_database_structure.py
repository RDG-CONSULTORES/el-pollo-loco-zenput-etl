#!/usr/bin/env python3
"""
🗄️ ESTRUCTURA DE BASE DE DATOS - EL POLLO LOCO SUPERVISIONES
Crear tablas PostgreSQL para dashboard de supervisiones
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from datetime import datetime
import json

def create_database_schema():
    """Crea la estructura completa de base de datos"""
    
    print("🗄️ CREANDO ESTRUCTURA DE BASE DE DATOS")
    print("=" * 60)
    
    # SQL para crear tablas
    schema_sql = """
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
    """
    
    return schema_sql

def main():
    """Función principal"""
    
    print("📋 GENERANDO ESTRUCTURA DE BASE DE DATOS")
    print("=" * 60)
    
    # Generar SQL
    schema = create_database_schema()
    
    # Guardar en archivo
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"sql/database_schema_{timestamp}.sql"
    
    # Crear directorio si no existe
    os.makedirs('sql', exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(f"-- ESTRUCTURA DE BASE DE DATOS - EL POLLO LOCO SUPERVISIONES\n")
        f.write(f"-- Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"-- Archivo: {output_file}\n\n")
        f.write(schema)
    
    print(f"✅ Estructura SQL generada en: {output_file}")
    
    # Generar también script de configuración inicial
    generate_initial_data_script()
    
    return True

def generate_initial_data_script():
    """Genera script con datos iniciales"""
    
    initial_data = """
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
    """
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"sql/initial_data_{timestamp}.sql"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(initial_data)
    
    print(f"✅ Datos iniciales generados en: {output_file}")

if __name__ == "__main__":
    main()