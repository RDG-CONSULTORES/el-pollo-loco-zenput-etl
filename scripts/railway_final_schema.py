#!/usr/bin/env python3
"""
🚀 ESQUEMA FINAL RAILWAY POSTGRESQL
Esquema completo con estructura organizacional definitiva
"""

import psycopg2
import json
import pandas as pd
from datetime import datetime
import os

def create_railway_final_schema():
    """Crea esquema final en Railway PostgreSQL con estructura organizacional completa"""
    
    print("🚀 CREANDO ESQUEMA FINAL RAILWAY POSTGRESQL")
    print("=" * 55)
    
    # Configuración Railway (Roberto debe proporcionar estos datos)
    print("⚠️ CONFIGURACIÓN RAILWAY REQUERIDA:")
    print("Roberto debe proporcionar:")
    print("   • HOST: tu-proyecto.railway.app")
    print("   • PORT: 5432")
    print("   • DATABASE: railway")
    print("   • USER: postgres")
    print("   • PASSWORD: tu-password-railway")
    print()
    
    # Por ahora, simular la creación del esquema
    print("📋 ESQUEMA SQL DEFINITIVO:")
    print("=" * 30)
    
    # 1. ESQUEMA PRINCIPAL
    sql_schema = """
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
    """
    
    print(sql_schema)
    
    # Guardar esquema SQL
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    sql_filename = f"railway_schema_final_{timestamp}.sql"
    
    with open(sql_filename, 'w', encoding='utf-8') as f:
        f.write(sql_schema)
    
    print(f"\n💾 ESQUEMA SQL GUARDADO: {sql_filename}")
    
    return sql_filename

def create_data_loading_script():
    """Crea script para cargar datos iniciales"""
    
    print(f"\n📊 CREANDO SCRIPT CARGA DATOS INICIALES")
    print("=" * 45)
    
    loading_script = """#!/usr/bin/env python3
\"\"\"
📊 CARGA DATOS INICIALES RAILWAY
Carga estructura organizacional completa en Railway PostgreSQL
\"\"\"

import psycopg2
import json
import pandas as pd
from datetime import datetime, date

def load_initial_data():
    \"\"\"Carga todos los datos iniciales en Railway\"\"\"
    
    print("📊 CARGANDO DATOS INICIALES EN RAILWAY")
    print("=" * 45)
    
    # Configuración Railway - Roberto debe completar
    railway_config = {
        'host': 'TU_HOST_RAILWAY.railway.app',
        'port': 5432,
        'database': 'railway',
        'user': 'postgres',
        'password': 'TU_PASSWORD_RAILWAY'
    }
    
    try:
        # Conectar a Railway
        conn = psycopg2.connect(**railway_config)
        cur = conn.cursor()
        
        print("✅ Conectado a Railway PostgreSQL")
        
        # 1. CARGAR GRUPOS OPERATIVOS
        load_grupos_operativos(cur)
        
        # 2. CARGAR SUCURSALES
        load_sucursales_master(cur)
        
        # 3. CARGAR PERIODOS 2025
        load_periodos_supervision(cur)
        
        # 4. CARGAR ÁREAS DE SUPERVISIÓN
        load_supervision_areas(cur)
        
        # 5. CARGAR USUARIOS ZENPUT
        load_usuarios_zenput(cur)
        
        # Confirmar transacciones
        conn.commit()
        print("\\n🎉 DATOS INICIALES CARGADOS EXITOSAMENTE")
        
    except Exception as e:
        print(f"❌ Error cargando datos: {e}")
        if 'conn' in locals():
            conn.rollback()
    
    finally:
        if 'conn' in locals():
            conn.close()

def load_grupos_operativos(cur):
    \"\"\"Carga 20 grupos operativos\"\"\"
    
    print("\\n👥 CARGANDO GRUPOS OPERATIVOS...")
    
    # Datos desde Excel Roberto
    grupos_data = [
        ('TEPEYAC', 7, ['Nuevo León'], 'arangel@epl.mx'),
        ('EXPO', 12, ['Nuevo León', 'Tamaulipas'], None),
        ('OGAS', 10, ['Nuevo León'], 'afarfan@epl.mx'),
        ('PLOG NUEVO LEON', 8, ['Nuevo León'], 'a.aguirre@plog.com.mx'),
        ('PLOG LAGUNA', 6, ['Coahuila', 'Durango'], 'a.aguirre@plog.com.mx'),
        ('PLOG QUERETARO', 4, ['Querétaro'], 'a.aguirre@plog.com.mx'),
        ('GRUPO SALTILLO', 6, ['Coahuila'], None),
        ('GRUPO MATAMOROS', 5, ['Tamaulipas'], None),
        ('OCHTER TAMPICO', 4, ['Tamaulipas'], None),
        ('GRUPO CANTERA ROSA (MORELIA)', 3, ['Michoacán'], None),
        ('TEC', 4, ['Nuevo León', 'Sinaloa'], None),
        ('EFM', 3, ['Nuevo León'], None),
        ('CRR', 3, ['Tamaulipas'], None),
        ('RAP', 3, ['Tamaulipas'], None),
        ('EPL SO', 2, ['Nuevo León'], None),
        ('GRUPO NUEVO LAREDO (RUELAS)', 2, ['Tamaulipas'], None),
        ('GRUPO PIEDRAS NEGRAS', 1, ['Coahuila'], None),
        ('GRUPO CENTRITO', 1, ['Nuevo León'], None),
        ('GRUPO SABINAS HIDALGO', 1, ['Nuevo León'], None),
        ('GRUPO RIO BRAVO', 1, ['Tamaulipas'], None)
    ]
    
    for nombre, total, estados, director_email in grupos_data:
        cur.execute(\"\"\"
        INSERT INTO grupos_operativos (nombre, total_sucursales, estados_cobertura, director_email)
        VALUES (%s, %s, %s, %s)
        \"\"\", (nombre, total, estados, director_email))
    
    print(f"   ✅ {len(grupos_data)} grupos operativos cargados")

def load_sucursales_master(cur):
    \"\"\"Carga 86 sucursales desde Excel Roberto\"\"\"
    
    print("\\n🏪 CARGANDO SUCURSALES MASTER...")
    
    # Leer Excel Roberto
    excel_path = "/Users/robertodavila/pollo-loco-tracking-gps/grupos_operativos_final_corregido.csv"
    df = pd.read_csv(excel_path)
    
    for _, row in df.iterrows():
        # Buscar grupo_operativo_id
        cur.execute("SELECT id FROM grupos_operativos WHERE nombre = %s", (row['Grupo_Operativo'],))
        grupo_id = cur.fetchone()
        
        if grupo_id:
            grupo_id = grupo_id[0]
            
            # Insertar sucursal
            coordenadas = None
            if pd.notna(row['Latitude']) and pd.notna(row['Longitude']):
                coordenadas = f"POINT({row['Longitude']} {row['Latitude']})"
            
            cur.execute(\"\"\"
            INSERT INTO sucursales_master 
            (numero, nombre_oficial, nombre_zenput, grupo_operativo_id, ciudad, estado, coordenadas, location_code)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            \"\"\", (
                int(row['Numero_Sucursal']),
                row['Nombre_Sucursal'],
                f"{int(row['Numero_Sucursal'])} - {row['Nombre_Sucursal']}",
                grupo_id,
                row['Ciudad'],
                row['Estado'],
                coordenadas,
                row.get('Location_Code')
            ))
    
    print(f"   ✅ {len(df)} sucursales cargadas")

def load_periodos_supervision(cur):
    \"\"\"Carga períodos de supervisión 2025\"\"\"
    
    print("\\n📅 CARGANDO PERÍODOS SUPERVISIÓN 2025...")
    
    periodos_2025 = [
        (2025, 'TRIMESTRE', 1, 'T1', '2025-01-01', '2025-03-31', 'Primer Trimestre 2025'),
        (2025, 'TRIMESTRE', 2, 'T2', '2025-04-01', '2025-06-30', 'Segundo Trimestre 2025'),
        (2025, 'TRIMESTRE', 3, 'T3', '2025-07-01', '2025-09-30', 'Tercer Trimestre 2025'),
        (2025, 'TRIMESTRE', 4, 'T4', '2025-10-01', '2025-12-31', 'Cuarto Trimestre 2025'),
        (2025, 'SEMESTRE', 1, 'S1', '2025-01-01', '2025-06-30', 'Primer Semestre 2025'),
        (2025, 'SEMESTRE', 2, 'S2', '2025-07-01', '2025-12-31', 'Segundo Semestre 2025')
    ]
    
    for year, tipo, numero, codigo, inicio, fin, desc in periodos_2025:
        cur.execute(\"\"\"
        INSERT INTO periodos_supervision (year, tipo, numero, codigo, fecha_inicio, fecha_fin, descripcion)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        \"\"\", (year, tipo, numero, codigo, inicio, fin, desc))
    
    print(f"   ✅ {len(periodos_2025)} períodos cargados")

def load_supervision_areas(cur):
    \"\"\"Carga 43 áreas de supervisión\"\"\"
    
    print("\\n📋 CARGANDO ÁREAS DE SUPERVISIÓN...")
    
    # Áreas Form 877139 - Control Operativo Seguridad (12 áreas)
    areas_seguridad = [
        'GENERAL', 'UNIFORME', 'APARIENCIA PERSONAL', 'COFIA Y MALLA',
        'LAVADO DE MANOS', 'RECEPCION DE ALIMENTOS', 'ALMACENAMIENTO',
        'PROCESO DE ALIMENTOS', 'EQUIPO DE LIMPIEZA', 'PREVENCION CONTAMINACION',
        'TEMPERATURA DE ALIMENTOS', 'SANITIZADO'
    ]
    
    for i, area in enumerate(areas_seguridad, 1):
        cur.execute(\"\"\"
        INSERT INTO supervision_areas (form_id, area_nombre, area_orden)
        VALUES (%s, %s, %s)
        \"\"\", (877139, area, i))
    
    # Áreas Form 877138 - Supervisión Operativa (31 áreas principales)
    areas_operativa = [
        'GENERAL', 'UNIFORME', 'APARIENCIA PERSONAL', 'INICIO DE OPERACION',
        'PROCESO DE COCINADO', 'CALIDAD DEL PRODUCTO', 'BEBIDAS',
        'PROCESO FRITANGA', 'PROCESO PARRILLA', 'EMPACADO Y ENTREGA',
        'ATENCION AL CLIENTE', 'CAJA DINERO', 'PROCESO LIMPIEZA',
        'LIMPIEZA COCINA', 'LIMPIEZA SALON', 'LIMPIEZA BAÑOS',
        'MANTENIMIENTO PREVENTIVO', 'TEMPERATURA EQUIPOS', 'INVENTARIO',
        'BODEGA SECOS', 'CUARTO FRIO', 'ALMACENAMIENTO VARIOS',
        'LIMPIEZA ALMACEN', 'PROTECCION CIVIL', 'SEGURIDAD',
        'DOCUMENTOS VARIOS', 'CAPACITACION', 'CIERRE OPERACION',
        'SEGURIDAD ADMINISTRATIVA', 'SISTEMA PUNTO VENTA', 'AUTOLOCO'
    ]
    
    for i, area in enumerate(areas_operativa, 1):
        cur.execute(\"\"\"
        INSERT INTO supervision_areas (form_id, area_nombre, area_orden)
        VALUES (%s, %s, %s)
        \"\"\", (877138, area, i))
    
    print(f"   ✅ {len(areas_seguridad)} áreas seguridad + {len(areas_operativa)} áreas operativa cargadas")

def load_usuarios_zenput(cur):
    \"\"\"Carga usuarios desde API datos\"\"\"
    
    print("\\n👤 CARGANDO USUARIOS ZENPUT...")
    
    # Leer datos de usuarios extraídos
    try:
        with open('data/users_data_20251217_182215.json', 'r') as f:
            users_data = json.load(f)
        
        for user in users_data['usuarios']:
            cur.execute(\"\"\"
            INSERT INTO usuarios_zenput 
            (id, username, nombre_completo, email, telefono, grupos, teams, 
             sucursales_asignadas, default_team_id, default_team_name, raw_user_data)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            \"\"\", (
                user['id'],
                user.get('username'),
                user.get('display_name'),
                user.get('email'),
                user.get('sms_number'),
                json.dumps(user.get('groups', [])),
                json.dumps(user.get('teams', [])),
                json.dumps(user.get('owned_locations', [])),
                user.get('default_team', {}).get('id'),
                user.get('default_team', {}).get('name'),
                json.dumps(user)
            ))
        
        print(f"   ✅ {len(users_data['usuarios'])} usuarios cargados")
        
    except FileNotFoundError:
        print("   ⚠️ Archivo usuarios no encontrado, cargar manualmente")

if __name__ == "__main__":
    print("📊 INICIANDO CARGA DATOS INICIALES RAILWAY")
    print("Roberto debe configurar credenciales Railway antes de ejecutar")
    print()
    
    load_initial_data()
"""
    
    script_filename = f"railway_load_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.py"
    
    with open(script_filename, 'w', encoding='utf-8') as f:
        f.write(loading_script)
    
    print(f"✅ Script carga datos: {script_filename}")
    
    return script_filename

def generate_railway_instructions():
    """Genera instrucciones detalladas para Roberto"""
    
    print(f"\n📋 GENERANDO INSTRUCCIONES RAILWAY PARA ROBERTO")
    print("=" * 55)
    
    instructions = """
# 🚀 INSTRUCCIONES RAILWAY POSTGRESQL - EL POLLO LOCO MÉXICO

## 1. CREAR PROYECTO RAILWAY

1. Ve a https://railway.app
2. Crea cuenta o inicia sesión
3. **New Project** → **Provision PostgreSQL**
4. Nombra el proyecto: `el-pollo-loco-zenput-etl`

## 2. OBTENER CREDENCIALES

Una vez creada la base de datos PostgreSQL:

1. Ve a tu proyecto en Railway
2. Click en la base de datos PostgreSQL
3. Ve a **Connect** tab
4. Copia las credenciales:

```
HOST: xxxxx.railway.app
PORT: 5432  
DATABASE: railway
USER: postgres
PASSWORD: [tu-password-aquí]
```

## 3. CONFIGURAR ESQUEMA

### Opción A: Usando Railway GUI
1. En Railway, ve a **Data** tab
2. Copia y pega el contenido del archivo `railway_schema_final_[timestamp].sql`
3. Click **Execute**

### Opción B: Usando psql (Terminal)
```bash
psql postgresql://postgres:[password]@[host]:5432/railway -f railway_schema_final_[timestamp].sql
```

## 4. CARGAR DATOS INICIALES

1. Edita el archivo `railway_load_data_[timestamp].py`
2. Actualiza las credenciales Railway:
   ```python
   railway_config = {
       'host': 'TU_HOST_RAILWAY.railway.app',  # Cambiar
       'port': 5432,
       'database': 'railway',
       'user': 'postgres',
       'password': 'TU_PASSWORD_RAILWAY'  # Cambiar
   }
   ```
3. Ejecuta el script:
   ```bash
   python3 railway_load_data_[timestamp].py
   ```

## 5. VERIFICAR INSTALACIÓN

Después de ejecutar, deberías tener:

### Datos Cargados:
- ✅ **20 grupos operativos** (TEPEYAC, EXPO, OGAS, etc.)
- ✅ **86 sucursales** con GPS coordinates
- ✅ **6 períodos 2025** (T1-T4, S1-S2)
- ✅ **43 áreas supervisión** (31 operativa + 12 seguridad)
- ✅ **20 usuarios Zenput** con teams asignados

### Consultas de Verificación:
```sql
-- Verificar grupos operativos
SELECT nombre, total_sucursales, director_email FROM grupos_operativos ORDER BY total_sucursales DESC;

-- Verificar sucursales por estado  
SELECT estado, COUNT(*) FROM sucursales_master GROUP BY estado ORDER BY COUNT(*) DESC;

-- Verificar períodos 2025
SELECT codigo, descripcion, fecha_inicio, fecha_fin FROM periodos_supervision ORDER BY fecha_inicio;

-- Verificar áreas de supervisión
SELECT form_id, COUNT(*) as areas FROM supervision_areas GROUP BY form_id;

-- Verificar usuarios con teams
SELECT email, default_team_name, JSON_ARRAY_LENGTH(teams) as num_teams FROM usuarios_zenput;
```

## 6. PRÓXIMOS PASOS

Una vez verificada la instalación:

1. **✅ Base Railway lista** 
2. **📊 Implementar ETL supervisiones** - Extraer 238+ supervisiones existentes
3. **📈 Dashboard básico** - Vistas por grupo operativo
4. **🔄 ETL automático** - Programar extracción diaria/semanal
5. **📱 Alertas WhatsApp** - Sistema de notificaciones Twilio

## 7. TROUBLESHOOTING

### Error de conexión:
- Verificar credenciales Railway
- Asegurar que Railway PostgreSQL esté corriendo
- Verificar firewall/VPN

### Error de permisos:
- El usuario `postgres` debe tener permisos completos
- Railway maneja permisos automáticamente

### Datos faltantes:
- Verificar que archivos JSON/CSV estén en directorio `data/`
- Re-ejecutar scripts de extracción si es necesario

## 8. ESTRUCTURA FINAL

```
Grupos Operativos (20)
    └── Sucursales (86) 
        └── Supervisiones (877138 + 877139)
            └── Áreas (43 total)
                └── Respuestas detalladas
                
Usuarios Zenput (20)
    └── Teams asignados
    └── Sucursales owned_locations
```

## 🎯 RESULTADO ESPERADO

Dashboard operativo con:
- **Vista por grupo operativo** - Performance por director
- **Vista por sucursal** - Histórico supervisiones  
- **Vista por período** - T1-T4, S1-S2 comparativos
- **Vista por área** - Identificar áreas problemáticas
- **Alertas automáticas** - WhatsApp para supervisiones pendientes

---

**Roberto**: Una vez tengas Railway funcionando, continúamos con la implementación del ETL completo para extraer las 238+ supervisiones existentes.
"""
    
    instructions_file = f"RAILWAY_INSTRUCTIONS_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    
    with open(instructions_file, 'w', encoding='utf-8') as f:
        f.write(instructions)
    
    print(f"✅ Instrucciones Railway: {instructions_file}")
    
    return instructions_file

if __name__ == "__main__":
    print("🚀 GENERANDO ESQUEMA FINAL RAILWAY")
    print("Estructura organizacional completa para PostgreSQL")
    print()
    
    # Crear esquema SQL
    schema_file = create_railway_final_schema()
    
    # Crear script de carga
    loading_file = create_data_loading_script()
    
    # Generar instrucciones
    instructions_file = generate_railway_instructions()
    
    print(f"\n🎉 ESQUEMA RAILWAY COMPLETO GENERADO")
    print("=" * 45)
    print(f"📁 Esquema SQL: {schema_file}")
    print(f"📁 Script carga: {loading_file}")  
    print(f"📁 Instrucciones: {instructions_file}")
    print()
    print(f"🎯 PRÓXIMO PASO PARA ROBERTO:")
    print(f"   1. Crear proyecto Railway PostgreSQL")
    print(f"   2. Ejecutar esquema SQL")
    print(f"   3. Cargar datos iniciales")
    print(f"   4. Verificar estructura completa")
    print(f"   5. Continuar con ETL supervisiones")