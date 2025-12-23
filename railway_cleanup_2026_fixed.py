#!/usr/bin/env python3
"""
🧹 LIMPIA TOTAL RAILWAY POSTGRESQL 2026 - CORREGIDA
Eliminar todo el mugregro y cargar estructura REAL de Roberto
"""

import psycopg2
import csv
from datetime import datetime

# Railway PostgreSQL
DATABASE_URL = 'postgresql://postgres:qGgdIUuKYKMKGtSNYzARpyapBWHsloOt@turntable.proxy.rlwy.net:24097/railway'

def ejecutar_limpia_corregida():
    """LIMPIA TOTAL CORREGIDA - Estructura real EPL 2026"""
    
    print("🧹 INICIANDO LIMPIA TOTAL RAILWAY 2026 - CORREGIDA")
    print("=" * 60)
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # 1. ELIMINAR MUGREGRO COMPLETO
        print("🗑️ PASO 1: ELIMINANDO TODO EL MUGREGRO...")
        
        # Eliminar TODAS las tablas
        cursor.execute("""
            SELECT tablename FROM pg_tables WHERE schemaname = 'public';
        """)
        todas_tablas = cursor.fetchall()
        
        for tabla in todas_tablas:
            tabla_name = tabla[0]
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {tabla_name} CASCADE;")
                print(f"  ✅ Eliminada: {tabla_name}")
            except Exception as e:
                print(f"  ⚠️ Error eliminando {tabla_name}: {e}")
        
        # Eliminar vistas
        cursor.execute("""
            SELECT viewname FROM pg_views WHERE schemaname = 'public';
        """)
        todas_vistas = cursor.fetchall()
        
        for vista in todas_vistas:
            vista_name = vista[0]
            try:
                cursor.execute(f"DROP VIEW IF EXISTS {vista_name} CASCADE;")
                print(f"  ✅ Eliminada vista: {vista_name}")
            except Exception as e:
                print(f"  ⚠️ Error eliminando vista {vista_name}: {e}")
        
        # 2. CREAR ESTRUCTURA FINAL LIMPIA
        print(f"\n🏗️ PASO 2: CREANDO ESTRUCTURA FINAL 2026...")
        
        # Tabla grupos operativos REAL
        cursor.execute("""
            CREATE TABLE grupos_operativos (
                id SERIAL PRIMARY KEY,
                nombre VARCHAR(50) UNIQUE NOT NULL,
                clasificacion VARCHAR(20) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        print(f"  ✅ Creada: grupos_operativos")
        
        # Tabla sucursales REAL
        cursor.execute("""
            CREATE TABLE sucursales (
                id SERIAL PRIMARY KEY,
                external_key VARCHAR(10) UNIQUE NOT NULL,
                location_code INTEGER,
                numero_sucursal INTEGER NOT NULL,
                nombre VARCHAR(100) NOT NULL,
                ciudad VARCHAR(100),
                estado VARCHAR(50),
                latitud DECIMAL(10,7),
                longitud DECIMAL(10,7),
                grupo_operativo_id INTEGER REFERENCES grupos_operativos(id),
                grupo_operativo_nombre VARCHAR(50),
                is_active BOOLEAN DEFAULT true,
                synced_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cursor.execute("CREATE INDEX idx_external_key ON sucursales (external_key);")
        cursor.execute("CREATE INDEX idx_location_code ON sucursales (location_code);")
        cursor.execute("CREATE INDEX idx_grupo_operativo ON sucursales (grupo_operativo_nombre);")
        print(f"  ✅ Creada: sucursales")
        
        # Tabla supervisiones final
        cursor.execute("""
            CREATE TABLE supervisiones_2026 (
                id SERIAL PRIMARY KEY,
                submission_id VARCHAR(50) UNIQUE NOT NULL,
                form_template_id VARCHAR(20) NOT NULL,
                form_type VARCHAR(20) NOT NULL,
                
                location_id INTEGER NOT NULL,
                location_name VARCHAR(100),
                external_key VARCHAR(10),
                sucursal_id INTEGER REFERENCES sucursales(id),
                grupo_operativo VARCHAR(50) NOT NULL,
                
                teams_ids INTEGER[],
                team_primary INTEGER,
                
                auditor_id INTEGER,
                auditor_nombre VARCHAR(100),
                auditor_email VARCHAR(100),
                
                fecha_supervision DATE NOT NULL,
                fecha_submission TIMESTAMP NOT NULL,
                time_to_complete INTEGER,
                
                puntos_maximos INTEGER DEFAULT 0,
                puntos_obtenidos INTEGER DEFAULT 0,
                calificacion_porcentaje DECIMAL(5,2) DEFAULT 0.0,
                score_zenput DECIMAL(5,2),
                
                latitude DECIMAL(10,7),
                longitude DECIMAL(10,7),
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Índices supervisiones
        indices = [
            "CREATE INDEX idx_fecha_grupo ON supervisiones_2026 (fecha_supervision, grupo_operativo);",
            "CREATE INDEX idx_form_fecha ON supervisiones_2026 (form_template_id, fecha_supervision);", 
            "CREATE INDEX idx_location_fecha ON supervisiones_2026 (location_id, fecha_supervision);",
            "CREATE INDEX idx_sucursal_fecha ON supervisiones_2026 (sucursal_id, fecha_supervision);"
        ]
        
        for indice in indices:
            cursor.execute(indice)
        print(f"  ✅ Creada: supervisiones_2026 con 4 índices")
        
        # Teams mapping table
        cursor.execute("""
            CREATE TABLE teams_grupos_mapping (
                team_id INTEGER PRIMARY KEY,
                grupo_operativo VARCHAR(50) NOT NULL,
                team_name VARCHAR(100),
                team_level VARCHAR(20) DEFAULT 'director',
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cursor.execute("CREATE INDEX idx_grupo_operativo_mapping ON teams_grupos_mapping (grupo_operativo);")
        print(f"  ✅ Creada: teams_grupos_mapping")
        
        # 3. CARGAR DATOS REALES DE ROBERTO
        print(f"\n📊 PASO 3: CARGANDO DATOS REALES...")
        
        # Leer CSV para extraer grupos únicos
        grupos_reales = set()
        with open('data/86_sucursales_master.csv', 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                grupos_reales.add(row['Grupo_Operativo'])
        
        # Cargar grupos operativos reales
        grupos_loaded = 0
        for grupo_nombre in sorted(grupos_reales):
            # Clasificar según patrón PLOG = FORANEA, otros = LOCAL
            if 'PLOG' in grupo_nombre or 'GRUPO' in grupo_nombre or 'OCHTER' in grupo_nombre:
                clasificacion = 'FORANEA'
            else:
                clasificacion = 'LOCAL'
            
            cursor.execute("""
                INSERT INTO grupos_operativos (nombre, clasificacion)
                VALUES (%s, %s)
            """, (grupo_nombre, clasificacion))
            grupos_loaded += 1
        
        print(f"  ✅ Cargados: {grupos_loaded} grupos operativos reales")
        
        # Cargar 86 sucursales reales
        sucursales_loaded = 0
        with open('data/86_sucursales_master.csv', 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Obtener grupo_id
                cursor.execute("SELECT id FROM grupos_operativos WHERE nombre = %s;", (row['Grupo_Operativo'],))
                grupo_result = cursor.fetchone()
                
                if grupo_result:
                    grupo_id = grupo_result[0]
                    
                    # Parse fecha
                    synced_at = None
                    if row.get('Synced_At'):
                        try:
                            synced_at = datetime.fromisoformat(row['Synced_At'].replace('Z', '+00:00'))
                        except:
                            pass
                    
                    cursor.execute("""
                        INSERT INTO sucursales (
                            external_key, location_code, numero_sucursal, nombre,
                            ciudad, estado, latitud, longitud, 
                            grupo_operativo_id, grupo_operativo_nombre,
                            is_active, synced_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        row['Numero_Sucursal'],
                        int(row['Location_Code']) if row['Location_Code'] else None,
                        int(row['Numero_Sucursal']),
                        row['Nombre_Sucursal'],
                        row['Ciudad'],
                        row['Estado'],
                        float(row['Latitude']) if row['Latitude'] else None,
                        float(row['Longitude']) if row['Longitude'] else None,
                        grupo_id,
                        row['Grupo_Operativo'],
                        True,
                        synced_at
                    ))
                    sucursales_loaded += 1
        
        print(f"  ✅ Cargadas: {sucursales_loaded} sucursales reales")
        
        # Insertar teams mapping descubierto
        teams_mapping = [
            (115097, "TEPEYAC", "TEPEYAC"),
            (115098, "EXPO", "EXPO"), 
            (115099, "PLOG NUEVO LEON", "PLOG Nuevo Leon"),
            (115100, "OGAS", "OGAS"),
            (115101, "EFM", "EFM"),
            (115102, "RAP", "RAP"),
            (115103, "CRR", "CRR"),
            (115104, "TEC", "TEC"),
            (115105, "EPL SO", "EPL SO"),
            (115106, "PLOG LAGUNA", "PLOG Laguna"),
            (115107, "PLOG QUERETARO", "PLOG Queretaro"),
            (115108, "GRUPO SALTILLO", "GRUPO SALTILLO"),
            (115109, "OCHTER TAMPICO", "OCHTER Tampico"),
            (115110, "GRUPO CANTERA ROSA (MORELIA)", "GRUPO CANTERA ROSA (MORELIA)"),
            (115111, "GRUPO MATAMOROS", "GRUPO MATAMOROS"),
            (115112, "GRUPO PIEDRAS NEGRAS", "GRUPO PIEDRAS NEGRAS"), 
            (115113, "GRUPO CENTRITO", "GRUPO CENTRITO"),
            (115114, "GRUPO SABINAS HIDALGO", "GRUPO SABINAS HIDALGO"),
            (115115, "GRUPO RIO BRAVO", "GRUPO RIO BRAVO"),
            (115116, "GRUPO NUEVO LAREDO (RUELAS)", "GRUPO NUEVO LAREDO (RUELAS)")
        ]
        
        cursor.executemany("""
            INSERT INTO teams_grupos_mapping (team_id, grupo_operativo, team_name)
            VALUES (%s, %s, %s)
        """, teams_mapping)
        print(f"  ✅ Insertado: {len(teams_mapping)} teams mapping")
        
        # 4. CREAR VISTA DASHBOARD
        print(f"\n📊 PASO 4: CREANDO VISTA DASHBOARD...")
        
        cursor.execute("""
            CREATE VIEW dashboard_supervision_2026 AS
            SELECT 
                s.grupo_operativo_nombre as grupo_operativo,
                go.clasificacion,
                COUNT(DISTINCT s.id) as total_sucursales,
                COUNT(DISTINCT sv.id) as total_supervisiones,
                COUNT(CASE WHEN sv.form_type = 'OPERATIVA' THEN 1 END) as operativas,
                COUNT(CASE WHEN sv.form_type = 'SEGURIDAD' THEN 1 END) as seguridad,
                COALESCE(AVG(sv.calificacion_porcentaje), 0) as promedio_calificacion,
                MAX(sv.fecha_supervision) as ultima_supervision
            FROM sucursales s
            JOIN grupos_operativos go ON s.grupo_operativo_id = go.id
            LEFT JOIN supervisiones_2026 sv ON s.id = sv.sucursal_id
            GROUP BY s.grupo_operativo_nombre, go.clasificacion
            ORDER BY go.clasificacion, s.grupo_operativo_nombre;
        """)
        print(f"  ✅ Creada vista: dashboard_supervision_2026")
        
        # 5. VERIFICACIÓN FINAL
        print(f"\n🔍 PASO 5: VERIFICACIÓN FINAL...")
        
        cursor.execute("SELECT COUNT(*) FROM grupos_operativos;")
        total_grupos = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM sucursales;")
        total_sucursales = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM teams_grupos_mapping;")
        total_teams = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM supervisiones_2026;")
        total_supervisiones = cursor.fetchone()[0]
        
        # Mostrar grupos por clasificación
        cursor.execute("SELECT clasificacion, COUNT(*) FROM grupos_operativos GROUP BY clasificacion;")
        clasificaciones = cursor.fetchall()
        
        print(f"  📊 Grupos operativos: {total_grupos}/20")
        for clasif, count in clasificaciones:
            print(f"      {clasif}: {count} grupos")
        print(f"  📊 Sucursales: {total_sucursales}/86")
        print(f"  📊 Teams mapping: {total_teams}/20")
        print(f"  📊 Supervisiones: {total_supervisiones} (vacía, lista para ETL)")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"\n🎉 LIMPIA COMPLETADA EXITOSAMENTE!")
        print("=" * 60)
        print("✅ Estructura 2026 lista y corregida")
        print("✅ Mugregro eliminado completamente")
        print("✅ Datos REALES de Roberto cargados")
        print("✅ Mapping teams → grupos configurado")
        print("✅ Base lista para ETL definitivo")
        print("🚀 PRÓXIMO: Ejecutar ETL para cargar las 476 supervisiones")
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR EN LIMPIA: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        return False

if __name__ == "__main__":
    ejecutar_limpia_corregida()