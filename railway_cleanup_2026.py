#!/usr/bin/env python3
"""
🧹 LIMPIA TOTAL RAILWAY POSTGRESQL 2026
Eliminar todo el mugregro y dejar base limpia y genuina
"""

import psycopg2
import csv
from datetime import datetime

# Railway PostgreSQL
DATABASE_URL = 'postgresql://postgres:qGgdIUuKYKMKGtSNYzARpyapBWHsloOt@turntable.proxy.rlwy.net:24097/railway'

def ejecutar_limpia_total():
    """LIMPIA TOTAL - Eliminar mugregro y crear estructura 2026"""
    
    print("🧹 INICIANDO LIMPIA TOTAL RAILWAY 2026")
    print("=" * 60)
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # 1. ELIMINAR MUGREGRO COMPLETO
        print("🗑️ PASO 1: ELIMINANDO MUGREGRO...")
        
        # Eliminar tablas basura
        tablas_basura = [
            'supervisions',           # Duplicada
            'supervisions_operativa', # Vacía
            'supervisions_seguridad', # Vacía
            'zenput_api_analysis',    # Basura
            'zenput_2026_readiness'   # Basura
        ]
        
        for tabla in tablas_basura:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {tabla} CASCADE;")
                print(f"  ✅ Eliminada: {tabla}")
            except Exception as e:
                print(f"  ⚠️ Error eliminando {tabla}: {e}")
        
        # Eliminar vistas viejas
        vistas_viejas = [
            'dashboard_heatmap_foraneas',
            'dashboard_heatmap_locales', 
            'dashboard_resumen_general'
        ]
        
        for vista in vistas_viejas:
            try:
                cursor.execute(f"DROP VIEW IF EXISTS {vista} CASCADE;")
                print(f"  ✅ Eliminada vista: {vista}")
            except Exception as e:
                print(f"  ⚠️ Error eliminando vista {vista}: {e}")
        
        # Limpiar sucursales demo
        cursor.execute("DELETE FROM sucursales;")
        print(f"  ✅ Limpiadas sucursales demo")
        
        # 2. CREAR ESTRUCTURA FINAL 2026
        print(f"\n🏗️ PASO 2: CREANDO ESTRUCTURA 2026...")
        
        # Tabla principal supervisiones
        cursor.execute("""
            CREATE TABLE supervisiones_2026 (
                id SERIAL PRIMARY KEY,
                submission_id VARCHAR(50) UNIQUE NOT NULL,
                form_template_id VARCHAR(20) NOT NULL,
                form_type VARCHAR(20) NOT NULL,
                
                location_id INTEGER NOT NULL,
                location_name VARCHAR(100),
                external_key VARCHAR(10),
                sucursal_id INTEGER,
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
        print(f"  ✅ Creada: supervisiones_2026")
        
        # Crear índices por separado
        indices = [
            "CREATE INDEX idx_fecha_grupo ON supervisiones_2026 (fecha_supervision, grupo_operativo);",
            "CREATE INDEX idx_form_fecha ON supervisiones_2026 (form_template_id, fecha_supervision);", 
            "CREATE INDEX idx_location_fecha ON supervisiones_2026 (location_id, fecha_supervision);",
            "CREATE INDEX idx_sucursal_fecha ON supervisiones_2026 (sucursal_id, fecha_supervision);"
        ]
        
        for indice in indices:
            cursor.execute(indice)
        print(f"  ✅ Creados: 4 índices optimizados")
        
        # Teams mapping table
        cursor.execute("""
            CREATE TABLE teams_grupos_mapping (
                team_id INTEGER PRIMARY KEY,
                grupo_operativo VARCHAR(50) NOT NULL,
                team_name VARCHAR(100),
                team_level VARCHAR(20) DEFAULT 'sucursal',
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cursor.execute("CREATE INDEX idx_grupo_operativo ON teams_grupos_mapping (grupo_operativo);")
        print(f"  ✅ Creada: teams_grupos_mapping")
        
        # Insertar mapping teams → grupos descubierto
        teams_mapping = [
            (115097, "TEPEYAC", "TEPEYAC", "director"),
            (115098, "EXPO", "EXPO", "director"), 
            (115099, "PLOG NUEVO LEON", "PLOG Nuevo Leon", "director"),
            (115100, "OGAS", "OGAS", "director"),
            (115101, "EFM", "EFM", "director"),
            (115102, "RAP", "RAP", "director"),
            (115103, "CRR", "CRR", "director"),
            (115104, "TEC", "TEC", "director"),
            (115105, "EPL SO", "EPL SO", "director"),
            (115106, "PLOG LAGUNA", "PLOG Laguna", "director"),
            (115107, "PLOG QUERETARO", "PLOG Queretaro", "director"),
            (115108, "GRUPO SALTILLO", "GRUPO SALTILLO", "director"),
            (115109, "OCHTER TAMPICO", "OCHTER Tampico", "director"),
            (115110, "GRUPO CANTERA ROSA (MORELIA)", "GRUPO CANTERA ROSA (MORELIA)", "director"),
            (115111, "GRUPO MATAMOROS", "GRUPO MATAMOROS", "director"),
            (115112, "GRUPO PIEDRAS NEGRAS", "GRUPO PIEDRAS NEGRAS", "director"), 
            (115113, "GRUPO CENTRITO", "GRUPO CENTRITO", "director"),
            (115114, "GRUPO SABINAS HIDALGO", "GRUPO SABINAS HIDALGO", "director"),
            (115115, "GRUPO RIO BRAVO", "GRUPO RIO BRAVO", "director"),
            (115116, "GRUPO NUEVO LAREDO (RUELAS)", "GRUPO NUEVO LAREDO (RUELAS)", "director")
        ]
        
        cursor.executemany("""
            INSERT INTO teams_grupos_mapping (team_id, grupo_operativo, team_name, team_level)
            VALUES (%s, %s, %s, %s)
        """, teams_mapping)
        print(f"  ✅ Insertado: {len(teams_mapping)} teams mapping")
        
        # 3. CARGAR DATOS REALES
        print(f"\n📊 PASO 3: CARGANDO DATOS REALES...")
        
        # Cargar 86 sucursales reales
        sucursales_cargadas = 0
        with open('data/86_sucursales_master.csv', 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Obtener grupo_id
                cursor.execute("SELECT id FROM grupos_operativos WHERE nombre = %s;", (row['Grupo_Operativo'],))
                grupo_result = cursor.fetchone()
                
                if grupo_result:
                    grupo_id = grupo_result[0]
                    
                    cursor.execute("""
                        INSERT INTO sucursales (
                            external_key, nombre, direccion, ciudad, estado, 
                            latitud, longitud, grupo_operativo_id, grupo_operativo_nombre,
                            is_active
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        row['Numero_Sucursal'],
                        row['Nombre_Sucursal'],
                        f"{row['Ciudad']}, {row['Estado']}", 
                        row['Ciudad'],
                        row['Estado'],
                        float(row['Latitude']) if row['Latitude'] else None,
                        float(row['Longitude']) if row['Longitude'] else None,
                        grupo_id,
                        row['Grupo_Operativo'],
                        True
                    ))
                    sucursales_cargadas += 1
        
        print(f"  ✅ Cargadas: {sucursales_cargadas} sucursales reales")
        
        # 4. CREAR VISTAS DASHBOARD 2026
        print(f"\n📊 PASO 4: CREANDO VISTAS DASHBOARD 2026...")
        
        cursor.execute("""
            CREATE VIEW dashboard_supervision_2026 AS
            SELECT 
                s.grupo_operativo,
                go.tipo_supervision,
                COUNT(*) as total_supervisiones,
                COUNT(CASE WHEN sv.form_type = 'OPERATIVA' THEN 1 END) as operativas,
                COUNT(CASE WHEN sv.form_type = 'SEGURIDAD' THEN 1 END) as seguridad,
                AVG(sv.calificacion_porcentaje) as promedio_calificacion,
                MAX(sv.fecha_supervision) as ultima_supervision
            FROM supervisiones_2026 sv
            JOIN sucursales s ON sv.sucursal_id = s.id
            JOIN grupos_operativos go ON s.grupo_operativo_id = go.id
            GROUP BY s.grupo_operativo, go.tipo_supervision;
        """)
        print(f"  ✅ Creada vista: dashboard_supervision_2026")
        
        # 5. VERIFICAR RESULTADO FINAL
        print(f"\n🔍 PASO 5: VERIFICACIÓN FINAL...")
        
        cursor.execute("SELECT COUNT(*) FROM sucursales;")
        total_sucursales = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM grupos_operativos;")
        total_grupos = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM teams_grupos_mapping;")
        total_teams = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM supervisiones_2026;")
        total_supervisiones = cursor.fetchone()[0]
        
        print(f"  📊 Sucursales: {total_sucursales}/86")
        print(f"  📊 Grupos operativos: {total_grupos}/20")
        print(f"  📊 Teams mapping: {total_teams}/20")
        print(f"  📊 Supervisiones: {total_supervisiones}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"\n🎉 LIMPIA COMPLETADA EXITOSAMENTE!")
        print("=" * 60)
        print("✅ Estructura 2026 lista")
        print("✅ Mugregro eliminado")
        print("✅ Datos reales cargados")
        print("✅ ETL production-ready")
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR EN LIMPIA: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        return False

if __name__ == "__main__":
    ejecutar_limpia_total()