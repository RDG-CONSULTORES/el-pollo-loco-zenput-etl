#!/usr/bin/env python3
"""
🚀 RAILWAY DEPLOYMENT SCRIPT
Conecta a Railway PostgreSQL y ejecuta schema completo
"""

import psycopg2
import os
from datetime import datetime, date
import json

# CONFIGURACIÓN RAILWAY - Roberto's PostgreSQL Credentials
RAILWAY_CONFIG = {
    'host': 'turntable.proxy.rlwy.net',
    'port': '24097', 
    'database': 'railway',
    'user': 'postgres',
    'password': 'qGgdIUuKYKMKGtSNYzARpyapBWHsloOt'
}

def conectar_railway():
    """Conectar a Railway PostgreSQL"""
    try:
        conn = psycopg2.connect(**RAILWAY_CONFIG)
        print("✅ Conexión exitosa a Railway PostgreSQL")
        return conn
    except Exception as e:
        print(f"❌ Error conectando a Railway: {e}")
        return None

def ejecutar_schema_completo(conn):
    """Ejecutar el schema completo en Railway"""
    
    print("📊 Ejecutando schema completo en Railway...")
    
    try:
        cursor = conn.cursor()
        
        # Leer schema completo
        with open('/Users/robertodavila/el-pollo-loco-zenput-etl/railway_schema_final.sql', 'r') as f:
            schema_sql = f.read()
        
        # Ejecutar schema
        cursor.execute(schema_sql)
        conn.commit()
        
        print("✅ Schema ejecutado correctamente en Railway")
        
        # Verificar tablas creadas
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        
        tablas = cursor.fetchall()
        print(f"\n📋 TABLAS CREADAS EN RAILWAY:")
        for tabla in tablas:
            print(f"   ✅ {tabla[0]}")
            
        cursor.close()
        return True
        
    except Exception as e:
        print(f"❌ Error ejecutando schema: {e}")
        conn.rollback()
        return False

def cargar_datos_iniciales(conn):
    """Cargar grupos, sucursales, directores y períodos"""
    
    print("\n📋 Cargando datos iniciales...")
    
    try:
        cursor = conn.cursor()
        
        # 1. CARGAR GRUPOS OPERATIVOS
        print("   📁 Cargando grupos operativos...")
        
        # Leer clasificación corregida
        with open('/Users/robertodavila/el-pollo-loco-zenput-etl/data/clasificacion_corregida_20251217_180947.json', 'r') as f:
            clasificacion = json.load(f)
        
        grupos_sql = """
            INSERT INTO grupos_operativos (nombre, tipo_supervision) VALUES
        """
        
        grupos_data = []
        for grupo in clasificacion['grupos_clasificados'].values():
            nombre = grupo['nombre']
            tipo = grupo['clasificacion']
            grupos_data.append(f"('{nombre}', '{tipo}')")
        
        grupos_sql += ",\n".join(grupos_data) + ";"
        
        cursor.execute(grupos_sql)
        print(f"   ✅ {len(grupos_data)} grupos operativos cargados")
        
        # 2. CARGAR SUCURSALES
        print("   🏪 Cargando sucursales...")
        
        # Leer sucursales con clasificación GPS
        with open('/Users/robertodavila/el-pollo-loco-zenput-etl/data/sucursales_clasificadas_20251217_180946.json', 'r') as f:
            sucursales = json.load(f)
        
        sucursales_sql = """
            INSERT INTO sucursales (
                id, nombre, nombre_completo, direccion, ciudad, estado, 
                codigo_postal, latitud, longitud, grupo_operativo_nombre, 
                tipo_ubicacion, external_key
            ) VALUES
        """
        
        sucursales_data = []
        for sucursal in sucursales:
            id_suc = sucursal['id']
            nombre = sucursal['name'].replace("'", "''")
            direccion = (sucursal.get('address', '') or '').replace("'", "''")
            ciudad = (sucursal.get('city', '') or '').replace("'", "''")
            estado = (sucursal.get('state', '') or '').replace("'", "''")
            codigo_postal = sucursal.get('zip_code', '') or ''
            latitud = sucursal.get('latitude')
            longitud = sucursal.get('longitude')
            grupo_nombre = sucursal.get('group_name', '').replace("'", "''")
            tipo_ubicacion = sucursal.get('clasificacion', 'LOCAL')
            external_key = sucursal.get('external_key', '')
            
            sucursales_data.append(f"""
                ({id_suc}, '{nombre}', '{nombre}', '{direccion}', 
                 '{ciudad}', '{estado}', '{codigo_postal}', 
                 {latitud if latitud else 'NULL'}, {longitud if longitud else 'NULL'}, 
                 '{grupo_nombre}', '{tipo_ubicacion}', '{external_key}')
            """)
        
        sucursales_sql += ",".join(sucursales_data) + ";"
        
        cursor.execute(sucursales_sql)
        print(f"   ✅ {len(sucursales_data)} sucursales cargadas")
        
        # 3. CARGAR PERÍODOS 2025
        print("   📅 Cargando períodos 2025...")
        
        periodos_sql = """
            INSERT INTO periodos_supervision (
                nombre, descripcion, fecha_inicio, fecha_fin, tipo, aplicable_a
            ) VALUES
            -- TRIMESTRALES (Locales)
            ('T1', 'Trimestral 1 - Locales NL + Saltillo', '2025-03-12', '2025-04-16', 'TRIMESTRAL', 'LOCAL'),
            ('T2', 'Trimestral 2 - Locales NL + Saltillo', '2025-06-11', '2025-08-18', 'TRIMESTRAL', 'LOCAL'),
            ('T3', 'Trimestral 3 - Locales NL + Saltillo', '2025-08-19', '2025-10-29', 'TRIMESTRAL', 'LOCAL'),
            ('T4', 'Trimestral 4 - Locales NL + Saltillo', '2025-10-30', '2025-12-31', 'TRIMESTRAL', 'LOCAL'),
            -- SEMESTRALES (Foráneas)
            ('S1', 'Semestral 1 - Foráneas fuera NL', '2025-04-10', '2025-06-09', 'SEMESTRAL', 'FORANEA'),
            ('S2', 'Semestral 2 - Foráneas fuera NL', '2025-07-30', '2025-11-07', 'SEMESTRAL', 'FORANEA');
        """
        
        cursor.execute(periodos_sql)
        print(f"   ✅ 6 períodos de supervisión cargados")
        
        # 4. ACTUALIZAR FOREIGN KEYS
        print("   🔗 Actualizando foreign keys...")
        
        cursor.execute("""
            UPDATE sucursales s SET grupo_operativo_id = g.id
            FROM grupos_operativos g 
            WHERE s.grupo_operativo_nombre = g.nombre;
        """)
        
        print(f"   ✅ Foreign keys actualizadas")
        
        conn.commit()
        cursor.close()
        
        print("✅ Datos iniciales cargados correctamente")
        return True
        
    except Exception as e:
        print(f"❌ Error cargando datos iniciales: {e}")
        conn.rollback()
        return False

def main():
    """Script principal de deployment Railway"""
    
    print("🚀 INICIANDO DEPLOYMENT EN RAILWAY")
    print("=" * 50)
    
    # Verificar credenciales
    if RAILWAY_CONFIG['host'] == 'YOUR_RAILWAY_HOST':
        print("❌ CONFIGURAR CREDENCIALES RAILWAY PRIMERO")
        print("\nInstrucciones:")
        print("1. Ve a Railway → Tu proyecto → PostgreSQL → Variables")
        print("2. Copia las credenciales de conexión")
        print("3. Reemplaza los valores en RAILWAY_CONFIG")
        return
    
    # Conectar a Railway
    conn = conectar_railway()
    if not conn:
        return
    
    try:
        # 1. Ejecutar schema
        if not ejecutar_schema_completo(conn):
            return
        
        # 2. Cargar datos iniciales
        if not cargar_datos_iniciales(conn):
            return
        
        print(f"\n🎉 DEPLOYMENT RAILWAY EXITOSO!")
        print("=" * 40)
        print("✅ Schema PostgreSQL ejecutado")
        print("✅ Grupos operativos cargados")
        print("✅ Sucursales con GPS cargadas") 
        print("✅ Períodos 2025 configurados")
        print("✅ Foreign keys actualizadas")
        print("✅ Vistas dashboard creadas")
        
        print(f"\n📊 PRÓXIMO PASO: ETL Supervisiones")
        print("Ahora ejecutar ETL para extraer TODAS las supervisiones 2025")
        
    except Exception as e:
        print(f"❌ Error en deployment: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()