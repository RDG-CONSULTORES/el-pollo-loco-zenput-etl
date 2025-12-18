#!/usr/bin/env python3
"""
📋 RAILWAY LOAD INITIAL DATA
Carga datos iniciales esenciales en Railway PostgreSQL
"""

import psycopg2
import requests
import json
from datetime import datetime

# CONFIGURACIÓN RAILWAY - Roberto's PostgreSQL Credentials
RAILWAY_CONFIG = {
    'host': 'turntable.proxy.rlwy.net',
    'port': '24097', 
    'database': 'railway',
    'user': 'postgres',
    'password': 'qGgdIUuKYKMKGtSNYzARpyapBWHsloOt'
}

# CONFIGURACIÓN ZENPUT
ZENPUT_CONFIG = {
    'base_url': 'https://api.zenput.com/api/v3',
    'headers': {'X-API-TOKEN': 'e52c41a1-c026-42fb-8264-d8a6e7c2aeb5'}
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

def obtener_sucursales_zenput():
    """Obtener sucursales desde Zenput API"""
    print("🏪 Obteniendo sucursales desde Zenput...")
    
    try:
        url = f"{ZENPUT_CONFIG['base_url']}/locations"
        params = {'per_page': 100}
        
        response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params)
        
        if response.status_code == 200:
            data = response.json()
            locations = data.get('locations', [])
            print(f"   ✅ {len(locations)} sucursales obtenidas")
            return locations
        else:
            print(f"   ❌ Error API: {response.status_code}")
            return []
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return []

def clasificar_ubicacion_gps(latitude, longitude):
    """Clasificar ubicación basado en coordenadas GPS"""
    if not latitude or not longitude:
        return 'LOCAL'  # Default para sucursales sin GPS
    
    lat = float(latitude)
    lon = float(longitude)
    
    # Nuevo León + Saltillo = LOCAL
    # Nuevo León: Aprox 24.5-27.0 lat, -101.5 a -98.5 lon
    # Saltillo: Aprox 25.3-25.5 lat, -101.1 a -100.8 lon
    if (24.5 <= lat <= 27.0 and -101.5 <= lon <= -98.5) or \
       (25.3 <= lat <= 25.5 and -101.1 <= lon <= -100.8):
        return 'LOCAL'
    else:
        return 'FORANEA'

def cargar_datos_iniciales(conn):
    """Cargar datos iniciales desde Zenput API"""
    
    print("\n📋 Cargando datos iniciales desde Zenput...")
    
    try:
        cursor = conn.cursor()
        
        # 1. OBTENER Y CARGAR SUCURSALES
        sucursales = obtener_sucursales_zenput()
        
        if not sucursales:
            print("❌ No se pudieron obtener sucursales")
            return False
        
        print("   🏪 Procesando y cargando sucursales...")
        
        # Agrupar sucursales por grupo operativo
        grupos_dict = {}
        
        for sucursal in sucursales:
            # Clasificar ubicación por GPS
            tipo_ubicacion = clasificar_ubicacion_gps(
                sucursal.get('lat'), 
                sucursal.get('lon')
            )
            
            # Agrupar por team
            team_name = sucursal.get('team', {}).get('name', 'SIN GRUPO')
            
            if team_name not in grupos_dict:
                grupos_dict[team_name] = {
                    'sucursales_local': 0,
                    'sucursales_foranea': 0,
                    'sucursales': []
                }
            
            # Contar tipos
            if tipo_ubicacion == 'LOCAL':
                grupos_dict[team_name]['sucursales_local'] += 1
            else:
                grupos_dict[team_name]['sucursales_foranea'] += 1
                
            grupos_dict[team_name]['sucursales'].append({
                'id': sucursal.get('id'),
                'name': sucursal.get('name'),
                'tipo': tipo_ubicacion,
                'lat': sucursal.get('lat'),
                'lon': sucursal.get('lon'),
                'external_key': sucursal.get('external_key'),
                'address': sucursal.get('address'),
                'city': sucursal.get('city'),
                'state': sucursal.get('state'),
                'zip_code': sucursal.get('zip_code')
            })
        
        # 2. CARGAR GRUPOS OPERATIVOS
        print("   👥 Cargando grupos operativos...")
        
        for grupo_nombre, grupo_data in grupos_dict.items():
            # Clasificar tipo de grupo
            locales = grupo_data['sucursales_local']
            foraneas = grupo_data['sucursales_foranea']
            
            if locales > 0 and foraneas > 0:
                tipo_supervision = 'MIXTO'
            elif foraneas > 0:
                tipo_supervision = 'FORANEA'
            else:
                tipo_supervision = 'LOCAL'
            
            # Insertar grupo
            cursor.execute("""
                INSERT INTO grupos_operativos (nombre, tipo_supervision) 
                VALUES (%s, %s) 
                ON CONFLICT (nombre) DO NOTHING
                RETURNING id;
            """, (grupo_nombre, tipo_supervision))
            
            result = cursor.fetchone()
            if result:
                grupo_id = result[0]
            else:
                # Obtener ID del grupo existente
                cursor.execute("SELECT id FROM grupos_operativos WHERE nombre = %s;", (grupo_nombre,))
                grupo_id = cursor.fetchone()[0]
            
            print(f"      ✅ {grupo_nombre}: {tipo_supervision} ({locales} local + {foraneas} foránea)")
            
            # 3. CARGAR SUCURSALES DEL GRUPO
            for sucursal in grupo_data['sucursales']:
                cursor.execute("""
                    INSERT INTO sucursales (
                        id, nombre, direccion, ciudad, estado, codigo_postal,
                        latitud, longitud, grupo_operativo_id, grupo_operativo_nombre,
                        tipo_ubicacion, external_key
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    ) ON CONFLICT (id) DO NOTHING;
                """, (
                    sucursal['id'],
                    sucursal['name'],
                    sucursal.get('address'),
                    sucursal.get('city'),
                    sucursal.get('state'),
                    sucursal.get('zip_code'),
                    sucursal.get('lat'),
                    sucursal.get('lon'),
                    grupo_id,
                    grupo_nombre,
                    sucursal['tipo'],
                    sucursal.get('external_key')
                ))
        
        # 4. CARGAR PERÍODOS 2025
        print("   📅 Cargando períodos 2025...")
        
        periodos_2025 = [
            ('T1', 'Trimestral 1 - Locales NL + Saltillo', '2025-03-12', '2025-04-16', 'TRIMESTRAL', 'LOCAL'),
            ('T2', 'Trimestral 2 - Locales NL + Saltillo', '2025-06-11', '2025-08-18', 'TRIMESTRAL', 'LOCAL'),
            ('T3', 'Trimestral 3 - Locales NL + Saltillo', '2025-08-19', '2025-10-29', 'TRIMESTRAL', 'LOCAL'),
            ('T4', 'Trimestral 4 - Locales NL + Saltillo', '2025-10-30', '2025-12-31', 'TRIMESTRAL', 'LOCAL'),
            ('S1', 'Semestral 1 - Foráneas fuera NL', '2025-04-10', '2025-06-09', 'SEMESTRAL', 'FORANEA'),
            ('S2', 'Semestral 2 - Foráneas fuera NL', '2025-07-30', '2025-11-07', 'SEMESTRAL', 'FORANEA')
        ]
        
        for periodo in periodos_2025:
            cursor.execute("""
                INSERT INTO periodos_supervision (
                    nombre, descripcion, fecha_inicio, fecha_fin, tipo, aplicable_a
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (nombre) DO NOTHING;
            """, periodo)
        
        print(f"      ✅ 6 períodos cargados")
        
        conn.commit()
        cursor.close()
        
        # 5. MOSTRAR RESUMEN
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM grupos_operativos;")
        total_grupos = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM sucursales;")
        total_sucursales = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM periodos_supervision;")
        total_periodos = cursor.fetchone()[0]
        
        cursor.close()
        
        print(f"\n✅ DATOS INICIALES CARGADOS EXITOSAMENTE:")
        print(f"   📊 Grupos operativos: {total_grupos}")
        print(f"   🏪 Sucursales: {total_sucursales}")  
        print(f"   📅 Períodos 2025: {total_periodos}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error cargando datos iniciales: {e}")
        conn.rollback()
        return False

def main():
    """Cargar datos iniciales en Railway"""
    
    print("📋 CARGANDO DATOS INICIALES EN RAILWAY")
    print("=" * 50)
    
    # Conectar a Railway
    conn = conectar_railway()
    if not conn:
        return
    
    try:
        # Cargar datos desde Zenput API
        if cargar_datos_iniciales(conn):
            print(f"\n🎉 DATOS INICIALES CARGADOS EXITOSAMENTE!")
            print("=" * 45)
            print("✅ Grupos operativos creados")
            print("✅ Sucursales con clasificación GPS")
            print("✅ Períodos 2025 configurados")
            print("✅ Base de datos lista para ETL")
            
            # Verificar en web app
            print(f"\n🌐 VERIFICAR EN WEB APP:")
            print("Ve a tu Railway app → /stats para ver estadísticas")
            
        else:
            print(f"\n❌ ERROR EN CARGA DE DATOS INICIALES")
            
    except Exception as e:
        print(f"❌ Error general: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()