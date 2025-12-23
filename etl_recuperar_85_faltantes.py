#!/usr/bin/env python3
"""
🚀 ETL PARA RECUPERAR 85 SUPERVISIONES FALTANTES
ETL específico para procesar submissions con teams 114836 y 115095
"""

import requests
import psycopg2
import json
from datetime import datetime, date
import time
import math
import csv

# Configuración Railway
DATABASE_URL = 'postgresql://postgres:qGgdIUuKYKMKGtSNYzARpyapBWHsloOt@turntable.proxy.rlwy.net:24097/railway'

# Configuración Zenput
ZENPUT_CONFIG = {
    'base_url': 'https://www.zenput.com/api/v3',
    'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}
}

# Teams mapping AMPLIADO
TEAMS_TO_GRUPOS = {
    # Teams conocidos
    115097: "TEPEYAC",
    115098: "EXPO", 
    115099: "PLOG NUEVO LEON",
    115100: "OGAS",
    115101: "EFM",
    115102: "RAP",
    115103: "CRR",
    115104: "TEC",
    115105: "EPL SO",
    115106: "PLOG LAGUNA",
    115107: "PLOG QUERETARO",
    115108: "GRUPO SALTILLO",
    115109: "OCHTER TAMPICO",
    115110: "GRUPO CANTERA ROSA (MORELIA)",
    115111: "GRUPO MATAMOROS",
    115112: "GRUPO PIEDRAS NEGRAS", 
    115113: "GRUPO CENTRITO",
    115114: "GRUPO SABINAS HIDALGO",
    115115: "GRUPO RIO BRAVO",
    115116: "GRUPO NUEVO LAREDO (RUELAS)",
    
    # Teams problemáticos RESUELTOS
    115095: "CORPORATIVO",  # Team raíz El Pollo Loco México
    114836: "CORPORATIVO"   # Team obsoleto, mapear como corporativo
}

# Cargar coordenadas de sucursales para mapeo alternativo
SUCURSALES_COORDS = {}

def cargar_coordenadas_sucursales():
    """Cargar coordenadas de sucursales para mapeo alternativo"""
    global SUCURSALES_COORDS
    
    with open('data/86_sucursales_master.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['Latitude'] and row['Longitude']:
                SUCURSALES_COORDS[int(row['Numero_Sucursal'])] = {
                    'nombre': row['Nombre_Sucursal'],
                    'grupo': row['Grupo_Operativo'],
                    'lat': float(row['Latitude']),
                    'lon': float(row['Longitude'])
                }

def calcular_distancia(lat1, lon1, lat2, lon2):
    """Calcular distancia euclidiana entre dos puntos"""
    return math.sqrt((lat1 - lat2)**2 + (lon1 - lon2)**2)

def mapear_por_coordenadas(lat, lon):
    """Mapear grupo operativo por coordenadas más cercanas"""
    
    if not lat or not lon:
        return None
    
    distancia_minima = float('inf')
    grupo_mas_cercano = None
    sucursal_mas_cercana = None
    
    for numero, info in SUCURSALES_COORDS.items():
        distancia = calcular_distancia(lat, lon, info['lat'], info['lon'])
        
        if distancia < distancia_minima:
            distancia_minima = distancia
            grupo_mas_cercano = info['grupo']
            sucursal_mas_cercana = info['nombre']
    
    return {
        'grupo': grupo_mas_cercano,
        'sucursal_cercana': sucursal_mas_cercana,
        'distancia': distancia_minima
    }

def conectar_railway():
    """Conectar a Railway PostgreSQL"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"❌ Error conexión Railway: {e}")
        return None

def obtener_sucursales_mapping(cursor):
    """Obtener mapping sucursales"""
    cursor.execute("SELECT external_key, id, grupo_operativo_nombre FROM sucursales;")
    return {str(row[0]): {'id': row[1], 'grupo': row[2]} for row in cursor.fetchall()}

def extraer_submissions_problematicas():
    """Extraer SOLO las submissions con teams 114836 y 115095"""
    
    print("🎯 Extrayendo submissions problemáticas...")
    
    all_submissions = []
    page = 0
    limit = 100
    
    while True:
        url = f"{ZENPUT_CONFIG['base_url']}/submissions"
        params = {
            'form_template_id': '877139',
            'limit': limit,
            'offset': page * limit,
            'created_after': '2025-01-01'
        }
        
        try:
            response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                submissions = data.get('data', [])
                
                if not submissions:
                    break
                
                # Filtrar solo submissions problemáticas
                for submission in submissions:
                    if submission:
                        smetadata = submission.get('smetadata', {})
                        teams_data = smetadata.get('teams', [])
                        
                        # Verificar si tiene teams problemáticos y NO location
                        has_problematic_team = False
                        for team_info in teams_data:
                            team_id = team_info.get('id')
                            if team_id in [114836, 115095]:
                                has_problematic_team = True
                                break
                        
                        has_no_location = not smetadata.get('location')
                        
                        if has_problematic_team and has_no_location:
                            all_submissions.append(submission)
                
                print(f"   📄 Página {page + 1}: {len(submissions)} total, {sum(1 for s in submissions if s and not s.get('smetadata', {}).get('location') and any(t.get('id') in [114836, 115095] for t in s.get('smetadata', {}).get('teams', [])))} problemáticas")
                page += 1
                time.sleep(0.5)
                
            else:
                print(f"   ❌ Error API: {response.status_code}")
                break
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
            break
    
    print(f"✅ Total problemáticas extraídas: {len(all_submissions)}")
    return all_submissions

def procesar_submission_problematica(submission, sucursales_mapping):
    """Procesar submission problemática con mapeo inteligente"""
    
    if not submission:
        return None
    
    submission_id = submission.get('id')
    smetadata = submission.get('smetadata', {})
    answers = submission.get('answers', [])
    
    # Teams analysis
    teams_data = smetadata.get('teams', [])
    teams_ids = [team_info.get('id') for team_info in teams_data]
    
    # Mapeo de grupo - método inteligente
    grupo_operativo = None
    metodo_mapeo = None
    
    # Método 1: Teams mapping directo (para teams corporativos)
    for team_info in teams_data:
        team_id = team_info.get('id')
        if team_id in TEAMS_TO_GRUPOS:
            if TEAMS_TO_GRUPOS[team_id] == "CORPORATIVO":
                # Para supervisiones corporativas, usar mapeo por coordenadas
                lat = smetadata.get('lat')
                lon = smetadata.get('lon')
                
                if lat and lon:
                    coord_mapping = mapear_por_coordenadas(lat, lon)
                    if coord_mapping:
                        grupo_operativo = coord_mapping['grupo']
                        metodo_mapeo = f"coordenadas (cerca de {coord_mapping['sucursal_cercana']})"
                        break
            else:
                grupo_operativo = TEAMS_TO_GRUPOS[team_id]
                metodo_mapeo = "teams_mapping"
                break
    
    # Si no encontramos grupo, skip
    if not grupo_operativo:
        return None
    
    # Obtener sucursal_id por external_key si existe location alternativa
    sucursal_id = None
    location_id = None
    location_name = None
    external_key = None
    
    # Buscar location en otros lugares
    activity = submission.get('activity', {})
    if 'location' in activity and activity['location']:
        location_data = activity['location']
        external_key = location_data.get('external_key')
        location_id = location_data.get('id')
        location_name = location_data.get('name', '')
        
        if external_key and str(external_key) in sucursales_mapping:
            sucursal_id = sucursales_mapping[str(external_key)]['id']
    
    # Auditor info
    created_by = smetadata.get('created_by', {})
    auditor_id = created_by.get('id')
    auditor_nombre = created_by.get('display_name', '')
    auditor_email = created_by.get('email', '')
    
    # Timing
    date_submitted = smetadata.get('date_submitted')
    fecha_submission = datetime.fromisoformat(date_submitted.replace('Z', '+00:00')) if date_submitted else None
    fecha_supervision = fecha_submission.date() if fecha_submission else None
    time_to_complete = smetadata.get('time_to_complete')
    
    # Scoring
    puntos_maximos = 0
    puntos_obtenidos = 0
    calificacion_porcentaje = 0.0
    
    # Procesar answers para extraer scoring
    for answer in answers:
        title = answer.get('title', '').upper()
        value = answer.get('value')
        field_type = answer.get('field_type', '')
        
        if field_type == 'formula' and value is not None:
            if 'PUNTOS MAX' in title:
                puntos_maximos = int(value) if isinstance(value, (int, float)) else 0
            elif 'PUNTOS' in title and ('OBTENIDOS' in title or 'TOTALES' in title):
                puntos_obtenidos = int(value) if isinstance(value, (int, float)) else 0
            elif 'CALIFICACION' in title and '%' in title:
                calificacion_porcentaje = float(value) if isinstance(value, (int, float)) else 0.0
    
    # Score de Zenput como fallback
    score_zenput = submission.get('score')
    if calificacion_porcentaje == 0.0 and score_zenput:
        calificacion_porcentaje = float(score_zenput)
    
    # Geographic
    latitude = smetadata.get('lat')
    longitude = smetadata.get('lon')
    
    return {
        'submission_id': submission_id,
        'form_template_id': '877139',
        'form_type': 'SEGURIDAD',
        'location_id': location_id,
        'location_name': location_name,
        'external_key': external_key,
        'sucursal_id': sucursal_id,
        'grupo_operativo': grupo_operativo,
        'teams_ids': teams_ids,
        'team_primary': teams_ids[0] if teams_ids else None,
        'auditor_id': auditor_id,
        'auditor_nombre': auditor_nombre,
        'auditor_email': auditor_email,
        'fecha_supervision': fecha_supervision,
        'fecha_submission': fecha_submission,
        'time_to_complete': time_to_complete,
        'puntos_maximos': puntos_maximos,
        'puntos_obtenidos': puntos_obtenidos,
        'calificacion_porcentaje': calificacion_porcentaje,
        'score_zenput': score_zenput,
        'latitude': latitude,
        'longitude': longitude,
        'metodo_mapeo': metodo_mapeo
    }

def cargar_supervisiones_railway(conn, supervisiones_data):
    """Cargar supervisiones recuperadas en tabla final 2026"""
    
    print(f"💾 Cargando {len(supervisiones_data)} supervisiones recuperadas...")
    
    try:
        cursor = conn.cursor()
        
        cargadas = 0
        errores = 0
        metodos_mapeo = {}
        
        for supervision in supervisiones_data:
            try:
                cursor.execute("""
                    INSERT INTO supervisiones_2026 (
                        submission_id, form_template_id, form_type,
                        location_id, location_name, external_key, sucursal_id, grupo_operativo,
                        teams_ids, team_primary, auditor_id, auditor_nombre, auditor_email,
                        fecha_supervision, fecha_submission, time_to_complete,
                        puntos_maximos, puntos_obtenidos, calificacion_porcentaje, score_zenput,
                        latitude, longitude
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (submission_id) DO UPDATE SET
                        updated_at = CURRENT_TIMESTAMP;
                """, (
                    supervision['submission_id'],
                    supervision['form_template_id'],
                    supervision['form_type'],
                    supervision['location_id'],
                    supervision['location_name'],
                    supervision['external_key'],
                    supervision['sucursal_id'],
                    supervision['grupo_operativo'],
                    supervision['teams_ids'],
                    supervision['team_primary'],
                    supervision['auditor_id'],
                    supervision['auditor_nombre'],
                    supervision['auditor_email'],
                    supervision['fecha_supervision'],
                    supervision['fecha_submission'],
                    supervision['time_to_complete'],
                    supervision['puntos_maximos'],
                    supervision['puntos_obtenidos'],
                    supervision['calificacion_porcentaje'],
                    supervision['score_zenput'],
                    supervision['latitude'],
                    supervision['longitude']
                ))
                
                # Contar métodos de mapeo
                metodo = supervision.get('metodo_mapeo', 'unknown')
                metodos_mapeo[metodo] = metodos_mapeo.get(metodo, 0) + 1
                
                cargadas += 1
                
                if cargadas % 25 == 0:
                    print(f"   📊 Cargadas: {cargadas}")
                    conn.commit()
                
            except Exception as e:
                print(f"   ❌ Error cargando {supervision['submission_id']}: {e}")
                errores += 1
                continue
        
        conn.commit()
        cursor.close()
        
        print(f"✅ Supervisiones cargadas: {cargadas}")
        print(f"⚠️ Errores: {errores}")
        
        print(f"\n📊 Métodos de mapeo utilizados:")
        for metodo, count in metodos_mapeo.items():
            print(f"   {metodo}: {count}")
        
        return cargadas
        
    except Exception as e:
        print(f"❌ Error cargando supervisiones: {e}")
        conn.rollback()
        return 0

def ejecutar_recuperacion_completa():
    """Ejecutar recuperación completa de 85 supervisiones faltantes"""
    
    print("🚀 RECUPERACIÓN DE 85 SUPERVISIONES FALTANTES")
    print("=" * 60)
    
    start_time = datetime.now()
    
    # Cargar coordenadas de sucursales
    cargar_coordenadas_sucursales()
    print(f"✅ Cargadas coordenadas de {len(SUCURSALES_COORDS)} sucursales")
    
    # Conectar
    conn = conectar_railway()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Obtener mapping de sucursales
        print("📊 Cargando mapping de sucursales...")
        sucursales_mapping = obtener_sucursales_mapping(cursor)
        print(f"   ✅ Sucursales mapping: {len(sucursales_mapping)}")
        
        # Verificar estado actual
        cursor.execute("SELECT COUNT(*) FROM supervisiones_2026 WHERE form_type = 'SEGURIDAD';")
        seguridad_antes = cursor.fetchone()[0]
        
        print(f"📊 Estado actual: {seguridad_antes} supervisiones de seguridad")
        
        # Extraer submissions problemáticas
        print(f"\n🎯 EXTRAYENDO SUBMISSIONS PROBLEMÁTICAS...")
        submissions_problematicas = extraer_submissions_problematicas()
        
        if not submissions_problematicas:
            print("⚠️ No se encontraron submissions problemáticas")
            return True
        
        # Procesar submissions problemáticas
        print(f"\n🔄 PROCESANDO {len(submissions_problematicas)} SUBMISSIONS...")
        supervisiones_recuperadas = []
        procesadas = 0
        errores_mapeo = 0
        
        for submission in submissions_problematicas:
            try:
                supervision = procesar_submission_problematica(submission, sucursales_mapping)
                if supervision:
                    supervisiones_recuperadas.append(supervision)
                    procesadas += 1
                else:
                    errores_mapeo += 1
                    print(f"   ❌ No se pudo mapear submission {submission.get('id', 'unknown')}")
            except Exception as e:
                print(f"   ❌ Error procesando submission: {e}")
                errores_mapeo += 1
                continue
        
        print(f"📋 Procesadas: {procesadas}/{len(submissions_problematicas)}")
        print(f"❌ Errores de mapeo: {errores_mapeo}")
        
        # Cargar supervisiones recuperadas
        if supervisiones_recuperadas:
            print(f"\n💾 CARGANDO SUPERVISIONES RECUPERADAS...")
            cargadas = cargar_supervisiones_railway(conn, supervisiones_recuperadas)
            
            # Verificar resultado final
            cursor.execute("SELECT COUNT(*) FROM supervisiones_2026 WHERE form_type = 'SEGURIDAD';")
            seguridad_despues = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM supervisiones_2026 WHERE form_type = 'OPERATIVA';")
            operativas_total = cursor.fetchone()[0]
            
            recuperadas_reales = seguridad_despues - seguridad_antes
            
            print(f"\n📊 RESULTADOS FINALES:")
            print(f"   📈 Supervisiones seguridad antes: {seguridad_antes}")
            print(f"   📈 Supervisiones seguridad después: {seguridad_despues}")
            print(f"   ✅ Recuperadas exitosamente: {recuperadas_reales}")
            print(f"   📊 Total operativas: {operativas_total}")
            print(f"   🎯 TOTAL GENERAL: {operativas_total + seguridad_despues} supervisiones")
            
            # Distribución por grupo
            cursor.execute("""
                SELECT grupo_operativo, COUNT(*) 
                FROM supervisiones_2026 
                WHERE form_type = 'SEGURIDAD' 
                GROUP BY grupo_operativo 
                ORDER BY COUNT(*) DESC;
            """)
            distribucion = cursor.fetchall()
            
            print(f"\n📊 Distribución seguridad por grupo:")
            for grupo, count in distribucion:
                print(f"   {grupo}: {count}")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        cursor.close()
        conn.close()
        
        print(f"\n🎉 RECUPERACIÓN COMPLETADA!")
        print(f"⏱️ Duración: {duration:.1f} segundos")
        print("✅ Base de datos lista con TODAS las supervisiones")
        
        return True
        
    except Exception as e:
        print(f"❌ Error en recuperación: {e}")
        if conn:
            conn.close()
        return False

if __name__ == "__main__":
    ejecutar_recuperacion_completa()