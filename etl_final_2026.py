#!/usr/bin/env python3
"""
🚀 ETL FINAL PRODUCTION 2026 
ETL definitivo para estructura limpia sin parches
"""

import requests
import psycopg2
import json
from datetime import datetime, date
import time

# Configuración Railway
DATABASE_URL = 'postgresql://postgres:qGgdIUuKYKMKGtSNYzARpyapBWHsloOt@turntable.proxy.rlwy.net:24097/railway'

# Configuración Zenput
ZENPUT_CONFIG = {
    'base_url': 'https://www.zenput.com/api/v3',
    'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}
}

# Forms
FORMS = {
    'operativa': '877138',
    'seguridad': '877139'
}

def conectar_railway():
    """Conectar a Railway PostgreSQL limpia"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"❌ Error conexión Railway: {e}")
        return None

def obtener_teams_mapping(cursor):
    """Obtener mapping teams → grupos operativos"""
    cursor.execute("SELECT team_id, grupo_operativo FROM teams_grupos_mapping WHERE is_active = true;")
    return {row[0]: row[1] for row in cursor.fetchall()}

def obtener_sucursales_mapping(cursor):
    """Obtener mapping sucursales"""
    cursor.execute("SELECT external_key, id, grupo_operativo_nombre FROM sucursales;")
    return {str(row[0]): {'id': row[1], 'grupo': row[2]} for row in cursor.fetchall()}

def extraer_submissions_zenput(form_id, fecha_desde='2025-01-01'):
    """Extraer TODAS las submissions de un formulario"""
    
    print(f"📊 Extrayendo submissions form {form_id}...")
    
    all_submissions = []
    page = 0
    limit = 100
    
    while True:
        url = f"{ZENPUT_CONFIG['base_url']}/submissions"
        params = {
            'form_template_id': form_id,
            'limit': limit,
            'offset': page * limit,
            'created_after': fecha_desde
        }
        
        try:
            response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                submissions = data.get('data', [])
                
                if not submissions:
                    break
                
                all_submissions.extend(submissions)
                print(f"   📄 Página {page + 1}: {len(submissions)} submissions")
                
                page += 1
                time.sleep(0.5)  # Rate limiting
                
            else:
                print(f"   ❌ Error API: {response.status_code}")
                break
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
            break
    
    print(f"✅ Total extraído: {len(all_submissions)} submissions")
    return all_submissions

def procesar_submission(submission, form_type, teams_mapping, sucursales_mapping):
    """Procesar submission a formato final"""
    
    # Validar submission no es None
    if not submission:
        return None
    
    submission_id = submission.get('id')
    smetadata = submission.get('smetadata', {})
    location = smetadata.get('location', {})
    answers = submission.get('answers', [])
    
    # Extraer location info
    location_id = location.get('id')
    external_key = location.get('external_key')
    location_name = location.get('name', '')
    
    # Obtener grupo operativo via teams
    teams_data = smetadata.get('teams', [])
    grupo_operativo = None
    teams_ids = []
    team_primary = None
    
    for team_info in teams_data:
        team_id = team_info.get('id')
        if team_id:
            teams_ids.append(team_id)
            if team_id in teams_mapping and not grupo_operativo:
                grupo_operativo = teams_mapping[team_id]
                team_primary = team_id
    
    # Fallback: buscar por external_key en sucursales
    if not grupo_operativo and external_key:
        sucursal_info = sucursales_mapping.get(str(external_key))
        if sucursal_info:
            grupo_operativo = sucursal_info['grupo']
    
    # Si no encontramos grupo, skip
    if not grupo_operativo:
        return None
    
    # Obtener sucursal_id
    sucursal_id = None
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
    latitude = location.get('lat')
    longitude = location.get('lon')
    
    return {
        'submission_id': submission_id,
        'form_template_id': FORMS[form_type.lower()],
        'form_type': form_type.upper(),
        'location_id': location_id,
        'location_name': location_name,
        'external_key': external_key,
        'sucursal_id': sucursal_id,
        'grupo_operativo': grupo_operativo,
        'teams_ids': teams_ids,
        'team_primary': team_primary,
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
        'longitude': longitude
    }

def cargar_supervisiones_railway(conn, supervisiones_data):
    """Cargar supervisiones en tabla final 2026"""
    
    print(f"💾 Cargando {len(supervisiones_data)} supervisiones...")
    
    try:
        cursor = conn.cursor()
        
        cargadas = 0
        errores = 0
        
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
                
                cargadas += 1
                
                if cargadas % 50 == 0:
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
        
        return cargadas
        
    except Exception as e:
        print(f"❌ Error cargando supervisiones: {e}")
        conn.rollback()
        return 0

def ejecutar_etl_completo():
    """ETL completo production 2026"""
    
    print("🚀 ETL PRODUCTION 2026 - EL POLLO LOCO")
    print("=" * 60)
    
    start_time = datetime.now()
    
    # Conectar
    conn = conectar_railway()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Obtener mappings
        print("📊 Cargando mappings...")
        teams_mapping = obtener_teams_mapping(cursor)
        sucursales_mapping = obtener_sucursales_mapping(cursor)
        
        print(f"   ✅ Teams mapping: {len(teams_mapping)}")
        print(f"   ✅ Sucursales mapping: {len(sucursales_mapping)}")
        
        total_cargadas = 0
        
        # ETL OPERATIVAS
        print(f"\n🔄 ETL SUPERVISIONES OPERATIVAS")
        print("-" * 40)
        
        submissions_operativa = extraer_submissions_zenput(FORMS['operativa'])
        
        if submissions_operativa:
            supervisiones_operativa = []
            for submission in submissions_operativa:
                supervision = procesar_submission(submission, 'OPERATIVA', teams_mapping, sucursales_mapping)
                if supervision:
                    supervisiones_operativa.append(supervision)
            
            print(f"📋 Procesadas: {len(supervisiones_operativa)}/{len(submissions_operativa)}")
            cargadas = cargar_supervisiones_railway(conn, supervisiones_operativa)
            total_cargadas += cargadas
        
        # ETL SEGURIDAD
        print(f"\n🛡️ ETL SUPERVISIONES SEGURIDAD")
        print("-" * 40)
        
        submissions_seguridad = extraer_submissions_zenput(FORMS['seguridad'])
        
        if submissions_seguridad:
            supervisiones_seguridad = []
            procesadas = 0
            errores = 0
            
            for submission in submissions_seguridad:
                try:
                    if submission:  # Validar submission no es None
                        supervision = procesar_submission(submission, 'SEGURIDAD', teams_mapping, sucursales_mapping)
                        if supervision:
                            supervisiones_seguridad.append(supervision)
                            procesadas += 1
                        else:
                            print(f"   ⚠️ Supervision None para submission {submission.get('id', 'unknown')}")
                            errores += 1
                    else:
                        print(f"   ❌ Submission None encontrada")
                        errores += 1
                except Exception as e:
                    print(f"   ❌ Error procesando submission: {e}")
                    errores += 1
                    continue
            
            print(f"📋 Procesadas: {procesadas}/{len(submissions_seguridad)}, Errores: {errores}")
            if supervisiones_seguridad:
                cargadas = cargar_supervisiones_railway(conn, supervisiones_seguridad)
                total_cargadas += cargadas
        
        # ESTADÍSTICAS FINALES
        print(f"\n📊 ESTADÍSTICAS FINALES")
        print("-" * 40)
        
        cursor.execute("SELECT COUNT(*) FROM supervisiones_2026 WHERE form_type = 'OPERATIVA';")
        total_operativas = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM supervisiones_2026 WHERE form_type = 'SEGURIDAD';")
        total_seguridad = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT grupo_operativo) FROM supervisiones_2026;")
        grupos_con_datos = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT sucursal_id) FROM supervisiones_2026;")
        sucursales_con_datos = cursor.fetchone()[0]
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f"✅ Supervisiones Operativas: {total_operativas}")
        print(f"🛡️ Supervisiones Seguridad: {total_seguridad}")
        print(f"🏪 Sucursales con datos: {sucursales_con_datos}")
        print(f"👥 Grupos con datos: {grupos_con_datos}")
        print(f"⏱️ Duración: {duration:.1f} segundos")
        
        cursor.close()
        conn.close()
        
        print(f"\n🎉 ETL 2026 COMPLETADO EXITOSAMENTE!")
        print("🚀 Railway PostgreSQL lista para producción")
        
        return True
        
    except Exception as e:
        print(f"❌ Error en ETL: {e}")
        if conn:
            conn.close()
        return False

if __name__ == "__main__":
    ejecutar_etl_completo()