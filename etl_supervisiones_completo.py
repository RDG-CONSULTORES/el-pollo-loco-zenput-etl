#!/usr/bin/env python3
"""
🔄 ETL COMPLETO SUPERVISIONES 2025
Extrae TODAS las supervisiones de Zenput y las carga en Railway PostgreSQL
"""

import requests
import psycopg2
import json
from datetime import datetime, date
import time

# CONFIGURACIÓN ZENPUT - USAR MISMO TOKEN QUE GITHUB ACTIONS EXITOSO
# Roberto: El token hard-coded no funciona, necesitamos el de GitHub Actions
ZENPUT_CONFIG = {
    'base_urls': {
        'v3': 'https://www.zenput.com/api/v3',
        'v1': 'https://www.zenput.com/api/v1'
    },
    'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}  # Token del ETL funcional
}

# CONFIGURACIÓN RAILWAY - Roberto's PostgreSQL Credentials
RAILWAY_CONFIG = {
    'host': 'turntable.proxy.rlwy.net',
    'port': '24097', 
    'database': 'railway',
    'user': 'postgres',
    'password': 'qGgdIUuKYKMKGtSNYzARpyapBWHsloOt'
}

# FORMS DE SUPERVISIÓN
FORMS_CONFIG = {
    'operativa': '877138',
    'seguridad': '877139'
}

def conectar_railway():
    """Conectar a Railway PostgreSQL"""
    try:
        conn = psycopg2.connect(**RAILWAY_CONFIG)
        return conn
    except Exception as e:
        print(f"❌ Error conectando a Railway: {e}")
        return None

def extraer_submissions_zenput(form_id, fecha_desde='2025-01-01', fecha_hasta='2025-12-18'):
    """Extraer TODAS las submissions de Zenput para un formulario específico con paginación completa"""
    
    print(f"🔄 Extrayendo TODAS las submissions form {form_id} desde {fecha_desde}...")
    
    all_submissions = []
    page = 1
    max_pages = 100  # Límite de seguridad para evitar loops infinitos
    
    while page <= max_pages:
        print(f"   📄 Procesando página {page}...")
        
        # USAR SOLO v3 COMO EN TU GITHUB ACTIONS EXITOSO
        endpoint_url = f"{ZENPUT_CONFIG['base_urls']['v3']}/submissions"
        
        # PARÁMETROS EXACTOS COMO TU GITHUB ACTIONS + FECHAS PARA 2025
        params = {
            'form_template_id': form_id,
            'limit': 100,  # Máximo por página
            'offset': (page - 1) * 100,  # Zenput puede usar offset en lugar de page
            'created_after': fecha_desde,
            'created_before': fecha_hasta
        }
        
        try:
            response = requests.get(endpoint_url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=30)
            
            print(f"   🧪 API v3 endpoint: {endpoint_url}")
            print(f"   📋 Params: form_template_id={form_id}, offset={params['offset']}, limit={params['limit']}")
            print(f"   🔍 Response: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                # Tu ETL funcional usa 'data' como array principal
                submissions = data.get('data', [])
                
                print(f"   ✅ SUCCESS with API v3")
                print(f"   📊 Total found in API: {data.get('count', len(submissions))}")
                print(f"   🔢 Items this page: {len(submissions)}")
                
                if not submissions:
                    print(f"   📄 No more submissions on page {page} - Finished!")
                    break
                
                all_submissions.extend(submissions)
                print(f"   📄 Página {page}: {len(submissions)} submissions agregados")
                print(f"   📈 Total acumulado: {len(all_submissions)} submissions")
                
                page += 1
                
                # Rate limiting entre páginas
                time.sleep(0.5)
                
            elif response.status_code == 403:
                print(f"   ❌ 403 Forbidden - Token sin permisos")
                break
            elif response.status_code == 404:
                print(f"   ❌ 404 Not Found - Endpoint o form_id incorrecto")  
                break
            else:
                print(f"   ❌ Error {response.status_code}: {response.text[:200]}")
                break
                
        except Exception as e:
            print(f"   ❌ Exception: {e}")
            break
    
    print(f"✅ Total extraído form {form_id}: {len(all_submissions)} submissions")
    return all_submissions

def procesar_submission_operativa(submission):
    """Procesar submission de formulario operativa 877138"""
    
    # Extraer información básica
    submission_id = submission.get('id')
    location = submission.get('location', {})
    sucursal_id = location.get('external_key')
    sucursal_nombre = location.get('name', '')
    
    metadata = submission.get('smetadata', {})
    created_by = metadata.get('created_by', {})
    auditor_nombre = created_by.get('display_name', '')
    auditor_email = created_by.get('email', '')
    
    fecha_submission = metadata.get('date_submitted')
    fecha_supervision = fecha_submission.split('T')[0] if fecha_submission else None
    
    latitude = location.get('lat')
    longitude = location.get('lon')
    score = submission.get('score')
    
    # Procesar answers para extraer puntos
    answers = submission.get('answers', [])
    puntos_max = 0
    puntos_obtenidos = 0
    calificacion_porcentaje = 0
    
    # Buscar campos de calificación general
    for answer in answers:
        title = answer.get('title', '')
        value = answer.get('value')
        
        if 'PUNTOS MAX' in title and 'TOTAL' in title:
            puntos_max = value if value else 0
        elif 'PUNTOS OBTENIDOS' in title and 'TOTAL' in title:
            puntos_obtenidos = value if value else 0
        elif 'CALIFICACION' in title and '%' in title:
            calificacion_porcentaje = value if value else 0
    
    # Si no encontramos en answers, usar score de Zenput
    if puntos_max == 0 and score is not None:
        calificacion_porcentaje = score
        # Estimar puntos basado en el score promedio de operativa (~100 puntos)
        puntos_max = 100
        puntos_obtenidos = int((score / 100.0) * puntos_max)
    
    return {
        'submission_id': submission_id,
        'form_id': FORMS_CONFIG['operativa'],
        'form_type': 'OPERATIVA',
        'sucursal_id': sucursal_id,
        'sucursal_nombre': sucursal_nombre,
        'auditor_nombre': auditor_nombre,
        'auditor_email': auditor_email,
        'fecha_supervision': fecha_supervision,
        'fecha_submission': fecha_submission,
        'puntos_max': puntos_max,
        'puntos_obtenidos': puntos_obtenidos,
        'calificacion_porcentaje': calificacion_porcentaje,
        'score_zenput': score,
        'latitude': latitude,
        'longitude': longitude
    }

def procesar_submission_seguridad(submission):
    """Procesar submission de formulario seguridad 877139"""
    
    # Extraer información básica
    submission_id = submission.get('id')
    location = submission.get('location', {})
    sucursal_id = location.get('external_key')
    sucursal_nombre = location.get('name', '')
    
    metadata = submission.get('smetadata', {})
    created_by = metadata.get('created_by', {})
    auditor_nombre = created_by.get('display_name', '')
    auditor_email = created_by.get('email', '')
    
    fecha_submission = metadata.get('date_submitted')
    fecha_supervision = fecha_submission.split('T')[0] if fecha_submission else None
    
    latitude = location.get('lat')
    longitude = location.get('lon')
    score = submission.get('score')
    
    # Procesar answers para extraer calificación general
    answers = submission.get('answers', [])
    puntos_max = 0
    puntos_obtenidos = 0
    calificacion_porcentaje = 0
    
    # Buscar campos específicos de seguridad (validados previamente)
    for answer in answers:
        title = answer.get('title', '')
        value = answer.get('value')
        
        if title == 'PUNTOS MAX':
            puntos_max = value if value else 0
        elif title == 'PUNTOS TOTALES OBTENIDOS':
            puntos_obtenidos = value if value else 0
        elif title == 'CALIFICACION PORCENTAJE %':
            calificacion_porcentaje = value if value else 0
    
    # Si no encontramos en answers, usar score de Zenput
    if puntos_max == 0 and score is not None:
        calificacion_porcentaje = score
        # Estimar puntos basado en análisis previo (~45 puntos para seguridad)
        puntos_max = 45
        puntos_obtenidos = int((score / 100.0) * puntos_max)
    
    return {
        'submission_id': submission_id,
        'form_id': FORMS_CONFIG['seguridad'],
        'form_type': 'SEGURIDAD',
        'sucursal_id': sucursal_id,
        'sucursal_nombre': sucursal_nombre,
        'auditor_nombre': auditor_nombre,
        'auditor_email': auditor_email,
        'fecha_supervision': fecha_supervision,
        'fecha_submission': fecha_submission,
        'puntos_max': puntos_max,
        'puntos_obtenidos': puntos_obtenidos,
        'calificacion_porcentaje': calificacion_porcentaje,
        'score_zenput': score,
        'latitude': latitude,
        'longitude': longitude
    }

def cargar_supervisions_railway(conn, supervisions_data):
    """Cargar supervisiones en Railway PostgreSQL"""
    
    print(f"📊 Cargando {len(supervisions_data)} supervisiones en Railway...")
    
    try:
        cursor = conn.cursor()
        
        # Obtener mapeo de sucursales para foreign keys
        cursor.execute("SELECT external_key, id, grupo_operativo_id FROM sucursales WHERE external_key IS NOT NULL;")
        sucursales_map = {row[0]: {'id': row[1], 'grupo_id': row[2]} for row in cursor.fetchall()}
        
        # Determinar período basado en fecha y tipo de sucursal
        cursor.execute("SELECT id, nombre, fecha_inicio, fecha_fin, aplicable_a FROM periodos_supervision;")
        periodos = cursor.fetchall()
        
        cargadas = 0
        errores = 0
        
        for supervision in supervisions_data:
            try:
                # Validar sucursal existe
                sucursal_key = str(supervision['sucursal_id'])
                if sucursal_key not in sucursales_map:
                    print(f"   ⚠️ Sucursal no encontrada: {sucursal_key}")
                    errores += 1
                    continue
                
                sucursal_info = sucursales_map[sucursal_key]
                supervision['grupo_operativo_id'] = sucursal_info['grupo_id']
                supervision['sucursal_id_bd'] = sucursal_info['id']
                
                # Determinar período (simplificado - mejorar lógica según necesidad)
                periodo_id = None
                if supervision['fecha_supervision']:
                    fecha_sup = datetime.strptime(supervision['fecha_supervision'], '%Y-%m-%d').date()
                    
                    for periodo in periodos:
                        fecha_inicio = periodo[2]
                        fecha_fin = periodo[3]
                        
                        if fecha_inicio <= fecha_sup <= fecha_fin:
                            periodo_id = periodo[0]
                            supervision['periodo_nombre'] = periodo[1]
                            break
                
                # Insertar en tabla principal
                insert_sql = """
                    INSERT INTO supervisions (
                        submission_id, form_id, form_type, sucursal_id, sucursal_nombre,
                        grupo_operativo_id, auditor_nombre, auditor_email, fecha_supervision,
                        fecha_submission, periodo_id, periodo_nombre, puntos_max, puntos_obtenidos,
                        calificacion_porcentaje, score_zenput, latitude, longitude
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    ) ON CONFLICT (submission_id) DO NOTHING;
                """
                
                cursor.execute(insert_sql, (
                    supervision['submission_id'],
                    supervision['form_id'],
                    supervision['form_type'],
                    supervision['sucursal_id_bd'],
                    supervision['sucursal_nombre'],
                    supervision['grupo_operativo_id'],
                    supervision['auditor_nombre'],
                    supervision['auditor_email'],
                    supervision['fecha_supervision'],
                    supervision['fecha_submission'],
                    periodo_id,
                    supervision.get('periodo_nombre'),
                    supervision['puntos_max'],
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
                print(f"   ❌ Error cargando supervision {supervision['submission_id']}: {e}")
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

def main():
    """ETL completo de supervisiones 2025"""
    
    print("🔄 ETL COMPLETO SUPERVISIONES 2025")
    print("=" * 50)
    
    # Verificar credenciales Railway
    if RAILWAY_CONFIG['host'] == 'YOUR_RAILWAY_HOST':
        print("❌ CONFIGURAR CREDENCIALES RAILWAY PRIMERO")
        return
    
    # Conectar a Railway
    conn = conectar_railway()
    if not conn:
        return
    
    try:
        total_cargadas = 0
        
        # 1. ETL SUPERVISIONES OPERATIVAS (877138)
        print(f"\n🔄 ETL SUPERVISIONES OPERATIVAS (877138)")
        print("-" * 40)
        
        submissions_operativa = extraer_submissions_zenput(FORMS_CONFIG['operativa'])
        
        if submissions_operativa:
            supervisions_operativa = []
            for submission in submissions_operativa:
                supervision = procesar_submission_operativa(submission)
                supervisions_operativa.append(supervision)
            
            cargadas = cargar_supervisions_railway(conn, supervisions_operativa)
            total_cargadas += cargadas
            
            # Guardar respaldo
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'data/etl_operativa_{timestamp}.json'
            with open(filename, 'w') as f:
                json.dump(supervisions_operativa, f, indent=2, default=str)
            print(f"💾 Respaldo operativa guardado: {filename}")
        
        # 2. ETL SUPERVISIONES SEGURIDAD (877139)
        print(f"\n🛡️ ETL SUPERVISIONES SEGURIDAD (877139)")
        print("-" * 40)
        
        submissions_seguridad = extraer_submissions_zenput(FORMS_CONFIG['seguridad'])
        
        if submissions_seguridad:
            supervisions_seguridad = []
            for submission in submissions_seguridad:
                supervision = procesar_submission_seguridad(submission)
                supervisions_seguridad.append(supervision)
            
            cargadas = cargar_supervisions_railway(conn, supervisions_seguridad)
            total_cargadas += cargadas
            
            # Guardar respaldo
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'data/etl_seguridad_{timestamp}.json'
            with open(filename, 'w') as f:
                json.dump(supervisions_seguridad, f, indent=2, default=str)
            print(f"💾 Respaldo seguridad guardado: {filename}")
        
        # 3. RESUMEN FINAL
        print(f"\n🎉 ETL COMPLETADO")
        print("=" * 30)
        print(f"✅ Total supervisiones cargadas: {total_cargadas}")
        
        # Verificar datos en Railway
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM supervisions WHERE form_type = 'OPERATIVA';")
        count_operativa = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM supervisions WHERE form_type = 'SEGURIDAD';")
        count_seguridad = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT sucursal_id) FROM supervisions;")
        sucursales_con_datos = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT grupo_operativo_id) FROM supervisions;")
        grupos_con_datos = cursor.fetchone()[0]
        
        print(f"📊 Supervisiones Operativas: {count_operativa}")
        print(f"🛡️ Supervisiones Seguridad: {count_seguridad}")
        print(f"🏪 Sucursales con datos: {sucursales_con_datos}/86")
        print(f"👥 Grupos con datos: {grupos_con_datos}/20")
        
        cursor.close()
        
        print(f"\n✅ RAILWAY POSTGRESQL LISTA PARA DASHBOARD")
        print("🚀 Datos completos de supervisions 2025 cargados")
        
    except Exception as e:
        print(f"❌ Error en ETL: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()