#!/usr/bin/env python3
"""
🔄 ETL COMPLETO SUPERVISIONES - BASE DE DATOS
Extrae supervisiones y las guarda en PostgreSQL con normalización automática
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.zenput_api import create_zenput_client
import psycopg2
import json
import re
from datetime import datetime, date
from collections import defaultdict

def connect_database():
    """Conecta a PostgreSQL (Railway o local)"""
    
    # Configuración de base de datos (Railway)
    db_config = {
        'host': os.getenv('RAILWAY_DB_HOST', 'localhost'),
        'port': os.getenv('RAILWAY_DB_PORT', '5432'),
        'database': os.getenv('RAILWAY_DB_NAME', 'epl_supervisiones'),
        'user': os.getenv('RAILWAY_DB_USER', 'postgres'),
        'password': os.getenv('RAILWAY_DB_PASSWORD', '')
    }
    
    try:
        conn = psycopg2.connect(**db_config)
        print(f"✅ Conectado a PostgreSQL: {db_config['host']}")
        return conn
    except Exception as e:
        print(f"❌ Error conectando a base de datos: {e}")
        print(f"💡 Configurar variables de entorno Railway:")
        print(f"   export RAILWAY_DB_HOST=xxxx.railway.app")
        print(f"   export RAILWAY_DB_PASSWORD=xxxx")
        return None

def normalize_sucursal_name(sucursal_name_zenput, conn):
    """Normaliza nombre de sucursal contra tabla maestro"""
    
    cursor = conn.cursor()
    
    # Extraer número de sucursal
    numero = extract_sucursal_number(sucursal_name_zenput)
    
    if numero:
        # Buscar en tabla maestro por número
        cursor.execute("""
            SELECT sucursal_numero, nombre_actual, nombres_historicos 
            FROM sucursales_master 
            WHERE sucursal_numero = %s AND activa = TRUE
        """, (numero,))
        
        result = cursor.fetchone()
        if result:
            return {
                'sucursal_numero': result[0],
                'nombre_normalizado': result[1],
                'nombre_zenput': sucursal_name_zenput,
                'normalizado': True
            }
    
    # Si no encontró por número, buscar por similitud de nombre
    cursor.execute("""
        SELECT sucursal_numero, nombre_actual, nombres_historicos
        FROM sucursales_master 
        WHERE %s = ANY(nombres_historicos) OR nombre_actual ILIKE %s
        AND activa = TRUE
    """, (sucursal_name_zenput, f"%{sucursal_name_zenput[:20]}%"))
    
    result = cursor.fetchone()
    if result:
        return {
            'sucursal_numero': result[0],
            'nombre_normalizado': result[1],
            'nombre_zenput': sucursal_name_zenput,
            'normalizado': True
        }
    
    cursor.close()
    
    # No encontrado - crear alerta
    return {
        'sucursal_numero': None,
        'nombre_normalizado': sucursal_name_zenput,
        'nombre_zenput': sucursal_name_zenput,
        'normalizado': False,
        'alerta': 'SUCURSAL_NO_ENCONTRADA'
    }

def extract_sucursal_number(sucursal_name):
    """Extrae número de sucursal del nombre"""
    
    patterns = [
        r'^(\d+)\s*-\s*',        # "53 - Lienzo Charro"
        r'^(\d+)\s+',            # "53 Lienzo Charro"
    ]
    
    for pattern in patterns:
        match = re.match(pattern, sucursal_name.strip())
        if match:
            try:
                numero = int(match.group(1))
                if 1 <= numero <= 100:
                    return numero
            except ValueError:
                continue
    
    return None

def determine_periodo_supervision(fecha_supervision, conn):
    """Determina periodo T1-T4 basado en fecha de supervisión"""
    
    cursor = conn.cursor()
    
    # Buscar periodo activo que contenga la fecha
    cursor.execute("""
        SELECT id, periodo_codigo, año, tipo_sucursal
        FROM periodos_supervision 
        WHERE %s BETWEEN fecha_inicio AND fecha_fin 
        AND activo = TRUE
        ORDER BY año DESC, periodo_codigo
        LIMIT 1
    """, (fecha_supervision.date() if isinstance(fecha_supervision, datetime) else fecha_supervision,))
    
    result = cursor.fetchone()
    cursor.close()
    
    if result:
        return {
            'periodo_id': result[0],
            'periodo_codigo': result[1],
            'año': result[2],
            'tipo_sucursal': result[3]
        }
    
    return None

def extract_supervision_data(submission):
    """Extrae datos estructurados de una submission"""
    
    # Datos básicos
    supervision_data = {
        'submission_id': submission.get('id'),
        'form_id': submission.get('form_id'),
        'form_name': submission.get('form_name', ''),
    }
    
    # Metadatos
    metadata = submission.get('smetadata', {})
    if metadata:
        # Supervisor
        created_by = metadata.get('created_by', {})
        supervision_data.update({
            'supervisor_id': created_by.get('id'),
            'supervisor_nombre': created_by.get('display_name', ''),
            'supervisor_rol': metadata.get('user_role', {}).get('name', ''),
        })
        
        # Sucursal de Zenput
        location = metadata.get('location', {})
        supervision_data.update({
            'sucursal_nombre_zenput': location.get('name', ''),
            'direccion': location.get('address', ''),
        })
        
        # Fechas
        supervision_data.update({
            'fecha_supervision': metadata.get('date_completed_local'),
            'fecha_completada': metadata.get('date_completed_local'), 
            'fecha_enviada': metadata.get('date_submitted_local'),
            'tiempo_supervision_ms': metadata.get('time_to_complete'),
        })
        
        # Ubicación GPS
        supervision_data.update({
            'coordenadas_lat': metadata.get('lat'),
            'coordenadas_lon': metadata.get('lon'),
            'distancia_sucursal_km': metadata.get('distance_to_account'),
        })
        
        # Metadatos técnicos
        supervision_data.update({
            'plataforma': metadata.get('platform', ''),
            'ambiente': metadata.get('environment', ''),
            'zona_horaria': metadata.get('time_zone', ''),
        })
    
    # Extraer datos de respuestas por área
    answers = submission.get('answers', [])
    areas_data = extract_areas_from_answers(answers)
    
    supervision_data['areas'] = areas_data
    
    # Extraer calificaciones globales del HEADER
    header_data = areas_data.get('HEADER', {})
    if header_data:
        supervision_data.update({
            'calificacion_general': header_data.get('calificacion_general'),
            'puntos_obtenidos': header_data.get('puntos_obtenidos'),
            'puntos_maximos': header_data.get('puntos_max'),
        })
    
    # Calcular estadísticas generales
    total_preguntas = sum([area.get('total_campos', 0) for area in areas_data.values()])
    total_respondidas = sum([area.get('campos_completados', 0) for area in areas_data.values()])
    total_imagenes = sum([area.get('evidencia_fotografica', 0) for area in areas_data.values()])
    total_si = sum([area.get('respuestas_si', 0) for area in areas_data.values()])
    total_no = sum([area.get('respuestas_no', 0) for area in areas_data.values()])
    
    supervision_data.update({
        'total_preguntas': total_preguntas,
        'total_respondidas': total_respondidas,
        'completitud_porcentaje': round((total_respondidas / total_preguntas * 100), 2) if total_preguntas > 0 else 0,
        'total_imagenes': total_imagenes,
        'respuestas_si': total_si,
        'respuestas_no': total_no,
    })
    
    return supervision_data

def extract_areas_from_answers(answers):
    """Extrae datos por área de las respuestas"""
    
    # Mapeo de áreas conocidas
    AREAS_MAP = {
        'CONTROL OPERATIVO DE SEGURIDAD': 'HEADER',
        'I. AREA COMEDOR': 'COMEDOR',
        'II. AREA ASADORES': 'ASADORES', 
        'III. AREA DE MARINADO': 'MARINADO',
        'IV. AREA DE BODEGA': 'BODEGA',
        'V. AREA DE HORNO': 'HORNO',
        'VI. AREA FREIDORAS': 'FREIDORAS',
        'VII. CENTRO DE CARGA': 'CENTRO_CARGA',
        'VIII. AREA AZOTEA': 'AZOTEA',
        'IX. AREA EXTERIOR': 'EXTERIOR',
        'X. PROGRAMA INTERNO PROTECCION CIVIL': 'PROTECCION_CIVIL',
        'XI. BITACORAS': 'BITACORAS',
        'XII. NOMBRES Y FIRMAS': 'FIRMAS'
    }
    
    areas_data = {}
    current_area = 'HEADER'
    
    # Inicializar áreas
    for area_code in AREAS_MAP.values():
        areas_data[area_code] = {
            'area_codigo': area_code,
            'area_nombre': '',
            'elementos_evaluados': 0,
            'elementos_conformes': 0,
            'elementos_no_conformes': 0,
            'campos_completados': 0,
            'total_campos': 0,
            'evidencia_fotografica': 0,
            'respuestas_si': 0,
            'respuestas_no': 0,
            'elementos_criticos_fallidos': [],
            'detalles': []
        }
    
    # Procesar respuestas
    for answer in answers:
        field_type = answer.get('field_type', '')
        title = answer.get('title', '')
        value = answer.get('value')
        is_answered = answer.get('is_answered', False)
        
        # Detectar cambio de área
        if field_type == 'section' and title in AREAS_MAP:
            current_area = AREAS_MAP[title]
            areas_data[current_area]['area_nombre'] = title
            continue
        
        # Procesar campo dentro del área actual
        area = areas_data[current_area]
        area['total_campos'] += 1
        
        if is_answered and value is not None:
            area['campos_completados'] += 1
            
            # Datos específicos del HEADER
            if current_area == 'HEADER':
                if 'PUNTOS MAX' in title:
                    area['puntos_max'] = value
                elif 'PUNTOS TOTALES OBTENIDOS' in title:
                    area['puntos_obtenidos'] = value
                elif 'CALIFICACION PORCENTAJE' in title:
                    area['calificacion_general'] = value
            
            # Procesar respuestas SI/NO
            if field_type == 'yesno':
                area['elementos_evaluados'] += 1
                yesno_value = answer.get('yesno_value')
                
                if yesno_value in ['true', True]:
                    area['elementos_conformes'] += 1
                    area['respuestas_si'] += 1
                elif yesno_value in ['false', False]:
                    area['elementos_no_conformes'] += 1
                    area['respuestas_no'] += 1
                    
                    # Elemento crítico fallido
                    area['elementos_criticos_fallidos'].append(title)
            
            # Contar imágenes
            elif field_type == 'image' and value:
                if isinstance(value, list):
                    area['evidencia_fotografica'] += len(value)
                else:
                    area['evidencia_fotografica'] += 1
    
    # Calcular porcentajes finales
    for area_data in areas_data.values():
        if area_data['elementos_evaluados'] > 0:
            area_data['conformidad_porcentaje'] = round(
                (area_data['elementos_conformes'] / area_data['elementos_evaluados']) * 100, 2
            )
        else:
            area_data['conformidad_porcentaje'] = None
            
        if area_data['total_campos'] > 0:
            area_data['completitud_porcentaje'] = round(
                (area_data['campos_completados'] / area_data['total_campos']) * 100, 2
            )
        else:
            area_data['completitud_porcentaje'] = 0
    
    return areas_data

def save_supervision_to_db(supervision_data, conn):
    """Guarda supervisión completa en base de datos"""
    
    cursor = conn.cursor()
    
    try:
        # 1. Normalizar sucursal
        sucursal_info = normalize_sucursal_name(supervision_data['sucursal_nombre_zenput'], conn)
        
        if not sucursal_info['normalizado']:
            print(f"⚠️ ALERTA: Sucursal no encontrada: {supervision_data['sucursal_nombre_zenput']}")
            # Log de alerta pero continuar procesamiento
        
        # 2. Determinar periodo
        fecha_supervision = datetime.fromisoformat(supervision_data['fecha_supervision'].replace('Z', '+00:00')) if supervision_data['fecha_supervision'] else None
        periodo_info = determine_periodo_supervision(fecha_supervision, conn) if fecha_supervision else None
        
        # 3. Insertar supervisión principal
        insert_supervision_sql = """
        INSERT INTO supervisiones (
            submission_id, form_id, form_name,
            sucursal_numero, sucursal_nombre_zenput, sucursal_nombre_normalizado,
            supervisor_id, supervisor_nombre, supervisor_rol,
            fecha_supervision, fecha_completada, fecha_enviada, 
            tiempo_supervision_minutos,
            calificacion_general, puntos_obtenidos, puntos_maximos,
            total_preguntas, total_respondidas, completitud_porcentaje,
            total_imagenes, respuestas_si, respuestas_no,
            coordenadas_lat, coordenadas_lon, distancia_sucursal_km,
            periodo_id, plataforma, ambiente, zona_horaria,
            procesada, fecha_procesamiento
        ) VALUES (
            %s, %s, %s,
            %s, %s, %s, 
            %s, %s, %s,
            %s, %s, %s, 
            %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s
        ) RETURNING id
        """
        
        # Convertir tiempo a minutos
        tiempo_minutos = supervision_data.get('tiempo_supervision_ms', 0) // (1000 * 60) if supervision_data.get('tiempo_supervision_ms') else None
        
        cursor.execute(insert_supervision_sql, (
            supervision_data['submission_id'],
            supervision_data['form_id'],
            supervision_data['form_name'][:150],  # Límite de caracteres
            sucursal_info['sucursal_numero'],
            supervision_data['sucursal_nombre_zenput'][:100],
            sucursal_info['nombre_normalizado'][:100],
            supervision_data.get('supervisor_id'),
            supervision_data.get('supervisor_nombre', '')[:100],
            supervision_data.get('supervisor_rol', '')[:50],
            fecha_supervision,
            datetime.fromisoformat(supervision_data['fecha_completada'].replace('Z', '+00:00')) if supervision_data.get('fecha_completada') else None,
            datetime.fromisoformat(supervision_data['fecha_enviada'].replace('Z', '+00:00')) if supervision_data.get('fecha_enviada') else None,
            tiempo_minutos,
            supervision_data.get('calificacion_general'),
            supervision_data.get('puntos_obtenidos'),
            supervision_data.get('puntos_maximos'),
            supervision_data.get('total_preguntas', 0),
            supervision_data.get('total_respondidas', 0),
            supervision_data.get('completitud_porcentaje', 0),
            supervision_data.get('total_imagenes', 0),
            supervision_data.get('respuestas_si', 0),
            supervision_data.get('respuestas_no', 0),
            supervision_data.get('coordenadas_lat'),
            supervision_data.get('coordenadas_lon'),
            supervision_data.get('distancia_sucursal_km'),
            periodo_info['periodo_id'] if periodo_info else None,
            supervision_data.get('plataforma', '')[:20],
            supervision_data.get('ambiente', '')[:20],
            supervision_data.get('zona_horaria', '')[:50],
            True,  # procesada
            datetime.now()
        ))
        
        supervision_id = cursor.fetchone()[0]
        
        # 4. Insertar áreas
        for area_code, area_data in supervision_data['areas'].items():
            if area_code == 'HEADER' or area_data['total_campos'] == 0:
                continue  # Skip HEADER y áreas vacías
            
            insert_area_sql = """
            INSERT INTO supervision_areas (
                supervision_id, area_codigo, area_nombre, area_orden,
                elementos_evaluados, elementos_conformes, elementos_no_conformes,
                conformidad_porcentaje, completitud_porcentaje,
                campos_completados, total_campos,
                evidencia_fotografica, respuestas_si, respuestas_no,
                elementos_criticos_fallidos
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s
            )
            """
            
            # Determinar orden del área
            area_orden = {
                'COMEDOR': 1, 'ASADORES': 2, 'MARINADO': 3, 'BODEGA': 4,
                'HORNO': 5, 'FREIDORAS': 6, 'CENTRO_CARGA': 7, 'AZOTEA': 8,
                'EXTERIOR': 9, 'PROTECCION_CIVIL': 10, 'BITACORAS': 11, 'FIRMAS': 12
            }.get(area_code, 99)
            
            cursor.execute(insert_area_sql, (
                supervision_id,
                area_code,
                area_data['area_nombre'][:100],
                area_orden,
                area_data['elementos_evaluados'],
                area_data['elementos_conformes'],
                area_data['elementos_no_conformes'],
                area_data['conformidad_porcentaje'],
                area_data['completitud_porcentaje'],
                area_data['campos_completados'],
                area_data['total_campos'],
                area_data['evidencia_fotografica'],
                area_data['respuestas_si'],
                area_data['respuestas_no'],
                area_data['elementos_criticos_fallidos']
            ))
        
        conn.commit()
        print(f"   ✅ Supervisión guardada: {sucursal_info['nombre_normalizado']} - {supervision_data.get('supervisor_nombre', 'N/A')}")
        
        return True
        
    except Exception as e:
        conn.rollback()
        print(f"   ❌ Error guardando supervisión {supervision_data['submission_id']}: {e}")
        return False
        
    finally:
        cursor.close()

def run_complete_etl():
    """Ejecuta ETL completo con base de datos"""
    
    print("🔄 ETL COMPLETO SUPERVISIONES - BASE DE DATOS")
    print("=" * 60)
    print("🎯 Extrae supervisiones → Normaliza sucursales → Guarda en PostgreSQL")
    print("=" * 60)
    
    # Conectar a API y BD
    client = create_zenput_client()
    if not client.validate_api_connection():
        print("❌ No se puede conectar a API Zenput")
        return False
    
    conn = connect_database()
    if not conn:
        return False
    
    # Procesar formularios
    supervision_forms = {
        '877138': 'Supervisión Operativa EPL CAS',
        '877139': 'Control Operativo de Seguridad EPL CAS'
    }
    
    total_procesadas = 0
    total_exitosas = 0
    
    for form_id, form_name in supervision_forms.items():
        print(f"\n🔍 === PROCESANDO {form_name} ({form_id}) ===")
        print("-" * 60)
        
        # Obtener supervisiones nuevas (últimos días)
        submissions = client.get_submissions_for_form(form_id, days_back=7)
        
        if not submissions:
            print(f"⚠️ No hay submissions nuevas para {form_id}")
            continue
        
        print(f"📊 Procesando {len(submissions)} submissions")
        
        for i, submission in enumerate(submissions, 1):
            # Verificar si ya existe
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM supervisiones WHERE submission_id = %s", (submission.get('id'),))
            exists = cursor.fetchone()
            cursor.close()
            
            if exists:
                # print(f"   ⏭️  {i:2d}. Ya procesada: {submission.get('id')}")
                continue
            
            # Extraer y guardar
            try:
                supervision_data = extract_supervision_data(submission)
                success = save_supervision_to_db(supervision_data, conn)
                
                total_procesadas += 1
                if success:
                    total_exitosas += 1
                    
            except Exception as e:
                print(f"   ❌ Error procesando submission {i}: {e}")
    
    conn.close()
    
    # Resumen final
    print(f"\n📊 === RESUMEN ETL COMPLETO ===")
    print(f"   ✅ Total supervisiones procesadas: {total_procesadas}")
    print(f"   💾 Guardadas exitosamente: {total_exitosas}")
    print(f"   ❌ Errores: {total_procesadas - total_exitosas}")
    
    if total_exitosas > 0:
        print(f"\n🎉 ETL ejecutado exitosamente - Datos listos para dashboard")
        return True
    else:
        print(f"\n⚠️ No se procesaron supervisiones nuevas")
        return False

def main():
    """Función principal"""
    
    try:
        return run_complete_etl()
    except Exception as e:
        print(f"💥 Error crítico en ETL: {e}")
        return False

if __name__ == "__main__":
    main()