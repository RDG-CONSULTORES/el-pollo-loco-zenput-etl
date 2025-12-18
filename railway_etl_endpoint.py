#!/usr/bin/env python3
"""
🚀 RAILWAY ETL ENDPOINT
Endpoint web para ejecutar ETL desde Railway (con conectividad a Zenput)
"""

from flask import Flask, jsonify, request
import requests
import psycopg2
import json
import os
from datetime import datetime
import time

app = Flask(__name__)

# Configuración desde variables de entorno Railway
DATABASE_URL = os.environ.get('DATABASE_URL')

# CONFIGURACIÓN ZENPUT
ZENPUT_CONFIG = {
    'base_url': 'https://api.zenput.com/api/v3',
    'headers': {'X-API-TOKEN': 'e52c41a1-c026-42fb-8264-d8a6e7c2aeb5'}
}

@app.route('/etl/run')
def run_etl():
    """Ejecutar ETL completo desde Railway"""
    
    if not DATABASE_URL:
        return jsonify({'error': 'DATABASE_URL not configured'}), 400
    
    try:
        # Test conexión Zenput
        zenput_test = requests.get(
            f"{ZENPUT_CONFIG['base_url']}/forms", 
            headers=ZENPUT_CONFIG['headers'],
            timeout=10
        )
        
        if zenput_test.status_code != 200:
            return jsonify({
                'error': 'Zenput API not accessible',
                'status_code': zenput_test.status_code
            }), 400
        
        # Ejecutar ETL
        result = execute_complete_etl()
        
        return jsonify({
            'status': 'success',
            'timestamp': datetime.now().isoformat(),
            'result': result
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

def execute_complete_etl():
    """Ejecutar ETL completo"""
    
    results = {
        'operativa_submissions': 0,
        'seguridad_submissions': 0,
        'total_loaded': 0,
        'errors': []
    }
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        
        # ETL Operativas (877138)
        operativas = extract_submissions('877138')
        if operativas:
            loaded = load_submissions_to_db(conn, operativas, 'OPERATIVA')
            results['operativa_submissions'] = len(operativas)
            results['total_loaded'] += loaded
        
        # ETL Seguridad (877139)
        seguridad = extract_submissions('877139')
        if seguridad:
            loaded = load_submissions_to_db(conn, seguridad, 'SEGURIDAD')
            results['seguridad_submissions'] = len(seguridad)
            results['total_loaded'] += loaded
        
        conn.close()
        
    except Exception as e:
        results['errors'].append(str(e))
    
    return results

def extract_submissions(form_id, limit_pages=5):
    """Extraer submissions de un formulario"""
    
    submissions = []
    page = 1
    
    while page <= limit_pages:
        try:
            url = f"{ZENPUT_CONFIG['base_url']}/submissions"
            params = {
                'form_id': form_id,
                'submitted_at_start': '2025-01-01',
                'submitted_at_end': '2025-12-31',
                'page': page,
                'per_page': 20
            }
            
            response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                page_submissions = data.get('submissions', [])
                
                if not page_submissions:
                    break
                
                submissions.extend(page_submissions)
                page += 1
                time.sleep(1)  # Rate limiting
                
            else:
                break
                
        except Exception as e:
            break
    
    return submissions

def load_submissions_to_db(conn, submissions, form_type):
    """Cargar submissions en base de datos"""
    
    cursor = conn.cursor()
    loaded = 0
    
    for submission in submissions:
        try:
            # Procesar submission básico
            submission_data = process_submission(submission, form_type)
            
            if submission_data:
                # Insertar en tabla principal
                cursor.execute("""
                    INSERT INTO supervisions (
                        submission_id, form_id, form_type, sucursal_nombre,
                        auditor_nombre, fecha_supervision, fecha_submission,
                        puntos_max, puntos_obtenidos, calificacion_porcentaje,
                        score_zenput, latitude, longitude
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    ) ON CONFLICT (submission_id) DO NOTHING;
                """, (
                    submission_data['submission_id'],
                    submission_data['form_id'],
                    submission_data['form_type'],
                    submission_data['sucursal_nombre'],
                    submission_data['auditor_nombre'],
                    submission_data['fecha_supervision'],
                    submission_data['fecha_submission'],
                    submission_data['puntos_max'],
                    submission_data['puntos_obtenidos'],
                    submission_data['calificacion_porcentaje'],
                    submission_data['score_zenput'],
                    submission_data['latitude'],
                    submission_data['longitude']
                ))
                
                loaded += 1
                
        except Exception as e:
            continue
    
    conn.commit()
    cursor.close()
    
    return loaded

def process_submission(submission, form_type):
    """Procesar submission individual"""
    
    try:
        # Información básica
        submission_id = submission.get('id')
        location = submission.get('location', {})
        sucursal_nombre = location.get('name', '')
        
        metadata = submission.get('smetadata', {})
        created_by = metadata.get('created_by', {})
        auditor_nombre = created_by.get('display_name', '')
        
        fecha_submission = metadata.get('date_submitted')
        fecha_supervision = fecha_submission.split('T')[0] if fecha_submission else None
        
        latitude = location.get('lat')
        longitude = location.get('lon')
        score = submission.get('score', 0)
        
        # Valores por defecto basados en análisis previo
        if form_type == 'OPERATIVA':
            form_id = '877138'
            puntos_max = 100  # Estimado operativa
            puntos_obtenidos = int((score / 100.0) * puntos_max) if score else 0
        else:  # SEGURIDAD
            form_id = '877139'
            puntos_max = 45   # Validado seguridad
            puntos_obtenidos = int((score / 100.0) * puntos_max) if score else 0
        
        return {
            'submission_id': submission_id,
            'form_id': form_id,
            'form_type': form_type,
            'sucursal_nombre': sucursal_nombre,
            'auditor_nombre': auditor_nombre,
            'fecha_supervision': fecha_supervision,
            'fecha_submission': fecha_submission,
            'puntos_max': puntos_max,
            'puntos_obtenidos': puntos_obtenidos,
            'calificacion_porcentaje': score,
            'score_zenput': score,
            'latitude': latitude,
            'longitude': longitude
        }
        
    except Exception as e:
        return None

@app.route('/etl/status')
def etl_status():
    """Status del ETL"""
    
    if not DATABASE_URL:
        return jsonify({'error': 'DATABASE_URL not configured'}), 400
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Estadísticas
        cursor.execute("SELECT COUNT(*) FROM supervisions;")
        total_supervisions = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM supervisions WHERE form_type = 'OPERATIVA';")
        operativas = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM supervisions WHERE form_type = 'SEGURIDAD';")
        seguridad = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT sucursal_nombre) FROM supervisions;")
        sucursales_con_datos = cursor.fetchone()[0]
        
        # Últimas supervisiones
        cursor.execute("""
            SELECT form_type, sucursal_nombre, fecha_supervision, calificacion_porcentaje
            FROM supervisions 
            ORDER BY fecha_submission DESC 
            LIMIT 10;
        """)
        ultimas = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'status': 'success',
            'statistics': {
                'total_supervisions': total_supervisions,
                'operativas': operativas,
                'seguridad': seguridad,
                'sucursales_con_datos': sucursales_con_datos
            },
            'latest_supervisions': [
                {
                    'form_type': row[0],
                    'sucursal': row[1],
                    'fecha': row[2].isoformat() if row[2] else None,
                    'calificacion': float(row[3]) if row[3] else None
                } for row in ultimas
            ],
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)