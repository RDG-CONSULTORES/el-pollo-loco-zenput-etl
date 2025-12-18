#!/usr/bin/env python3
"""
🚀 EL POLLO LOCO ZENPUT ETL - RAILWAY ENTRY POINT
Entry point para Railway deployment con endpoints web básicos
"""

from flask import Flask, jsonify
import os
import psycopg2
import requests
import time
from datetime import datetime

app = Flask(__name__)

# Configuración desde variables de entorno Railway
DATABASE_URL = os.environ.get('DATABASE_URL')

@app.route('/')
def home():
    """Endpoint principal - status del sistema"""
    print(f"🌐 Home endpoint accessed at {datetime.now()}")
    return jsonify({
        'project': 'El Pollo Loco Zenput ETL',
        'status': 'active',
        'timestamp': datetime.now().isoformat(),
        'description': 'ETL System for El Pollo Loco México - Zenput API Integration',
        'version': '1.0.0',
        'environment': 'Railway',
        'port': os.environ.get('PORT', '8080'),
        'services': {
            'database': 'PostgreSQL on Railway',
            'api': 'Zenput API v3',
            'etl': 'Python Scripts'
        },
        'endpoints': {
            '/': 'System status',
            '/health': 'Health check',
            '/database': 'Database connection test',
            '/stats': 'ETL statistics',
            '/etl/run': 'Execute ETL process'
        }
    })

@app.route('/health')
def health():
    """Health check endpoint"""
    try:
        # Test database connection
        if DATABASE_URL:
            conn = psycopg2.connect(DATABASE_URL)
            cursor = conn.cursor()
            cursor.execute("SELECT 1;")
            cursor.close()
            conn.close()
            db_status = "connected"
        else:
            db_status = "no_credentials"
        
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'database': db_status,
            'environment': 'Railway'
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/database')
def database_test():
    """Test database connection and show basic info"""
    if not DATABASE_URL:
        return jsonify({
            'error': 'DATABASE_URL not configured',
            'message': 'Configure PostgreSQL service in Railway'
        }), 400
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Test basic queries
        cursor.execute("SELECT version();")
        postgres_version = cursor.fetchone()[0]
        
        cursor.execute("SELECT current_database();")
        current_db = cursor.fetchone()[0]
        
        # Check if our tables exist
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'status': 'connected',
            'postgres_version': postgres_version,
            'database': current_db,
            'tables_count': len(tables),
            'tables': tables,
            'schema_ready': 'grupos_operativos' in tables,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/stats')
def etl_stats():
    """Show ETL statistics if database is ready"""
    if not DATABASE_URL:
        return jsonify({'error': 'DATABASE_URL not configured'}), 400
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        stats = {}
        
        # Check if tables exist
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name IN 
            ('grupos_operativos', 'sucursales', 'supervisions')
            ORDER BY table_name;
        """)
        existing_tables = [row[0] for row in cursor.fetchall()]
        
        if 'grupos_operativos' in existing_tables:
            cursor.execute("SELECT COUNT(*) FROM grupos_operativos;")
            stats['grupos_operativos'] = cursor.fetchone()[0]
        
        if 'sucursales' in existing_tables:
            cursor.execute("SELECT COUNT(*) FROM sucursales;")
            stats['sucursales'] = cursor.fetchone()[0]
        
        if 'supervisions' in existing_tables:
            cursor.execute("SELECT COUNT(*) FROM supervisions;")
            stats['total_supervisions'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM supervisions WHERE form_type = 'OPERATIVA';")
            stats['supervisions_operativa'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM supervisions WHERE form_type = 'SEGURIDAD';")
            stats['supervisions_seguridad'] = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'status': 'success',
            'existing_tables': existing_tables,
            'stats': stats,
            'schema_ready': len(existing_tables) == 3,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/etl/run')
def run_etl():
    """Ejecutar ETL completo desde Railway"""
    
    if not DATABASE_URL:
        return jsonify({'error': 'DATABASE_URL not configured'}), 400
    
    # Configuración Zenput - USAR TOKEN DEL ETL FUNCIONAL
    zenput_config = {
        'base_url': 'https://www.zenput.com/api/v3',
        'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}  # Token del ETL de 189 días
    }
    
    try:
        # Configurar sesión con retry automático y DNS backup
        session = requests.Session()
        from requests.adapters import HTTPAdapter
        from requests.packages.urllib3.util.retry import Retry
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Test conexión Zenput con retry
        zenput_test = session.get(
            f"{zenput_config['base_url']}/forms", 
            headers=zenput_config['headers'],
            timeout=20
        )
        
        if zenput_test.status_code != 200:
            return jsonify({
                'error': 'Zenput API not accessible',
                'status_code': zenput_test.status_code
            }), 400
        
        # Extraer submissions básicas
        operativas_count = extract_and_count_submissions('877138', zenput_config)
        seguridad_count = extract_and_count_submissions('877139', zenput_config)
        
        return jsonify({
            'status': 'etl_test_completed',
            'timestamp': datetime.now().isoformat(),
            'zenput_accessible': True,
            'found_operativas': operativas_count,
            'found_seguridad': seguridad_count,
            'message': 'ETL test successful - ready for full extraction'
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/etl/test-connection')
def test_connection():
    """Test conectividad y DNS para diagnostico"""
    import socket
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'tests': {}
    }
    
    # Test 1: DNS Resolution
    try:
        ip = socket.gethostbyname('api.zenput.com')
        results['tests']['dns'] = {'status': 'OK', 'ip': ip}
    except Exception as e:
        results['tests']['dns'] = {'status': 'FAILED', 'error': str(e)}
    
    # Test 2: Basic HTTP to external site
    try:
        test_response = requests.get('https://httpbin.org/status/200', timeout=10)
        results['tests']['external_http'] = {'status': 'OK', 'code': test_response.status_code}
    except Exception as e:
        results['tests']['external_http'] = {'status': 'FAILED', 'error': str(e)}
    
    # Test 3: Zenput API with MULTIPLE VERSIONS
    api_tests = [
        ('v3_forms', 'https://www.zenput.com/api/v3/forms'),
        ('v1_forms', 'https://www.zenput.com/api/v1/forms'),
        ('v3_submissions', 'https://www.zenput.com/api/v3/submissions'),
        ('v1_submissions', 'https://www.zenput.com/api/v1/submissions')
    ]
    
    for test_name, api_url in api_tests:
        try:
            response = requests.get(
                api_url,
                headers={'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'},
                timeout=10
            )
            results['tests'][test_name] = {
                'status': 'OK' if response.status_code == 200 else 'ERROR',
                'code': response.status_code,
                'url': api_url
            }
        except Exception as e:
            results['tests'][test_name] = {'status': 'FAILED', 'error': str(e), 'url': api_url}
    
    return jsonify(results)

@app.route('/etl/debug')
def debug_submission_structure():
    """Debug para ver estructura de submissions sin cargar a DB"""
    
    if not DATABASE_URL:
        return jsonify({'error': 'DATABASE_URL not configured'}), 400
    
    # Configuración Zenput
    zenput_config = {
        'base_url': 'https://www.zenput.com/api/v3',
        'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}
    }
    
    try:
        # Obtener solo 1 submission para debug
        endpoint_url = f"{zenput_config['base_url']}/submissions"
        params = {
            'form_template_id': '877138',
            'limit': 1,
            'created_after': '2025-01-01',
            'created_before': '2025-12-18'
        }
        
        response = requests.get(endpoint_url, headers=zenput_config['headers'], params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            submissions = data.get('data', [])
            
            if submissions:
                first_submission = submissions[0]
                
                return jsonify({
                    'status': 'debug_success',
                    'api_response_structure': {
                        'total_count': data.get('count', len(submissions)),
                        'keys_in_response': list(data.keys()),
                        'submissions_count': len(submissions)
                    },
                    'submission_structure': {
                        'available_keys': list(first_submission.keys()),
                        'location': first_submission.get('location'),
                        'smetadata_keys': list(first_submission.get('smetadata', {}).keys()) if first_submission.get('smetadata') else None,
                        'smetadata_location': first_submission.get('smetadata', {}).get('location') if first_submission.get('smetadata') else None,
                        'sample_answers_count': len(first_submission.get('answers', [])),
                        'form_template': first_submission.get('form_template'),
                        'id': first_submission.get('id')
                    },
                    'timestamp': datetime.now().isoformat()
                })
            else:
                return jsonify({
                    'status': 'no_submissions',
                    'message': 'No submissions found',
                    'api_response': data
                })
        else:
            return jsonify({
                'status': 'api_error',
                'status_code': response.status_code,
                'error': response.text
            })
            
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/etl/debug-sucursales')
def debug_sucursales():
    """Debug para ver qué sucursales IDs tenemos en Railway vs Zenput"""
    
    if not DATABASE_URL:
        return jsonify({'error': 'DATABASE_URL not configured'}), 400
    
    try:
        # 1. Ver sucursales en Railway PostgreSQL
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        cursor.execute("SELECT external_key, id, nombre FROM sucursales WHERE external_key IS NOT NULL ORDER BY external_key;")
        railway_sucursales = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # 2. Ver sucursales de Zenput (sample de 5 submissions)
        zenput_config = {
            'base_url': 'https://www.zenput.com/api/v3',
            'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}
        }
        
        endpoint_url = f"{zenput_config['base_url']}/submissions"
        params = {
            'form_template_id': '877138',
            'limit': 10,
            'created_after': '2025-01-01',
            'created_before': '2025-12-18'
        }
        
        response = requests.get(endpoint_url, headers=zenput_config['headers'], params=params, timeout=30)
        
        zenput_sucursales = []
        if response.status_code == 200:
            data = response.json()
            submissions = data.get('data', [])
            
            for submission in submissions[:5]:  # Solo 5 para debug
                smetadata = submission.get('smetadata') or {}
                location = smetadata.get('location') or {}
                
                sucursal_id = location.get('external_key') or location.get('id')
                sucursal_nombre = location.get('name', '')
                
                zenput_sucursales.append({
                    'submission_id': submission.get('id'),
                    'sucursal_id': sucursal_id,
                    'sucursal_nombre': sucursal_nombre,
                    'external_key': location.get('external_key'),
                    'location_id': location.get('id')
                })
        
        return jsonify({
            'status': 'debug_success',
            'railway_sucursales_count': len(railway_sucursales),
            'railway_sucursales_sample': railway_sucursales[:10],
            'zenput_sucursales_sample': zenput_sucursales,
            'comparison': {
                'railway_external_keys': [str(row[0]) for row in railway_sucursales],
                'zenput_sucursal_ids': [str(item['sucursal_id']) for item in zenput_sucursales if item['sucursal_id']]
            },
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/etl/load-real-data')
def load_real_epl_data():
    """Cargar estructura real completa de El Pollo Loco (86 sucursales, 20 grupos)"""
    
    if not DATABASE_URL:
        return jsonify({'error': 'DATABASE_URL not configured'}), 400
    
    try:
        import csv
        import os
        
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # 1. CARGAR 20 GRUPOS OPERATIVOS REALES
        print("👥 Cargando 20 grupos operativos reales...")
        
        grupos_reales = [
            ('TEPEYAC', 'LOCAL'),
            ('OGAS', 'LOCAL'), 
            ('EFM', 'LOCAL'),
            ('TEC', 'LOCAL'),
            ('EXPO', 'LOCAL'),
            ('EPL SO', 'LOCAL'),
            ('GRUPO CENTRITO', 'LOCAL'),
            ('PLOG NUEVO LEON', 'FORANEA'),
            ('PLOG LAGUNA', 'FORANEA'),
            ('PLOG QUERETARO', 'FORANEA'),
            ('GRUPO SALTILLO', 'FORANEA'),
            ('OCHTER TAMPICO', 'FORANEA'),
            ('GRUPO MATAMOROS', 'FORANEA'),
            ('CRR', 'FORANEA'),
            ('RAP', 'FORANEA'),
            ('GRUPO RIO BRAVO', 'FORANEA'),
            ('GRUPO NUEVO LAREDO (RUELAS)', 'FORANEA'),
            ('GRUPO PIEDRAS NEGRAS', 'FORANEA'),
            ('GRUPO SABINAS HIDALGO', 'FORANEA'),
            ('GRUPO CANTERA ROSA (MORELIA)', 'FORANEA')
        ]
        
        for nombre, clasificacion in grupos_reales:
            cursor.execute("""
                INSERT INTO grupos_operativos (nombre, clasificacion)
                VALUES (%s, %s)
                ON CONFLICT (nombre) DO UPDATE SET clasificacion = EXCLUDED.clasificacion;
            """, (nombre, clasificacion))
        
        grupos_loaded = len(grupos_reales)
        
        # 2. CARGAR 86 SUCURSALES REALES DESDE CSV
        print("🏪 Cargando 86 sucursales reales...")
        
        csv_path = 'data/86_sucursales_master.csv'
        sucursales_loaded = 0
        
        if os.path.exists(csv_path):
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Obtener grupo_id
                    cursor.execute("SELECT id FROM grupos_operativos WHERE nombre = %s;", (row['Grupo_Operativo'],))
                    grupo_result = cursor.fetchone()
                    
                    if grupo_result:
                        grupo_id = grupo_result[0]
                        
                        cursor.execute("""
                            INSERT INTO sucursales (external_key, nombre, ciudad, estado, latitude, longitude, grupo_operativo_id)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (external_key) DO UPDATE SET
                                nombre = EXCLUDED.nombre,
                                grupo_operativo_id = EXCLUDED.grupo_operativo_id,
                                latitude = EXCLUDED.latitude,
                                longitude = EXCLUDED.longitude;
                        """, (
                            row['Numero_Sucursal'],
                            row['Nombre_Sucursal'],
                            row['Ciudad'],
                            row['Estado'],
                            float(row['Latitude']) if row['Latitude'] else None,
                            float(row['Longitude']) if row['Longitude'] else None,
                            grupo_id
                        ))
                        sucursales_loaded += 1
        
        # 3. VERIFICAR CARGA
        cursor.execute("SELECT COUNT(*) FROM grupos_operativos;")
        total_grupos = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM sucursales;")
        total_sucursales = cursor.fetchone()[0]
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'status': 'real_data_loaded',
            'message': 'Estructura completa de El Pollo Loco cargada',
            'data': {
                'grupos_operativos': total_grupos,
                'sucursales': total_sucursales,
                'grupos_loaded': grupos_loaded,
                'sucursales_loaded': sucursales_loaded
            },
            'ready_for_etl': True,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/etl/full')
def run_full_etl():
    """Ejecutar ETL COMPLETO con mapping directo Teams → Grupos"""
    
    if not DATABASE_URL:
        return jsonify({'error': 'DATABASE_URL not configured'}), 400
    
    # MAPPING DIRECTO Teams → Grupos Operativos (descubierto de Teams API)
    TEAMS_TO_GRUPOS = {
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
        115116: "GRUPO NUEVO LAREDO (RUELAS)"
    }
    
    try:
        # Configuración Zenput
        zenput_config = {
            'base_url': 'https://www.zenput.com/api/v3',
            'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}
        }
        
        # 1. Extraer submissions operativas
        print("🔄 Extrayendo submissions operativas...")
        operativas_url = f"{zenput_config['base_url']}/submissions"
        operativas_params = {
            'form_template_id': '877138',
            'limit': 100,
            'created_after': '2025-01-01'
        }
        
        operativas_response = requests.get(operativas_url, headers=zenput_config['headers'], params=operativas_params, timeout=30)
        
        if operativas_response.status_code != 200:
            return jsonify({
                'error': 'Failed to fetch operativas submissions',
                'status_code': operativas_response.status_code
            }), 400
        
        operativas_data = operativas_response.json()
        operativas_submissions = operativas_data.get('data', [])
        
        print(f"✅ Found {len(operativas_submissions)} operativas submissions")
        
        # 2. Conectar a PostgreSQL y crear tabla de test
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Crear tabla de test si no existe
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS supervisions_test (
                id SERIAL PRIMARY KEY,
                submission_id VARCHAR(50) UNIQUE NOT NULL,
                form_type VARCHAR(20),
                location_id INTEGER,
                location_name VARCHAR(100),
                grupo_operativo VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # 3. Procesar submissions con mapping directo
        loaded_count = 0
        error_count = 0
        
        for submission in operativas_submissions[:10]:  # Limitamos a 10 para test
            try:
                # Extraer datos básicos
                submission_id = submission.get('id')
                smetadata = submission.get('smetadata', {})
                location = smetadata.get('location', {})
                
                # Obtener grupo operativo via teams mapping
                teams_data = smetadata.get('teams', [])
                grupo_operativo = None
                
                for team_info in teams_data:
                    team_id = team_info.get('id')
                    if team_id in TEAMS_TO_GRUPOS:
                        grupo_operativo = TEAMS_TO_GRUPOS[team_id]
                        break
                
                if not grupo_operativo:
                    error_count += 1
                    continue
                
                # Insertar en PostgreSQL
                cursor.execute("""
                    INSERT INTO supervisions_test (
                        submission_id, form_type, location_id, location_name,
                        grupo_operativo, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (submission_id) DO NOTHING
                """, (
                    submission_id,
                    'OPERATIVA',
                    location.get('id'),
                    location.get('name'),
                    grupo_operativo,
                    datetime.now()
                ))
                
                loaded_count += 1
                
            except Exception as e:
                print(f"Error processing submission {submission.get('id')}: {e}")
                error_count += 1
                continue
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'status': 'etl_completed',
            'message': 'ETL con mapping directo ejecutado',
            'total_submissions': len(operativas_submissions),
            'loaded_count': loaded_count,
            'error_count': error_count,
            'teams_mapping_used': len(TEAMS_TO_GRUPOS),
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'status': 'etl_error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

def extract_and_count_submissions(form_id, zenput_config, max_pages=3):
    """Extraer y contar submissions de un formulario"""
    
    total_count = 0
    
    try:
        for page in range(1, max_pages + 1):
            url = f"{zenput_config['base_url']}/submissions"
            params = {
                'form_id': form_id,
                'submitted_at_start': '2025-01-01',
                'submitted_at_end': '2025-12-18',
                'page': page,
                'per_page': 100
            }
            
            response = requests.get(url, headers=zenput_config['headers'], params=params, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                submissions = data.get('submissions', [])
                
                # Debug: mostrar información de la respuesta
                print(f"🔍 Form {form_id} Page {page}: {len(submissions)} submissions found")
                
                if not submissions:
                    break
                
                total_count += len(submissions)
                time.sleep(0.5)  # Rate limiting
                
            else:
                break
                
    except Exception as e:
        pass
    
    return total_count

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    print(f"🚀 Starting Flask app on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)