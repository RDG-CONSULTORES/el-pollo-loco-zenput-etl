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

@app.route('/etl/full')
def run_full_etl():
    """Ejecutar ETL COMPLETO - todas las supervisiones 2025"""
    
    if not DATABASE_URL:
        return jsonify({'error': 'DATABASE_URL not configured'}), 400
    
    try:
        # Importar y ejecutar nuestro ETL completo
        import subprocess
        import os
        
        # Configurar environment variables
        env = os.environ.copy()
        env['DATABASE_URL'] = DATABASE_URL
        
        # Ejecutar ETL completo
        result = subprocess.run(
            ['python3', 'etl_supervisiones_completo.py'],
            capture_output=True,
            text=True,
            timeout=1800,  # 30 minutos max
            env=env
        )
        
        if result.returncode == 0:
            return jsonify({
                'status': 'etl_completed',
                'message': 'ETL completo ejecutado exitosamente',
                'output': result.stdout[-1000:],  # Últimas 1000 chars
                'timestamp': datetime.now().isoformat()
            })
        else:
            return jsonify({
                'status': 'etl_failed',
                'error': result.stderr,
                'output': result.stdout,
                'return_code': result.returncode,
                'timestamp': datetime.now().isoformat()
            }), 500
            
    except subprocess.TimeoutExpired:
        return jsonify({
            'status': 'etl_timeout',
            'error': 'ETL execution timed out (30 minutes)',
            'timestamp': datetime.now().isoformat()
        }), 500
    
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