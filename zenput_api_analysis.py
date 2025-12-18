#!/usr/bin/env python3
"""
🔍 ZENPUT API EMPIRICAL ANALYSIS
Análisis empírico de límites y características del API de Zenput para ETL producción
"""

import requests
import time
import json
from datetime import datetime, timedelta
import psycopg2

# Configuración
ZENPUT_CONFIG = {
    'base_url': 'https://api.zenput.com/api/v3',
    'headers': {'X-API-TOKEN': 'e52c41a1-c026-42fb-8264-d8a6e7c2aeb5'}
}

RAILWAY_CONFIG = {
    'host': 'turntable.proxy.rlwy.net',
    'port': '24097',
    'database': 'railway',
    'user': 'postgres',
    'password': 'qGgdIUuKYKMKGtSNYzARpyapBWHsloOt'
}

def test_api_limits():
    """Test empírico de límites del API"""
    
    print("🔍 INICIANDO ANÁLISIS EMPÍRICO ZENPUT API")
    print("=" * 60)
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'tests': {}
    }
    
    # Test 1: Rate Limiting
    print("\n📊 TEST 1: RATE LIMITING")
    rate_limit_results = test_rate_limits()
    results['tests']['rate_limiting'] = rate_limit_results
    
    # Test 2: Pagination Limits
    print("\n📄 TEST 2: PAGINATION LIMITS")
    pagination_results = test_pagination_limits()
    results['tests']['pagination'] = pagination_results
    
    # Test 3: Date Range Limits
    print("\n📅 TEST 3: DATE RANGE LIMITS")
    date_range_results = test_date_range_limits()
    results['tests']['date_ranges'] = date_range_results
    
    # Test 4: Token Validation
    print("\n🔑 TEST 4: TOKEN VALIDATION")
    token_results = test_token_validation()
    results['tests']['token'] = token_results
    
    # Test 5: Error Response Analysis
    print("\n❌ TEST 5: ERROR RESPONSES")
    error_results = test_error_responses()
    results['tests']['errors'] = error_results
    
    # Guardar resultados
    with open('zenput_api_analysis_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n✅ Análisis completado - resultados guardados en zenput_api_analysis_results.json")
    return results

def test_rate_limits():
    """Test límites de rate limiting"""
    
    results = {
        'start_time': datetime.now().isoformat(),
        'requests_made': 0,
        'rate_limit_hit': False,
        'response_times': [],
        'status_codes': []
    }
    
    print("   🚀 Enviando requests secuenciales...")
    
    for i in range(20):  # Test con 20 requests rápidos
        start = time.time()
        
        try:
            response = requests.get(
                f"{ZENPUT_CONFIG['base_url']}/forms",
                headers=ZENPUT_CONFIG['headers'],
                timeout=10
            )
            
            end = time.time()
            response_time = (end - start) * 1000  # ms
            
            results['requests_made'] += 1
            results['response_times'].append(response_time)
            results['status_codes'].append(response.status_code)
            
            print(f"   Request {i+1}: {response.status_code} - {response_time:.0f}ms")
            
            if response.status_code == 429:  # Rate limit
                results['rate_limit_hit'] = True
                results['rate_limit_request'] = i + 1
                break
                
            elif response.status_code != 200:
                results['error_at_request'] = i + 1
                results['error_status'] = response.status_code
                break
                
            time.sleep(0.5)  # Pequeña pausa entre requests
            
        except Exception as e:
            results['error'] = str(e)
            results['error_at_request'] = i + 1
            break
    
    results['end_time'] = datetime.now().isoformat()
    results['avg_response_time'] = sum(results['response_times']) / len(results['response_times']) if results['response_times'] else 0
    
    return results

def test_pagination_limits():
    """Test límites de paginación"""
    
    results = {
        'start_time': datetime.now().isoformat(),
        'max_per_page_tested': 100,
        'pagination_working': False,
        'max_per_page_found': None,
        'total_submissions_found': 0
    }
    
    # Test diferentes tamaños de página
    for per_page in [10, 20, 50, 100, 200, 500]:
        print(f"   📄 Testing per_page={per_page}...")
        
        try:
            response = requests.get(
                f"{ZENPUT_CONFIG['base_url']}/submissions",
                headers=ZENPUT_CONFIG['headers'],
                params={
                    'form_id': '877138',  # Forma operativa
                    'submitted_at_start': '2025-01-01',
                    'submitted_at_end': '2025-12-31',
                    'page': 1,
                    'per_page': per_page
                },
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                submissions_returned = len(data.get('submissions', []))
                
                results['max_per_page_found'] = per_page
                results['total_submissions_found'] = submissions_returned
                results['pagination_working'] = True
                
                print(f"   ✅ per_page={per_page}: {submissions_returned} submissions")
            else:
                print(f"   ❌ per_page={per_page}: Error {response.status_code}")
                break
                
        except Exception as e:
            print(f"   ❌ per_page={per_page}: Exception {e}")
            break
    
    return results

def test_date_range_limits():
    """Test límites de rangos de fecha"""
    
    results = {
        'start_time': datetime.now().isoformat(),
        'date_ranges_tested': [],
        'max_range_working': None,
        'errors': []
    }
    
    # Test diferentes rangos de fecha
    base_date = datetime(2025, 1, 1)
    
    for days in [1, 7, 30, 90, 180, 365, 730]:  # hasta 2 años
        end_date = base_date + timedelta(days=days)
        
        print(f"   📅 Testing range: {days} days ({base_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')})")
        
        try:
            response = requests.get(
                f"{ZENPUT_CONFIG['base_url']}/submissions",
                headers=ZENPUT_CONFIG['headers'],
                params={
                    'form_id': '877138',
                    'submitted_at_start': base_date.strftime('%Y-%m-%d'),
                    'submitted_at_end': end_date.strftime('%Y-%m-%d'),
                    'page': 1,
                    'per_page': 10
                },
                timeout=15
            )
            
            range_info = {
                'days': days,
                'start_date': base_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d'),
                'status_code': response.status_code,
                'working': response.status_code == 200
            }
            
            if response.status_code == 200:
                data = response.json()
                range_info['submissions_found'] = len(data.get('submissions', []))
                results['max_range_working'] = days
                print(f"   ✅ {days} days: {range_info['submissions_found']} submissions")
            else:
                range_info['error'] = f"HTTP {response.status_code}"
                print(f"   ❌ {days} days: Error {response.status_code}")
            
            results['date_ranges_tested'].append(range_info)
            
        except Exception as e:
            error_info = {
                'days': days,
                'error': str(e)
            }
            results['errors'].append(error_info)
            print(f"   ❌ {days} days: Exception {e}")
    
    return results

def test_token_validation():
    """Test validación y características del token"""
    
    results = {
        'start_time': datetime.now().isoformat(),
        'token_working': False,
        'endpoints_tested': [],
        'permissions': {}
    }
    
    endpoints_to_test = [
        ('forms', '/forms'),
        ('submissions', '/submissions'),
        ('users', '/users'),
        ('organizations', '/organizations'),
        ('teams', '/teams')
    ]
    
    for name, endpoint in endpoints_to_test:
        print(f"   🔑 Testing endpoint: {endpoint}")
        
        try:
            response = requests.get(
                f"{ZENPUT_CONFIG['base_url']}{endpoint}",
                headers=ZENPUT_CONFIG['headers'],
                timeout=10
            )
            
            endpoint_info = {
                'endpoint': endpoint,
                'status_code': response.status_code,
                'accessible': response.status_code == 200,
                'response_size': len(response.text) if response.text else 0
            }
            
            if response.status_code == 200:
                results['token_working'] = True
                results['permissions'][name] = 'READ'
                print(f"   ✅ {endpoint}: Accessible ({endpoint_info['response_size']} bytes)")
            elif response.status_code == 401:
                results['permissions'][name] = 'UNAUTHORIZED'
                print(f"   🚫 {endpoint}: Unauthorized")
            elif response.status_code == 403:
                results['permissions'][name] = 'FORBIDDEN'
                print(f"   🚫 {endpoint}: Forbidden")
            else:
                results['permissions'][name] = f'ERROR_{response.status_code}'
                print(f"   ❌ {endpoint}: Error {response.status_code}")
            
            results['endpoints_tested'].append(endpoint_info)
            
        except Exception as e:
            error_info = {
                'endpoint': endpoint,
                'error': str(e)
            }
            results['endpoints_tested'].append(error_info)
            print(f"   ❌ {endpoint}: Exception {e}")
    
    return results

def test_error_responses():
    """Test diferentes tipos de respuestas de error"""
    
    results = {
        'start_time': datetime.now().isoformat(),
        'error_tests': []
    }
    
    # Test con token inválido
    print("   ❌ Testing invalid token...")
    try:
        response = requests.get(
            f"{ZENPUT_CONFIG['base_url']}/forms",
            headers={'X-API-TOKEN': 'invalid-token-12345'},
            timeout=10
        )
        
        results['error_tests'].append({
            'test': 'invalid_token',
            'status_code': response.status_code,
            'response': response.text[:200] if response.text else None
        })
        
    except Exception as e:
        results['error_tests'].append({
            'test': 'invalid_token',
            'error': str(e)
        })
    
    # Test con endpoint inexistente
    print("   ❌ Testing non-existent endpoint...")
    try:
        response = requests.get(
            f"{ZENPUT_CONFIG['base_url']}/nonexistent",
            headers=ZENPUT_CONFIG['headers'],
            timeout=10
        )
        
        results['error_tests'].append({
            'test': 'nonexistent_endpoint',
            'status_code': response.status_code,
            'response': response.text[:200] if response.text else None
        })
        
    except Exception as e:
        results['error_tests'].append({
            'test': 'nonexistent_endpoint',
            'error': str(e)
        })
    
    # Test con parámetros inválidos
    print("   ❌ Testing invalid parameters...")
    try:
        response = requests.get(
            f"{ZENPUT_CONFIG['base_url']}/submissions",
            headers=ZENPUT_CONFIG['headers'],
            params={
                'form_id': 'invalid',
                'submitted_at_start': 'invalid-date',
                'page': -1,
                'per_page': 99999
            },
            timeout=10
        )
        
        results['error_tests'].append({
            'test': 'invalid_parameters',
            'status_code': response.status_code,
            'response': response.text[:200] if response.text else None
        })
        
    except Exception as e:
        results['error_tests'].append({
            'test': 'invalid_parameters',
            'error': str(e)
        })
    
    return results

def generate_report(results):
    """Generar reporte final del análisis"""
    
    print("\n" + "="*60)
    print("📊 REPORTE FINAL - ANÁLISIS ZENPUT API")
    print("="*60)
    
    # Rate Limiting
    rate_info = results['tests'].get('rate_limiting', {})
    print(f"\n🚀 RATE LIMITING:")
    print(f"   Requests exitosos: {rate_info.get('requests_made', 0)}")
    print(f"   Rate limit detectado: {'SÍ' if rate_info.get('rate_limit_hit') else 'NO'}")
    print(f"   Tiempo promedio respuesta: {rate_info.get('avg_response_time', 0):.0f}ms")
    
    # Paginación
    pagination_info = results['tests'].get('pagination', {})
    print(f"\n📄 PAGINACIÓN:")
    print(f"   Max per_page funcionando: {pagination_info.get('max_per_page_found', 'N/A')}")
    print(f"   Paginación funcionando: {'SÍ' if pagination_info.get('pagination_working') else 'NO'}")
    
    # Rangos de fecha
    date_info = results['tests'].get('date_ranges', {})
    print(f"\n📅 RANGOS DE FECHA:")
    print(f"   Máximo rango funcionando: {date_info.get('max_range_working', 'N/A')} días")
    
    # Token
    token_info = results['tests'].get('token', {})
    print(f"\n🔑 TOKEN & PERMISOS:")
    print(f"   Token funcionando: {'SÍ' if token_info.get('token_working') else 'NO'}")
    for endpoint, permission in token_info.get('permissions', {}).items():
        print(f"   {endpoint}: {permission}")
    
    return results

def save_to_database(results):
    """Guardar resultados en PostgreSQL Railway"""
    
    try:
        conn = psycopg2.connect(**RAILWAY_CONFIG)
        cursor = conn.cursor()
        
        # Crear tabla si no existe
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS zenput_api_analysis (
                id SERIAL PRIMARY KEY,
                analysis_date TIMESTAMP DEFAULT NOW(),
                analysis_type VARCHAR(50),
                results JSONB,
                summary TEXT
            )
        """)
        
        # Insertar resultados
        cursor.execute("""
            INSERT INTO zenput_api_analysis (analysis_type, results, summary)
            VALUES (%s, %s, %s)
        """, (
            'comprehensive_api_limits',
            json.dumps(results),
            f"Análisis empírico completo - {results['timestamp']}"
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("✅ Resultados guardados en PostgreSQL Railway")
        
    except Exception as e:
        print(f"❌ Error guardando en base: {e}")

if __name__ == '__main__':
    # Ejecutar análisis completo
    results = test_api_limits()
    
    # Generar reporte
    generate_report(results)
    
    # Guardar en base de datos
    save_to_database(results)
    
    print(f"\n🎯 ANÁLISIS COMPLETADO")
    print(f"📄 Resultados detallados en: zenput_api_analysis_results.json")
    print(f"🗄️ Resultados guardados en PostgreSQL Railway")