#!/usr/bin/env python3
"""
🔍 DEBUG API ZENPUT
Probar diferentes configuraciones del API para encontrar el problema
"""

import requests
import json
from datetime import datetime

def test_api_configurations():
    """Probar diferentes configuraciones del API"""
    
    print("🔍 DEBUG API ZENPUT")
    print("=" * 50)
    
    token = 'cb908e0d4e0f5501c635325c611db314'
    
    # Diferentes URLs a probar
    urls_to_test = [
        'https://www.zenput.com/api/v3/submissions',
        'https://api.zenput.com/api/v3/submissions',
        'https://zenput.com/api/v3/submissions',
        'https://www.zenput.com/api/submissions',
        'https://api.zenput.com/submissions'
    ]
    
    # Headers a probar
    headers_configs = [
        {'X-API-TOKEN': token, 'Content-Type': 'application/json'},
        {'X-API-TOKEN': token},
        {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'},
        {'Authorization': f'Token {token}', 'Content-Type': 'application/json'}
    ]
    
    # Parámetros
    params = {
        'form_template_id': 877138,  # Operativas
        'start': 0,
        'limit': 10  # Solo 10 para test
    }
    
    print(f"🔑 Token: {token[:20]}...")
    print(f"📋 Form template: {params['form_template_id']}")
    print("\n" + "=" * 70)
    
    for i, url in enumerate(urls_to_test, 1):
        print(f"\n{i}. PROBANDO URL: {url}")
        print("-" * 50)
        
        for j, headers in enumerate(headers_configs, 1):
            print(f"   {i}.{j} Headers: {list(headers.keys())}")
            
            try:
                response = requests.get(url, headers=headers, params=params, timeout=10)
                
                print(f"        Status: {response.status_code}")
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        if isinstance(data, dict):
                            if 'submissions' in data:
                                submissions = data['submissions']
                                print(f"        ✅ SUCCESS: {len(submissions)} submissions")
                                
                                if len(submissions) > 0:
                                    print(f"        📊 Primera submission:")
                                    first = submissions[0]
                                    print(f"           ID: {first.get('id')}")
                                    print(f"           Date: {first.get('submitted_at', 'N/A')}")
                                    print(f"           User: {first.get('submitted_by', {}).get('name', 'N/A')}")
                                    
                                    # Esta configuración funciona!
                                    return url, headers, data
                            else:
                                print(f"        ⚠️ JSON sin 'submissions': {list(data.keys())}")
                        else:
                            print(f"        ⚠️ Response no es dict: {type(data)}")
                    except json.JSONDecodeError as e:
                        print(f"        ❌ JSON Error: {e}")
                        print(f"        Response text: {response.text[:200]}")
                        
                elif response.status_code == 401:
                    print(f"        ❌ UNAUTHORIZED - Token incorrecto")
                elif response.status_code == 403:
                    print(f"        ❌ FORBIDDEN - Sin permisos")
                elif response.status_code == 404:
                    print(f"        ❌ NOT FOUND - URL incorrecta")
                else:
                    print(f"        ❌ Error: {response.status_code}")
                    print(f"        Response: {response.text[:200]}")
                    
            except requests.exceptions.ConnectTimeout:
                print(f"        ❌ TIMEOUT")
            except requests.exceptions.ConnectionError as e:
                print(f"        ❌ CONNECTION ERROR: {e}")
            except Exception as e:
                print(f"        ❌ ERROR: {e}")
    
    print(f"\n❌ Ninguna configuración funcionó")
    return None, None, None

def test_specific_endpoints():
    """Probar endpoints específicos de Zenput"""
    
    print(f"\n🔍 PROBANDO ENDPOINTS ESPECÍFICOS")
    print("=" * 50)
    
    token = 'cb908e0d4e0f5501c635325c611db314'
    headers = {'X-API-TOKEN': token, 'Content-Type': 'application/json'}
    
    # Endpoints a probar
    endpoints = [
        'https://www.zenput.com/api/v3/form_templates',
        'https://www.zenput.com/api/v3/locations',
        'https://www.zenput.com/api/v3/users',
        'https://www.zenput.com/api/form_templates',
        'https://www.zenput.com/api/locations',
        'https://www.zenput.com/api/submissions'
    ]
    
    for endpoint in endpoints:
        print(f"\n📡 GET {endpoint}")
        try:
            response = requests.get(endpoint, headers=headers, timeout=10)
            print(f"   Status: {response.status_code}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    if isinstance(data, dict):
                        print(f"   ✅ Keys: {list(data.keys())}")
                        if 'form_templates' in data:
                            templates = data['form_templates']
                            print(f"   📋 Form templates: {len(templates)}")
                            for template in templates[:3]:
                                print(f"      {template.get('id')}: {template.get('name')}")
                        elif 'locations' in data:
                            locations = data['locations']
                            print(f"   📍 Locations: {len(locations)}")
                        elif 'users' in data:
                            users = data['users']
                            print(f"   👥 Users: {len(users)}")
                    else:
                        print(f"   📊 Type: {type(data)}, Length: {len(data) if hasattr(data, '__len__') else 'N/A'}")
                except:
                    print(f"   📄 Text response: {response.text[:100]}")
            else:
                print(f"   ❌ {response.status_code}: {response.text[:100]}")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")

def main():
    """Función principal"""
    
    print("🔍 DEBUG API ZENPUT")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Encontrar configuración correcta del API")
    print("=" * 80)
    
    # 1. Probar configuraciones de submissions
    url_working, headers_working, sample_data = test_api_configurations()
    
    # 2. Probar endpoints específicos
    test_specific_endpoints()
    
    if url_working:
        print(f"\n✅ CONFIGURACIÓN QUE FUNCIONA:")
        print(f"   URL: {url_working}")
        print(f"   Headers: {headers_working}")
        print(f"   Sample data keys: {list(sample_data.keys())}")
    else:
        print(f"\n❌ NO SE ENCONTRÓ CONFIGURACIÓN FUNCIONAL")
        print(f"   🔍 Revisar token, URLs o parámetros")

if __name__ == "__main__":
    main()