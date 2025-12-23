#!/usr/bin/env python3
"""
🧪 TEST API ZENPUT REAL
Probar conectividad y obtener muestra de datos
"""

import requests
import json
from datetime import datetime

ZENPUT_CONFIG = {
    'base_url': 'https://www.zenput.com/api/v3',
    'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}
}

def test_api_connectivity():
    """Probar conectividad básica"""
    
    print("🔧 PROBANDO CONECTIVIDAD API ZENPUT")
    print("=" * 50)
    
    endpoints = [
        '/forms',
        '/locations',
        '/submissions',
        '/users'
    ]
    
    for endpoint in endpoints:
        try:
            url = f"{ZENPUT_CONFIG['base_url']}{endpoint}"
            response = requests.get(url, headers=ZENPUT_CONFIG['headers'], timeout=10, params={'limit': 1})
            
            print(f"📡 {endpoint}: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print(f"    ✅ Respuesta válida, {len(data.get('data', []))} items")
            else:
                print(f"    ❌ Error: {response.text[:100]}")
                
        except Exception as e:
            print(f"    💥 Error: {e}")
        
        print()

def test_submissions_2025():
    """Probar obtener submissions del 2025"""
    
    print("📋 PROBANDO SUBMISSIONS 2025")
    print("=" * 50)
    
    # Formularios a probar
    form_ids = ['877138', '877139']
    
    for form_id in form_ids:
        try:
            url = f"{ZENPUT_CONFIG['base_url']}/submissions"
            params = {
                'form_template_id': form_id,
                'created_after': '2025-01-01T00:00:00Z',
                'created_before': '2025-12-31T23:59:59Z',
                'page': 1,
                'page_size': 5
            }
            
            response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=30)
            
            print(f"🎯 Form {form_id}: Status {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                submissions = data.get('data', [])
                
                print(f"    ✅ {len(submissions)} submissions encontradas")
                
                if submissions:
                    # Analizar estructura de la primera
                    first = submissions[0]
                    print(f"    📊 Estructura:")
                    print(f"        ID: {first.get('id', 'N/A')}")
                    print(f"        Created: {first.get('created_at', 'N/A')}")
                    
                    # Buscar coordenadas
                    if 'delivery_location' in first:
                        loc = first['delivery_location']
                        print(f"        Delivery Loc: {loc.get('latitude', 'N/A')}, {loc.get('longitude', 'N/A')}")
                    
                    if 'location' in first:
                        loc = first['location']
                        print(f"        Location: {loc.get('lat', 'N/A')}, {loc.get('lon', 'N/A')}")
                        print(f"        Location Name: {loc.get('name', 'N/A')}")
                        
                    # Mostrar estructura completa de la primera
                    print(f"    🔍 Estructura completa:")
                    print(json.dumps(first, indent=2)[:1000])
                
            else:
                print(f"    ❌ Error: {response.text[:200]}")
                
        except Exception as e:
            print(f"    💥 Error: {e}")
        
        print("-" * 50)

def main():
    """Función principal de test"""
    
    print("🧪 TEST API ZENPUT REAL")
    print("=" * 70)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"API Token: {ZENPUT_CONFIG['headers']['X-API-TOKEN'][:20]}...")
    print("=" * 70)
    
    # 1. Test conectividad
    test_api_connectivity()
    
    # 2. Test submissions 2025
    test_submissions_2025()
    
    print("🎉 TEST COMPLETADO")

if __name__ == "__main__":
    main()