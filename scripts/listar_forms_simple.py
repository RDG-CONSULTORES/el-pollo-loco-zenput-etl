#!/usr/bin/env python3
"""
🔍 LISTADO SIMPLE FORMS
Lista todos los formularios disponibles para identificar el de seguridad
"""

import requests
import json

def listar_forms_simple():
    """Lista formularios de forma simple"""
    
    print("📋 LISTADO FORMULARIOS ZENPUT API")
    print("=" * 40)
    
    # Configuración API
    api_token = "cb908e0d4e0f5501c635325c611db314"
    headers = {
        'X-API-TOKEN': api_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    base_url = 'https://www.zenput.com/api/v3'
    
    try:
        forms_url = f"{base_url}/forms"
        print(f"🌐 Consultando: {forms_url}")
        
        response = requests.get(forms_url, headers=headers, timeout=30)
        
        print(f"📡 Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"📊 Respuesta JSON structure: {list(data.keys())}")
            
            # Ver estructura de la respuesta
            print(f"\n🔍 ESTRUCTURA RESPUESTA:")
            if 'data' in data:
                forms = data['data']
                print(f"   • Tipo 'data': {type(forms)}")
                print(f"   • Cantidad: {len(forms) if isinstance(forms, list) else 'No es lista'}")
                
                if isinstance(forms, list) and len(forms) > 0:
                    print(f"\n📋 FORMULARIOS ENCONTRADOS:")
                    print("-" * 30)
                    
                    for i, form in enumerate(forms, 1):
                        print(f"\n📝 FORM {i}:")
                        if isinstance(form, dict):
                            form_id = form.get('id', 'N/A')
                            form_name = form.get('name', 'N/A')
                            form_status = form.get('status', 'N/A')
                            
                            print(f"   • ID: {form_id}")
                            print(f"   • Nombre: {form_name}")
                            print(f"   • Status: {form_status}")
                            
                            # Identificar posibles forms de seguridad
                            form_name_lower = form_name.lower() if isinstance(form_name, str) else ''
                            if any(keyword in form_name_lower for keyword in ['seguridad', 'security', 'control']):
                                print(f"   🛡️ ← POSIBLE FORMULARIO DE SEGURIDAD")
                        else:
                            print(f"   ⚠️ Form no es dict: {type(form)} - {form}")
                else:
                    print("   ⚠️ No hay formularios o 'data' no es lista")
            else:
                print("   ❌ No se encontró 'data' en respuesta")
                print(f"   📋 Claves disponibles: {list(data.keys())}")
        else:
            print(f"❌ Error HTTP: {response.status_code}")
            try:
                error_data = response.json()
                print(f"📋 Error details: {error_data}")
            except:
                print(f"📋 Error text: {response.text}")
                
    except Exception as e:
        print(f"❌ Error de conexión: {e}")

if __name__ == "__main__":
    listar_forms_simple()