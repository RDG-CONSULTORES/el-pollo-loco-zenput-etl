#!/usr/bin/env python3
"""
🔍 MOSTRAR ESTRUCTURA COMPLETA DE SUBMISSIONS
Extraer TODOS los campos de submissions reales
"""

import requests
import json
from datetime import datetime

ZENPUT_CONFIG = {
    'base_url': 'https://www.zenput.com/api/v3',
    'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}
}

FORMULARIOS = {
    '877138': 'SUPERVISION OPERATIVA',
    '877139': 'SUPERVISION SEGURIDAD'
}

def obtener_estructura_completa_formulario(form_id, form_name):
    """Obtener estructura completa de un formulario"""
    
    print(f"\n📋 FORMULARIO {form_id}: {form_name}")
    print("=" * 80)
    
    try:
        url = f"{ZENPUT_CONFIG['base_url']}/submissions"
        params = {
            'form_template_id': form_id,
            'created_after': '2025-01-01T00:00:00Z',
            'created_before': '2025-12-31T23:59:59Z',
            'page': 1,
            'page_size': 1  # Solo 1 submission para ver estructura
        }
        
        response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            submissions = data.get('data', [])
            
            if submissions:
                submission = submissions[0]
                
                print(f"🎯 SUBMISSION ID: {submission.get('id', 'N/A')}")
                print(f"📊 TOTAL CAMPOS: {len(submission.keys())}")
                print("\n🔍 ESTRUCTURA COMPLETA:")
                print("=" * 80)
                
                # Mostrar estructura completa con indentación
                print(json.dumps(submission, indent=2, ensure_ascii=False))
                
                print("\n" + "=" * 80)
                print(f"📝 CAMPOS PRINCIPALES IDENTIFICADOS:")
                print("=" * 80)
                
                # Listar campos principales
                for key, value in submission.items():
                    tipo_valor = type(value).__name__
                    
                    if isinstance(value, dict):
                        sub_keys = list(value.keys()) if value else []
                        print(f"  📁 {key} ({tipo_valor}): {sub_keys[:5]}{'...' if len(sub_keys) > 5 else ''}")
                    elif isinstance(value, list):
                        print(f"  📝 {key} ({tipo_valor}): {len(value)} items")
                        if value and isinstance(value[0], dict):
                            print(f"       └─ Ejemplo: {list(value[0].keys())[:3]}...")
                    else:
                        valor_str = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
                        print(f"  📄 {key} ({tipo_valor}): {valor_str}")
                
                # Guardar estructura completa
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"estructura_completa_{form_id}_{timestamp}.json"
                
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(submission, f, indent=2, ensure_ascii=False)
                
                print(f"\n💾 Estructura guardada en: {filename}")
                
                # Buscar campos específicos importantes
                print(f"\n🎯 CAMPOS CRÍTICOS ENCONTRADOS:")
                print("=" * 50)
                
                campos_importantes = [
                    'location', 'delivery_location', 'coordinates', 'lat', 'lon', 'latitude', 'longitude',
                    'user', 'auditor', 'inspector', 'created_at', 'updated_at', 'submitted_at', 'start_time', 'end_time',
                    'teams', 'user_teams', 'location_teams', 'answers', 'form_data', 'fields'
                ]
                
                for campo in campos_importantes:
                    if campo in submission:
                        valor = submission[campo]
                        if valor:
                            print(f"  ✅ {campo}: {type(valor).__name__}")
                            if isinstance(valor, dict) and valor:
                                print(f"       └─ Subcampos: {list(valor.keys())}")
                        else:
                            print(f"  ⚠️ {campo}: (vacío)")
                    else:
                        print(f"  ❌ {campo}: (no existe)")
                
                return submission
            else:
                print("❌ No se encontraron submissions")
                return None
        else:
            print(f"❌ Error {response.status_code}: {response.text[:200]}")
            return None
            
    except Exception as e:
        print(f"💥 Error: {e}")
        return None

def main():
    """Función principal"""
    
    print("🔍 ANÁLISIS COMPLETO DE ESTRUCTURA DE SUBMISSIONS")
    print("=" * 100)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"API Token: {ZENPUT_CONFIG['headers']['X-API-TOKEN'][:20]}...")
    print("=" * 100)
    
    # Analizar cada formulario
    for form_id, form_name in FORMULARIOS.items():
        submission = obtener_estructura_completa_formulario(form_id, form_name)
        
        if submission:
            print(f"\n✅ Formulario {form_id} analizado exitosamente")
        else:
            print(f"\n❌ Error analizando formulario {form_id}")
    
    print(f"\n🎉 ANÁLISIS COMPLETADO")
    print("Revisa los archivos .json generados para ver la estructura completa")

if __name__ == "__main__":
    main()