#!/usr/bin/env python3
"""
🔍 OBTENER MUESTRA COMPLETA DE SUBMISSIONS
Ver estructura completa para extraer coordenadas y fechas
"""

import requests
import json
from datetime import datetime

ZENPUT_CONFIG = {
    'base_url': 'https://www.zenput.com/api/v3',
    'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}
}

def obtener_submission_completa(submission_id):
    """Obtener submission individual completa"""
    
    try:
        url = f"{ZENPUT_CONFIG['base_url']}/submissions/{submission_id}"
        response = requests.get(url, headers=ZENPUT_CONFIG['headers'], timeout=30)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"❌ Error {response.status_code} para {submission_id}: {response.text[:100]}")
            return None
            
    except Exception as e:
        print(f"💥 Error: {e}")
        return None

def analizar_estructura_submissions():
    """Analizar estructura de varias submissions"""
    
    print("🔍 ANALIZANDO ESTRUCTURA DE SUBMISSIONS")
    print("=" * 60)
    
    # Obtener lista de submissions
    form_ids = ['877138', '877139']
    
    for form_id in form_ids:
        print(f"\n📋 FORMULARIO {form_id}")
        print("-" * 40)
        
        try:
            url = f"{ZENPUT_CONFIG['base_url']}/submissions"
            params = {
                'form_template_id': form_id,
                'created_after': '2025-01-01T00:00:00Z',
                'created_before': '2025-12-31T23:59:59Z',
                'page': 1,
                'page_size': 3  # Solo 3 para análisis
            }
            
            response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                submissions = data.get('data', [])
                
                print(f"✅ {len(submissions)} submissions obtenidas")
                
                for i, submission in enumerate(submissions, 1):
                    print(f"\n🎯 SUBMISSION {i}: {submission.get('id')}")
                    
                    # Buscar diferentes campos de coordenadas y fechas
                    print("📍 COORDENADAS:")
                    
                    # Delivery location
                    if 'delivery_location' in submission:
                        dl = submission['delivery_location']
                        print(f"    delivery_location: lat={dl.get('latitude')}, lon={dl.get('longitude')}")
                    
                    # Location
                    if 'location' in submission:
                        loc = submission['location']
                        print(f"    location: lat={loc.get('lat')}, lon={loc.get('lon')}")
                        print(f"    location name: {loc.get('name')}")
                        print(f"    location id: {loc.get('id')}")
                    
                    # Usuario que hizo la submission
                    if 'user' in submission:
                        user = submission['user']
                        print(f"    👤 Usuario: {user.get('name')} ({user.get('email')})")
                    
                    # Fechas
                    print("📅 FECHAS:")
                    for field in ['created_at', 'updated_at', 'submitted_at', 'start_time', 'end_time']:
                        if field in submission:
                            print(f"    {field}: {submission[field]}")
                    
                    # Teams
                    print("👥 TEAMS:")
                    for field in ['user_teams', 'location_teams']:
                        if field in submission:
                            teams = submission[field]
                            print(f"    {field}: {[t.get('id') for t in teams]}")
                    
                    # Obtener submission completa
                    submission_completa = obtener_submission_completa(submission['id'])
                    
                    if submission_completa:
                        print("📋 SUBMISSION COMPLETA:")
                        
                        # Buscar más campos
                        if 'answers' in submission_completa:
                            answers = submission_completa['answers']
                            print(f"    🔢 Respuestas: {len(answers)} campos")
                            
                            # Buscar campos de fecha/tiempo
                            for answer in answers[:5]:  # Solo primeros 5
                                if 'datetime' in str(answer.get('field_type', '')).lower():
                                    print(f"        📅 {answer.get('title')}: {answer.get('value')}")
                        
                        # Guardar una submission completa como ejemplo
                        if i == 1:
                            with open(f'submission_completa_{form_id}.json', 'w') as f:
                                json.dump(submission_completa, f, indent=2)
                            print(f"    💾 Guardada en: submission_completa_{form_id}.json")
                    
                    print("-" * 30)
            
            else:
                print(f"❌ Error: {response.text[:200]}")
                
        except Exception as e:
            print(f"💥 Error: {e}")

def main():
    """Función principal"""
    
    print("🔍 OBTENER MUESTRA COMPLETA DE SUBMISSIONS")
    print("=" * 70)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    analizar_estructura_submissions()
    
    print("\n🎉 ANÁLISIS COMPLETADO")

if __name__ == "__main__":
    main()