#!/usr/bin/env python3
"""
🔍 REVISAR DATOS API REALES
Ver qué datos reales está devolviendo el API sin filtro de año
"""

import requests
import pandas as pd
import json
from datetime import datetime

def revisar_submissions_sin_filtro(form_template_id, tipo, limit=10):
    """Revisar submissions sin filtro de año"""
    
    print(f"🔍 REVISAR {tipo.upper()} SIN FILTRO DE AÑO")
    print("=" * 50)
    
    url = "https://www.zenput.com/api/v3/submissions"
    headers = {
        'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314',
        'Content-Type': 'application/json'
    }
    
    params = {
        'form_template_id': form_template_id,
        'start': 0,
        'limit': limit
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            submissions = data.get('data', [])
            
            print(f"✅ Respuesta exitosa: {len(submissions)} submissions encontradas")
            
            if len(submissions) > 0:
                print(f"\n📋 MUESTRA DE DATOS ({tipo.upper()}):")
                print(f"{'#':<3} {'ID':<15} {'Fecha':<20} {'Usuario':<15} {'Location'}")
                print("-" * 80)
                
                fechas_encontradas = []
                
                for i, submission in enumerate(submissions, 1):
                    sub_id = submission.get('id', 'N/A')
                    submitted_at = submission.get('submitted_at', 'N/A')
                    
                    # Usuario
                    submitted_by_info = submission.get('submitted_by', {})
                    usuario = submitted_by_info.get('name', 'N/A') if submitted_by_info else 'N/A'
                    
                    # Location
                    location_info = submission.get('location', {})
                    location = location_info.get('name', 'SIN_LOCATION') if location_info else 'SIN_LOCATION'
                    
                    print(f"{i:<3} {str(sub_id)[:14]:<15} {str(submitted_at)[:19]:<20} {str(usuario)[:14]:<15} {str(location)[:25]}")
                    
                    # Recopilar fechas
                    if submitted_at and submitted_at != 'N/A':
                        try:
                            fecha_dt = pd.to_datetime(submitted_at)
                            fechas_encontradas.append(fecha_dt)
                        except:
                            pass
                
                # Análisis de fechas
                if fechas_encontradas:
                    fechas_df = pd.DataFrame({'fecha': fechas_encontradas})
                    fechas_df['año'] = fechas_df['fecha'].dt.year
                    años_count = fechas_df['año'].value_counts().sort_index()
                    
                    print(f"\n📅 ANÁLISIS DE FECHAS:")
                    for año, count in años_count.items():
                        print(f"   {año}: {count} submissions")
                    
                    # Fechas más recientes y más antiguas
                    fecha_min = fechas_df['fecha'].min()
                    fecha_max = fechas_df['fecha'].max()
                    print(f"\n📊 RANGO DE FECHAS:")
                    print(f"   Más antigua: {fecha_min}")
                    print(f"   Más reciente: {fecha_max}")
                    
                    return años_count
                else:
                    print(f"\n❌ No se pudieron procesar fechas")
                    return None
            else:
                print(f"❌ No hay submissions en esta respuesta")
                return None
                
        else:
            print(f"❌ Error API: {response.status_code}")
            print(f"Response: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def revisar_estructura_completa(form_template_id, tipo):
    """Revisar estructura completa de una submission"""
    
    print(f"\n🔍 ESTRUCTURA COMPLETA {tipo.upper()}")
    print("=" * 50)
    
    url = "https://www.zenput.com/api/v3/submissions"
    headers = {
        'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314',
        'Content-Type': 'application/json'
    }
    
    params = {
        'form_template_id': form_template_id,
        'start': 0,
        'limit': 1
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            submissions = data.get('data', [])
            
            if len(submissions) > 0:
                submission = submissions[0]
                
                print(f"📋 ESTRUCTURA DE SUBMISSION:")
                print(f"   Keys principales: {list(submission.keys())}")
                
                # Location
                if 'location' in submission:
                    location = submission['location']
                    if location:
                        print(f"   Location keys: {list(location.keys()) if isinstance(location, dict) else type(location)}")
                
                # Submitted by
                if 'submitted_by' in submission:
                    submitted_by = submission['submitted_by']
                    if submitted_by:
                        print(f"   Submitted_by keys: {list(submitted_by.keys()) if isinstance(submitted_by, dict) else type(submitted_by)}")
                
                # Responses
                if 'responses' in submission:
                    responses = submission['responses']
                    print(f"   Responses count: {len(responses) if responses else 0}")
                    
                    if responses and len(responses) > 0:
                        print(f"   Primera response keys: {list(responses[0].keys()) if isinstance(responses[0], dict) else type(responses[0])}")
                
                # Smetadata
                if 'smetadata' in submission:
                    smetadata = submission['smetadata']
                    if smetadata:
                        print(f"   Smetadata keys: {list(smetadata.keys()) if isinstance(smetadata, dict) else type(smetadata)}")
                        if 'lat' in smetadata:
                            print(f"      lat: {smetadata.get('lat')}")
                        if 'lon' in smetadata:
                            print(f"      lon: {smetadata.get('lon')}")
                
                return submission
            else:
                print(f"❌ No hay submissions para analizar estructura")
                return None
        else:
            print(f"❌ Error: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def main():
    """Función principal"""
    
    print("🔍 REVISAR DATOS API REALES")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Ver qué datos reales devuelve el API")
    print("=" * 80)
    
    form_operativas = 877138
    form_seguridad = 877139
    
    # 1. Revisar operativas
    años_ops = revisar_submissions_sin_filtro(form_operativas, "operativas", limit=20)
    
    # 2. Revisar seguridad
    años_seg = revisar_submissions_sin_filtro(form_seguridad, "seguridad", limit=20)
    
    # 3. Revisar estructura completa
    estructura_ops = revisar_estructura_completa(form_operativas, "operativas")
    
    # 4. Resumen
    print(f"\n🎯 RESUMEN:")
    if años_ops is not None:
        print(f"   🏗️ Operativas por año: {dict(años_ops)}")
    if años_seg is not None:
        print(f"   🛡️ Seguridad por año: {dict(años_seg)}")
    
    print(f"\n💡 Si no hay datos 2025, revisar:")
    print(f"   1. Filtro de fechas en el API")
    print(f"   2. Formato de fecha esperado")
    print(f"   3. Usar todos los años disponibles")

if __name__ == "__main__":
    main()