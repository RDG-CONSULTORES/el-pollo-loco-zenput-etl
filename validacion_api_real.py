#!/usr/bin/env python3
"""
🔍 VALIDACIÓN REAL CONTRA API ZENPUT
1. Ir DIRECTAMENTE a cada submission en la API
2. Extraer coordenadas REALES de entrega
3. Extraer fechas REALES de submission
4. Mapear contra coordenadas normalizadas del CSV
5. Aplicar reglas correctas: Locales 4+4, Foráneas 2+2
"""

import psycopg2
import requests
import csv
import math
import json
from datetime import datetime
import time

# Configuración
DATABASE_URL = 'postgresql://postgres:qGgdIUuKYKMKGtSNYzARpyapBWHsloOt@turntable.proxy.rlwy.net:24097/railway'

ZENPUT_CONFIG = {
    'base_url': 'https://www.zenput.com/api/v3',
    'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}
}

# Reglas de supervisiones por tipo de sucursal
REGLAS_MAXIMAS = {
    'LOCAL': {'operativas': 4, 'seguridad': 4},
    'FORANEA': {'operativas': 2, 'seguridad': 2}
}

# Sucursales locales (Nuevo León + Saltillo)
SUCURSALES_LOCALES = [
    # Nuevo León (OGAS, TEC, TEPEYAC, PLOG NUEVO LEON, etc.)
    'OGAS', 'TEC', 'TEPEYAC', 'PLOG NUEVO LEON', 'GRUPO CENTRITO',
    # Saltillo
    'GRUPO SALTILLO'
]

def cargar_coordenadas_normalizadas():
    """Cargar coordenadas normalizadas del CSV master"""
    sucursales_coords = {}
    
    with open('data/86_sucursales_master.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['Latitude'] and row['Longitude']:
                sucursal_key = f"{row['Nombre_Sucursal']}_{row['Grupo_Operativo']}"
                sucursales_coords[sucursal_key] = {
                    'numero': int(row['Numero_Sucursal']),
                    'nombre': row['Nombre_Sucursal'],
                    'grupo': row['Grupo_Operativo'],
                    'lat': float(row['Latitude']),
                    'lon': float(row['Longitude']),
                    'location_code': int(row['Location_Code']) if row['Location_Code'] else None,
                    'tipo': 'LOCAL' if row['Grupo_Operativo'] in SUCURSALES_LOCALES else 'FORANEA'
                }
    
    return sucursales_coords

def calcular_distancia(lat1, lon1, lat2, lon2):
    """Calcular distancia euclidiana"""
    try:
        return math.sqrt((float(lat1) - float(lat2))**2 + (float(lon1) - float(lon2))**2)
    except (TypeError, ValueError):
        return float('inf')

def obtener_submission_desde_api(submission_id, form_template_id):
    """Obtener submission específica desde la API de Zenput"""
    
    try:
        # Construir URL para submission específica
        url = f"{ZENPUT_CONFIG['base_url']}/submissions/{submission_id}"
        
        response = requests.get(url, headers=ZENPUT_CONFIG['headers'], timeout=30)
        
        if response.status_code == 200:
            submission_data = response.json()
            
            # Extraer información relevante
            delivery_location = submission_data.get('delivery_location', {})
            created_at = submission_data.get('created_at')
            
            if delivery_location:
                lat = delivery_location.get('latitude')
                lon = delivery_location.get('longitude')
                
                return {
                    'submission_id': submission_id,
                    'form_template_id': form_template_id,
                    'created_at': created_at,
                    'delivery_latitude': lat,
                    'delivery_longitude': lon,
                    'api_success': True,
                    'raw_data': submission_data
                }
            else:
                print(f"⚠️ Submission {submission_id}: Sin delivery_location")
                return {
                    'submission_id': submission_id,
                    'api_success': False,
                    'error': 'Sin delivery_location'
                }
        else:
            print(f"❌ API Error {response.status_code} para {submission_id}")
            return {
                'submission_id': submission_id,
                'api_success': False,
                'error': f'HTTP {response.status_code}'
            }
            
    except Exception as e:
        print(f"❌ Error obteniendo {submission_id}: {e}")
        return {
            'submission_id': submission_id,
            'api_success': False,
            'error': str(e)
        }

def validar_todas_submissions():
    """Validar TODAS las submissions contra la API"""
    
    print("🔍 VALIDANDO TODAS LAS SUBMISSIONS CONTRA API ZENPUT")
    print("=" * 70)
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Obtener todas las submissions de la BD
        cursor.execute("""
            SELECT submission_id, form_template_id, form_type
            FROM supervisiones_2026
            ORDER BY submission_id
        """)
        
        submissions_bd = cursor.fetchall()
        
        print(f"📊 Submissions en BD: {len(submissions_bd)}")
        
        # Validar cada una contra la API
        submissions_validadas = []
        errores = 0
        exitosas = 0
        
        for i, (sub_id, template_id, form_type) in enumerate(submissions_bd, 1):
            print(f"\r🔍 Validando {i}/{len(submissions_bd)}: {sub_id}", end="", flush=True)
            
            # Obtener datos reales de la API
            api_data = obtener_submission_desde_api(sub_id, template_id)
            
            if api_data['api_success']:
                exitosas += 1
                submissions_validadas.append({
                    'submission_id': sub_id,
                    'form_template_id': template_id,
                    'form_type': form_type,
                    'api_lat': api_data['delivery_latitude'],
                    'api_lon': api_data['delivery_longitude'],
                    'api_created_at': api_data['created_at'],
                    'api_success': True
                })
            else:
                errores += 1
                submissions_validadas.append({
                    'submission_id': sub_id,
                    'form_template_id': template_id,
                    'form_type': form_type,
                    'api_success': False,
                    'error': api_data.get('error', 'Unknown')
                })
            
            # Pequeña pausa para no sobrecargar la API
            time.sleep(0.1)
        
        print(f"\n\n📊 VALIDACIÓN COMPLETADA:")
        print(f"   ✅ Exitosas: {exitosas}")
        print(f"   ❌ Errores: {errores}")
        
        cursor.close()
        conn.close()
        
        return submissions_validadas
        
    except Exception as e:
        print(f"❌ Error validando submissions: {e}")
        return []

def mapear_coordenadas_api_vs_normalizadas(submissions_validadas, sucursales_coords, tolerancia=0.01):
    """Mapear coordenadas de API contra coordenadas normalizadas"""
    
    print(f"\n🎯 MAPEANDO COORDENADAS API vs NORMALIZADAS")
    print("=" * 60)
    
    mapeos_exitosos = []
    sin_mapeo = []
    
    for submission in submissions_validadas:
        if not submission['api_success']:
            continue
            
        api_lat = submission['api_lat']
        api_lon = submission['api_lon']
        
        if not api_lat or not api_lon:
            sin_mapeo.append(submission)
            continue
        
        # Buscar la sucursal normalizada más cercana
        sucursal_mas_cercana = None
        distancia_minima = float('inf')
        
        for sucursal_key, datos_sucursal in sucursales_coords.items():
            distancia = calcular_distancia(
                api_lat, api_lon,
                datos_sucursal['lat'], datos_sucursal['lon']
            )
            
            if distancia < distancia_minima and distancia <= tolerancia:
                distancia_minima = distancia
                sucursal_mas_cercana = {
                    'key': sucursal_key,
                    'datos': datos_sucursal,
                    'distancia': distancia
                }
        
        if sucursal_mas_cercana:
            mapeos_exitosos.append({
                **submission,
                'sucursal_mapeada': sucursal_mas_cercana['datos']['nombre'],
                'grupo_mapeado': sucursal_mas_cercana['datos']['grupo'],
                'tipo_sucursal': sucursal_mas_cercana['datos']['tipo'],
                'distancia_mapeo': sucursal_mas_cercana['distancia'],
                'lat_normalizada': sucursal_mas_cercana['datos']['lat'],
                'lon_normalizada': sucursal_mas_cercana['datos']['lon']
            })
        else:
            sin_mapeo.append(submission)
    
    print(f"📊 Mapeos exitosos: {len(mapeos_exitosos)}")
    print(f"⚠️ Sin mapeo: {len(sin_mapeo)}")
    
    return mapeos_exitosos, sin_mapeo

def aplicar_reglas_maximas(mapeos_exitosos):
    """Aplicar reglas de máximas supervisiones por tipo de sucursal"""
    
    print(f"\n📏 APLICANDO REGLAS MÁXIMAS")
    print("=" * 50)
    
    # Agrupar por sucursal
    por_sucursal = {}
    
    for mapeo in mapeos_exitosos:
        sucursal_key = f"{mapeo['sucursal_mapeada']}_{mapeo['grupo_mapeado']}"
        
        if sucursal_key not in por_sucursal:
            por_sucursal[sucursal_key] = {
                'sucursal': mapeo['sucursal_mapeada'],
                'grupo': mapeo['grupo_mapeado'],
                'tipo': mapeo['tipo_sucursal'],
                'operativas': [],
                'seguridad': []
            }
        
        if mapeo['form_type'] == 'OPERATIVA':
            por_sucursal[sucursal_key]['operativas'].append(mapeo)
        elif mapeo['form_type'] == 'SEGURIDAD':
            por_sucursal[sucursal_key]['seguridad'].append(mapeo)
    
    print(f"📊 Sucursales encontradas: {len(por_sucursal)}")
    
    # Analizar cumplimiento de reglas
    locales = 0
    foraneas = 0
    reglas_cumplidas = 0
    reglas_violadas = 0
    
    print(f"\n📋 ANÁLISIS POR SUCURSAL:")
    
    for sucursal_key, datos in sorted(por_sucursal.items()):
        tipo = datos['tipo']
        ops = len(datos['operativas'])
        segs = len(datos['seguridad'])
        
        max_ops = REGLAS_MAXIMAS[tipo]['operativas']
        max_segs = REGLAS_MAXIMAS[tipo]['seguridad']
        
        cumple_reglas = ops <= max_ops and segs <= max_segs
        
        if tipo == 'LOCAL':
            locales += 1
        else:
            foraneas += 1
        
        if cumple_reglas:
            reglas_cumplidas += 1
            status = "✅"
        else:
            reglas_violadas += 1
            status = "❌"
        
        print(f"   {status} {datos['sucursal']} ({tipo}): {ops}/{max_ops} Op, {segs}/{max_segs} Seg")
    
    print(f"\n📊 RESUMEN:")
    print(f"   🏪 Sucursales locales: {locales}")
    print(f"   🌍 Sucursales foráneas: {foraneas}")
    print(f"   ✅ Cumplen reglas: {reglas_cumplidas}")
    print(f"   ❌ Violan reglas: {reglas_violadas}")
    
    return por_sucursal

def generar_listado_real_validado(por_sucursal):
    """Generar listado REAL validado contra API"""
    
    print(f"\n📋 LISTADO REAL VALIDADO CONTRA API")
    print("=" * 70)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_file = f"listado_real_validado_api_{timestamp}.csv"
    
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Numero', 'Sucursal', 'Grupo_Operativo', 'Tipo', 
            'Operativas', 'Seguridad', 'Max_Op', 'Max_Seg',
            'Cumple_Reglas', 'Status'
        ])
        
        for i, (sucursal_key, datos) in enumerate(sorted(por_sucursal.items()), 1):
            tipo = datos['tipo']
            ops = len(datos['operativas'])
            segs = len(datos['seguridad'])
            
            max_ops = REGLAS_MAXIMAS[tipo]['operativas']
            max_segs = REGLAS_MAXIMAS[tipo]['seguridad']
            
            cumple_reglas = ops <= max_ops and segs <= max_segs
            status = "CORRECTO" if cumple_reglas else "VIOLA_REGLAS"
            
            writer.writerow([
                i, datos['sucursal'], datos['grupo'], tipo,
                ops, segs, max_ops, max_segs,
                cumple_reglas, status
            ])
            
            print(f"{i:2d}. {datos['sucursal']} ({tipo})")
            print(f"    {ops}/{max_ops} Op, {segs}/{max_segs} Seg - {status}")
    
    print(f"\n💾 Listado guardado en: {csv_file}")
    
    return csv_file

def main():
    """Función principal de validación real"""
    
    print("🔍 VALIDACIÓN REAL CONTRA API ZENPUT")
    print("=" * 80)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # 1. Cargar coordenadas normalizadas
    print("📍 Cargando coordenadas normalizadas...")
    sucursales_coords = cargar_coordenadas_normalizadas()
    print(f"✅ Cargadas {len(sucursales_coords)} sucursales")
    
    # 2. Validar todas las submissions contra la API
    submissions_validadas = validar_todas_submissions()
    
    if not submissions_validadas:
        print("❌ No se pudieron validar submissions")
        return
    
    # 3. Mapear coordenadas API vs normalizadas
    mapeos_exitosos, sin_mapeo = mapear_coordenadas_api_vs_normalizadas(
        submissions_validadas, sucursales_coords
    )
    
    # 4. Aplicar reglas máximas
    por_sucursal = aplicar_reglas_maximas(mapeos_exitosos)
    
    # 5. Generar listado real validado
    csv_file = generar_listado_real_validado(por_sucursal)
    
    print(f"\n🎉 VALIDACIÓN REAL COMPLETADA")
    print(f"📁 Resultado en: {csv_file}")

if __name__ == "__main__":
    main()