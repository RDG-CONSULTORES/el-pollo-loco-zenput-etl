#!/usr/bin/env python3
"""
🎯 ETL REAL 2025 - DESDE API ZENPUT
Extraer TODAS las supervisiones REALES del 2025
Aplicar reglas correctas: Locales 4+4, Foráneas 2+2
Mapear coordenadas reales vs CSV normalizado
"""

import requests
import csv
import math
import json
import psycopg2
from datetime import datetime, date
from collections import defaultdict
import time

# Configuración
DATABASE_URL = 'postgresql://postgres:qGgdIUuKYKMKGtSNYzARpyapBWHsloOt@turntable.proxy.rlwy.net:24097/railway'

ZENPUT_CONFIG = {
    'base_url': 'https://www.zenput.com/api/v3',
    'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}
}

# Formularios objetivo
FORMULARIOS_2025 = {
    '877138': 'SUPERVISION OPERATIVA CAS 1.1 REV 250125',
    '877139': 'CONTROL OPERATIVO DE SEGURIDAD CAS 1.1 REV. 25012025'
}

# Reglas de supervisiones por tipo de sucursal
REGLAS_MAXIMAS = {
    'LOCAL': {'operativas': 4, 'seguridad': 4},     # Nuevo León + Saltillo
    'FORANEA': {'operativas': 2, 'seguridad': 2}   # Resto
}

# Grupos operativos locales (4+4)
GRUPOS_LOCALES = [
    'OGAS', 'TEC', 'TEPEYAC', 'PLOG NUEVO LEON', 'GRUPO CENTRITO',
    'GRUPO SALTILLO'
]

def cargar_sucursales_master():
    """Cargar sucursales del CSV master con clasificación Local/Foránea"""
    sucursales = {}
    
    with open('data/86_sucursales_master.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['Latitude'] and row['Longitude']:
                sucursal_key = row['Nombre_Sucursal']
                sucursales[sucursal_key] = {
                    'numero': int(row['Numero_Sucursal']),
                    'nombre': row['Nombre_Sucursal'],
                    'grupo': row['Grupo_Operativo'],
                    'lat': float(row['Latitude']),
                    'lon': float(row['Longitude']),
                    'location_code': int(row['Location_Code']) if row['Location_Code'] else None,
                    'tipo': 'LOCAL' if row['Grupo_Operativo'] in GRUPOS_LOCALES else 'FORANEA'
                }
    
    print(f"📊 Cargadas {len(sucursales)} sucursales master")
    return sucursales

def obtener_submissions_2025(form_template_id):
    """Obtener TODAS las submissions del 2025 para un formulario"""
    
    print(f"\n📋 EXTRAYENDO SUBMISSIONS 2025 - Formulario {form_template_id}")
    print(f"    {FORMULARIOS_2025[form_template_id]}")
    print("=" * 70)
    
    all_submissions = []
    page = 1
    
    while True:
        try:
            url = f"{ZENPUT_CONFIG['base_url']}/submissions"
            params = {
                'form_template_id': form_template_id,
                'created_after': '2025-01-01T00:00:00Z',
                'created_before': '2025-12-31T23:59:59Z',
                'page': page,
                'page_size': 100
            }
            
            response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                submissions = data.get('data', [])
                
                if not submissions:
                    print(f"    ✅ Página {page}: Sin más submissions")
                    break
                
                print(f"    📄 Página {page}: {len(submissions)} submissions")
                all_submissions.extend(submissions)
                page += 1
                time.sleep(0.5)  # Para no sobrecargar la API
                
            elif response.status_code == 404:
                print(f"    ❌ Error 404: Formulario {form_template_id} no encontrado")
                break
            else:
                print(f"    ❌ Error {response.status_code}: {response.text[:200]}")
                break
                
        except Exception as e:
            print(f"    💥 Error: {e}")
            break
    
    print(f"📊 TOTAL EXTRAÍDO: {len(all_submissions)} submissions")
    return all_submissions

def extraer_coordenadas_y_fecha_real(submission):
    """Extraer coordenadas reales y fecha de una submission"""
    
    # Coordenadas de entrega (delivery_location o location)
    delivery_lat = None
    delivery_lon = None
    
    # Intentar diferentes fuentes de coordenadas
    if 'delivery_location' in submission:
        delivery_lat = submission['delivery_location'].get('latitude')
        delivery_lon = submission['delivery_location'].get('longitude')
    elif 'location' in submission:
        delivery_lat = submission['location'].get('lat')
        delivery_lon = submission['location'].get('lon')
    
    # Fecha real de creación
    fecha_real = None
    if 'created_at' in submission:
        try:
            fecha_real = datetime.fromisoformat(submission['created_at'].replace('Z', '+00:00')).date()
        except:
            pass
    
    return delivery_lat, delivery_lon, fecha_real

def calcular_distancia(lat1, lon1, lat2, lon2):
    """Calcular distancia euclidiana"""
    try:
        return math.sqrt((float(lat1) - float(lat2))**2 + (float(lon1) - float(lon2))**2)
    except (TypeError, ValueError):
        return float('inf')

def mapear_submission_a_sucursal(submission, sucursales_master, tolerancia=0.01):
    """Mapear una submission a la sucursal más cercana"""
    
    lat, lon, fecha = extraer_coordenadas_y_fecha_real(submission)
    
    if not lat or not lon or not fecha:
        return None
    
    # Buscar sucursal más cercana
    sucursal_mas_cercana = None
    distancia_minima = float('inf')
    
    for sucursal_key, datos_sucursal in sucursales_master.items():
        distancia = calcular_distancia(lat, lon, datos_sucursal['lat'], datos_sucursal['lon'])
        
        if distancia <= tolerancia and distancia < distancia_minima:
            distancia_minima = distancia
            sucursal_mas_cercana = datos_sucursal.copy()
            sucursal_mas_cercana['distancia_mapeo'] = distancia
    
    if sucursal_mas_cercana:
        return {
            'submission_id': submission.get('id'),
            'form_template_id': submission.get('form_template_id', submission.get('activity', {}).get('id')),
            'fecha_real': fecha,
            'lat_submission': lat,
            'lon_submission': lon,
            'sucursal_mapeada': sucursal_mas_cercana['nombre'],
            'grupo_operativo': sucursal_mas_cercana['grupo'],
            'tipo_sucursal': sucursal_mas_cercana['tipo'],
            'distancia_mapeo': sucursal_mas_cercana['distancia_mapeo'],
            'location_code': sucursal_mas_cercana['location_code']
        }
    
    return None

def procesar_todas_submissions_2025():
    """Procesar todas las submissions del 2025"""
    
    print("🎯 PROCESANDO TODAS LAS SUBMISSIONS 2025")
    print("=" * 80)
    
    # Cargar sucursales master
    sucursales_master = cargar_sucursales_master()
    
    todas_submissions = []
    
    # Procesar cada formulario
    for form_id in FORMULARIOS_2025.keys():
        submissions = obtener_submissions_2025(form_id)
        
        print(f"\n🔄 Mapeando {len(submissions)} submissions del formulario {form_id}")
        
        for i, submission in enumerate(submissions, 1):
            mapeo = mapear_submission_a_sucursal(submission, sucursales_master)
            
            if mapeo:
                mapeo['form_type'] = 'OPERATIVA' if form_id == '877138' else 'SEGURIDAD'
                todas_submissions.append(mapeo)
            
            if i % 50 == 0:
                print(f"    📊 Procesadas: {i}/{len(submissions)}")
    
    print(f"\n📊 SUBMISSIONS MAPEADAS EXITOSAMENTE: {len(todas_submissions)}")
    
    return todas_submissions

def analizar_cumplimiento_reglas(submissions_mapeadas):
    """Analizar cumplimiento de reglas máximas por sucursal"""
    
    print("\n📏 ANALIZANDO CUMPLIMIENTO DE REGLAS MÁXIMAS")
    print("=" * 60)
    
    # Agrupar por sucursal
    por_sucursal = defaultdict(lambda: {
        'operativas': [],
        'seguridad': [],
        'tipo': None,
        'grupo': None
    })
    
    for submission in submissions_mapeadas:
        sucursal = submission['sucursal_mapeada']
        tipo_form = submission['form_type']
        
        por_sucursal[sucursal][tipo_form.lower() + 's'].append(submission)
        por_sucursal[sucursal]['tipo'] = submission['tipo_sucursal']
        por_sucursal[sucursal]['grupo'] = submission['grupo_operativo']
    
    # Analizar cada sucursal
    cumple_reglas = 0
    viola_reglas = 0
    
    print(f"📋 ANÁLISIS POR SUCURSAL:")
    print("-" * 80)
    
    for sucursal, datos in sorted(por_sucursal.items()):
        tipo = datos['tipo']
        grupo = datos['grupo']
        ops_count = len(datos['operativas'])
        segs_count = len(datos['seguridad'])
        
        max_ops = REGLAS_MAXIMAS[tipo]['operativas']
        max_segs = REGLAS_MAXIMAS[tipo]['seguridad']
        
        cumple = ops_count <= max_ops and segs_count <= max_segs
        
        if cumple:
            cumple_reglas += 1
            status = "✅"
        else:
            viola_reglas += 1
            status = "❌"
        
        print(f"{status} {sucursal} ({tipo}):")
        print(f"    {grupo}")
        print(f"    Operativas: {ops_count}/{max_ops}")
        print(f"    Seguridad: {segs_count}/{max_segs}")
        
        # Mostrar fechas
        fechas_ops = [s['fecha_real'].strftime('%Y-%m-%d') for s in datos['operativas']]
        fechas_segs = [s['fecha_real'].strftime('%Y-%m-%d') for s in datos['seguridad']]
        
        if fechas_ops:
            print(f"    Fechas Op: {', '.join(fechas_ops)}")
        if fechas_segs:
            print(f"    Fechas Seg: {', '.join(fechas_segs)}")
        
        print()
    
    print("=" * 80)
    print(f"📊 RESUMEN FINAL:")
    print(f"   🏪 Total sucursales con supervisiones: {len(por_sucursal)}")
    print(f"   ✅ Cumplen reglas: {cumple_reglas}")
    print(f"   ❌ Violan reglas: {viola_reglas}")
    
    # Resumen por tipo
    locales = sum(1 for d in por_sucursal.values() if d['tipo'] == 'LOCAL')
    foraneas = sum(1 for d in por_sucursal.values() if d['tipo'] == 'FORANEA')
    
    print(f"   🏢 Sucursales locales: {locales}")
    print(f"   🌍 Sucursales foráneas: {foraneas}")
    
    total_ops = sum(len(d['operativas']) for d in por_sucursal.values())
    total_segs = sum(len(d['seguridad']) for d in por_sucursal.values())
    
    print(f"   📈 Total supervisiones: {total_ops} Op + {total_segs} Seg = {total_ops + total_segs}")
    
    return por_sucursal

def generar_listado_final_real(por_sucursal):
    """Generar listado final con datos REALES de la API"""
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_file = f"listado_final_real_api_2025_{timestamp}.csv"
    
    print(f"\n💾 GENERANDO LISTADO FINAL: {csv_file}")
    
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Numero', 'Sucursal', 'Grupo_Operativo', 'Tipo',
            'Operativas', 'Seguridad', 'Max_Op', 'Max_Seg',
            'Cumple_Reglas', 'Status', 'Fechas_Operativas', 'Fechas_Seguridad'
        ])
        
        for i, (sucursal, datos) in enumerate(sorted(por_sucursal.items()), 1):
            tipo = datos['tipo']
            grupo = datos['grupo']
            ops_count = len(datos['operativas'])
            segs_count = len(datos['seguridad'])
            
            max_ops = REGLAS_MAXIMAS[tipo]['operativas']
            max_segs = REGLAS_MAXIMAS[tipo]['seguridad']
            
            cumple = ops_count <= max_ops and segs_count <= max_segs
            status = "CORRECTO" if cumple else "VIOLA_REGLAS"
            
            fechas_ops = '|'.join([s['fecha_real'].strftime('%Y-%m-%d') for s in datos['operativas']])
            fechas_segs = '|'.join([s['fecha_real'].strftime('%Y-%m-%d') for s in datos['seguridad']])
            
            writer.writerow([
                i, sucursal, grupo, tipo,
                ops_count, segs_count, max_ops, max_segs,
                cumple, status, fechas_ops, fechas_segs
            ])
    
    print(f"✅ Listado guardado en: {csv_file}")
    return csv_file

def main():
    """Función principal del ETL real 2025"""
    
    print("🎯 ETL REAL 2025 - DESDE API ZENPUT")
    print("=" * 80)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"API Token: {ZENPUT_CONFIG['headers']['X-API-TOKEN'][:20]}...")
    print("=" * 80)
    
    # 1. Procesar todas las submissions del 2025
    submissions_mapeadas = procesar_todas_submissions_2025()
    
    if not submissions_mapeadas:
        print("❌ No se pudieron obtener submissions del 2025")
        return
    
    # 2. Analizar cumplimiento de reglas
    por_sucursal = analizar_cumplimiento_reglas(submissions_mapeadas)
    
    # 3. Generar listado final
    csv_file = generar_listado_final_real(por_sucursal)
    
    # 4. Guardar datos raw para análisis
    json_file = f"submissions_raw_2025_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(json_file, 'w') as f:
        json.dump(submissions_mapeadas, f, indent=2, default=str)
    
    print(f"\n🎉 ETL COMPLETADO")
    print(f"📁 Listado final: {csv_file}")
    print(f"📁 Datos raw: {json_file}")

if __name__ == "__main__":
    main()