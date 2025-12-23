#!/usr/bin/env python3
"""
🎯 ETL DIRECTO DE SUBMISSIONS 2025
Extraer datos directamente del listado /submissions
NO hacer llamadas individuales
"""

import requests
import csv
import math
import json
from datetime import datetime, date
from collections import defaultdict
import time

# Configuración
ZENPUT_CONFIG = {
    'base_url': 'https://www.zenput.com/api/v3',
    'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}
}

# Formularios objetivo
FORMULARIOS_2025 = {
    '877138': 'OPERATIVA',
    '877139': 'SEGURIDAD'
}

# Reglas máximas
REGLAS_MAXIMAS = {
    'LOCAL': {'operativas': 4, 'seguridad': 4},
    'FORANEA': {'operativas': 2, 'seguridad': 2}
}

# Grupos locales
GRUPOS_LOCALES = [
    'OGAS', 'TEC', 'TEPEYAC', 'PLOG NUEVO LEON', 'GRUPO CENTRITO', 'GRUPO SALTILLO'
]

def cargar_sucursales_master():
    """Cargar sucursales master"""
    sucursales = {}
    
    with open('data/86_sucursales_master.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['Latitude'] and row['Longitude']:
                sucursales[row['Nombre_Sucursal']] = {
                    'numero': int(row['Numero_Sucursal']),
                    'nombre': row['Nombre_Sucursal'],
                    'grupo': row['Grupo_Operativo'],
                    'lat': float(row['Latitude']),
                    'lon': float(row['Longitude']),
                    'location_code': int(row['Location_Code']) if row['Location_Code'] else None,
                    'tipo': 'LOCAL' if row['Grupo_Operativo'] in GRUPOS_LOCALES else 'FORANEA'
                }
    
    return sucursales

def extraer_todas_submissions_2025():
    """Extraer TODAS las submissions del 2025 directamente"""
    
    print("📋 EXTRAYENDO TODAS LAS SUBMISSIONS 2025")
    print("=" * 60)
    
    todas_submissions = []
    
    for form_id, tipo_form in FORMULARIOS_2025.items():
        print(f"\n🎯 Formulario {form_id} ({tipo_form})")
        print("-" * 40)
        
        page = 1
        form_submissions = []
        
        while True:
            try:
                url = f"{ZENPUT_CONFIG['base_url']}/submissions"
                params = {
                    'form_template_id': form_id,
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
                        print(f"    ✅ Fin en página {page}")
                        break
                    
                    print(f"    📄 Página {page}: {len(submissions)} submissions")
                    
                    # Procesar cada submission directamente
                    for submission in submissions:
                        # Agregar tipo de formulario
                        submission['form_type'] = tipo_form
                        form_submissions.append(submission)
                    
                    page += 1
                    time.sleep(0.2)  # Pausa pequeña
                    
                else:
                    print(f"    ❌ Error {response.status_code}")
                    break
                    
            except Exception as e:
                print(f"    💥 Error: {e}")
                break
        
        print(f"📊 Total {tipo_form}: {len(form_submissions)} submissions")
        todas_submissions.extend(form_submissions)
    
    print(f"\n📊 TOTAL GENERAL: {len(todas_submissions)} submissions")
    return todas_submissions

def extraer_datos_submission(submission):
    """Extraer datos relevantes de una submission"""
    
    # Buscar coordenadas en diferentes lugares
    lat = None
    lon = None
    location_name = None
    usuario = None
    fecha = None
    
    # Location data
    if 'location' in submission:
        location = submission['location']
        lat = location.get('lat')
        lon = location.get('lon') 
        location_name = location.get('name')
    
    # User data
    if 'user' in submission:
        user = submission['user']
        usuario = user.get('name')
    
    # Fecha - buscar en diferentes campos
    for field in ['created_at', 'updated_at', 'submitted_at']:
        if field in submission and submission[field]:
            try:
                fecha_str = submission[field]
                if 'T' in fecha_str:
                    fecha = datetime.fromisoformat(fecha_str.replace('Z', '+00:00')).date()
                    break
            except:
                continue
    
    # Activity info
    activity_name = None
    if 'activity' in submission:
        activity_name = submission['activity'].get('name')
    
    return {
        'submission_id': submission.get('id'),
        'form_type': submission.get('form_type'),
        'lat': lat,
        'lon': lon,
        'location_name': location_name,
        'usuario': usuario,
        'fecha': fecha,
        'activity_name': activity_name,
        'raw_submission': submission  # Para debugging
    }

def calcular_distancia(lat1, lon1, lat2, lon2):
    """Calcular distancia euclidiana"""
    try:
        return math.sqrt((float(lat1) - float(lat2))**2 + (float(lon1) - float(lon2))**2)
    except (TypeError, ValueError):
        return float('inf')

def mapear_a_sucursales(submissions_extraidas, sucursales_master):
    """Mapear submissions a sucursales master"""
    
    print("\n🎯 MAPEANDO SUBMISSIONS A SUCURSALES")
    print("=" * 50)
    
    mapeadas = []
    sin_mapear = []
    tolerancia = 0.01
    
    for submission_data in submissions_extraidas:
        lat = submission_data['lat']
        lon = submission_data['lon']
        
        if not lat or not lon:
            sin_mapear.append(submission_data)
            continue
        
        # Buscar sucursal más cercana
        sucursal_encontrada = None
        distancia_minima = float('inf')
        
        for sucursal_key, datos_sucursal in sucursales_master.items():
            distancia = calcular_distancia(lat, lon, datos_sucursal['lat'], datos_sucursal['lon'])
            
            if distancia <= tolerancia and distancia < distancia_minima:
                distancia_minima = distancia
                sucursal_encontrada = datos_sucursal.copy()
                sucursal_encontrada['distancia_mapeo'] = distancia
        
        if sucursal_encontrada:
            submission_data['sucursal_mapeada'] = sucursal_encontrada['nombre']
            submission_data['grupo_operativo'] = sucursal_encontrada['grupo']
            submission_data['tipo_sucursal'] = sucursal_encontrada['tipo']
            submission_data['distancia_mapeo'] = sucursal_encontrada['distancia_mapeo']
            mapeadas.append(submission_data)
        else:
            sin_mapear.append(submission_data)
    
    print(f"✅ Mapeadas: {len(mapeadas)}")
    print(f"❌ Sin mapear: {len(sin_mapear)}")
    
    return mapeadas, sin_mapear

def analizar_cumplimiento_reglas(submissions_mapeadas):
    """Analizar cumplimiento de reglas por sucursal"""
    
    print("\n📏 ANALIZANDO CUMPLIMIENTO DE REGLAS")
    print("=" * 50)
    
    # Agrupar por sucursal
    por_sucursal = defaultdict(lambda: {
        'operativas': [],
        'seguridad': [],
        'tipo': None,
        'grupo': None
    })
    
    for sub in submissions_mapeadas:
        sucursal = sub['sucursal_mapeada']
        tipo_form = sub['form_type'].lower()
        
        por_sucursal[sucursal][tipo_form + 's'].append(sub)
        por_sucursal[sucursal]['tipo'] = sub['tipo_sucursal']
        por_sucursal[sucursal]['grupo'] = sub['grupo_operativo']
    
    # Analizar cada sucursal
    cumple = 0
    viola = 0
    
    print("📋 ANÁLISIS DETALLADO:")
    print("-" * 60)
    
    for sucursal, datos in sorted(por_sucursal.items()):
        tipo = datos['tipo']
        grupo = datos['grupo']
        ops_count = len(datos['operativas'])
        segs_count = len(datos['seguridad'])
        
        max_ops = REGLAS_MAXIMAS[tipo]['operativas']
        max_segs = REGLAS_MAXIMAS[tipo]['seguridad']
        
        cumple_reglas = ops_count <= max_ops and segs_count <= max_segs
        
        if cumple_reglas:
            cumple += 1
            status = "✅"
        else:
            viola += 1
            status = "❌"
        
        print(f"{status} {sucursal} ({tipo})")
        print(f"    Grupo: {grupo}")
        print(f"    Op: {ops_count}/{max_ops}, Seg: {segs_count}/{max_segs}")
        
        # Mostrar fechas únicas
        fechas_ops = list(set([s['fecha'].strftime('%Y-%m-%d') for s in datos['operativas'] if s['fecha']]))
        fechas_segs = list(set([s['fecha'].strftime('%Y-%m-%d') for s in datos['seguridad'] if s['fecha']]))
        
        if fechas_ops:
            print(f"    Fechas Op: {', '.join(sorted(fechas_ops))}")
        if fechas_segs:
            print(f"    Fechas Seg: {', '.join(sorted(fechas_segs))}")
        
        print()
    
    print("=" * 60)
    print(f"📊 RESUMEN:")
    print(f"   🏪 Sucursales totales: {len(por_sucursal)}")
    print(f"   ✅ Cumplen reglas: {cumple}")
    print(f"   ❌ Violan reglas: {viola}")
    
    # Contar por tipo
    locales = sum(1 for d in por_sucursal.values() if d['tipo'] == 'LOCAL')
    foraneas = sum(1 for d in por_sucursal.values() if d['tipo'] == 'FORANEA')
    
    print(f"   🏢 Locales (4+4): {locales}")
    print(f"   🌍 Foráneas (2+2): {foraneas}")
    
    # Total supervisiones
    total_ops = sum(len(d['operativas']) for d in por_sucursal.values())
    total_segs = sum(len(d['seguridad']) for d in por_sucursal.values())
    
    print(f"   📈 Total: {total_ops} Op + {total_segs} Seg = {total_ops + total_segs}")
    
    return por_sucursal

def generar_listado_final(por_sucursal):
    """Generar listado final"""
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_file = f"listado_real_directo_{timestamp}.csv"
    
    print(f"\n💾 GENERANDO: {csv_file}")
    
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Numero', 'Sucursal', 'Grupo_Operativo', 'Tipo',
            'Operativas', 'Seguridad', 'Max_Op', 'Max_Seg',
            'Cumple_Reglas', 'Status', 'Fechas_Op', 'Fechas_Seg'
        ])
        
        for i, (sucursal, datos) in enumerate(sorted(por_sucursal.items()), 1):
            tipo = datos['tipo']
            grupo = datos['grupo']
            ops_count = len(datos['operativas'])
            segs_count = len(datos['seguridad'])
            
            max_ops = REGLAS_MAXIMAS[tipo]['operativas']
            max_segs = REGLAS_MAXIMAS[tipo]['seguridad']
            
            cumple = ops_count <= max_ops and segs_count <= max_segs
            status = "CORRECTO" if cumple else "EXCEDE_LIMITE"
            
            fechas_ops = '|'.join(sorted(list(set([s['fecha'].strftime('%Y-%m-%d') for s in datos['operativas'] if s['fecha']]))))
            fechas_segs = '|'.join(sorted(list(set([s['fecha'].strftime('%Y-%m-%d') for s in datos['seguridad'] if s['fecha']]))))
            
            writer.writerow([
                i, sucursal, grupo, tipo,
                ops_count, segs_count, max_ops, max_segs,
                cumple, status, fechas_ops, fechas_segs
            ])
    
    print(f"✅ Guardado: {csv_file}")
    return csv_file

def main():
    """Función principal"""
    
    print("🎯 ETL DIRECTO SUBMISSIONS 2025")
    print("=" * 70)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    # 1. Cargar sucursales master
    sucursales_master = cargar_sucursales_master()
    print(f"📊 Sucursales master: {len(sucursales_master)}")
    
    # 2. Extraer submissions
    todas_submissions = extraer_todas_submissions_2025()
    
    if not todas_submissions:
        print("❌ No se obtuvieron submissions")
        return
    
    # 3. Extraer datos de cada submission
    print(f"\n🔄 Extrayendo datos de {len(todas_submissions)} submissions...")
    submissions_extraidas = []
    
    for submission in todas_submissions:
        datos = extraer_datos_submission(submission)
        submissions_extraidas.append(datos)
    
    # 4. Mapear a sucursales
    mapeadas, sin_mapear = mapear_a_sucursales(submissions_extraidas, sucursales_master)
    
    # 5. Analizar cumplimiento
    por_sucursal = analizar_cumplimiento_reglas(mapeadas)
    
    # 6. Generar listado final
    csv_file = generar_listado_final(por_sucursal)
    
    # 7. Guardar datos raw
    json_file = f"datos_raw_directo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(json_file, 'w') as f:
        json.dump(submissions_extraidas, f, indent=2, default=str)
    
    print(f"\n🎉 COMPLETADO")
    print(f"📁 Listado: {csv_file}")
    print(f"📁 Raw data: {json_file}")

if __name__ == "__main__":
    main()