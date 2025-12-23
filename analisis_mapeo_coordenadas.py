#!/usr/bin/env python3
"""
🎯 ANÁLISIS Y MAPEO DE COORDENADAS DE ENTREGA
Mapear submissions sin location o con location incorrecta usando coordenadas reales
"""

import requests
import csv
import math
import json
from datetime import datetime, date
from collections import defaultdict

ZENPUT_CONFIG = {
    'base_url': 'https://www.zenput.com/api/v3',
    'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}
}

FORMULARIOS_2025 = {
    '877138': 'OPERATIVA',
    '877139': 'SEGURIDAD'
}

REGLAS_MAXIMAS = {
    'LOCAL': {'operativas': 4, 'seguridad': 4},
    'FORANEA': {'operativas': 2, 'seguridad': 2}
}

GRUPOS_LOCALES = ['OGAS', 'TEC', 'TEPEYAC', 'PLOG NUEVO LEON', 'GRUPO CENTRITO', 'GRUPO SALTILLO']

def cargar_sucursales_normalizadas():
    """Cargar sucursales con coordenadas normalizadas"""
    sucursales = {}
    
    print("📂 Cargando sucursales normalizadas...")
    
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
    
    print(f"✅ {len(sucursales)} sucursales cargadas")
    return sucursales

def obtener_todas_submissions_completas():
    """Obtener TODAS las submissions 2025 con metadatos completos"""
    
    print("\n🔄 EXTRAYENDO TODAS LAS SUBMISSIONS 2025 CON METADATOS")
    print("=" * 80)
    
    todas_submissions = []
    
    for form_id, tipo_form in FORMULARIOS_2025.items():
        print(f"\n📋 Formulario {form_id} ({tipo_form})")
        print("-" * 50)
        
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
                    'page_size': 50  # Más pequeño para análisis
                }
                
                response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    submissions = data.get('data', [])
                    
                    if not submissions:
                        break
                    
                    print(f"    📄 Página {page}: {len(submissions)} submissions")
                    
                    for submission in submissions:
                        submission['form_type'] = tipo_form
                        form_submissions.append(submission)
                    
                    page += 1
                    
                    # Sin límite - extraer TODAS las submissions
                    # if page > 10:  # Límite de seguridad 
                    #     print(f"    ⚠️ Limitando a {page-1} páginas")
                    #     break
                    
                else:
                    print(f"    ❌ Error {response.status_code}")
                    break
                    
            except Exception as e:
                print(f"    💥 Error: {e}")
                break
        
        print(f"📊 Total {tipo_form}: {len(form_submissions)} submissions")
        todas_submissions.extend(form_submissions)
    
    print(f"\n📊 TOTAL SUBMISSIONS PARA ANÁLISIS: {len(todas_submissions)}")
    return todas_submissions

def extraer_datos_completos_submission(submission):
    """Extraer TODOS los datos relevantes de una submission"""
    
    # Datos básicos
    submission_id = submission.get('id')
    form_type = submission.get('form_type')
    
    # Metadatos (donde están las coordenadas reales)
    smetadata = submission.get('smetadata', {})
    
    # Coordenadas de ENTREGA REAL
    lat_entrega = smetadata.get('lat')
    lon_entrega = smetadata.get('lon')
    
    # Location asignada en Zenput
    location_zenput = smetadata.get('location', {})
    location_id = location_zenput.get('id')
    location_name = location_zenput.get('name')
    location_external_key = location_zenput.get('external_key')
    lat_zenput = location_zenput.get('lat')
    lon_zenput = location_zenput.get('lon')
    
    # Usuario y fechas
    created_by = smetadata.get('created_by', {})
    usuario = created_by.get('display_name')
    
    # Fechas
    fecha_submitted = smetadata.get('date_submitted')
    fecha_completed = smetadata.get('date_completed')
    fecha_created = smetadata.get('date_created')
    
    # Parsear fecha principal
    fecha = None
    for fecha_str in [fecha_submitted, fecha_completed, fecha_created]:
        if fecha_str:
            try:
                fecha = datetime.fromisoformat(fecha_str.replace('Z', '+00:00')).date()
                break
            except:
                continue
    
    # Teams para determinar grupo operativo
    location_teams = smetadata.get('location_teams', [])
    user_teams = smetadata.get('user_teams', [])
    
    return {
        'submission_id': submission_id,
        'form_type': form_type,
        'fecha': fecha,
        'usuario': usuario,
        
        # Coordenadas de entrega REAL
        'lat_entrega': lat_entrega,
        'lon_entrega': lon_entrega,
        
        # Location asignada en Zenput
        'location_id': location_id,
        'location_name': location_name,
        'location_external_key': location_external_key,
        'lat_zenput': lat_zenput,
        'lon_zenput': lon_zenput,
        
        # Teams
        'location_teams': [team.get('id') for team in location_teams],
        'user_teams': [team.get('id') for team in user_teams],
        
        # Raw para análisis
        'smetadata': smetadata
    }

def calcular_distancia_km(lat1, lon1, lat2, lon2):
    """Calcular distancia en kilómetros usando fórmula haversine"""
    try:
        from math import radians, sin, cos, sqrt, atan2
        
        lat1, lon1, lat2, lon2 = map(radians, [float(lat1), float(lon1), float(lat2), float(lon2)])
        
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1-a))
        
        R = 6371  # Radio de la Tierra en km
        distancia = R * c
        
        return distancia
    except:
        return float('inf')

def mapear_coordenadas_entrega(submissions_extraidas, sucursales_normalizadas):
    """Mapear coordenadas de entrega contra sucursales normalizadas"""
    
    print(f"\n🎯 MAPEANDO COORDENADAS DE ENTREGA")
    print("=" * 60)
    
    # Análisis de casos
    con_location_correcto = []
    con_location_incorrecto = []
    sin_location = []
    sin_coordenadas_entrega = []
    
    # Configuración de tolerancias
    TOLERANCIA_PERFECTA = 0.5  # 500m
    TOLERANCIA_BUENA = 1.0     # 1km
    TOLERANCIA_MAXIMA = 5.0    # 5km
    
    for submission_data in submissions_extraidas:
        lat_entrega = submission_data['lat_entrega']
        lon_entrega = submission_data['lon_entrega']
        location_id = submission_data['location_id']
        location_name = submission_data['location_name']
        
        if not lat_entrega or not lon_entrega:
            sin_coordenadas_entrega.append(submission_data)
            continue
        
        # Buscar la sucursal más cercana usando coordenadas de entrega
        mejor_match = None
        distancia_minima = float('inf')
        
        for sucursal_key, datos_sucursal in sucursales_normalizadas.items():
            distancia = calcular_distancia_km(
                lat_entrega, lon_entrega,
                datos_sucursal['lat'], datos_sucursal['lon']
            )
            
            if distancia < distancia_minima:
                distancia_minima = distancia
                mejor_match = {
                    'sucursal': datos_sucursal,
                    'distancia_km': distancia
                }
        
        # Agregar información del mapeo
        submission_data['mapeo_resultado'] = mejor_match
        submission_data['distancia_km'] = distancia_minima
        
        # Clasificar el caso
        if not location_id:
            # Sin location asignada
            sin_location.append(submission_data)
        else:
            # Con location asignada - verificar si es correcta
            if mejor_match and distancia_minima <= TOLERANCIA_BUENA:
                # Location probablemente correcta
                con_location_correcto.append(submission_data)
            else:
                # Location posiblemente incorrecta
                con_location_incorrecto.append(submission_data)
    
    print(f"📊 ANÁLISIS DE MAPEO:")
    print(f"   ✅ Con location correcto: {len(con_location_correcto)}")
    print(f"   ⚠️ Con location incorrecto: {len(con_location_incorrecto)}")
    print(f"   ❌ Sin location: {len(sin_location)}")
    print(f"   💥 Sin coordenadas entrega: {len(sin_coordenadas_entrega)}")
    
    return {
        'con_location_correcto': con_location_correcto,
        'con_location_incorrecto': con_location_incorrecto,
        'sin_location': sin_location,
        'sin_coordenadas_entrega': sin_coordenadas_entrega
    }

def analizar_patrones_mismo_dia(resultados_mapeo):
    """Analizar patrones de supervisiones el mismo día"""
    
    print(f"\n📅 ANÁLISIS DE SUPERVISIONES MISMO DÍA")
    print("=" * 60)
    
    # Combinar todos los casos que necesitan mapeo
    necesitan_mapeo = (
        resultados_mapeo['sin_location'] + 
        resultados_mapeo['con_location_incorrecto']
    )
    
    print(f"📊 Submissions que necesitan mapeo: {len(necesitan_mapeo)}")
    
    # Agrupar por fecha y sucursal mapeada
    por_fecha_sucursal = defaultdict(lambda: {'operativas': [], 'seguridad': []})
    
    for submission in necesitan_mapeo:
        if submission['mapeo_resultado'] and submission['fecha']:
            sucursal_nombre = submission['mapeo_resultado']['sucursal']['nombre']
            fecha_str = submission['fecha'].strftime('%Y-%m-%d')
            key = f"{fecha_str}_{sucursal_nombre}"
            
            tipo_form = submission['form_type'].lower()
            por_fecha_sucursal[key][tipo_form + 's'].append(submission)
    
    # Analizar coincidencias mismo día
    coincidencias_mismo_dia = []
    operativas_solas = []
    seguridad_solas = []
    
    for key, datos in por_fecha_sucursal.items():
        ops = datos['operativas']
        segs = datos['seguridad']
        
        if ops and segs:
            # Hay ambas el mismo día
            coincidencias_mismo_dia.append({
                'key': key,
                'operativas': ops,
                'seguridad': segs,
                'total': len(ops) + len(segs)
            })
        elif ops:
            operativas_solas.extend(ops)
        elif segs:
            seguridad_solas.extend(segs)
    
    print(f"✅ Coincidencias mismo día: {len(coincidencias_mismo_dia)} grupos")
    print(f"⚠️ Operativas solas: {len(operativas_solas)}")
    print(f"⚠️ Seguridad solas: {len(seguridad_solas)}")
    
    # Mostrar ejemplos de coincidencias
    print(f"\n📋 EJEMPLOS DE COINCIDENCIAS MISMO DÍA:")
    for i, coincidencia in enumerate(coincidencias_mismo_dia[:5]):
        fecha, sucursal = coincidencia['key'].split('_', 1)
        ops_count = len(coincidencia['operativas'])
        segs_count = len(coincidencia['seguridad'])
        
        print(f"   {i+1}. {fecha} - {sucursal}")
        print(f"      Operativas: {ops_count}, Seguridad: {segs_count}")
        
        # Mostrar distancias
        for op in coincidencia['operativas']:
            print(f"      Op: {op['distancia_km']:.2f}km - {op['usuario']}")
        for seg in coincidencia['seguridad']:
            print(f"      Seg: {seg['distancia_km']:.2f}km - {seg['usuario']}")
    
    return {
        'coincidencias_mismo_dia': coincidencias_mismo_dia,
        'operativas_solas': operativas_solas,
        'seguridad_solas': seguridad_solas,
        'por_fecha_sucursal': dict(por_fecha_sucursal)
    }

def verificar_cumplimiento_reglas(analisis_dias, sucursales_normalizadas):
    """Verificar cumplimiento de reglas de máximos"""
    
    print(f"\n📏 VERIFICACIÓN DE REGLAS DE MÁXIMOS")
    print("=" * 60)
    
    # Agrupar por sucursal para todo el año
    por_sucursal_anual = defaultdict(lambda: {'operativas': [], 'seguridad': []})
    
    # Procesar coincidencias mismo día
    for coincidencia in analisis_dias['coincidencias_mismo_dia']:
        sucursal_nombre = coincidencia['key'].split('_', 1)[1]
        por_sucursal_anual[sucursal_nombre]['operativas'].extend(coincidencia['operativas'])
        por_sucursal_anual[sucursal_nombre]['seguridad'].extend(coincidencia['seguridad'])
    
    # Procesar submissions solas
    for submission in analisis_dias['operativas_solas'] + analisis_dias['seguridad_solas']:
        if submission['mapeo_resultado']:
            sucursal_nombre = submission['mapeo_resultado']['sucursal']['nombre']
            tipo_form = submission['form_type'].lower()
            por_sucursal_anual[sucursal_nombre][tipo_form + 's'].append(submission)
    
    # Verificar reglas
    cumple_reglas = 0
    viola_reglas = 0
    sucursales_con_superviciones = []
    
    for sucursal_nombre, datos in por_sucursal_anual.items():
        # Encontrar datos de la sucursal
        sucursal_info = None
        for _, info in sucursales_normalizadas.items():
            if info['nombre'] == sucursal_nombre:
                sucursal_info = info
                break
        
        if not sucursal_info:
            continue
        
        tipo_sucursal = sucursal_info['tipo']
        ops_count = len(datos['operativas'])
        segs_count = len(datos['seguridad'])
        
        max_ops = REGLAS_MAXIMAS[tipo_sucursal]['operativas']
        max_segs = REGLAS_MAXIMAS[tipo_sucursal]['seguridad']
        
        cumple = ops_count <= max_ops and segs_count <= max_segs
        
        sucursal_resultado = {
            'nombre': sucursal_nombre,
            'tipo': tipo_sucursal,
            'grupo': sucursal_info['grupo'],
            'operativas': ops_count,
            'seguridad': segs_count,
            'max_ops': max_ops,
            'max_segs': max_segs,
            'cumple': cumple,
            'datos': datos
        }
        
        sucursales_con_superviciones.append(sucursal_resultado)
        
        if cumple:
            cumple_reglas += 1
        else:
            viola_reglas += 1
    
    print(f"📊 RESUMEN DE CUMPLIMIENTO:")
    print(f"   🏪 Sucursales con supervisiones: {len(sucursales_con_superviciones)}")
    print(f"   ✅ Cumplen reglas: {cumple_reglas}")
    print(f"   ❌ Violan reglas: {viola_reglas}")
    
    # Mostrar violaciones
    if viola_reglas > 0:
        print(f"\n⚠️ SUCURSALES QUE VIOLAN REGLAS:")
        for sucursal in sucursales_con_superviciones:
            if not sucursal['cumple']:
                print(f"   • {sucursal['nombre']} ({sucursal['tipo']})")
                print(f"     Op: {sucursal['operativas']}/{sucursal['max_ops']}, Seg: {sucursal['seguridad']}/{sucursal['max_segs']}")
    
    return sucursales_con_superviciones

def main():
    """Función principal de análisis"""
    
    print("🎯 ANÁLISIS Y MAPEO DE COORDENADAS DE ENTREGA")
    print("=" * 100)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 100)
    
    # 1. Cargar sucursales normalizadas
    sucursales_normalizadas = cargar_sucursales_normalizadas()
    
    # 2. Obtener submissions con metadatos
    todas_submissions = obtener_todas_submissions_completas()
    
    if not todas_submissions:
        print("❌ No se obtuvieron submissions")
        return
    
    # 3. Extraer datos completos
    print(f"\n🔄 Extrayendo datos completos de {len(todas_submissions)} submissions...")
    submissions_extraidas = []
    
    for submission in todas_submissions:
        datos = extraer_datos_completos_submission(submission)
        submissions_extraidas.append(datos)
    
    # 4. Mapear coordenadas de entrega
    resultados_mapeo = mapear_coordenadas_entrega(submissions_extraidas, sucursales_normalizadas)
    
    # 5. Analizar patrones mismo día
    analisis_dias = analizar_patrones_mismo_dia(resultados_mapeo)
    
    # 6. Verificar cumplimiento de reglas
    sucursales_con_superviciones = verificar_cumplimiento_reglas(analisis_dias, sucursales_normalizadas)
    
    # 7. Guardar análisis completo
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    analisis_completo = {
        'timestamp': timestamp,
        'total_submissions': len(submissions_extraidas),
        'resultados_mapeo': {k: len(v) if isinstance(v, list) else v for k, v in resultados_mapeo.items()},
        'analisis_dias': {
            'coincidencias_mismo_dia': len(analisis_dias['coincidencias_mismo_dia']),
            'operativas_solas': len(analisis_dias['operativas_solas']),
            'seguridad_solas': len(analisis_dias['seguridad_solas'])
        },
        'sucursales_con_superviciones': len(sucursales_con_superviciones),
        'cumple_reglas': sum(1 for s in sucursales_con_superviciones if s['cumple']),
        'viola_reglas': sum(1 for s in sucursales_con_superviciones if not s['cumple'])
    }
    
    filename = f"analisis_mapeo_coordenadas_{timestamp}.json"
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump({
            'analisis_completo': analisis_completo,
            'submissions_extraidas': submissions_extraidas,
            'resultados_mapeo': resultados_mapeo,
            'analisis_dias': analisis_dias,
            'sucursales_con_superviciones': sucursales_con_superviciones
        }, f, indent=2, default=str, ensure_ascii=False)
    
    print(f"\n💾 Análisis guardado en: {filename}")
    print(f"\n🎉 ANÁLISIS COMPLETADO")
    
    return analisis_completo

if __name__ == "__main__":
    main()