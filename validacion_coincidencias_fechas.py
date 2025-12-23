#!/usr/bin/env python3
"""
🎯 VALIDACIÓN DE COINCIDENCIAS DE FECHAS
Extraer TODAS las submissions y validar coincidencias operativas/seguridad mismo día
"""

import requests
import csv
import math
import json
from datetime import datetime, date
from collections import defaultdict
import time

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

def calcular_distancia_km(lat1, lon1, lat2, lon2):
    """Calcular distancia en km usando fórmula haversine"""
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

def extraer_todas_submissions_completas():
    """Extraer TODAS las submissions 2025"""
    
    print(f"\n🔄 EXTRAYENDO TODAS LAS SUBMISSIONS 2025")
    print("=" * 80)
    
    todas_submissions = []
    
    for form_id, tipo_form in FORMULARIOS_2025.items():
        print(f"\n📋 Extrayendo {tipo_form} (Form {form_id})")
        print("-" * 50)
        
        page = 1
        form_submissions = []
        submissions_procesadas = 0
        
        while True:
            try:
                print(f"    📄 Procesando página {page}...", end=" ", flush=True)
                
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
                        print("✅ Fin")
                        break
                    
                    print(f"{len(submissions)} submissions")
                    
                    for submission in submissions:
                        submission['form_type'] = tipo_form
                        form_submissions.append(submission)
                        submissions_procesadas += 1
                    
                    page += 1
                    time.sleep(0.1)  # Pausa pequeña para no sobrecargar API
                    
                else:
                    print(f"❌ Error {response.status_code}")
                    break
                    
            except Exception as e:
                print(f"💥 Error: {e}")
                break
        
        print(f"📊 Total {tipo_form}: {len(form_submissions)} submissions")
        todas_submissions.extend(form_submissions)
    
    print(f"\n📊 TOTAL GENERAL: {len(todas_submissions)} submissions extraídas")
    return todas_submissions

def extraer_datos_submission_completos(submission):
    """Extraer TODOS los datos relevantes de una submission"""
    
    # Datos básicos
    submission_id = submission.get('id')
    form_type = submission.get('form_type')
    
    # Metadatos completos
    smetadata = submission.get('smetadata', {})
    
    # Coordenadas de ENTREGA REAL (las que necesitamos)
    lat_entrega = smetadata.get('lat')
    lon_entrega = smetadata.get('lon')
    
    # Location asignada en Zenput (para comparación)
    location_zenput = smetadata.get('location', {})
    location_id = location_zenput.get('id')
    location_name = location_zenput.get('name')
    location_external_key = location_zenput.get('external_key')
    lat_zenput = location_zenput.get('lat')
    lon_zenput = location_zenput.get('lon')
    
    # Usuario que hizo la supervisión
    created_by = smetadata.get('created_by', {})
    usuario_id = created_by.get('id')
    usuario_nombre = created_by.get('display_name')
    
    # Fechas múltiples para mejor precisión
    fecha_submitted = smetadata.get('date_submitted')
    fecha_completed = smetadata.get('date_completed')
    fecha_created = smetadata.get('date_created')
    fecha_local = smetadata.get('date_submitted_local')
    
    # Parsear fecha principal (prioridad: submitted > completed > created)
    fecha = None
    fecha_str_original = None
    
    for campo_fecha, fecha_str in [
        ('submitted', fecha_submitted),
        ('completed', fecha_completed), 
        ('created', fecha_created)
    ]:
        if fecha_str:
            try:
                fecha = datetime.fromisoformat(fecha_str.replace('Z', '+00:00')).date()
                fecha_str_original = f"{campo_fecha}:{fecha_str}"
                break
            except:
                continue
    
    # Teams y roles
    user_role = smetadata.get('user_role', {})
    rol_nombre = user_role.get('name')
    
    location_teams = smetadata.get('location_teams', [])
    user_teams = smetadata.get('user_teams', [])
    
    # Información de tiempo de completado
    tiempo_completado = smetadata.get('time_to_complete')  # en millisegundos
    
    return {
        # IDs básicos
        'submission_id': submission_id,
        'form_type': form_type,
        
        # Fechas
        'fecha': fecha,
        'fecha_str_original': fecha_str_original,
        'fecha_submitted': fecha_submitted,
        'fecha_completed': fecha_completed,
        'fecha_created': fecha_created,
        'fecha_local': fecha_local,
        
        # Usuario
        'usuario_id': usuario_id,
        'usuario_nombre': usuario_nombre,
        'rol_nombre': rol_nombre,
        
        # Coordenadas REALES de entrega
        'lat_entrega': lat_entrega,
        'lon_entrega': lon_entrega,
        
        # Location asignada en Zenput (para comparación)
        'location_id': location_id,
        'location_name': location_name,
        'location_external_key': location_external_key,
        'lat_zenput': lat_zenput,
        'lon_zenput': lon_zenput,
        
        # Metadatos adicionales
        'tiempo_completado_ms': tiempo_completado,
        'location_teams_ids': [team.get('id') for team in location_teams],
        'user_teams_ids': [team.get('id') for team in user_teams],
        
        # Para debugging
        'smetadata_completo': smetadata
    }

def mapear_a_sucursales_reales(submissions_extraidas, sucursales_normalizadas):
    """Mapear submissions usando coordenadas REALES de entrega"""
    
    print(f"\n🎯 MAPEANDO CON COORDENADAS REALES DE ENTREGA")
    print("=" * 70)
    
    submissions_mapeadas = []
    submissions_sin_coordenadas = []
    estadisticas_mapeo = {
        'total_procesadas': 0,
        'mapeadas_exitosas': 0,
        'sin_coordenadas': 0,
        'distancias': []
    }
    
    TOLERANCIA_MAXIMA = 2.0  # 2km máximo para considerar válido
    
    for i, submission_data in enumerate(submissions_extraidas):
        if i % 50 == 0 and i > 0:
            print(f"    🔄 Procesadas: {i}/{len(submissions_extraidas)}")
        
        estadisticas_mapeo['total_procesadas'] += 1
        
        lat_entrega = submission_data['lat_entrega']
        lon_entrega = submission_data['lon_entrega']
        
        if not lat_entrega or not lon_entrega:
            submissions_sin_coordenadas.append(submission_data)
            estadisticas_mapeo['sin_coordenadas'] += 1
            continue
        
        # Buscar la sucursal MÁS CERCANA usando coordenadas de entrega
        mejor_match = None
        distancia_minima = float('inf')
        
        for sucursal_key, datos_sucursal in sucursales_normalizadas.items():
            distancia = calcular_distancia_km(
                lat_entrega, lon_entrega,
                datos_sucursal['lat'], datos_sucursal['lon']
            )
            
            if distancia < distancia_minima:
                distancia_minima = distancia
                mejor_match = datos_sucursal.copy()
        
        # Solo aceptar si está dentro de tolerancia razonable
        if distancia_minima <= TOLERANCIA_MAXIMA:
            # Agregar información del mapeo
            submission_data['sucursal_mapeada'] = mejor_match['nombre']
            submission_data['sucursal_numero'] = mejor_match['numero']
            submission_data['sucursal_grupo'] = mejor_match['grupo']
            submission_data['sucursal_tipo'] = mejor_match['tipo']
            submission_data['distancia_km'] = distancia_minima
            submission_data['mapeo_exitoso'] = True
            
            submissions_mapeadas.append(submission_data)
            estadisticas_mapeo['mapeadas_exitosas'] += 1
            estadisticas_mapeo['distancias'].append(distancia_minima)
        else:
            # Muy lejos de cualquier sucursal conocida
            submission_data['mapeo_exitoso'] = False
            submission_data['distancia_km'] = distancia_minima
            submission_data['razon_fallo'] = f"Muy lejos ({distancia_minima:.2f}km)"
            submissions_sin_coordenadas.append(submission_data)
    
    print(f"\n📊 ESTADÍSTICAS DE MAPEO:")
    print(f"   📋 Total procesadas: {estadisticas_mapeo['total_procesadas']}")
    print(f"   ✅ Mapeadas exitosas: {estadisticas_mapeo['mapeadas_exitosas']}")
    print(f"   ❌ Sin coordenadas/fallidas: {estadisticas_mapeo['sin_coordenadas']}")
    
    if estadisticas_mapeo['distancias']:
        distancias = estadisticas_mapeo['distancias']
        print(f"   📏 Distancia promedio: {sum(distancias)/len(distancias):.3f}km")
        print(f"   📏 Distancia máxima: {max(distancias):.3f}km")
        print(f"   📏 Distancia mínima: {min(distancias):.3f}km")
    
    return submissions_mapeadas, submissions_sin_coordenadas, estadisticas_mapeo

def analizar_coincidencias_fechas(submissions_mapeadas):
    """Analizar coincidencias de operativas/seguridad el mismo día"""
    
    print(f"\n📅 ANALIZANDO COINCIDENCIAS DE FECHAS")
    print("=" * 70)
    
    # Agrupar por sucursal + fecha
    por_sucursal_fecha = defaultdict(lambda: {'operativas': [], 'seguridad': []})
    
    for submission in submissions_mapeadas:
        if submission['fecha']:
            sucursal = submission['sucursal_mapeada']
            fecha_str = submission['fecha'].strftime('%Y-%m-%d')
            key = f"{sucursal}_{fecha_str}"
            
            tipo_form = submission['form_type'].lower()
            if tipo_form == 'operativa':
                por_sucursal_fecha[key]['operativas'].append(submission)
            elif tipo_form == 'seguridad':
                por_sucursal_fecha[key]['seguridad'].append(submission)
    
    # Analizar patrones de coincidencias
    coincidencias_perfectas = []  # Ambas el mismo día
    operativas_solas = []
    seguridad_solas = []
    dias_con_multiples = []
    
    for key, datos in por_sucursal_fecha.items():
        sucursal, fecha_str = key.rsplit('_', 1)
        operativas = datos['operativas']
        seguridad = datos['seguridad']
        
        if operativas and seguridad:
            # ✅ COINCIDENCIA PERFECTA
            coincidencias_perfectas.append({
                'sucursal': sucursal,
                'fecha': fecha_str,
                'operativas': operativas,
                'seguridad': seguridad,
                'total_ops': len(operativas),
                'total_segs': len(seguridad)
            })
            
            # Verificar si hay múltiples supervisiones el mismo día
            if len(operativas) > 1 or len(seguridad) > 1:
                dias_con_multiples.append({
                    'sucursal': sucursal,
                    'fecha': fecha_str,
                    'operativas': len(operativas),
                    'seguridad': len(seguridad)
                })
        
        elif operativas:
            # Solo operativas
            operativas_solas.extend(operativas)
        
        elif seguridad:
            # Solo seguridad
            seguridad_solas.extend(seguridad)
    
    print(f"📊 RESULTADOS DE COINCIDENCIAS:")
    print(f"   ✅ Coincidencias perfectas (mismo día): {len(coincidencias_perfectas)}")
    print(f"   ⚠️ Solo operativas: {len(operativas_solas)}")
    print(f"   ⚠️ Solo seguridad: {len(seguridad_solas)}")
    print(f"   🔄 Días con múltiples supervisiones: {len(dias_con_multiples)}")
    
    # Mostrar ejemplos de coincidencias perfectas
    print(f"\n🎯 EJEMPLOS DE COINCIDENCIAS PERFECTAS:")
    for i, coincidencia in enumerate(coincidencias_perfectas[:5]):
        print(f"   {i+1}. {coincidencia['fecha']} - {coincidencia['sucursal']}")
        print(f"      Operativas: {coincidencia['total_ops']}, Seguridad: {coincidencia['total_segs']}")
        
        # Mostrar usuarios
        usuarios_ops = [op['usuario_nombre'] for op in coincidencia['operativas']]
        usuarios_segs = [seg['usuario_nombre'] for seg in coincidencia['seguridad']]
        print(f"      Usuarios Op: {', '.join(usuarios_ops)}")
        print(f"      Usuarios Seg: {', '.join(usuarios_segs)}")
    
    # Mostrar días con múltiples supervisiones
    if dias_con_multiples:
        print(f"\n⚠️ DÍAS CON MÚLTIPLES SUPERVISIONES:")
        for i, multiple in enumerate(dias_con_multiples[:5]):
            print(f"   {i+1}. {multiple['fecha']} - {multiple['sucursal']}")
            print(f"      {multiple['operativas']} operativas, {multiple['seguridad']} seguridad")
    
    return {
        'coincidencias_perfectas': coincidencias_perfectas,
        'operativas_solas': operativas_solas,
        'seguridad_solas': seguridad_solas,
        'dias_con_multiples': dias_con_multiples,
        'por_sucursal_fecha': dict(por_sucursal_fecha)
    }

def verificar_reglas_maximas(analisis_coincidencias, sucursales_normalizadas):
    """Verificar cumplimiento de reglas máximas por sucursal"""
    
    print(f"\n📏 VERIFICANDO REGLAS MÁXIMAS POR SUCURSAL")
    print("=" * 70)
    
    # Agrupar por sucursal para todo el año 2025
    por_sucursal_anual = defaultdict(lambda: {
        'operativas': [],
        'seguridad': [],
        'fechas_operativas': set(),
        'fechas_seguridad': set(),
        'info_sucursal': None
    })
    
    # Procesar coincidencias perfectas
    for coincidencia in analisis_coincidencias['coincidencias_perfectas']:
        sucursal = coincidencia['sucursal']
        por_sucursal_anual[sucursal]['operativas'].extend(coincidencia['operativas'])
        por_sucursal_anual[sucursal]['seguridad'].extend(coincidencia['seguridad'])
        
        # Fechas únicas
        fecha_str = coincidencia['fecha']
        por_sucursal_anual[sucursal]['fechas_operativas'].add(fecha_str)
        por_sucursal_anual[sucursal]['fechas_seguridad'].add(fecha_str)
    
    # Procesar operativas solas
    for operativa in analisis_coincidencias['operativas_solas']:
        sucursal = operativa['sucursal_mapeada']
        fecha_str = operativa['fecha'].strftime('%Y-%m-%d') if operativa['fecha'] else 'SIN_FECHA'
        
        por_sucursal_anual[sucursal]['operativas'].append(operativa)
        if operativa['fecha']:
            por_sucursal_anual[sucursal]['fechas_operativas'].add(fecha_str)
    
    # Procesar seguridad solas
    for seguridad in analisis_coincidencias['seguridad_solas']:
        sucursal = seguridad['sucursal_mapeada']
        fecha_str = seguridad['fecha'].strftime('%Y-%m-%d') if seguridad['fecha'] else 'SIN_FECHA'
        
        por_sucursal_anual[sucursal]['seguridad'].append(seguridad)
        if seguridad['fecha']:
            por_sucursal_anual[sucursal]['fechas_seguridad'].add(fecha_str)
    
    # Verificar reglas y calcular estadísticas
    sucursales_finales = []
    cumple_reglas = 0
    viola_reglas = 0
    
    for sucursal_nombre, datos in por_sucursal_anual.items():
        # Encontrar información de la sucursal
        info_sucursal = None
        for _, info in sucursales_normalizadas.items():
            if info['nombre'] == sucursal_nombre:
                info_sucursal = info
                break
        
        if not info_sucursal:
            continue
        
        # Contar supervisiones
        ops_count = len(datos['operativas'])
        segs_count = len(datos['seguridad'])
        
        # Obtener límites según tipo de sucursal
        tipo_sucursal = info_sucursal['tipo']
        max_ops = REGLAS_MAXIMAS[tipo_sucursal]['operativas']
        max_segs = REGLAS_MAXIMAS[tipo_sucursal]['seguridad']
        
        # Verificar cumplimiento
        cumple = ops_count <= max_ops and segs_count <= max_segs
        
        sucursal_final = {
            'nombre': sucursal_nombre,
            'numero': info_sucursal['numero'],
            'grupo': info_sucursal['grupo'],
            'tipo': tipo_sucursal,
            'operativas_count': ops_count,
            'seguridad_count': segs_count,
            'max_ops': max_ops,
            'max_segs': max_segs,
            'cumple_reglas': cumple,
            'fechas_operativas': sorted(list(datos['fechas_operativas'])),
            'fechas_seguridad': sorted(list(datos['fechas_seguridad'])),
            'fechas_operativas_count': len(datos['fechas_operativas']),
            'fechas_seguridad_count': len(datos['fechas_seguridad']),
            'submissions_operativas': datos['operativas'],
            'submissions_seguridad': datos['seguridad']
        }
        
        sucursales_finales.append(sucursal_final)
        
        if cumple:
            cumple_reglas += 1
        else:
            viola_reglas += 1
    
    print(f"📊 RESUMEN FINAL:")
    print(f"   🏪 Sucursales con supervisiones: {len(sucursales_finales)}")
    print(f"   ✅ Cumplen reglas: {cumple_reglas}")
    print(f"   ❌ Violan reglas: {viola_reglas}")
    
    # Contar por tipo
    locales = sum(1 for s in sucursales_finales if s['tipo'] == 'LOCAL')
    foraneas = sum(1 for s in sucursales_finales if s['tipo'] == 'FORANEA')
    
    print(f"   🏢 Locales (máx 4+4): {locales}")
    print(f"   🌍 Foráneas (máx 2+2): {foraneas}")
    
    # Mostrar violaciones si las hay
    if viola_reglas > 0:
        print(f"\n⚠️ SUCURSALES QUE VIOLAN REGLAS:")
        violadoras = [s for s in sucursales_finales if not s['cumple_reglas']]
        for sucursal in violadoras[:5]:  # Solo primeras 5
            print(f"   • {sucursal['nombre']} ({sucursal['tipo']})")
            print(f"     Op: {sucursal['operativas_count']}/{sucursal['max_ops']}, Seg: {sucursal['seguridad_count']}/{sucursal['max_segs']}")
    
    # Total de supervisiones
    total_ops = sum(s['operativas_count'] for s in sucursales_finales)
    total_segs = sum(s['seguridad_count'] for s in sucursales_finales)
    
    print(f"\n📈 TOTALES GENERALES:")
    print(f"   📋 Total operativas: {total_ops}")
    print(f"   🔒 Total seguridad: {total_segs}")
    print(f"   📊 Total supervisiones: {total_ops + total_segs}")
    
    return sorted(sucursales_finales, key=lambda x: x['numero'])

def main():
    """Función principal"""
    
    print("🎯 VALIDACIÓN DE COINCIDENCIAS DE FECHAS Y MAPEO")
    print("=" * 100)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 100)
    
    # 1. Cargar sucursales normalizadas
    sucursales_normalizadas = cargar_sucursales_normalizadas()
    
    # 2. Extraer TODAS las submissions
    todas_submissions = extraer_todas_submissions_completas()
    
    if not todas_submissions:
        print("❌ No se obtuvieron submissions")
        return
    
    # 3. Extraer datos completos
    print(f"\n🔄 Procesando datos completos de {len(todas_submissions)} submissions...")
    submissions_extraidas = []
    
    for submission in todas_submissions:
        datos = extraer_datos_submission_completos(submission)
        submissions_extraidas.append(datos)
    
    # 4. Mapear a sucursales usando coordenadas REALES
    submissions_mapeadas, submissions_fallidas, estadisticas_mapeo = mapear_a_sucursales_reales(
        submissions_extraidas, sucursales_normalizadas
    )
    
    # 5. Analizar coincidencias de fechas
    analisis_coincidencias = analizar_coincidencias_fechas(submissions_mapeadas)
    
    # 6. Verificar reglas máximas
    sucursales_finales = verificar_reglas_maximas(analisis_coincidencias, sucursales_normalizadas)
    
    # 7. Guardar resultados completos
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"validacion_coincidencias_completa_{timestamp}.json"
    
    resultado_completo = {
        'timestamp': timestamp,
        'total_submissions_extraidas': len(todas_submissions),
        'total_submissions_procesadas': len(submissions_extraidas),
        'total_submissions_mapeadas': len(submissions_mapeadas),
        'total_submissions_fallidas': len(submissions_fallidas),
        'estadisticas_mapeo': estadisticas_mapeo,
        'coincidencias_perfectas': len(analisis_coincidencias['coincidencias_perfectas']),
        'operativas_solas': len(analisis_coincidencias['operativas_solas']),
        'seguridad_solas': len(analisis_coincidencias['seguridad_solas']),
        'sucursales_con_supervisiones': len(sucursales_finales),
        'sucursales_cumplen_reglas': sum(1 for s in sucursales_finales if s['cumple_reglas']),
        'sucursales_violan_reglas': sum(1 for s in sucursales_finales if not s['cumple_reglas']),
        
        # Datos completos para análisis posterior
        'sucursales_finales': sucursales_finales,
        'analisis_coincidencias': analisis_coincidencias,
        'submissions_mapeadas': submissions_mapeadas,
        'submissions_fallidas': submissions_fallidas
    }
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(resultado_completo, f, indent=2, default=str, ensure_ascii=False)
    
    print(f"\n💾 Resultados completos guardados en: {filename}")
    print(f"🎉 VALIDACIÓN COMPLETADA")
    
    return resultado_completo

if __name__ == "__main__":
    main()