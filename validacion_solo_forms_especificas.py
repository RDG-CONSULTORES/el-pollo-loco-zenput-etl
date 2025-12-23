#!/usr/bin/env python3
"""
🎯 VALIDACIÓN SOLO FORMULARIOS ESPECÍFICOS
Extraer SOLO de forms 877138 (OPERATIVA) y 877139 (SEGURIDAD)
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

# SOLO LOS FORMULARIOS ESPECÍFICOS
FORMULARIOS_OBJETIVO = {
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

def contar_submissions_forms_especificas():
    """Contar submissions SOLO de los formularios específicos"""
    
    print(f"\n📊 CONTANDO SUBMISSIONS DE FORMULARIOS ESPECÍFICOS")
    print("=" * 70)
    
    totales = {}
    
    for form_id, tipo_form in FORMULARIOS_OBJETIVO.items():
        print(f"\n📋 Contando {tipo_form} (Form {form_id})...")
        
        try:
            # Contar SIN filtro de fecha para ver total en sistema
            url = f"{ZENPUT_CONFIG['base_url']}/submissions"
            params = {
                'form_template_id': form_id,  # FILTRO ESPECÍFICO POR FORM
                'page': 1,
                'page_size': 1
            }
            
            response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                # Como el count está bugueado, contar páginas reales
                submissions_muestra = data.get('data', [])
                
                if submissions_muestra:
                    print(f"   ✅ Datos encontrados - procediendo a contar páginas...")
                    
                    # Contar páginas hasta encontrar el final
                    page = 1
                    total_submissions = 0
                    
                    while True:
                        params['page'] = page
                        params['page_size'] = 100
                        
                        response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=30)
                        
                        if response.status_code == 200:
                            data = response.json()
                            submissions = data.get('data', [])
                            
                            if not submissions:
                                break
                            
                            total_submissions += len(submissions)
                            print(f"   📄 Página {page}: {len(submissions)} submissions")
                            page += 1
                            
                            # Limitar a 10 páginas para este conteo
                            if page > 10:
                                print(f"   ⚠️ Limitado a 10 páginas para conteo rápido")
                                total_submissions = f"{total_submissions}+"
                                break
                        else:
                            break
                    
                    totales[form_id] = {
                        'tipo': tipo_form,
                        'total': total_submissions,
                        'paginas_contadas': page - 1
                    }
                    
                    print(f"   📊 Total {tipo_form}: {total_submissions} submissions")
                else:
                    print(f"   ❌ No se encontraron datos")
                    totales[form_id] = {
                        'tipo': tipo_form,
                        'total': 0,
                        'paginas_contadas': 0
                    }
                    
            else:
                print(f"   ❌ Error {response.status_code}: {response.text[:100]}")
                
        except Exception as e:
            print(f"   💥 Error: {e}")
    
    return totales

def extraer_submissions_forms_especificas():
    """Extraer TODAS las submissions de formularios específicos"""
    
    print(f"\n🔄 EXTRAYENDO SUBMISSIONS DE FORMULARIOS ESPECÍFICOS")
    print("=" * 80)
    
    todas_submissions = []
    
    for form_id, tipo_form in FORMULARIOS_OBJETIVO.items():
        print(f"\n📋 Extrayendo {tipo_form} (Form {form_id})")
        print("-" * 50)
        
        page = 1
        form_submissions = []
        
        while True:
            try:
                print(f"    📄 Página {page}...", end=" ", flush=True)
                
                url = f"{ZENPUT_CONFIG['base_url']}/submissions"
                params = {
                    'form_template_id': form_id,  # FILTRO ESPECÍFICO
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
                    
                    # Filtrar por fechas 2025 en el cliente
                    submissions_2025 = []
                    for submission in submissions:
                        smetadata = submission.get('smetadata', {})
                        fecha_submitted = smetadata.get('date_submitted')
                        
                        if fecha_submitted:
                            try:
                                fecha_dt = datetime.fromisoformat(fecha_submitted.replace('Z', '+00:00'))
                                if fecha_dt.year == 2025:
                                    submission['form_type'] = tipo_form
                                    submissions_2025.append(submission)
                            except:
                                continue
                    
                    form_submissions.extend(submissions_2025)
                    print(f"        └─ 2025: {len(submissions_2025)} submissions")
                    
                    page += 1
                    time.sleep(0.1)  # Pausa pequeña
                    
                else:
                    print(f"❌ Error {response.status_code}")
                    break
                    
            except Exception as e:
                print(f"💥 Error: {e}")
                break
        
        print(f"📊 Total {tipo_form} (2025): {len(form_submissions)} submissions")
        todas_submissions.extend(form_submissions)
    
    print(f"\n📊 TOTAL GENERAL 2025: {len(todas_submissions)} submissions")
    return todas_submissions

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

def procesar_y_mapear_submissions(submissions_extraidas, sucursales_normalizadas):
    """Procesar submissions y mapear a sucursales"""
    
    print(f"\n🎯 PROCESANDO Y MAPEANDO {len(submissions_extraidas)} SUBMISSIONS")
    print("=" * 70)
    
    submissions_procesadas = []
    sin_coordenadas = 0
    mapeadas_exitosas = 0
    
    TOLERANCIA_MAXIMA = 2.0  # 2km máximo
    
    for i, submission in enumerate(submissions_extraidas):
        if i % 25 == 0 and i > 0:
            print(f"    🔄 Procesadas: {i}/{len(submissions_extraidas)}")
        
        # Extraer datos básicos
        submission_id = submission.get('id')
        form_type = submission.get('form_type')
        smetadata = submission.get('smetadata', {})
        
        # Coordenadas de entrega REAL
        lat_entrega = smetadata.get('lat')
        lon_entrega = smetadata.get('lon')
        
        # Usuario
        created_by = smetadata.get('created_by', {})
        usuario_nombre = created_by.get('display_name')
        
        # Fechas
        fecha_submitted = smetadata.get('date_submitted')
        fecha = None
        
        if fecha_submitted:
            try:
                fecha = datetime.fromisoformat(fecha_submitted.replace('Z', '+00:00')).date()
            except:
                pass
        
        # Location asignada en Zenput
        location_zenput = smetadata.get('location', {})
        location_name = location_zenput.get('name')
        location_external_key = location_zenput.get('external_key')
        
        datos_submission = {
            'submission_id': submission_id,
            'form_type': form_type,
            'fecha': fecha,
            'usuario_nombre': usuario_nombre,
            'lat_entrega': lat_entrega,
            'lon_entrega': lon_entrega,
            'location_zenput_name': location_name,
            'location_zenput_key': location_external_key,
            'mapeo_exitoso': False,
            'sucursal_mapeada': None,
            'distancia_km': None
        }
        
        # Mapear a sucursal si tenemos coordenadas
        if lat_entrega and lon_entrega:
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
            
            # Solo aceptar si está dentro de tolerancia
            if distancia_minima <= TOLERANCIA_MAXIMA:
                datos_submission.update({
                    'mapeo_exitoso': True,
                    'sucursal_mapeada': mejor_match['nombre'],
                    'sucursal_numero': mejor_match['numero'],
                    'sucursal_grupo': mejor_match['grupo'],
                    'sucursal_tipo': mejor_match['tipo'],
                    'distancia_km': distancia_minima
                })
                mapeadas_exitosas += 1
            else:
                datos_submission['distancia_km'] = distancia_minima
        else:
            sin_coordenadas += 1
        
        submissions_procesadas.append(datos_submission)
    
    print(f"\n📊 RESULTADOS DEL PROCESAMIENTO:")
    print(f"   📋 Total procesadas: {len(submissions_procesadas)}")
    print(f"   ✅ Mapeadas exitosas: {mapeadas_exitosas}")
    print(f"   ❌ Sin coordenadas: {sin_coordenadas}")
    print(f"   ⚠️ Fuera de tolerancia: {len(submissions_procesadas) - mapeadas_exitosas - sin_coordenadas}")
    
    return submissions_procesadas

def analizar_coincidencias_fechas(submissions_procesadas):
    """Analizar coincidencias operativas/seguridad mismo día"""
    
    print(f"\n📅 ANALIZANDO COINCIDENCIAS MISMO DÍA")
    print("=" * 70)
    
    # Solo submissions mapeadas exitosas
    submissions_mapeadas = [s for s in submissions_procesadas if s['mapeo_exitoso']]
    print(f"📊 Submissions mapeadas para análisis: {len(submissions_mapeadas)}")
    
    # Agrupar por sucursal + fecha
    por_sucursal_fecha = defaultdict(lambda: {'operativas': [], 'seguridad': []})
    
    for submission in submissions_mapeadas:
        if submission['fecha']:
            sucursal = submission['sucursal_mapeada']
            fecha_str = submission['fecha'].strftime('%Y-%m-%d')
            key = f"{sucursal}_{fecha_str}"
            
            if submission['form_type'] == 'OPERATIVA':
                por_sucursal_fecha[key]['operativas'].append(submission)
            elif submission['form_type'] == 'SEGURIDAD':
                por_sucursal_fecha[key]['seguridad'].append(submission)
    
    # Analizar patrones
    coincidencias_perfectas = []  # Ambas el mismo día
    operativas_solas = []
    seguridad_solas = []
    dias_multiples = []
    
    for key, datos in por_sucursal_fecha.items():
        sucursal, fecha_str = key.rsplit('_', 1)
        ops = datos['operativas']
        segs = datos['seguridad']
        
        if ops and segs:
            # COINCIDENCIA PERFECTA ✅
            coincidencias_perfectas.append({
                'sucursal': sucursal,
                'fecha': fecha_str,
                'operativas': ops,
                'seguridad': segs,
                'total_ops': len(ops),
                'total_segs': len(segs)
            })
            
            # Verificar múltiples mismo día
            if len(ops) > 1 or len(segs) > 1:
                dias_multiples.append({
                    'sucursal': sucursal,
                    'fecha': fecha_str,
                    'ops': len(ops),
                    'segs': len(segs)
                })
        elif ops:
            operativas_solas.extend(ops)
        elif segs:
            seguridad_solas.extend(segs)
    
    print(f"📊 ANÁLISIS DE COINCIDENCIAS:")
    print(f"   ✅ Coincidencias perfectas: {len(coincidencias_perfectas)}")
    print(f"   ⚠️ Solo operativas: {len(operativas_solas)}")
    print(f"   ⚠️ Solo seguridad: {len(seguridad_solas)}")
    print(f"   🔄 Días con múltiples: {len(dias_multiples)}")
    
    # Mostrar ejemplos
    print(f"\n🎯 EJEMPLOS DE COINCIDENCIAS PERFECTAS:")
    for i, coincidencia in enumerate(coincidencias_perfectas[:5]):
        print(f"   {i+1}. {coincidencia['fecha']} - {coincidencia['sucursal']}")
        print(f"      Ops: {coincidencia['total_ops']}, Segs: {coincidencia['total_segs']}")
        
        usuarios_ops = [op['usuario_nombre'] for op in coincidencia['operativas']]
        usuarios_segs = [seg['usuario_nombre'] for seg in coincidencia['seguridad']]
        print(f"      Usuarios Op: {', '.join(usuarios_ops)}")
        print(f"      Usuarios Seg: {', '.join(usuarios_segs)}")
    
    return {
        'coincidencias_perfectas': coincidencias_perfectas,
        'operativas_solas': operativas_solas,
        'seguridad_solas': seguridad_solas,
        'dias_multiples': dias_multiples,
        'por_sucursal_fecha': dict(por_sucursal_fecha)
    }

def verificar_cumplimiento_reglas(analisis_coincidencias):
    """Verificar cumplimiento de reglas máximas por sucursal"""
    
    print(f"\n📏 VERIFICANDO CUMPLIMIENTO DE REGLAS")
    print("=" * 70)
    
    # Consolidar por sucursal
    por_sucursal = defaultdict(lambda: {
        'operativas': [],
        'seguridad': [],
        'fechas_ops': set(),
        'fechas_segs': set(),
        'tipo_sucursal': None
    })
    
    # Procesar coincidencias
    for coincidencia in analisis_coincidencias['coincidencias_perfectas']:
        sucursal = coincidencia['sucursal']
        fecha = coincidencia['fecha']
        
        por_sucursal[sucursal]['operativas'].extend(coincidencia['operativas'])
        por_sucursal[sucursal]['seguridad'].extend(coincidencia['seguridad'])
        por_sucursal[sucursal]['fechas_ops'].add(fecha)
        por_sucursal[sucursal]['fechas_segs'].add(fecha)
    
    # Procesar solas
    for operativa in analisis_coincidencias['operativas_solas']:
        sucursal = operativa['sucursal_mapeada']
        fecha = operativa['fecha'].strftime('%Y-%m-%d')
        
        por_sucursal[sucursal]['operativas'].append(operativa)
        por_sucursal[sucursal]['fechas_ops'].add(fecha)
    
    for seguridad in analisis_coincidencias['seguridad_solas']:
        sucursal = seguridad['sucursal_mapeada']
        fecha = seguridad['fecha'].strftime('%Y-%m-%d')
        
        por_sucursal[sucursal]['seguridad'].append(seguridad)
        por_sucursal[sucursal]['fechas_segs'].add(fecha)
    
    # Verificar reglas
    sucursales_resultado = []
    cumple = 0
    viola = 0
    
    for sucursal, datos in por_sucursal.items():
        # Determinar tipo de sucursal por primera submission
        tipo_sucursal = None
        if datos['operativas']:
            tipo_sucursal = datos['operativas'][0]['sucursal_tipo']
        elif datos['seguridad']:
            tipo_sucursal = datos['seguridad'][0]['sucursal_tipo']
        
        if not tipo_sucursal:
            continue
        
        ops_count = len(datos['operativas'])
        segs_count = len(datos['seguridad'])
        
        max_ops = REGLAS_MAXIMAS[tipo_sucursal]['operativas']
        max_segs = REGLAS_MAXIMAS[tipo_sucursal]['seguridad']
        
        cumple_reglas = ops_count <= max_ops and segs_count <= max_segs
        
        sucursal_info = {
            'nombre': sucursal,
            'tipo': tipo_sucursal,
            'operativas_count': ops_count,
            'seguridad_count': segs_count,
            'max_ops': max_ops,
            'max_segs': max_segs,
            'cumple_reglas': cumple_reglas,
            'fechas_ops': sorted(list(datos['fechas_ops'])),
            'fechas_segs': sorted(list(datos['fechas_segs']))
        }
        
        sucursales_resultado.append(sucursal_info)
        
        if cumple_reglas:
            cumple += 1
        else:
            viola += 1
    
    print(f"📊 RESUMEN FINAL:")
    print(f"   🏪 Sucursales con supervisiones: {len(sucursales_resultado)}")
    print(f"   ✅ Cumplen reglas: {cumple}")
    print(f"   ❌ Violan reglas: {viola}")
    
    # Contar por tipo
    locales = sum(1 for s in sucursales_resultado if s['tipo'] == 'LOCAL')
    foraneas = sum(1 for s in sucursales_resultado if s['tipo'] == 'FORANEA')
    
    print(f"   🏢 Locales (máx 4+4): {locales}")
    print(f"   🌍 Foráneas (máx 2+2): {foraneas}")
    
    # Totales
    total_ops = sum(s['operativas_count'] for s in sucursales_resultado)
    total_segs = sum(s['seguridad_count'] for s in sucursales_resultado)
    
    print(f"   📈 Total operativas: {total_ops}")
    print(f"   📈 Total seguridad: {total_segs}")
    print(f"   📈 Total supervisiones: {total_ops + total_segs}")
    
    return sorted(sucursales_resultado, key=lambda x: x['nombre'])

def main():
    """Función principal"""
    
    print("🎯 VALIDACIÓN FORMULARIOS ESPECÍFICOS 877138 y 877139")
    print("=" * 100)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 100)
    
    # 1. Cargar sucursales
    sucursales_normalizadas = cargar_sucursales_normalizadas()
    
    # 2. Contar submissions de formularios específicos
    conteo = contar_submissions_forms_especificas()
    
    # 3. Extraer submissions de formularios específicos
    submissions_extraidas = extraer_submissions_forms_especificas()
    
    if not submissions_extraidas:
        print("❌ No se obtuvieron submissions de 2025")
        return
    
    # 4. Procesar y mapear
    submissions_procesadas = procesar_y_mapear_submissions(submissions_extraidas, sucursales_normalizadas)
    
    # 5. Analizar coincidencias
    analisis_coincidencias = analizar_coincidencias_fechas(submissions_procesadas)
    
    # 6. Verificar reglas
    sucursales_finales = verificar_cumplimiento_reglas(analisis_coincidencias)
    
    # 7. Guardar resultados
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"validacion_forms_especificas_{timestamp}.json"
    
    resultado_final = {
        'timestamp': timestamp,
        'formularios_procesados': FORMULARIOS_OBJETIVO,
        'conteo_inicial': conteo,
        'total_submissions_2025': len(submissions_extraidas),
        'total_procesadas': len(submissions_procesadas),
        'total_mapeadas': len([s for s in submissions_procesadas if s['mapeo_exitoso']]),
        'coincidencias_perfectas': len(analisis_coincidencias['coincidencias_perfectas']),
        'sucursales_finales': sucursales_finales,
        'submissions_procesadas': submissions_procesadas,
        'analisis_coincidencias': analisis_coincidencias
    }
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(resultado_final, f, indent=2, default=str, ensure_ascii=False)
    
    print(f"\n💾 Resultados guardados en: {filename}")
    print(f"🎉 VALIDACIÓN DE FORMULARIOS ESPECÍFICOS COMPLETADA")
    
    return resultado_final

if __name__ == "__main__":
    main()