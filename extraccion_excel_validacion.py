#!/usr/bin/env python3
"""
📊 EXTRACCIÓN PARA EXCEL - VALIDACIÓN MANUAL
Extraer submissions para revisión en Excel antes de base de datos
"""

import requests
import csv
import math
import json
import pandas as pd
from datetime import datetime, date
from collections import defaultdict
import time

ZENPUT_CONFIG = {
    'base_url': 'https://www.zenput.com/api/v3',
    'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}
}

FORMULARIOS_OBJETIVO = {
    '877138': 'OPERATIVA',
    '877139': 'SEGURIDAD'
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
                    'tipo': 'LOCAL' if row['Grupo_Operativo'] in GRUPOS_LOCALES else 'FORANEA'
                }
    
    print(f"✅ {len(sucursales)} sucursales cargadas")
    return sucursales

def calcular_distancia_km(lat1, lon1, lat2, lon2):
    """Calcular distancia en km"""
    try:
        from math import radians, sin, cos, sqrt, atan2
        
        lat1, lon1, lat2, lon2 = map(radians, [float(lat1), float(lon1), float(lat2), float(lon2)])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1-a))
        R = 6371
        return R * c
    except:
        return float('inf')

def extraer_submissions_por_lotes():
    """Extraer submissions por lotes para evitar timeout"""
    
    print(f"🔄 EXTRAYENDO SUBMISSIONS POR LOTES")
    print("=" * 60)
    
    todas_submissions = []
    LOTE_SIZE = 50  # Páginas por lote
    
    for form_id, tipo_form in FORMULARIOS_OBJETIVO.items():
        print(f"\n📋 Extrayendo {tipo_form} (Form {form_id})")
        print("-" * 40)
        
        page = 1
        form_submissions = []
        
        while page <= LOTE_SIZE:  # Limitar a 50 páginas por form = ~5,000 submissions
            try:
                print(f"    📄 Página {page}...", end=" ", flush=True)
                
                url = f"{ZENPUT_CONFIG['base_url']}/submissions"
                params = {
                    'form_template_id': form_id,
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
                    
                    # Filtrar solo 2025
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
                    time.sleep(0.1)
                    
                else:
                    print(f"❌ Error {response.status_code}")
                    break
                    
            except Exception as e:
                print(f"💥 Error: {e}")
                break
        
        print(f"📊 Total {tipo_form}: {len(form_submissions)} submissions")
        todas_submissions.extend(form_submissions)
    
    print(f"\n📊 TOTAL GENERAL: {len(todas_submissions)} submissions")
    return todas_submissions

def encontrar_sucursal_mas_cercana(lat_entrega, lon_entrega, sucursales_normalizadas):
    """Encontrar sucursal más cercana"""
    
    mejor_sucursal = None
    distancia_minima = float('inf')
    
    for _, datos_sucursal in sucursales_normalizadas.items():
        distancia = calcular_distancia_km(
            lat_entrega, lon_entrega,
            datos_sucursal['lat'], datos_sucursal['lon']
        )
        
        if distancia < distancia_minima:
            distancia_minima = distancia
            mejor_sucursal = datos_sucursal.copy()
    
    return mejor_sucursal, distancia_minima

def procesar_submissions_para_excel(todas_submissions, sucursales_normalizadas):
    """Procesar submissions para Excel"""
    
    print(f"\n🔄 PROCESANDO {len(todas_submissions)} SUBMISSIONS PARA EXCEL")
    print("=" * 70)
    
    submissions_procesadas = []
    
    TOLERANCIA_LOCATION = 1.0  # 1km para location correcto
    TOLERANCIA_MAPEO = 2.0     # 2km para mapeo por coordenadas
    
    for i, submission in enumerate(todas_submissions):
        if i % 100 == 0 and i > 0:
            print(f"    🔄 Procesadas: {i}/{len(todas_submissions)}")
        
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
        
        # Fecha
        fecha_submitted = smetadata.get('date_submitted')
        fecha_str = None
        if fecha_submitted:
            try:
                fecha = datetime.fromisoformat(fecha_submitted.replace('Z', '+00:00')).date()
                fecha_str = fecha.strftime('%Y-%m-%d')
            except:
                pass
        
        # Location asignada en Zenput
        location_zenput = smetadata.get('location', {})
        location_id = location_zenput.get('id')
        location_name = location_zenput.get('name')
        lat_zenput = location_zenput.get('lat')
        lon_zenput = location_zenput.get('lon')
        
        # Datos base
        datos = {
            'submission_id': submission_id,
            'form_type': form_type,
            'fecha': fecha_str,
            'usuario_nombre': usuario_nombre,
            'lat_entrega': lat_entrega,
            'lon_entrega': lon_entrega,
            'location_id': location_id,
            'location_name': location_name,
            'lat_zenput': lat_zenput,
            'lon_zenput': lon_zenput,
            'status': '',
            'sucursal_final': '',
            'sucursal_numero': '',
            'sucursal_tipo': '',
            'distancia_km': '',
            'metodo': '',
            'requiere_revision': '',
            'notas': ''
        }
        
        # Clasificar
        if not lat_entrega or not lon_entrega:
            # SIN COORDENADAS DE ENTREGA
            datos.update({
                'status': 'SIN_COORDENADAS_ENTREGA',
                'sucursal_final': location_name or 'NO_ASIGNABLE',
                'metodo': 'ZENPUT_LOCATION' if location_name else 'NO_ASIGNABLE',
                'requiere_revision': 'NO',
                'notas': 'Sin coordenadas de entrega real'
            })
            
        elif not location_id:
            # SIN LOCATION ASIGNADA - MAPEAR
            mejor_sucursal, distancia_min = encontrar_sucursal_mas_cercana(lat_entrega, lon_entrega, sucursales_normalizadas)
            
            if mejor_sucursal and distancia_min <= TOLERANCIA_MAPEO:
                datos.update({
                    'status': 'MAPEADA_POR_COORDENADAS',
                    'sucursal_final': mejor_sucursal['nombre'],
                    'sucursal_numero': mejor_sucursal['numero'],
                    'sucursal_tipo': mejor_sucursal['tipo'],
                    'distancia_km': round(distancia_min, 3),
                    'metodo': 'COORDENADAS_ENTREGA',
                    'requiere_revision': 'SI',
                    'notas': f'Mapeada por coordenadas - REVISAR'
                })
            else:
                datos.update({
                    'status': 'NO_MAPEABLE',
                    'distancia_km': round(distancia_min, 3) if distancia_min != float('inf') else '',
                    'metodo': 'NO_ASIGNABLE',
                    'requiere_revision': 'SI',
                    'notas': f'Sin location y muy lejos de sucursales ({distancia_min:.2f}km)'
                })
        
        else:
            # CON LOCATION ASIGNADA - VERIFICAR
            if lat_zenput and lon_zenput:
                distancia_location = calcular_distancia_km(lat_entrega, lon_entrega, lat_zenput, lon_zenput)
                
                if distancia_location <= TOLERANCIA_LOCATION:
                    # LOCATION CORRECTO
                    datos.update({
                        'status': 'LOCATION_CORRECTO',
                        'sucursal_final': location_name,
                        'distancia_km': round(distancia_location, 3),
                        'metodo': 'ZENPUT_LOCATION',
                        'requiere_revision': 'NO',
                        'notas': 'Location correcto'
                    })
                else:
                    # LOCATION DUDOSO
                    mejor_sucursal, distancia_min = encontrar_sucursal_mas_cercana(lat_entrega, lon_entrega, sucursales_normalizadas)
                    
                    datos.update({
                        'status': 'LOCATION_DUDOSO',
                        'distancia_km': round(distancia_location, 3),
                        'metodo': 'ZENPUT_LOCATION',
                        'requiere_revision': 'SI',
                        'notas': f'Location lejos ({distancia_location:.2f}km)'
                    })
                    
                    if mejor_sucursal and distancia_min <= TOLERANCIA_MAPEO:
                        datos['notas'] += f' - Sugerencia: {mejor_sucursal["nombre"]} ({distancia_min:.2f}km)'
                        datos['sucursal_final'] = location_name
                    else:
                        datos['sucursal_final'] = location_name
            else:
                # Location sin coordenadas
                datos.update({
                    'status': 'LOCATION_SIN_COORDENADAS',
                    'sucursal_final': location_name,
                    'metodo': 'ZENPUT_LOCATION',
                    'requiere_revision': 'NO',
                    'notas': 'Location sin coordenadas para verificar'
                })
        
        submissions_procesadas.append(datos)
    
    print(f"✅ Procesamiento completado")
    return submissions_procesadas

def generar_excel_completo(submissions_procesadas, sucursales_normalizadas):
    """Generar archivos CSV para validación (alternativa a Excel)"""
    
    print(f"\n📊 GENERANDO CSVs PARA VALIDACIÓN")
    print("=" * 50)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Crear DataFrame principal
    df = pd.DataFrame(submissions_procesadas)
    
    # Reordenar columnas
    columnas_excel = [
        'requiere_revision',
        'status',
        'submission_id',
        'form_type',
        'fecha',
        'usuario_nombre',
        'sucursal_final',
        'sucursal_numero',
        'sucursal_tipo',
        'metodo',
        'distancia_km',
        'location_name',
        'lat_entrega',
        'lon_entrega',
        'notas'
    ]
    
    df = df.reindex(columns=columnas_excel)
    
    # 1. RESUMEN
    estadisticas = {
        'Categoria': [
            'TOTAL SUBMISSIONS',
            'Location Correcto',
            'Location Dudoso',
            'Sin Location (Mapeadas)',
            'Sin Coordenadas',
            '',
            'REQUIEREN REVISION',
            'NO REQUIEREN REVISION',
            '',
            'OPERATIVAS',
            'SEGURIDAD'
        ],
        'Cantidad': [
            len(submissions_procesadas),
            len([s for s in submissions_procesadas if s['status'] == 'LOCATION_CORRECTO']),
            len([s for s in submissions_procesadas if s['status'] == 'LOCATION_DUDOSO']),
            len([s for s in submissions_procesadas if s['status'] == 'MAPEADA_POR_COORDENADAS']),
            len([s for s in submissions_procesadas if s['status'] == 'SIN_COORDENADAS_ENTREGA']),
            '',
            len([s for s in submissions_procesadas if s['requiere_revision'] == 'SI']),
            len([s for s in submissions_procesadas if s['requiere_revision'] == 'NO']),
            '',
            len([s for s in submissions_procesadas if s['form_type'] == 'OPERATIVA']),
            len([s for s in submissions_procesadas if s['form_type'] == 'SEGURIDAD'])
        ]
    }
    
    resumen_filename = f"RESUMEN_{timestamp}.csv"
    pd.DataFrame(estadisticas).to_csv(resumen_filename, index=False, encoding='utf-8')
    
    # 2. TODAS LAS SUBMISSIONS
    todas_filename = f"TODAS_SUBMISSIONS_{timestamp}.csv"
    df.to_csv(todas_filename, index=False, encoding='utf-8')
    
    # 3. REQUIEREN REVISIÓN
    revision_df = df[df['requiere_revision'] == 'SI'].copy()
    if not revision_df.empty:
        revision_filename = f"REQUIEREN_REVISION_{timestamp}.csv"
        revision_df.to_csv(revision_filename, index=False, encoding='utf-8')
        print(f"📁 Requieren revisión: {revision_filename}")
    
    # 4. POR SUCURSAL Y FECHA
    analisis_sucursal_fecha = generar_analisis_sucursal_fecha_csv(submissions_procesadas)
    analisis_filename = f"ANALISIS_SUCURSAL_FECHA_{timestamp}.csv"
    pd.DataFrame(analisis_sucursal_fecha).to_csv(analisis_filename, index=False, encoding='utf-8')
    
    # 5. SUCURSALES MASTER
    sucursales_df = pd.DataFrame([
        {
            'numero': info['numero'],
            'nombre': info['nombre'],
            'grupo': info['grupo'],
            'tipo': info['tipo'],
            'lat': info['lat'],
            'lon': info['lon']
        }
        for info in sucursales_normalizadas.values()
    ])
    sucursales_filename = f"SUCURSALES_MASTER_{timestamp}.csv"
    sucursales_df.to_csv(sucursales_filename, index=False, encoding='utf-8')
    
    print(f"📁 Archivos CSV generados:")
    print(f"   📊 Resumen: {resumen_filename}")
    print(f"   📄 Todas: {todas_filename}")
    print(f"   📈 Análisis: {analisis_filename}")
    print(f"   🏪 Sucursales: {sucursales_filename}")
    
    return todas_filename

def generar_analisis_sucursal_fecha_csv(submissions_procesadas):
    """Generar análisis por sucursal y fecha (versión CSV)"""
    
    # Agrupar por sucursal + fecha
    por_sucursal_fecha = defaultdict(lambda: {'operativas': [], 'seguridad': []})
    
    for submission in submissions_procesadas:
        if submission['sucursal_final'] and submission['fecha']:
            sucursal = submission['sucursal_final']
            fecha = submission['fecha']
            key = f"{sucursal}_{fecha}"
            
            if submission['form_type'] == 'OPERATIVA':
                por_sucursal_fecha[key]['operativas'].append(submission)
            elif submission['form_type'] == 'SEGURIDAD':
                por_sucursal_fecha[key]['seguridad'].append(submission)
    
    # Generar análisis
    analisis = []
    
    for key, datos in por_sucursal_fecha.items():
        sucursal, fecha = key.rsplit('_', 1)
        ops = datos['operativas']
        segs = datos['seguridad']
        
        analisis.append({
            'sucursal': sucursal,
            'fecha': fecha,
            'operativas_count': len(ops),
            'seguridad_count': len(segs),
            'coincidencia_mismo_dia': 'SI' if ops and segs else 'NO',
            'usuarios_ops': ', '.join(set([op['usuario_nombre'] for op in ops if op['usuario_nombre']])),
            'usuarios_segs': ', '.join(set([seg['usuario_nombre'] for seg in segs if seg['usuario_nombre']])),
            'requiere_revision_ops': 'SI' if any(op['requiere_revision'] == 'SI' for op in ops) else 'NO',
            'requiere_revision_segs': 'SI' if any(seg['requiere_revision'] == 'SI' for seg in segs) else 'NO'
        })
    
    # Ordenar
    analisis.sort(key=lambda x: (x['sucursal'], x['fecha']))
    
    return analisis

def generar_analisis_sucursal_fecha(submissions_procesadas, excel_writer):
    """Generar análisis por sucursal y fecha (versión Excel legacy)"""
    analisis = generar_analisis_sucursal_fecha_csv(submissions_procesadas)
    pd.DataFrame(analisis).to_excel(excel_writer, sheet_name='ANALISIS_SUCURSAL_FECHA', index=False)

def main():
    """Función principal"""
    
    print("📊 EXTRACCIÓN PARA VALIDACIÓN EN EXCEL")
    print("=" * 80)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # 1. Cargar sucursales
    sucursales_normalizadas = cargar_sucursales_normalizadas()
    
    # 2. Extraer submissions por lotes
    todas_submissions = extraer_submissions_por_lotes()
    
    if not todas_submissions:
        print("❌ No se obtuvieron submissions")
        return
    
    # 3. Procesar para Excel
    submissions_procesadas = procesar_submissions_para_excel(todas_submissions, sucursales_normalizadas)
    
    # 4. Generar Excel
    excel_filename = generar_excel_completo(submissions_procesadas, sucursales_normalizadas)
    
    # Estadísticas finales
    requieren_revision = len([s for s in submissions_procesadas if s['requiere_revision'] == 'SI'])
    total = len(submissions_procesadas)
    
    print(f"\n🎉 EXTRACCIÓN COMPLETADA")
    print(f"📁 Archivo Excel: {excel_filename}")
    print(f"📊 Total submissions: {total}")
    print(f"⚠️ Requieren revisión: {requieren_revision}")
    print(f"✅ No requieren revisión: {total - requieren_revision}")
    
    print(f"\n📋 PRÓXIMOS PASOS:")
    print(f"   1. Abrir Excel: {excel_filename}")
    print(f"   2. Revisar hoja 'REQUIEREN_REVISION'")
    print(f"   3. Validar mapeos de coordenadas")
    print(f"   4. Confirmar sucursales y fechas")
    print(f"   5. Dar VoBo para insertar en base de datos")
    
    return excel_filename

if __name__ == "__main__":
    main()