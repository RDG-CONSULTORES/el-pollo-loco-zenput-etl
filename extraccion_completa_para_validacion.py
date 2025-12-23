#!/usr/bin/env python3
"""
📊 EXTRACCIÓN COMPLETA PARA VALIDACIÓN
Extraer TODAS las submissions para revisión en Excel antes de base de datos
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

def extraer_todas_submissions_2025():
    """Extraer TODAS las submissions 2025 por lotes para evitar timeout"""
    
    print(f"\n🔄 EXTRAYENDO TODAS LAS SUBMISSIONS 2025")
    print("=" * 80)
    
    todas_submissions = []
    
    for form_id, tipo_form in FORMULARIOS_OBJETIVO.items():
        print(f"\n📋 Extrayendo {tipo_form} (Form {form_id})")
        print("-" * 50)
        
        page = 1
        form_submissions = []
        sin_datos_2025 = 0
        
        while True:
            try:
                if page % 10 == 0:
                    print(f"    📊 Página {page} - Total {tipo_form}: {len(form_submissions)}")
                else:
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
                        print("✅ Fin" if page % 10 != 0 else "")
                        break
                    
                    if page % 10 != 0:
                        print(f"{len(submissions)} submissions")
                    
                    # Filtrar por 2025 y procesar básico
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
                                else:
                                    sin_datos_2025 += 1
                            except:
                                sin_datos_2025 += 1
                        else:
                            sin_datos_2025 += 1
                    
                    form_submissions.extend(submissions_2025)
                    
                    if page % 10 != 0:
                        print(f"        └─ 2025: {len(submissions_2025)} submissions")\n                    \n                    page += 1\n                    time.sleep(0.05)  # Pausa muy pequeña\n                    \n                    # Límite de seguridad por timeout\n                    if page > 150:  # ~15,000 submissions máximo por form\n                        print(f\"    ⚠️ Límite de seguridad alcanzado en página {page}\")\n                        break\n                    \n                else:\n                    print(f\"❌ Error {response.status_code}\")\n                    break\n                    \n            except Exception as e:\n                print(f\"💥 Error: {e}\")\n                break\n        \n        print(f\"📊 Total {tipo_form} (2025): {len(form_submissions)} submissions\")\n        print(f\"📊 Descartadas (no 2025): {sin_datos_2025}\")\n        todas_submissions.extend(form_submissions)\n    \n    print(f\"\\n📊 TOTAL GENERAL 2025: {len(todas_submissions)} submissions\")\n    return todas_submissions\n\ndef procesar_y_clasificar_submissions(todas_submissions, sucursales_normalizadas):\n    \"\"\"Procesar y clasificar submissions según su estado de location\"\"\"\n    \n    print(f\"\\n🔄 PROCESANDO Y CLASIFICANDO {len(todas_submissions)} SUBMISSIONS\")\n    print(\"=\" * 80)\n    \n    # Clasificaciones\n    con_location_correcto = []\n    con_location_dudoso = []\n    sin_location = []\n    sin_coordenadas_entrega = []\n    \n    TOLERANCIA_LOCATION = 1.0  # 1km para considerar location correcto\n    TOLERANCIA_MAPEO = 2.0     # 2km para mapear sin location\n    \n    for i, submission in enumerate(todas_submissions):\n        if i % 50 == 0 and i > 0:\n            print(f\"    🔄 Procesadas: {i}/{len(todas_submissions)}\")\n        \n        # Extraer datos básicos\n        submission_id = submission.get('id')\n        form_type = submission.get('form_type')\n        smetadata = submission.get('smetadata', {})\n        \n        # Coordenadas de entrega REAL\n        lat_entrega = smetadata.get('lat')\n        lon_entrega = smetadata.get('lon')\n        \n        # Usuario y fechas\n        created_by = smetadata.get('created_by', {})\n        usuario_id = created_by.get('id')\n        usuario_nombre = created_by.get('display_name')\n        \n        fecha_submitted = smetadata.get('date_submitted')\n        fecha_completed = smetadata.get('date_completed')\n        fecha_created = smetadata.get('date_created')\n        \n        fecha = None\n        fecha_str = None\n        for campo, fecha_raw in [('submitted', fecha_submitted), ('completed', fecha_completed), ('created', fecha_created)]:\n            if fecha_raw:\n                try:\n                    fecha = datetime.fromisoformat(fecha_raw.replace('Z', '+00:00')).date()\n                    fecha_str = fecha.strftime('%Y-%m-%d')\n                    break\n                except:\n                    continue\n        \n        # Location asignada en Zenput\n        location_zenput = smetadata.get('location', {})\n        location_id = location_zenput.get('id')\n        location_name = location_zenput.get('name')\n        location_external_key = location_zenput.get('external_key')\n        lat_zenput = location_zenput.get('lat')\n        lon_zenput = location_zenput.get('lon')\n        \n        # Datos base para todas las clasificaciones\n        datos_base = {\n            'submission_id': submission_id,\n            'form_type': form_type,\n            'fecha': fecha_str,\n            'usuario_id': usuario_id,\n            'usuario_nombre': usuario_nombre,\n            'lat_entrega': lat_entrega,\n            'lon_entrega': lon_entrega,\n            'location_id': location_id,\n            'location_name': location_name,\n            'location_external_key': location_external_key,\n            'lat_zenput': lat_zenput,\n            'lon_zenput': lon_zenput,\n            'status_mapeo': '',\n            'sucursal_final': '',\n            'sucursal_numero': '',\n            'sucursal_grupo': '',\n            'sucursal_tipo': '',\n            'distancia_entrega_km': '',\n            'metodo_asignacion': '',\n            'notas': ''\n        }\n        \n        # Clasificar según estado de location y coordenadas\n        if not lat_entrega or not lon_entrega:\n            # SIN COORDENADAS DE ENTREGA\n            datos_base.update({\n                'status_mapeo': 'SIN_COORDENADAS_ENTREGA',\n                'metodo_asignacion': 'ZENPUT_LOCATION' if location_name else 'NO_ASIGNABLE',\n                'sucursal_final': location_name or 'NO_ASIGNABLE',\n                'notas': 'No tiene coordenadas de entrega real'\n            })\n            sin_coordenadas_entrega.append(datos_base)\n            \n        elif not location_id:\n            # SIN LOCATION ASIGNADA - MAPEAR CON COORDENADAS\n            mejor_sucursal, distancia_min = encontrar_sucursal_mas_cercana(lat_entrega, lon_entrega, sucursales_normalizadas)\n            \n            if mejor_sucursal and distancia_min <= TOLERANCIA_MAPEO:\n                datos_base.update({\n                    'status_mapeo': 'MAPEADA_POR_COORDENADAS',\n                    'sucursal_final': mejor_sucursal['nombre'],\n                    'sucursal_numero': mejor_sucursal['numero'],\n                    'sucursal_grupo': mejor_sucursal['grupo'],\n                    'sucursal_tipo': mejor_sucursal['tipo'],\n                    'distancia_entrega_km': round(distancia_min, 3),\n                    'metodo_asignacion': 'COORDENADAS_ENTREGA',\n                    'notas': f'Mapeada por coordenadas (sin location asignada)'\n                })\n                sin_location.append(datos_base)\n            else:\n                datos_base.update({\n                    'status_mapeo': 'NO_MAPEABLE',\n                    'distancia_entrega_km': round(distancia_min, 3) if distancia_min != float('inf') else 'N/A',\n                    'metodo_asignacion': 'NO_ASIGNABLE',\n                    'notas': f'Sin location y muy lejos de sucursales conocidas ({distancia_min:.2f}km)'\n                })\n                sin_location.append(datos_base)\n        \n        else:\n            # CON LOCATION ASIGNADA - VERIFICAR SI ES CORRECTA\n            if lat_zenput and lon_zenput:\n                distancia_location = calcular_distancia_km(lat_entrega, lon_entrega, lat_zenput, lon_zenput)\n                \n                if distancia_location <= TOLERANCIA_LOCATION:\n                    # LOCATION CORRECTO\n                    datos_base.update({\n                        'status_mapeo': 'LOCATION_CORRECTO',\n                        'sucursal_final': location_name,\n                        'distancia_entrega_km': round(distancia_location, 3),\n                        'metodo_asignacion': 'ZENPUT_LOCATION',\n                        'notas': 'Location asignada correcta (entrega cerca del location)'\n                    })\n                    con_location_correcto.append(datos_base)\n                else:\n                    # LOCATION DUDOSO - VERIFICAR CON COORDENADAS\n                    mejor_sucursal, distancia_min = encontrar_sucursal_mas_cercana(lat_entrega, lon_entrega, sucursales_normalizadas)\n                    \n                    datos_base.update({\n                        'status_mapeo': 'LOCATION_DUDOSO',\n                        'distancia_entrega_km': round(distancia_location, 3),\n                        'notas': f'Location asignado lejos ({distancia_location:.2f}km). '\n                    })\n                    \n                    if mejor_sucursal and distancia_min <= TOLERANCIA_MAPEO:\n                        # Usar mapeo por coordenadas\n                        datos_base.update({\n                            'sucursal_final': f'{location_name} → {mejor_sucursal[\"nombre\"]}',\n                            'sucursal_numero': mejor_sucursal['numero'],\n                            'sucursal_grupo': mejor_sucursal['grupo'],\n                            'sucursal_tipo': mejor_sucursal['tipo'],\n                            'metodo_asignacion': 'COORDENADAS_ENTREGA',\n                            'notas': datos_base['notas'] + f'Remapeada por coordenadas a {mejor_sucursal[\"nombre\"]} ({distancia_min:.2f}km)'\n                        })\n                    else:\n                        # Mantener location original\n                        datos_base.update({\n                            'sucursal_final': location_name,\n                            'metodo_asignacion': 'ZENPUT_LOCATION',\n                            'notas': datos_base['notas'] + 'Mantenida location original por falta de alternativa cercana'\n                        })\n                    \n                    con_location_dudoso.append(datos_base)\n            else:\n                # Location sin coordenadas\n                datos_base.update({\n                    'status_mapeo': 'LOCATION_SIN_COORDENADAS',\n                    'sucursal_final': location_name,\n                    'metodo_asignacion': 'ZENPUT_LOCATION',\n                    'notas': 'Location asignada sin coordenadas para verificar'\n                })\n                con_location_correcto.append(datos_base)\n    \n    # Estadísticas\n    print(f\"\\n📊 CLASIFICACIÓN COMPLETADA:\")\n    print(f\"   ✅ Con location correcto: {len(con_location_correcto)}\")\n    print(f\"   ⚠️ Con location dudoso: {len(con_location_dudoso)}\")\n    print(f\"   ❌ Sin location (mapeadas): {len(sin_location)}\")\n    print(f\"   💥 Sin coordenadas entrega: {len(sin_coordenadas_entrega)}\")\n    \n    return {\n        'con_location_correcto': con_location_correcto,\n        'con_location_dudoso': con_location_dudoso,\n        'sin_location': sin_location,\n        'sin_coordenadas_entrega': sin_coordenadas_entrega\n    }\n\ndef encontrar_sucursal_mas_cercana(lat_entrega, lon_entrega, sucursales_normalizadas):\n    \"\"\"Encontrar la sucursal más cercana a las coordenadas de entrega\"\"\"\n    \n    mejor_sucursal = None\n    distancia_minima = float('inf')\n    \n    for sucursal_key, datos_sucursal in sucursales_normalizadas.items():\n        distancia = calcular_distancia_km(\n            lat_entrega, lon_entrega,\n            datos_sucursal['lat'], datos_sucursal['lon']\n        )\n        \n        if distancia < distancia_minima:\n            distancia_minima = distancia\n            mejor_sucursal = datos_sucursal.copy()\n    \n    return mejor_sucursal, distancia_minima\n\ndef generar_excel_validacion(clasificacion_submissions, sucursales_normalizadas):\n    \"\"\"Generar Excel con todas las submissions para validación manual\"\"\"\n    \n    print(f\"\\n📊 GENERANDO EXCEL PARA VALIDACIÓN\")\n    print(\"=\" * 50)\n    \n    timestamp = datetime.now().strftime(\"%Y%m%d_%H%M%S\")\n    filename = f\"validacion_submissions_completa_{timestamp}.xlsx\"\n    \n    # Combinar todas las clasificaciones\n    todas_procesadas = (\n        clasificacion_submissions['con_location_correcto'] +\n        clasificacion_submissions['con_location_dudoso'] +\n        clasificacion_submissions['sin_location'] +\n        clasificacion_submissions['sin_coordenadas_entrega']\n    )\n    \n    # Crear DataFrame\n    df = pd.DataFrame(todas_procesadas)\n    \n    # Reordenar columnas para mejor visualización\n    columnas_ordenadas = [\n        'status_mapeo',\n        'submission_id',\n        'form_type',\n        'fecha',\n        'usuario_nombre',\n        'sucursal_final',\n        'sucursal_numero',\n        'sucursal_tipo',\n        'metodo_asignacion',\n        'distancia_entrega_km',\n        'location_name',\n        'lat_entrega',\n        'lon_entrega',\n        'lat_zenput',\n        'lon_zenput',\n        'notas'\n    ]\n    \n    df = df.reindex(columns=columnas_ordenadas)\n    \n    # Crear Excel con múltiples hojas\n    with pd.ExcelWriter(filename, engine='openpyxl') as writer:\n        \n        # Hoja 1: RESUMEN\n        resumen_data = {\n            'Categoría': [\n                'TOTAL SUBMISSIONS',\n                'Con Location Correcto',\n                'Con Location Dudoso', \n                'Sin Location (Mapeadas)',\n                'Sin Coordenadas Entrega',\n                '',\n                'OPERATIVAS',\n                'SEGURIDAD',\n                '',\n                'SUCURSALES ÚNICAS',\n                'FECHAS ÚNICAS'\n            ],\n            'Cantidad': [\n                len(todas_procesadas),\n                len(clasificacion_submissions['con_location_correcto']),\n                len(clasificacion_submissions['con_location_dudoso']),\n                len(clasificacion_submissions['sin_location']),\n                len(clasificacion_submissions['sin_coordenadas_entrega']),\n                '',\n                len([s for s in todas_procesadas if s['form_type'] == 'OPERATIVA']),\n                len([s for s in todas_procesadas if s['form_type'] == 'SEGURIDAD']),\n                '',\n                len(set([s['sucursal_final'] for s in todas_procesadas if s['sucursal_final']])),\n                len(set([s['fecha'] for s in todas_procesadas if s['fecha']]))\n            ]\n        }\n        \n        pd.DataFrame(resumen_data).to_excel(writer, sheet_name='RESUMEN', index=False)\n        \n        # Hoja 2: TODAS LAS SUBMISSIONS\n        df.to_excel(writer, sheet_name='TODAS_SUBMISSIONS', index=False)\n        \n        # Hoja 3: NECESITAN REVISIÓN (dudosas y sin location)\n        necesitan_revision = (\n            clasificacion_submissions['con_location_dudoso'] +\n            clasificacion_submissions['sin_location']\n        )\n        \n        if necesitan_revision:\n            df_revision = pd.DataFrame(necesitan_revision).reindex(columns=columnas_ordenadas)\n            df_revision.to_excel(writer, sheet_name='NECESITAN_REVISION', index=False)\n        \n        # Hoja 4: POR SUCURSAL Y FECHA\n        analizar_por_sucursal_fecha(todas_procesadas, writer)\n        \n        # Hoja 5: SUCURSALES MASTER\n        sucursales_df = pd.DataFrame([\n            {\n                'numero': info['numero'],\n                'nombre': info['nombre'],\n                'grupo': info['grupo'],\n                'tipo': info['tipo'],\n                'lat': info['lat'],\n                'lon': info['lon']\n            }\n            for info in sucursales_normalizadas.values()\n        ])\n        sucursales_df.to_excel(writer, sheet_name='SUCURSALES_MASTER', index=False)\n    \n    print(f\"📁 Excel generado: {filename}\")\n    \n    return filename, df\n\ndef analizar_por_sucursal_fecha(todas_procesadas, excel_writer):\n    \"\"\"Analizar submissions por sucursal y fecha\"\"\"\n    \n    # Agrupar por sucursal + fecha\n    por_sucursal_fecha = defaultdict(lambda: {'operativas': [], 'seguridad': []})\n    \n    for submission in todas_procesadas:\n        if submission['sucursal_final'] and submission['fecha']:\n            sucursal = submission['sucursal_final']\n            fecha = submission['fecha']\n            key = f\"{sucursal}_{fecha}\"\n            \n            if submission['form_type'] == 'OPERATIVA':\n                por_sucursal_fecha[key]['operativas'].append(submission)\n            elif submission['form_type'] == 'SEGURIDAD':\n                por_sucursal_fecha[key]['seguridad'].append(submission)\n    \n    # Generar análisis\n    analisis = []\n    \n    for key, datos in por_sucursal_fecha.items():\n        sucursal, fecha = key.rsplit('_', 1)\n        ops = datos['operativas']\n        segs = datos['seguridad']\n        \n        analisis.append({\n            'sucursal': sucursal,\n            'fecha': fecha,\n            'operativas_count': len(ops),\n            'seguridad_count': len(segs),\n            'coincidencia': 'SI' if ops and segs else 'NO',\n            'usuarios_ops': ', '.join(set([op['usuario_nombre'] for op in ops if op['usuario_nombre']])),\n            'usuarios_segs': ', '.join(set([seg['usuario_nombre'] for seg in segs if seg['usuario_nombre']])),\n            'submission_ids_ops': ', '.join([op['submission_id'] for op in ops]),\n            'submission_ids_segs': ', '.join([seg['submission_id'] for seg in segs])\n        })\n    \n    # Ordenar por sucursal y fecha\n    analisis.sort(key=lambda x: (x['sucursal'], x['fecha']))\n    \n    pd.DataFrame(analisis).to_excel(excel_writer, sheet_name='POR_SUCURSAL_FECHA', index=False)\n\ndef main():\n    \"\"\"Función principal\"\"\"\n    \n    print(\"📊 EXTRACCIÓN COMPLETA PARA VALIDACIÓN MANUAL\")\n    print(\"=\" * 100)\n    print(f\"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\")\n    print(\"=\" * 100)\n    \n    # 1. Cargar sucursales normalizadas\n    sucursales_normalizadas = cargar_sucursales_normalizadas()\n    \n    # 2. Extraer TODAS las submissions 2025\n    todas_submissions = extraer_todas_submissions_2025()\n    \n    if not todas_submissions:\n        print(\"❌ No se obtuvieron submissions de 2025\")\n        return\n    \n    # 3. Procesar y clasificar\n    clasificacion = procesar_y_clasificar_submissions(todas_submissions, sucursales_normalizadas)\n    \n    # 4. Generar Excel para validación\n    excel_filename, df_completo = generar_excel_validacion(clasificacion, sucursales_normalizadas)\n    \n    # 5. Guardar datos JSON para backup\n    json_filename = f\"backup_submissions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json\"\n    \n    resultado_completo = {\n        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),\n        'total_submissions_2025': len(todas_submissions),\n        'clasificacion_counts': {\n            'con_location_correcto': len(clasificacion['con_location_correcto']),\n            'con_location_dudoso': len(clasificacion['con_location_dudoso']),\n            'sin_location': len(clasificacion['sin_location']),\n            'sin_coordenadas_entrega': len(clasificacion['sin_coordenadas_entrega'])\n        },\n        'clasificacion_completa': clasificacion\n    }\n    \n    with open(json_filename, 'w', encoding='utf-8') as f:\n        json.dump(resultado_completo, f, indent=2, default=str, ensure_ascii=False)\n    \n    print(f\"\\n🎉 EXTRACCIÓN COMPLETADA\")\n    print(f\"📁 Excel para validación: {excel_filename}\")\n    print(f\"📁 Backup JSON: {json_filename}\")\n    print(f\"\\n📊 SIGUIENTE PASO:\")\n    print(f\"   1. Revisar Excel hoja 'NECESITAN_REVISION'\")\n    print(f\"   2. Validar mapeos de coordenadas\")\n    print(f\"   3. Confirmar sucursales y fechas\")\n    print(f\"   4. Dar VoBo para insertar en base de datos\")\n    \n    return excel_filename, df_completo\n\nif __name__ == \"__main__\":\n    main()