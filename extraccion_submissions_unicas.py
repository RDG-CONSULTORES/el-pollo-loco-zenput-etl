#!/usr/bin/env python3
"""
🎯 EXTRACCIÓN DE SUBMISSIONS ÚNICAS
Obtener exactamente las 238 operativas + 238 seguridad únicas de 2025
"""

import requests
import csv
import json
import pandas as pd
from datetime import datetime
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

def cargar_sucursales_master():
    """Cargar sucursales master"""
    sucursales = {}
    
    with open('SUCURSALES_MASTER_20251218_110913.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            numero = int(row['numero'])
            nombre = row['nombre']
            
            sucursales[nombre] = row
            sucursales[numero] = row
            sucursales[f"{numero} - {nombre}"] = row
    
    return sucursales

def extraer_submissions_unicas():
    """Extraer TODAS las submissions únicas de 2025"""
    
    print("🔄 EXTRAYENDO TODAS LAS SUBMISSIONS ÚNICAS DE 2025")
    print("=" * 80)
    
    submissions_unicas = {}  # key = submission_id, value = submission data
    
    for form_id, tipo_form in FORMULARIOS_OBJETIVO.items():
        print(f"\n📋 Extrayendo {tipo_form} (Form {form_id})")
        print("-" * 50)
        
        page = 1
        submissions_vistas = set()
        sin_nuevas_consecutivas = 0
        
        while True:
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
                        print("✅ Fin (sin datos)")
                        break
                    
                    nuevas_en_esta_pagina = 0
                    submissions_2025_pagina = []
                    
                    for submission in submissions:
                        submission_id = submission.get('id')
                        
                        # Verificar si ya la hemos visto
                        if submission_id in submissions_vistas:
                            continue
                        
                        submissions_vistas.add(submission_id)
                        
                        # Verificar fecha 2025
                        smetadata = submission.get('smetadata', {})
                        fecha_submitted = smetadata.get('date_submitted')
                        
                        if fecha_submitted:
                            try:
                                fecha_dt = datetime.fromisoformat(fecha_submitted.replace('Z', '+00:00'))
                                if fecha_dt.year == 2025:
                                    submission['form_type'] = tipo_form
                                    submissions_unicas[submission_id] = submission
                                    submissions_2025_pagina.append(submission)
                                    nuevas_en_esta_pagina += 1
                            except:
                                continue
                    
                    print(f"{len(submissions)} total, {nuevas_en_esta_pagina} nuevas 2025")
                    
                    # Control de parada: si no encontramos nuevas submissions en varias páginas consecutivas
                    if nuevas_en_esta_pagina == 0:
                        sin_nuevas_consecutivas += 1
                        if sin_nuevas_consecutivas >= 5:  # 5 páginas sin nuevas = probable fin
                            print(f"    ⚠️ 5 páginas consecutivas sin nuevas submissions - terminando")
                            break
                    else:
                        sin_nuevas_consecutivas = 0
                    
                    page += 1
                    time.sleep(0.1)
                    
                    # Límite de seguridad para evitar bucles infinitos
                    if page > 200:
                        print(f"    ⚠️ Límite de seguridad alcanzado (200 páginas)")
                        break
                        
                else:
                    print(f"❌ Error {response.status_code}")
                    break
                    
            except Exception as e:
                print(f"💥 Error: {e}")
                break
        
        form_count = len([s for s in submissions_unicas.values() if s.get('form_type') == tipo_form])
        print(f"📊 Total {tipo_form} únicas: {form_count}")
    
    todas_submissions = list(submissions_unicas.values())
    print(f"\n📊 TOTAL SUBMISSIONS ÚNICAS 2025: {len(todas_submissions)}")
    
    # Breakdown por formulario
    operativas = [s for s in todas_submissions if s.get('form_type') == 'OPERATIVA']
    seguridad = [s for s in todas_submissions if s.get('form_type') == 'SEGURIDAD']
    
    print(f"   📋 Operativas únicas: {len(operativas)}")
    print(f"   📋 Seguridad únicas: {len(seguridad)}")
    
    return todas_submissions

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

def encontrar_sucursal_mas_cercana(lat_entrega, lon_entrega, sucursales_master):
    """Encontrar sucursal más cercana"""
    mejor_sucursal = None
    distancia_minima = float('inf')
    
    for key, datos_sucursal in sucursales_master.items():
        if isinstance(key, str) and 'lat' in datos_sucursal:  # Solo procesar entradas completas
            try:
                distancia = calcular_distancia_km(
                    lat_entrega, lon_entrega,
                    float(datos_sucursal['lat']), float(datos_sucursal['lon'])
                )
                
                if distancia < distancia_minima:
                    distancia_minima = distancia
                    mejor_sucursal = datos_sucursal.copy()
            except:
                continue
    
    return mejor_sucursal, distancia_minima

def mapear_location_inteligente(location_name, sucursales_master):
    """Mapeo inteligente de location usando normalización"""
    
    if not location_name:
        return None, "SIN_LOCATION", 0.0
    
    # 1. Match exacto directo
    if location_name in sucursales_master:
        return sucursales_master[location_name], "EXACTO_DIRECTO", 1.0
    
    # 2. Match por número-nombre (ej: "53 - Lienzo Charro")
    import re
    match = re.match(r'^(\d+)\s*-\s*(.+)$', location_name.strip())
    if match:
        numero = int(match.group(1))
        nombre = match.group(2).strip()
        
        # Buscar por número-nombre completo
        clave_completa = f"{numero} - {nombre}"
        if clave_completa in sucursales_master:
            return sucursales_master[clave_completa], "EXACTO_NUMERO_NOMBRE", 1.0
        
        # Buscar solo por nombre
        if nombre in sucursales_master:
            return sucursales_master[nombre], "EXACTO_NOMBRE", 1.0
        
        # Buscar solo por número
        if numero in sucursales_master:
            return sucursales_master[numero], "EXACTO_NUMERO", 1.0
    
    return None, "NO_ENCONTRADO", 0.0

def procesar_submissions_unicas(submissions_unicas, sucursales_master):
    """Procesar submissions únicas para asignación final"""
    
    print(f"\n🎯 PROCESANDO {len(submissions_unicas)} SUBMISSIONS ÚNICAS")
    print("=" * 70)
    
    submissions_procesadas = []
    
    for i, submission in enumerate(submissions_unicas):
        if i % 25 == 0:
            print(f"   🔄 Procesadas: {i}/{len(submissions_unicas)}")
        
        # Datos básicos
        submission_id = submission.get('id')
        form_type = submission.get('form_type')
        smetadata = submission.get('smetadata', {})
        
        # Coordenadas de entrega
        lat_entrega = smetadata.get('lat')
        lon_entrega = smetadata.get('lon')
        
        # Usuario y fecha
        created_by = smetadata.get('created_by', {})
        usuario_nombre = created_by.get('display_name')
        
        fecha_submitted = smetadata.get('date_submitted')
        fecha_str = None
        if fecha_submitted:
            try:
                fecha = datetime.fromisoformat(fecha_submitted.replace('Z', '+00:00')).date()
                fecha_str = fecha.strftime('%Y-%m-%d')
            except:
                pass
        
        # Location de Zenput
        location_zenput = smetadata.get('location', {})
        location_name = location_zenput.get('name')
        
        # Estrategia de asignación
        sucursal_final = None
        metodo = "NO_ASIGNADA"
        confianza = 0.0
        distancia_km = None
        notas = ""
        
        # 1. Intentar mapeo por location_name
        if location_name:
            sucursal_location, metodo_location, confianza_location = mapear_location_inteligente(location_name, sucursales_master)
            if sucursal_location and confianza_location >= 0.8:
                sucursal_final = sucursal_location
                metodo = f"LOCATION_{metodo_location}"
                confianza = confianza_location
                notas = f"Asignada por location_name: {location_name}"
        
        # 2. Si no hay location o no se pudo mapear, usar coordenadas
        if not sucursal_final and lat_entrega and lon_entrega:
            sucursal_coordenadas, distancia_min = encontrar_sucursal_mas_cercana(lat_entrega, lon_entrega, sucursales_master)
            if sucursal_coordenadas and distancia_min <= 2.0:  # Máximo 2km
                sucursal_final = sucursal_coordenadas
                metodo = "COORDENADAS_ENTREGA"
                confianza = max(0.5, 1.0 - (distancia_min / 2.0))  # Confianza basada en distancia
                distancia_km = distancia_min
                notas = f"Asignada por coordenadas de entrega (distancia: {distancia_min:.2f}km)"
        
        # 3. Preparar datos finales
        if sucursal_final:
            datos_submission = {
                'submission_id': submission_id,
                'form_type': form_type,
                'fecha': fecha_str,
                'usuario_nombre': usuario_nombre,
                'sucursal_numero': sucursal_final.get('numero'),
                'sucursal_nombre': sucursal_final.get('nombre'),
                'sucursal_grupo': sucursal_final.get('grupo'),
                'sucursal_tipo': sucursal_final.get('tipo'),
                'metodo_asignacion': metodo,
                'confianza': round(confianza, 3),
                'distancia_km': round(distancia_km, 3) if distancia_km else None,
                'location_zenput': location_name,
                'lat_entrega': lat_entrega,
                'lon_entrega': lon_entrega,
                'notas': notas,
                'estado': 'ASIGNADA'
            }
        else:
            # No se pudo asignar
            datos_submission = {
                'submission_id': submission_id,
                'form_type': form_type,
                'fecha': fecha_str,
                'usuario_nombre': usuario_nombre,
                'sucursal_numero': None,
                'sucursal_nombre': None,
                'sucursal_grupo': None,
                'sucursal_tipo': None,
                'metodo_asignacion': 'NO_ASIGNABLE',
                'confianza': 0.0,
                'distancia_km': None,
                'location_zenput': location_name,
                'lat_entrega': lat_entrega,
                'lon_entrega': lon_entrega,
                'notas': 'No se pudo asignar a ninguna sucursal conocida',
                'estado': 'SIN_ASIGNAR'
            }
        
        submissions_procesadas.append(datos_submission)
    
    print(f"✅ Procesamiento completado")
    
    # Estadísticas
    asignadas = [s for s in submissions_procesadas if s['estado'] == 'ASIGNADA']
    sin_asignar = [s for s in submissions_procesadas if s['estado'] == 'SIN_ASIGNAR']
    
    print(f"\n📊 RESULTADOS FINALES:")
    print(f"   ✅ Asignadas: {len(asignadas)}")
    print(f"   ❌ Sin asignar: {len(sin_asignar)}")
    print(f"   📈 Tasa de éxito: {len(asignadas)/len(submissions_procesadas)*100:.1f}%")
    
    # Breakdown por formulario
    ops_asignadas = len([s for s in asignadas if s['form_type'] == 'OPERATIVA'])
    seg_asignadas = len([s for s in asignadas if s['form_type'] == 'SEGURIDAD'])
    
    print(f"\n📋 BREAKDOWN POR FORMULARIO:")
    print(f"   📊 Operativas asignadas: {ops_asignadas}")
    print(f"   📊 Seguridad asignadas: {seg_asignadas}")
    
    return submissions_procesadas

def analizar_coincidencias_finales(submissions_procesadas):
    """Analizar coincidencias finales por sucursal y fecha"""
    
    print(f"\n📅 ANÁLISIS DE COINCIDENCIAS FINALES")
    print("=" * 50)
    
    # Solo submissions asignadas
    asignadas = [s for s in submissions_procesadas if s['estado'] == 'ASIGNADA' and s['fecha']]
    
    # Agrupar por sucursal + fecha
    por_sucursal_fecha = defaultdict(lambda: {'operativas': [], 'seguridad': []})
    
    for submission in asignadas:
        sucursal = submission['sucursal_nombre']
        fecha = submission['fecha']
        key = f"{sucursal}_{fecha}"
        
        if submission['form_type'] == 'OPERATIVA':
            por_sucursal_fecha[key]['operativas'].append(submission)
        elif submission['form_type'] == 'SEGURIDAD':
            por_sucursal_fecha[key]['seguridad'].append(submission)
    
    # Análisis
    coincidencias_perfectas = []
    operativas_solas = []
    seguridad_solas = []
    
    for key, datos in por_sucursal_fecha.items():
        sucursal, fecha = key.rsplit('_', 1)
        ops = datos['operativas']
        segs = datos['seguridad']
        
        if ops and segs:
            coincidencias_perfectas.append({
                'sucursal': sucursal,
                'fecha': fecha,
                'operativas_count': len(ops),
                'seguridad_count': len(segs)
            })
        elif ops:
            operativas_solas.extend(ops)
        elif segs:
            seguridad_solas.extend(segs)
    
    print(f"📊 COINCIDENCIAS:")
    print(f"   ✅ Días con ambas: {len(coincidencias_perfectas)}")
    print(f"   ⚠️ Solo operativas: {len(operativas_solas)}")
    print(f"   ⚠️ Solo seguridad: {len(seguridad_solas)}")
    
    return {
        'coincidencias_perfectas': coincidencias_perfectas,
        'operativas_solas': operativas_solas,
        'seguridad_solas': seguridad_solas
    }

def main():
    """Función principal"""
    
    print("🎯 EXTRACCIÓN DE SUBMISSIONS ÚNICAS PARA ASIGNACIÓN FINAL")
    print("=" * 100)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 100)
    
    # 1. Cargar sucursales master
    sucursales_master = cargar_sucursales_master()
    print(f"📂 Sucursales master cargadas: {len([k for k in sucursales_master.keys() if isinstance(k, str)])}")
    
    # 2. Extraer submissions únicas
    submissions_unicas = extraer_submissions_unicas()
    
    if len(submissions_unicas) == 0:
        print("❌ No se obtuvieron submissions únicas")
        return
    
    # 3. Procesar para asignación final
    submissions_procesadas = procesar_submissions_unicas(submissions_unicas, sucursales_master)
    
    # 4. Analizar coincidencias
    analisis_coincidencias = analizar_coincidencias_finales(submissions_procesadas)
    
    # 5. Guardar resultados
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Dataset final
    df_final = pd.DataFrame(submissions_procesadas)
    filename_final = f"SUBMISSIONS_UNICAS_ASIGNADAS_{timestamp}.csv"
    df_final.to_csv(filename_final, index=False, encoding='utf-8')
    
    # Solo las asignadas (para inserción en BD)
    df_asignadas = df_final[df_final['estado'] == 'ASIGNADA']
    filename_asignadas = f"SUBMISSIONS_PARA_BD_{timestamp}.csv"
    df_asignadas.to_csv(filename_asignadas, index=False, encoding='utf-8')
    
    # Análisis de coincidencias
    if analisis_coincidencias['coincidencias_perfectas']:
        df_coincidencias = pd.DataFrame(analisis_coincidencias['coincidencias_perfectas'])
        filename_coincidencias = f"COINCIDENCIAS_FINALES_{timestamp}.csv"
        df_coincidencias.to_csv(filename_coincidencias, index=False, encoding='utf-8')
    
    print(f"\n📁 ARCHIVOS GENERADOS:")
    print(f"   📄 Dataset completo: {filename_final}")
    print(f"   ✅ Para BD: {filename_asignadas}")
    if analisis_coincidencias['coincidencias_perfectas']:
        print(f"   📈 Coincidencias: {filename_coincidencias}")
    
    # Resumen final
    asignadas = len([s for s in submissions_procesadas if s['estado'] == 'ASIGNADA'])
    operativas = len([s for s in submissions_procesadas if s['form_type'] == 'OPERATIVA' and s['estado'] == 'ASIGNADA'])
    seguridad = len([s for s in submissions_procesadas if s['form_type'] == 'SEGURIDAD' and s['estado'] == 'ASIGNADA'])
    
    print(f"\n🎉 EXTRACCIÓN COMPLETADA")
    print(f"📊 RESUMEN FINAL:")
    print(f"   📋 Total submissions únicas procesadas: {len(submissions_procesadas)}")
    print(f"   ✅ Asignadas exitosamente: {asignadas}")
    print(f"   📊 Operativas: {operativas}")
    print(f"   📊 Seguridad: {seguridad}")
    print(f"   📈 Tasa de éxito: {asignadas/len(submissions_procesadas)*100:.1f}%")
    
    return submissions_procesadas

if __name__ == "__main__":
    main()