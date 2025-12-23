#!/usr/bin/env python3
"""
🔄 FASE 1: EXTRACCIÓN COMPLETA SIN FILTROS
Extraer TODAS las submissions de Operativas (877138) y Seguridad (877139)
Período: 12 Marzo - 31 Diciembre 2025
"""

import requests
import json
import pandas as pd
from datetime import datetime
import time

ZENPUT_CONFIG = {
    'base_url': 'https://www.zenput.com/api/v3',
    'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}
}

FORMULARIOS_OBJETIVO = {
    '877138': 'OPERATIVA',
    '877139': 'SEGURIDAD'
}

def extraer_todas_submissions_2025():
    """Extraer TODAS las submissions únicas de 2025 sin límites"""
    
    print("🔄 FASE 1: EXTRACCIÓN COMPLETA DE SUBMISSIONS 2025")
    print("=" * 80)
    print("📅 Período: 12 Marzo - 31 Diciembre 2025")
    print("📋 Formularios: 877138 (OPERATIVA) y 877139 (SEGURIDAD)")
    print("🎯 Objetivo: Encontrar exactamente 238+238=476 submissions")
    print("=" * 80)
    
    submissions_unicas = {}  # key = submission_id, value = submission data
    total_paginas_procesadas = 0
    
    for form_id, tipo_form in FORMULARIOS_OBJETIVO.items():
        print(f"\n📋 EXTRAYENDO {tipo_form} (Form {form_id})")
        print("-" * 60)
        
        page = 1
        submissions_vistas = set()
        paginas_sin_datos = 0
        submissions_form = []
        
        while True:
            try:
                if page % 10 == 0:
                    print(f"    📊 Página {page} | Únicas hasta ahora: {len(submissions_form)}")
                else:
                    print(f"    📄 Página {page}...", end=" ", flush=True)
                
                url = f"{ZENPUT_CONFIG['base_url']}/submissions"
                params = {
                    'form_template_id': form_id,
                    'page': page,
                    'page_size': 100
                }
                
                response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=30)
                total_paginas_procesadas += 1
                
                if response.status_code == 200:
                    data = response.json()
                    submissions = data.get('data', [])
                    
                    if not submissions:
                        if page % 10 != 0:
                            print("❌ Sin datos")
                        paginas_sin_datos += 1
                        if paginas_sin_datos >= 3:
                            print(f"    ✅ Fin confirmado después de {paginas_sin_datos} páginas vacías")
                            break
                        page += 1
                        continue
                    
                    paginas_sin_datos = 0  # Reset contador
                    
                    if page % 10 != 0:
                        print(f"✅ {len(submissions)} submissions")
                    
                    nuevas_2025 = 0
                    for submission in submissions:
                        submission_id = submission.get('id')
                        
                        # Saltar duplicados
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
                                    # Verificar que sea después del 12 de marzo
                                    if fecha_dt.month >= 3 and (fecha_dt.month > 3 or fecha_dt.day >= 12):
                                        submission['form_type'] = tipo_form
                                        submission['fecha_dt'] = fecha_dt
                                        submissions_unicas[submission_id] = submission
                                        submissions_form.append(submission)
                                        nuevas_2025 += 1
                            except Exception as e:
                                print(f"      ⚠️ Error parseando fecha: {e}")
                                continue
                    
                    if page % 10 != 0 and nuevas_2025 > 0:
                        print(f"      └─ 2025 nuevas: {nuevas_2025}")
                    
                    page += 1
                    time.sleep(0.05)  # Pausa muy pequeña
                    
                else:
                    print(f"❌ Error HTTP {response.status_code}: {response.text[:100]}")
                    break
                    
            except Exception as e:
                print(f"💥 Error en página {page}: {e}")
                break
        
        print(f"\n📊 RESUMEN {tipo_form}:")
        print(f"   📄 Páginas procesadas: {page-1}")
        print(f"   🔍 Submissions vistas (todas): {len(submissions_vistas)}")
        print(f"   ✅ Submissions 2025 válidas: {len(submissions_form)}")
        
        # Mostrar rango de fechas
        if submissions_form:
            fechas = [s['fecha_dt'] for s in submissions_form]
            fecha_min = min(fechas)
            fecha_max = max(fechas)
            print(f"   📅 Rango fechas: {fecha_min.strftime('%Y-%m-%d')} a {fecha_max.strftime('%Y-%m-%d')}")
    
    todas_submissions = list(submissions_unicas.values())
    
    print(f"\n" + "=" * 80)
    print(f"🎉 FASE 1 COMPLETADA")
    print(f"=" * 80)
    print(f"📊 RESULTADOS FINALES:")
    print(f"   📄 Total páginas API procesadas: {total_paginas_procesadas}")
    print(f"   🔍 Submissions únicas encontradas: {len(todas_submissions)}")
    
    # Breakdown por formulario
    operativas = [s for s in todas_submissions if s.get('form_type') == 'OPERATIVA']
    seguridad = [s for s in todas_submissions if s.get('form_type') == 'SEGURIDAD']
    
    print(f"   📋 Operativas: {len(operativas)}")
    print(f"   📋 Seguridad: {len(seguridad)}")
    print(f"   📈 Total: {len(operativas)} + {len(seguridad)} = {len(todas_submissions)}")
    
    # Verificar objetivo
    objetivo_total = 476
    objetivo_ops = 238
    objetivo_segs = 238
    
    print(f"\n🎯 VERIFICACIÓN vs OBJETIVO:")
    print(f"   🎯 Objetivo: {objetivo_ops} ops + {objetivo_segs} seg = {objetivo_total} total")
    print(f"   ✅ Encontrado: {len(operativas)} ops + {len(seguridad)} seg = {len(todas_submissions)} total")
    
    if len(todas_submissions) == objetivo_total:
        print(f"   🎉 ¡PERFECTO! Encontramos exactamente las {objetivo_total} submissions esperadas")
    elif len(todas_submissions) < objetivo_total:
        print(f"   ⚠️ FALTANTES: {objetivo_total - len(todas_submissions)} submissions")
        print(f"      Posibles causas: datos en otros períodos, formularios adicionales")
    else:
        print(f"   ℹ️ EXTRAS: {len(todas_submissions) - objetivo_total} submissions adicionales")
        print(f"      Esto es normal, pueden ser submissions de prueba o duplicados")
    
    # Estadísticas de fechas
    if todas_submissions:
        fechas_todas = [s['fecha_dt'] for s in todas_submissions]
        fecha_min = min(fechas_todas)
        fecha_max = max(fechas_todas)
        
        print(f"\n📅 ANÁLISIS TEMPORAL:")
        print(f"   📅 Primera submission: {fecha_min.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   📅 Última submission: {fecha_max.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   📊 Período total: {(fecha_max - fecha_min).days} días")
        
        # Distribución por mes
        from collections import defaultdict
        por_mes = defaultdict(int)
        for submission in todas_submissions:
            mes = submission['fecha_dt'].strftime('%Y-%m')
            por_mes[mes] += 1
        
        print(f"\n📈 DISTRIBUCIÓN POR MES:")
        for mes in sorted(por_mes.keys()):
            print(f"   {mes}: {por_mes[mes]} submissions")
    
    return todas_submissions

def analizar_structure_submissions(todas_submissions):
    """Analizar estructura de las submissions encontradas"""
    
    print(f"\n🔍 FASE 1B: ANÁLISIS DE ESTRUCTURA")
    print("=" * 60)
    
    if not todas_submissions:
        print("❌ No hay submissions para analizar")
        return
    
    # Tomar muestra de 5 submissions para análisis
    muestra = todas_submissions[:5]
    
    print(f"📊 Analizando estructura de {len(muestra)} submissions de muestra...")
    
    campos_location = []
    campos_coordenadas = []
    
    for i, submission in enumerate(muestra):
        submission_id = submission.get('id')
        form_type = submission.get('form_type')
        smetadata = submission.get('smetadata', {})
        
        # Analizar location
        location = smetadata.get('location', {})
        location_name = location.get('name')
        location_id = location.get('id')
        
        # Analizar coordenadas
        lat_entrega = smetadata.get('lat')
        lon_entrega = smetadata.get('lon')
        
        campos_location.append({
            'submission_id': submission_id,
            'form_type': form_type,
            'tiene_location_name': bool(location_name),
            'location_name': location_name,
            'tiene_location_id': bool(location_id),
            'location_id': location_id
        })
        
        campos_coordenadas.append({
            'submission_id': submission_id,
            'form_type': form_type,
            'tiene_lat': bool(lat_entrega),
            'tiene_lon': bool(lon_entrega),
            'lat_entrega': lat_entrega,
            'lon_entrega': lon_entrega
        })
        
        print(f"\n   📋 Submission {i+1}: {submission_id} ({form_type})")
        print(f"      📍 Location name: {'✅' if location_name else '❌'} {location_name}")
        print(f"      📍 Location ID: {'✅' if location_id else '❌'} {location_id}")
        print(f"      🌍 Coordenadas: {'✅' if lat_entrega and lon_entrega else '❌'} ({lat_entrega}, {lon_entrega})")
    
    # Estadísticas generales
    total_con_location = sum(1 for s in todas_submissions if s.get('smetadata', {}).get('location', {}).get('name'))
    total_sin_location = len(todas_submissions) - total_con_location
    
    total_con_coords = sum(1 for s in todas_submissions 
                          if s.get('smetadata', {}).get('lat') and s.get('smetadata', {}).get('lon'))
    total_sin_coords = len(todas_submissions) - total_con_coords
    
    print(f"\n📊 ESTADÍSTICAS GENERALES:")
    print(f"   📍 CON location_name: {total_con_location} ({total_con_location/len(todas_submissions)*100:.1f}%)")
    print(f"   ❌ SIN location_name: {total_sin_location} ({total_sin_location/len(todas_submissions)*100:.1f}%)")
    print(f"   🌍 CON coordenadas: {total_con_coords} ({total_con_coords/len(todas_submissions)*100:.1f}%)")
    print(f"   ❌ SIN coordenadas: {total_sin_coords} ({total_sin_coords/len(todas_submissions)*100:.1f}%)")
    
    return {
        'total_con_location': total_con_location,
        'total_sin_location': total_sin_location,
        'total_con_coords': total_con_coords,
        'total_sin_coords': total_sin_coords,
        'porcentaje_con_location': total_con_location/len(todas_submissions)*100,
        'porcentaje_sin_location': total_sin_location/len(todas_submissions)*100
    }

def guardar_resultados_fase1(todas_submissions, estadisticas):
    """Guardar resultados de la Fase 1"""
    
    print(f"\n💾 GUARDANDO RESULTADOS FASE 1")
    print("=" * 40)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 1. Dataset completo con información básica
    datos_basicos = []
    for submission in todas_submissions:
        smetadata = submission.get('smetadata', {})
        location = smetadata.get('location', {})
        created_by = smetadata.get('created_by', {})
        
        datos_basicos.append({
            'submission_id': submission.get('id'),
            'form_type': submission.get('form_type'),
            'fecha': submission['fecha_dt'].strftime('%Y-%m-%d %H:%M:%S'),
            'fecha_solo': submission['fecha_dt'].strftime('%Y-%m-%d'),
            'usuario_nombre': created_by.get('display_name'),
            'usuario_id': created_by.get('id'),
            'location_name': location.get('name'),
            'location_id': location.get('id'),
            'lat_entrega': smetadata.get('lat'),
            'lon_entrega': smetadata.get('lon'),
            'tiene_location': bool(location.get('name')),
            'tiene_coordenadas': bool(smetadata.get('lat') and smetadata.get('lon'))
        })
    
    filename_completo = f"FASE1_SUBMISSIONS_COMPLETAS_{timestamp}.csv"
    df_completo = pd.DataFrame(datos_basicos)
    df_completo.to_csv(filename_completo, index=False, encoding='utf-8')
    
    # 2. Solo las CON location_name
    con_location = df_completo[df_completo['tiene_location'] == True]
    if not con_location.empty:
        filename_con_location = f"FASE1_CON_LOCATION_{timestamp}.csv"
        con_location.to_csv(filename_con_location, index=False, encoding='utf-8')
    
    # 3. Solo las SIN location_name
    sin_location = df_completo[df_completo['tiene_location'] == False]
    if not sin_location.empty:
        filename_sin_location = f"FASE1_SIN_LOCATION_{timestamp}.csv"
        sin_location.to_csv(filename_sin_location, index=False, encoding='utf-8')
    
    # 4. Resumen estadístico
    resumen = {
        'timestamp': timestamp,
        'total_submissions': len(todas_submissions),
        'operativas': len([s for s in todas_submissions if s.get('form_type') == 'OPERATIVA']),
        'seguridad': len([s for s in todas_submissions if s.get('form_type') == 'SEGURIDAD']),
        'con_location': estadisticas['total_con_location'],
        'sin_location': estadisticas['total_sin_location'],
        'con_coordenadas': estadisticas['total_con_coords'],
        'sin_coordenadas': estadisticas['total_sin_coords']
    }
    
    filename_resumen = f"FASE1_RESUMEN_{timestamp}.json"
    with open(filename_resumen, 'w', encoding='utf-8') as f:
        json.dump(resumen, f, indent=2, default=str, ensure_ascii=False)
    
    print(f"📁 ARCHIVOS GENERADOS:")
    print(f"   📄 Dataset completo: {filename_completo}")
    if not con_location.empty:
        print(f"   ✅ Con location: {filename_con_location}")
    if not sin_location.empty:
        print(f"   ❌ Sin location: {filename_sin_location}")
    print(f"   📊 Resumen JSON: {filename_resumen}")
    
    return {
        'filename_completo': filename_completo,
        'filename_con_location': filename_con_location if not con_location.empty else None,
        'filename_sin_location': filename_sin_location if not sin_location.empty else None,
        'filename_resumen': filename_resumen
    }

def main():
    """Función principal - Fase 1"""
    
    print("🔄 INICIANDO FASE 1: EXTRACCIÓN COMPLETA")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 1. Extraer todas las submissions
    todas_submissions = extraer_todas_submissions_2025()
    
    if not todas_submissions:
        print("❌ ERROR: No se encontraron submissions de 2025")
        return
    
    # 2. Analizar estructura
    estadisticas = analizar_structure_submissions(todas_submissions)
    
    # 3. Guardar resultados
    archivos = guardar_resultados_fase1(todas_submissions, estadisticas)
    
    print(f"\n🎉 FASE 1 COMPLETADA EXITOSAMENTE")
    print("=" * 80)
    print(f"⏰ Fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"✅ {len(todas_submissions)} submissions extraídas y guardadas")
    print(f"📁 Archivos listos para Fase 2")
    
    # Preparación para Fase 2
    print(f"\n🔜 PREPARACIÓN PARA FASE 2:")
    if estadisticas['total_con_location'] > 0:
        print(f"   ✅ {estadisticas['total_con_location']} submissions CON location - mapeo directo")
    if estadisticas['total_sin_location'] > 0:
        print(f"   🌍 {estadisticas['total_sin_location']} submissions SIN location - mapeo por coordenadas")
    
    return todas_submissions, archivos

if __name__ == "__main__":
    main()