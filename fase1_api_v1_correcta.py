#!/usr/bin/env python3
"""
🔄 FASE 1: EXTRACCIÓN CORRECTA USANDO API V1
Usar la documentación oficial de Zenput API v1 para obtener TODAS las 238+238 submissions
"""

import requests
import json
import pandas as pd
from datetime import datetime, date
import time

ZENPUT_CONFIG = {
    'base_url_v1': 'https://www.zenput.com/api/v1',
    'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}
}

FORMULARIOS_OBJETIVO = {
    '877138': 'OPERATIVA',
    '877139': 'SEGURIDAD'
}

def obtener_total_submissions(template_id):
    """Obtener el total de submissions usando el parámetro count=true"""
    
    print(f"🔍 Obteniendo total de submissions para template {template_id}...")
    
    try:
        url = f"{ZENPUT_CONFIG['base_url_v1']}/forms/list/{template_id}"
        params = {
            'limit': 1,  # Solo 1 submission
            'count': 'true',  # Obtener el total
            'date_start': '20250301',  # Desde 1 marzo 2025
            'date_end': '20251231',   # Hasta 31 diciembre 2025
            'output': 'json'
        }
        
        response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=15)
        
        if response.status_code == 200:
            data = response.json()
            total_count = data.get('total_count', 0)
            submissions = data.get('submissions', [])
            
            print(f"   ✅ Total submissions disponibles: {total_count}")
            print(f"   📊 Muestra obtenida: {len(submissions)}")
            
            if submissions:
                sample = submissions[0]
                print(f"   📋 ID muestra: {sample.get('id')}")
                smetadata = sample.get('smetadata', {})
                print(f"   📅 Fecha muestra: {smetadata.get('date_submitted', 'N/A')}")
            
            return total_count
            
        else:
            print(f"   ❌ Error {response.status_code}: {response.text[:200]}")
            return 0
            
    except Exception as e:
        print(f"   💥 Error: {e}")
        return 0

def extraer_submissions_completas_v1(template_id, tipo_form, total_esperado):
    """Extraer TODAS las submissions usando API v1 con paginación correcta"""
    
    print(f"\n📋 EXTRAYENDO {tipo_form} (Template {template_id})")
    print(f"🎯 Objetivo: {total_esperado} submissions")
    print("-" * 60)
    
    todas_submissions = []
    limite_por_pagina = 100  # Máximo permitido por la API
    start = 0
    
    while True:
        try:
            print(f"   📄 Página {start//limite_por_pagina + 1} (start={start})...", end=" ", flush=True)
            
            url = f"{ZENPUT_CONFIG['base_url_v1']}/forms/list/{template_id}"
            params = {
                'start': start,
                'limit': limite_por_pagina,
                'date_start': '20250301',  # 1 marzo 2025
                'date_end': '20251231',   # 31 diciembre 2025
                'use_utc_date_range': 'false',  # Usar fecha local
                'output': 'json'
            }
            
            response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                submissions = data.get('submissions', [])
                
                if not submissions:
                    print("✅ Fin (sin más submissions)")
                    break
                
                print(f"✅ {len(submissions)} submissions")
                
                # Agregar tipo de formulario y procesar
                for submission in submissions:
                    submission['form_type'] = tipo_form
                    
                    # Verificar que la fecha esté en 2025 y después del 12 de marzo
                    smetadata = submission.get('smetadata', {})
                    fecha_submitted = smetadata.get('date_submitted')
                    
                    if fecha_submitted:
                        try:
                            fecha_dt = datetime.fromisoformat(fecha_submitted.replace('Z', '+00:00'))
                            if fecha_dt.year == 2025 and (fecha_dt.month > 3 or (fecha_dt.month == 3 and fecha_dt.day >= 12)):
                                todas_submissions.append(submission)
                        except:
                            # Si no se puede parsear la fecha, incluir la submission
                            todas_submissions.append(submission)
                
                print(f"      └─ Total acumuladas: {len(todas_submissions)}")
                
                # Incrementar para la siguiente página
                start += limite_por_pagina
                
                # Pausa pequeña para no saturar la API
                time.sleep(0.2)
                
                # Verificar si ya obtuvimos todas las esperadas
                if len(todas_submissions) >= total_esperado:
                    print(f"   🎉 ¡Objetivo alcanzado! {len(todas_submissions)}/{total_esperado}")
                    break
                
            else:
                print(f"❌ Error {response.status_code}: {response.text[:100]}")
                break
                
        except Exception as e:
            print(f"💥 Error: {e}")
            break
    
    # Remover duplicados por ID (por si acaso)
    submissions_unicas = {}
    for sub in todas_submissions:
        sub_id = sub.get('id')
        if sub_id and sub_id not in submissions_unicas:
            submissions_unicas[sub_id] = sub
    
    lista_final = list(submissions_unicas.values())
    
    print(f"\n📊 RESUMEN {tipo_form}:")
    print(f"   📥 Total extraídas: {len(todas_submissions)}")
    print(f"   🔍 Únicas (sin duplicados): {len(lista_final)}")
    print(f"   🎯 vs Objetivo ({total_esperado}): {len(lista_final)}/{total_esperado}")
    
    if len(lista_final) < total_esperado:
        print(f"   ⚠️ FALTANTES: {total_esperado - len(lista_final)} submissions")
    elif len(lista_final) > total_esperado:
        print(f"   ℹ️ EXTRAS: {len(lista_final) - total_esperado} submissions adicionales")
    else:
        print(f"   🎉 ¡PERFECTO! Exactamente las {total_esperado} esperadas")
    
    return lista_final

def analizar_estructura_submissions_v1(todas_submissions):
    """Analizar la estructura de las submissions obtenidas"""
    
    print(f"\n🔍 ANÁLISIS DE ESTRUCTURA DE {len(todas_submissions)} SUBMISSIONS")
    print("=" * 70)
    
    if not todas_submissions:
        print("❌ No hay submissions para analizar")
        return {}
    
    # Estadísticas generales
    con_location = 0
    sin_location = 0
    con_coordenadas = 0
    sin_coordenadas = 0
    
    # Análisis por fecha
    fechas_submissions = []
    locations_encontrados = set()
    usuarios_encontrados = set()
    
    # Muestra para inspección
    print("📋 MUESTRA DE 5 SUBMISSIONS:")
    muestra = todas_submissions[:5]
    
    for i, submission in enumerate(muestra):
        sub_id = submission.get('id')
        form_type = submission.get('form_type')
        smetadata = submission.get('smetadata', {})
        
        # Location
        location = smetadata.get('location', {})
        location_name = location.get('name')
        
        # Coordenadas
        lat = smetadata.get('lat')
        lon = smetadata.get('lon')
        
        # Usuario
        created_by = smetadata.get('created_by', {})
        usuario = created_by.get('display_name')
        
        # Fecha
        fecha_submitted = smetadata.get('date_submitted', '')
        
        print(f"   {i+1}. {sub_id} ({form_type})")
        print(f"      📅 Fecha: {fecha_submitted[:16]}")
        print(f"      📍 Location: {'✅' if location_name else '❌'} {location_name}")
        print(f"      🌍 Coords: {'✅' if lat and lon else '❌'} ({lat}, {lon})")
        print(f"      👤 Usuario: {usuario}")
    
    # Procesar todas las submissions para estadísticas
    for submission in todas_submissions:
        smetadata = submission.get('smetadata', {})
        
        # Location
        location = smetadata.get('location', {})
        location_name = location.get('name')
        if location_name:
            con_location += 1
            locations_encontrados.add(location_name)
        else:
            sin_location += 1
        
        # Coordenadas
        lat = smetadata.get('lat')
        lon = smetadata.get('lon')
        if lat and lon:
            con_coordenadas += 1
        else:
            sin_coordenadas += 1
        
        # Usuario
        created_by = smetadata.get('created_by', {})
        usuario = created_by.get('display_name')
        if usuario:
            usuarios_encontrados.add(usuario)
        
        # Fecha
        fecha_submitted = smetadata.get('date_submitted')
        if fecha_submitted:
            try:
                fecha_dt = datetime.fromisoformat(fecha_submitted.replace('Z', '+00:00'))
                fechas_submissions.append(fecha_dt)
            except:
                pass
    
    # Estadísticas generales
    total = len(todas_submissions)
    
    print(f"\n📊 ESTADÍSTICAS GENERALES:")
    print(f"   📋 Total submissions: {total}")
    print(f"   📍 CON location: {con_location} ({con_location/total*100:.1f}%)")
    print(f"   ❌ SIN location: {sin_location} ({sin_location/total*100:.1f}%)")
    print(f"   🌍 CON coordenadas: {con_coordenadas} ({con_coordenadas/total*100:.1f}%)")
    print(f"   ❌ SIN coordenadas: {sin_coordenadas} ({sin_coordenadas/total*100:.1f}%)")
    
    print(f"\n📈 ANÁLISIS DETALLADO:")
    print(f"   🏪 Locations únicas: {len(locations_encontrados)}")
    print(f"   👥 Usuarios únicos: {len(usuarios_encontrados)}")
    
    if fechas_submissions:
        fecha_min = min(fechas_submissions)
        fecha_max = max(fechas_submissions)
        print(f"   📅 Rango fechas: {fecha_min.strftime('%Y-%m-%d')} a {fecha_max.strftime('%Y-%m-%d')}")
        print(f"   📊 Días cubiertos: {(fecha_max - fecha_min).days + 1}")
    
    # Mostrar algunos locations y usuarios
    print(f"\n📋 LOCATIONS ENCONTRADOS (primeros 10):")
    for location in sorted(list(locations_encontrados))[:10]:
        print(f"   - {location}")
    
    print(f"\n👥 USUARIOS ENCONTRADOS:")
    for usuario in sorted(list(usuarios_encontrados)):
        print(f"   - {usuario}")
    
    return {
        'total_submissions': total,
        'con_location': con_location,
        'sin_location': sin_location,
        'con_coordenadas': con_coordenadas,
        'sin_coordenadas': sin_coordenadas,
        'locations_unicas': len(locations_encontrados),
        'usuarios_unicos': len(usuarios_encontrados),
        'locations_list': list(locations_encontrados),
        'usuarios_list': list(usuarios_encontrados)
    }

def main():
    """Función principal - Fase 1 Correcta"""
    
    print("🔄 FASE 1: EXTRACCIÓN CORRECTA CON API V1 DE ZENPUT")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("📚 Usando documentación oficial: /api/v1/forms/list/")
    print("🎯 Objetivo: 238 Operativas + 238 Seguridad = 476 submissions")
    print("=" * 80)
    
    todas_submissions = []
    
    # Procesar cada formulario
    for template_id, tipo_form in FORMULARIOS_OBJETIVO.items():
        
        # 1. Obtener total disponible
        total_disponible = obtener_total_submissions(template_id)
        
        if total_disponible == 0:
            print(f"⚠️ No se encontraron submissions para {tipo_form}")
            continue
        
        # 2. Extraer todas las submissions
        submissions_form = extraer_submissions_completas_v1(template_id, tipo_form, 238)
        
        todas_submissions.extend(submissions_form)
    
    # 3. Análisis final
    if todas_submissions:
        print(f"\n" + "=" * 80)
        print(f"🎉 FASE 1 COMPLETADA")
        print("=" * 80)
        
        # Breakdown por formulario
        operativas = [s for s in todas_submissions if s.get('form_type') == 'OPERATIVA']
        seguridad = [s for s in todas_submissions if s.get('form_type') == 'SEGURIDAD']
        
        print(f"📊 RESULTADOS FINALES:")
        print(f"   📋 Total submissions: {len(todas_submissions)}")
        print(f"   📊 Operativas: {len(operativas)}")
        print(f"   📊 Seguridad: {len(seguridad)}")
        
        # Verificar objetivo
        objetivo_total = 476
        print(f"\n🎯 VERIFICACIÓN vs OBJETIVO:")
        print(f"   🎯 Esperado: 238 + 238 = 476")
        print(f"   ✅ Obtenido: {len(operativas)} + {len(seguridad)} = {len(todas_submissions)}")
        
        if len(todas_submissions) == objetivo_total:
            print(f"   🎉 ¡PERFECTO! Exactamente las 476 submissions esperadas")
        elif len(todas_submissions) < objetivo_total:
            print(f"   ⚠️ FALTANTES: {objetivo_total - len(todas_submissions)} submissions")
        else:
            print(f"   ✅ EXTRAS: {len(todas_submissions) - objetivo_total} submissions adicionales")
        
        # 4. Análisis de estructura
        estadisticas = analizar_estructura_submissions_v1(todas_submissions)
        
        # 5. Guardar resultados
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Dataset completo
        datos_completos = []
        for submission in todas_submissions:
            smetadata = submission.get('smetadata', {})
            location = smetadata.get('location', {})
            created_by = smetadata.get('created_by', {})
            
            datos_completos.append({
                'submission_id': submission.get('id'),
                'form_type': submission.get('form_type'),
                'fecha': smetadata.get('date_submitted', ''),
                'usuario_nombre': created_by.get('display_name'),
                'location_name': location.get('name'),
                'location_id': location.get('id'),
                'lat_entrega': smetadata.get('lat'),
                'lon_entrega': smetadata.get('lon'),
                'tiene_location': bool(location.get('name')),
                'tiene_coordenadas': bool(smetadata.get('lat') and smetadata.get('lon'))
            })
        
        filename_completo = f"FASE1_COMPLETA_API_V1_{timestamp}.csv"
        df = pd.DataFrame(datos_completos)
        df.to_csv(filename_completo, index=False, encoding='utf-8')
        
        print(f"\n📁 ARCHIVO GENERADO: {filename_completo}")
        print(f"✅ {len(todas_submissions)} submissions listas para Fase 2")
        
        return todas_submissions, filename_completo
        
    else:
        print("❌ ERROR: No se obtuvieron submissions")
        return None, None

if __name__ == "__main__":
    main()