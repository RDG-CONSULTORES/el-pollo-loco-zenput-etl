#!/usr/bin/env python3
"""
🔄 FASE 1: EXTRACCIÓN CORRECTA CON API V3 - PARÁMETROS OFICIALES
Usar los parámetros correctos de la documentación oficial de Zenput
"""

import requests
import json
import pandas as pd
from datetime import datetime, date
import time

ZENPUT_CONFIG = {
    'base_url': 'https://www.zenput.com/api/v3',
    'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}
}

FORMULARIOS_OBJETIVO = {
    '877138': 'OPERATIVA',
    '877139': 'SEGURIDAD'
}

def obtener_total_con_meta(form_template_id):
    """Obtener el total usando el campo meta del response"""
    
    print(f"🔍 Obteniendo metadata para form_template_id {form_template_id}...")
    
    try:
        url = f"{ZENPUT_CONFIG['base_url']}/submissions"
        params = {
            'form_template_id': form_template_id,
            'limit': 1,  # Solo 1 para obtener metadata
            'start': 0
        }
        
        response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=15)
        
        if response.status_code == 200:
            data = response.json()
            
            # Buscar información de total en meta o en el root
            meta = data.get('meta', {})
            count_meta = meta.get('count')
            count_root = data.get('count')
            total_count = count_meta or count_root or 0
            
            submissions = data.get('data', [])
            
            print(f"   📊 Meta: {meta}")
            print(f"   📊 Count en meta: {count_meta}")
            print(f"   📊 Count en root: {count_root}")
            print(f"   📊 Total calculado: {total_count}")
            print(f"   📊 Submissions en respuesta: {len(submissions)}")
            
            if submissions:
                sample = submissions[0]
                print(f"   📋 ID muestra: {sample.get('id')}")
                smetadata = sample.get('smetadata', {})
                print(f"   📅 Fecha muestra: {smetadata.get('date_submitted', 'N/A')}")
            
            return total_count, meta
            
        else:
            print(f"   ❌ Error {response.status_code}: {response.text[:200]}")
            return 0, {}
            
    except Exception as e:
        print(f"   💥 Error: {e}")
        return 0, {}

def extraer_todas_con_paginacion_correcta(form_template_id, tipo_form):
    """Extraer todas las submissions usando start/limit en lugar de page/page_size"""
    
    print(f"\n📋 EXTRAYENDO {tipo_form} (Form {form_template_id})")
    print("-" * 60)
    
    # 1. Obtener metadata inicial
    total_estimado, meta_info = obtener_total_con_meta(form_template_id)
    
    todas_submissions = {}  # Dict para evitar duplicados por ID
    limit = 100  # Tamaño de página
    start = 0
    pagina_actual = 1
    submissions_consecutivas_vacias = 0
    
    print(f"🎯 Iniciando extracción completa...")
    print(f"📊 Total estimado: {total_estimado}")
    
    while True:
        try:
            print(f"   📄 Página {pagina_actual} (start={start}, limit={limit})...", end=" ", flush=True)
            
            url = f"{ZENPUT_CONFIG['base_url']}/submissions"
            params = {
                'form_template_id': form_template_id,
                'start': start,
                'limit': limit
            }
            
            response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                submissions = data.get('data', [])
                meta = data.get('meta', {})
                
                print(f"✅ {len(submissions)} submissions")
                
                if not submissions:
                    submissions_consecutivas_vacias += 1
                    print(f"      ⚠️ Página vacía ({submissions_consecutivas_vacias}/3)")
                    
                    if submissions_consecutivas_vacias >= 3:
                        print(f"      ✅ Fin confirmado después de 3 páginas vacías")
                        break
                else:
                    submissions_consecutivas_vacias = 0  # Reset
                
                # Procesar submissions encontradas
                nuevas_2025 = 0
                for submission in submissions:
                    submission_id = submission.get('id')
                    
                    # Evitar duplicados
                    if submission_id in todas_submissions:
                        continue
                    
                    # Verificar fecha 2025
                    smetadata = submission.get('smetadata', {})
                    fecha_submitted = smetadata.get('date_submitted')
                    
                    if fecha_submitted:
                        try:
                            fecha_dt = datetime.fromisoformat(fecha_submitted.replace('Z', '+00:00'))
                            if fecha_dt.year == 2025:
                                # Verificar que sea después del 12 marzo
                                if fecha_dt.month > 3 or (fecha_dt.month == 3 and fecha_dt.day >= 12):
                                    submission['form_type'] = tipo_form
                                    submission['fecha_dt'] = fecha_dt
                                    todas_submissions[submission_id] = submission
                                    nuevas_2025 += 1
                        except Exception as e:
                            # Si hay error parseando fecha, incluir la submission
                            submission['form_type'] = tipo_form
                            todas_submissions[submission_id] = submission
                            nuevas_2025 += 1
                
                if nuevas_2025 > 0 or len(submissions) > 0:
                    print(f"      └─ 2025 válidas: {nuevas_2025}, Total acumuladas: {len(todas_submissions)}")
                
                # Información de paginación
                if meta:
                    print(f"      📊 Meta: {meta}")
                
                # Siguiente página
                start += limit
                pagina_actual += 1
                time.sleep(0.1)  # Pausa pequeña
                
                # Límite de seguridad para evitar bucles infinitos
                if pagina_actual > 500:  # 500 páginas = 50,000 submissions máximo
                    print(f"      ⚠️ Límite de seguridad alcanzado (500 páginas)")
                    break
                
            else:
                print(f"❌ Error {response.status_code}: {response.text[:100]}")
                break
                
        except Exception as e:
            print(f"💥 Error: {e}")
            break
    
    submissions_finales = list(todas_submissions.values())
    
    print(f"\n📊 RESUMEN {tipo_form}:")
    print(f"   📄 Páginas procesadas: {pagina_actual - 1}")
    print(f"   📥 Total extraídas: {len(submissions_finales)}")
    print(f"   🎯 vs Estimado ({total_estimado}): {len(submissions_finales)}/{total_estimado}")
    
    # Mostrar rango de fechas
    if submissions_finales:
        fechas = [s['fecha_dt'] for s in submissions_finales if 'fecha_dt' in s]
        if fechas:
            fecha_min = min(fechas)
            fecha_max = max(fechas)
            print(f"   📅 Rango: {fecha_min.strftime('%Y-%m-%d')} a {fecha_max.strftime('%Y-%m-%d')}")
    
    return submissions_finales

def main():
    """Función principal - Fase 1 con API v3 correcta"""
    
    print("🔄 FASE 1: EXTRACCIÓN CON API V3 USANDO PARÁMETROS CORRECTOS")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("📚 Endpoint: /api/v3/submissions")
    print("🎯 Objetivo: Encontrar todas las submissions de 2025")
    print("🔧 Parámetros: form_template_id, start, limit (según documentación)")
    print("=" * 80)
    
    todas_submissions = []
    
    # Procesar cada formulario
    for form_template_id, tipo_form in FORMULARIOS_OBJETIVO.items():
        submissions_form = extraer_todas_con_paginacion_correcta(form_template_id, tipo_form)
        todas_submissions.extend(submissions_form)
    
    if todas_submissions:
        print(f"\n" + "=" * 80)
        print(f"🎉 FASE 1 COMPLETADA CON ÉXITO")
        print("=" * 80)
        
        # Estadísticas
        operativas = [s for s in todas_submissions if s.get('form_type') == 'OPERATIVA']
        seguridad = [s for s in todas_submissions if s.get('form_type') == 'SEGURIDAD']
        
        print(f"📊 RESULTADOS FINALES:")
        print(f"   📋 Total submissions: {len(todas_submissions)}")
        print(f"   📊 Operativas: {len(operativas)}")
        print(f"   📊 Seguridad: {len(seguridad)}")
        
        # Comparar con expectativa
        print(f"\n🎯 ANÁLISIS vs EXPECTATIVA (238+238=476):")
        total_esperado = 476
        if len(todas_submissions) >= total_esperado * 0.9:  # Al menos 90% de lo esperado
            print(f"   🎉 ¡ÉXITO! Encontramos {len(todas_submissions)} submissions")
            if len(todas_submissions) >= total_esperado:
                print(f"   ✅ Alcanzamos o superamos el objetivo de 476")
            else:
                print(f"   📊 Estamos cerca del objetivo ({len(todas_submissions)}/476 = {len(todas_submissions)/476*100:.1f}%)")
        else:
            print(f"   ⚠️ Encontramos menos de lo esperado: {len(todas_submissions)}")
            print(f"   💡 Posibles causas: filtros de fecha, permisos API, datos en otros períodos")
        
        # Análisis de estructura rápido
        con_location = sum(1 for s in todas_submissions if (s.get('smetadata') or {}).get('location', {}).get('name'))
        sin_location = len(todas_submissions) - con_location
        
        print(f"\n📋 ANÁLISIS DE LOCATION:")
        print(f"   ✅ CON location_name: {con_location} ({con_location/len(todas_submissions)*100:.1f}%)")
        print(f"   ❌ SIN location_name: {sin_location} ({sin_location/len(todas_submissions)*100:.1f}%)")
        
        # Guardar resultados
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"FASE1_API_V3_CORRECTA_{timestamp}.csv"
        
        # Preparar datos para CSV
        datos_csv = []
        for submission in todas_submissions:
            smetadata = submission.get('smetadata') or {}
            location = smetadata.get('location') or {}
            created_by = smetadata.get('created_by') or {}
            
            datos_csv.append({
                'submission_id': submission.get('id'),
                'form_type': submission.get('form_type'),
                'fecha': smetadata.get('date_submitted', ''),
                'usuario_nombre': created_by.get('display_name'),
                'usuario_id': created_by.get('id'),
                'location_name': location.get('name'),
                'location_id': location.get('id'),
                'lat_entrega': smetadata.get('lat'),
                'lon_entrega': smetadata.get('lon'),
                'tiene_location': bool(location.get('name')),
                'tiene_coordenadas': bool(smetadata.get('lat') and smetadata.get('lon'))
            })
        
        df = pd.DataFrame(datos_csv)
        df.to_csv(filename, index=False, encoding='utf-8')
        
        print(f"\n📁 ARCHIVO GENERADO: {filename}")
        print(f"✅ Datos listos para Fase 2: Análisis de Location")
        
        # Preparación para Fase 2
        print(f"\n🔜 SIGUIENTE PASO - FASE 2:")
        if con_location > 0:
            print(f"   ✅ {con_location} submissions CON location → mapeo directo")
        if sin_location > 0:
            print(f"   🌍 {sin_location} submissions SIN location → mapeo por coordenadas")
        print(f"   🎯 Objetivo: Asignar todas a las ~80 sucursales activas")
        
        return todas_submissions, filename
    
    else:
        print("❌ ERROR: No se obtuvieron submissions")
        return None, None

if __name__ == "__main__":
    main()