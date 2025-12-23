#!/usr/bin/env python3
"""
🚀 ETL LIMPIO DESDE API ZENPUT V3
Partir de cero con extracción completa del API
238 operativas + 238 seguridad = 476 submissions
Reglas: LOCAL 4+4, FORÁNEA 2+2, ESPECIALES 3+3 (Pino Suarez, Felix U Gomez, Madero, Matamoros)
"""

import pandas as pd
import requests
import json
import math
import time
from datetime import datetime

def configurar_api():
    """Configurar parámetros del API Zenput v3"""
    
    print("🔧 CONFIGURAR API ZENPUT V3")
    print("=" * 40)
    
    config = {
        'base_url': 'https://www.zenput.com/api/v3',
        'headers': {
            'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314',
            'Content-Type': 'application/json'
        },
        'form_templates': {
            'operativa': 877138,
            'seguridad': 877139
        },
        'year': 2025
    }
    
    print(f"✅ API configurado:")
    print(f"   📍 URL: {config['base_url']}")
    print(f"   🔑 Token: {config['headers']['X-API-TOKEN'][:20]}...")
    print(f"   📋 Operativa: {config['form_templates']['operativa']}")
    print(f"   📋 Seguridad: {config['form_templates']['seguridad']}")
    print(f"   📅 Año: {config['year']}")
    
    return config

def extraer_submissions_api(config, form_template_id, tipo):
    """Extraer submissions del API v3"""
    
    print(f"\n📥 EXTRAYENDO {tipo.upper()} DESDE API")
    print("=" * 50)
    
    url = f"{config['base_url']}/submissions"
    
    params = {
        'form_template_id': form_template_id,
        'start': 0,
        'limit': 500  # Máximo por request
    }
    
    todas_submissions = []
    total_extraidas = 0
    
    while True:
        print(f"📡 Request: start={params['start']}, limit={params['limit']}")
        
        try:
            response = requests.get(url, headers=config['headers'], params=params)
            
            if response.status_code == 200:
                data = response.json()
                submissions = data.get('submissions', [])
                
                if not submissions:
                    print(f"✅ No más submissions. Total extraídas: {total_extraidas}")
                    break
                
                # Filtrar por año 2025
                submissions_2025 = []
                for submission in submissions:
                    submitted_at = submission.get('submitted_at', '')
                    if '2025' in submitted_at:
                        submissions_2025.append(submission)
                
                todas_submissions.extend(submissions_2025)
                total_extraidas += len(submissions_2025)
                
                print(f"   ✅ Extraídas: {len(submissions_2025)} del 2025 (de {len(submissions)} total)")
                
                # Siguiente página
                params['start'] += params['limit']
                
                # Pausa para no sobrecargar API
                time.sleep(0.5)
                
            else:
                print(f"❌ Error API: {response.status_code}")
                print(f"   Response: {response.text}")
                break
                
        except Exception as e:
            print(f"❌ Error extrayendo: {e}")
            break
    
    print(f"\n📊 RESUMEN EXTRACCIÓN {tipo.upper()}:")
    print(f"   ✅ Total submissions 2025: {len(todas_submissions)}")
    
    return todas_submissions

def procesar_submissions_extraidas(submissions, tipo):
    """Procesar submissions extraídas en DataFrame estructurado"""
    
    print(f"\n🔄 PROCESANDO {tipo.upper()}")
    print("=" * 40)
    
    datos_procesados = []
    
    for i, submission in enumerate(submissions, 1):
        try:
            # Datos básicos
            submission_id = submission.get('id')
            submitted_at = submission.get('submitted_at')
            submitted_by = submission.get('submitted_by', {}).get('name', 'DESCONOCIDO')
            location = submission.get('location', {}).get('name') if submission.get('location') else None
            
            # Coordenadas de entrega (smetadata)
            smetadata = submission.get('smetadata', {})
            lat_entrega = smetadata.get('lat')
            lon_entrega = smetadata.get('lon')
            
            # Datos de respuestas (para campo Sucursal si existe)
            responses = submission.get('responses', [])
            sucursal_campo = None
            location_map = None
            
            # Buscar campo Sucursal en respuestas
            for response in responses:
                if response.get('question', {}).get('name') in ['Sucursal', 'sucursal']:
                    sucursal_campo = response.get('answer')
                elif 'location' in response.get('question', {}).get('name', '').lower():
                    if 'map' in response.get('question', {}).get('name', '').lower():
                        location_map = response.get('answer')
            
            # Convertir fecha
            fecha_dt = pd.to_datetime(submitted_at) if submitted_at else None
            
            datos_procesados.append({
                'submission_id': submission_id,
                'submitted_at': submitted_at,
                'fecha': fecha_dt,
                'fecha_str': fecha_dt.strftime('%Y-%m-%d') if fecha_dt else None,
                'submitted_by': submitted_by,
                'location_asignado': location,
                'lat_entrega': lat_entrega,
                'lon_entrega': lon_entrega,
                'sucursal_campo': sucursal_campo,
                'location_map': location_map,
                'tipo': tipo,
                'tiene_coordenadas': lat_entrega is not None and lon_entrega is not None,
                'tiene_location': location is not None,
                'submission_raw': submission  # Guardar datos completos por si acaso
            })
            
        except Exception as e:
            print(f"⚠️ Error procesando submission {i}: {e}")
            continue
    
    df = pd.DataFrame(datos_procesados)
    
    print(f"📊 PROCESAMIENTO {tipo.upper()}:")
    print(f"   ✅ Total procesadas: {len(df)}")
    print(f"   📍 Con location asignado: {len(df[df['tiene_location']])}")
    print(f"   🌐 Con coordenadas: {len(df[df['tiene_coordenadas']])}")
    print(f"   ❌ Sin location: {len(df[~df['tiene_location']])}")
    
    return df

def cargar_sucursales_master():
    """Cargar catálogo master de sucursales con reglas"""
    
    print(f"\n📋 CARGAR CATÁLOGO MASTER SUCURSALES")
    print("=" * 50)
    
    df_sucursales = pd.read_csv('SUCURSALES_MASTER_20251218_110913.csv')
    
    # Definir reglas especiales confirmadas por Roberto
    reglas_especiales = {
        '1 - Pino Suarez': {'ops': 3, 'seg': 3, 'total': 6, 'tipo_especial': 'ESPECIAL_3_3'},
        '5 - Felix U. Gomez': {'ops': 3, 'seg': 3, 'total': 6, 'tipo_especial': 'ESPECIAL_3_3'},
        '2 - Madero': {'ops': 3, 'seg': 3, 'total': 6, 'tipo_especial': 'ESPECIAL_3_3'},
        '3 - Matamoros': {'ops': 3, 'seg': 3, 'total': 6, 'tipo_especial': 'ESPECIAL_3_3'}
    }
    
    sucursales_con_reglas = []
    
    for _, row in df_sucursales.iterrows():
        if pd.notna(row['numero']) and pd.notna(row['nombre']):
            numero = int(row['numero'])
            nombre = row['nombre']
            location_key = f"{numero} - {nombre}"
            
            # Aplicar reglas
            if location_key in reglas_especiales:
                regla = reglas_especiales[location_key]
                ops_esperadas = regla['ops']
                seg_esperadas = regla['seg']
                total_esperado = regla['total']
                tipo_regla = regla['tipo_especial']
            else:
                tipo = row.get('tipo', 'LOCAL')
                if tipo == 'FORANEA':
                    ops_esperadas = 2
                    seg_esperadas = 2
                    total_esperado = 4
                    tipo_regla = 'FORANEA_2_2'
                else:  # LOCAL
                    ops_esperadas = 4
                    seg_esperadas = 4
                    total_esperado = 8
                    tipo_regla = 'LOCAL_4_4'
            
            sucursales_con_reglas.append({
                'numero': numero,
                'nombre': nombre,
                'location_key': location_key,
                'lat': float(row['lat']) if pd.notna(row['lat']) else None,
                'lon': float(row['lon']) if pd.notna(row['lon']) else None,
                'tipo': row.get('tipo', 'LOCAL'),
                'grupo': row.get('grupo', ''),
                'ops_esperadas': ops_esperadas,
                'seg_esperadas': seg_esperadas,
                'total_esperado': total_esperado,
                'tipo_regla': tipo_regla
            })
    
    df_reglas = pd.DataFrame(sucursales_con_reglas)
    
    print(f"📊 SUCURSALES CON REGLAS:")
    print(f"   📋 Total sucursales: {len(df_reglas)}")
    print(f"   🏢 LOCAL 4+4: {len(df_reglas[df_reglas['tipo_regla'] == 'LOCAL_4_4'])}")
    print(f"   🌍 FORÁNEA 2+2: {len(df_reglas[df_reglas['tipo_regla'] == 'FORANEA_2_2'])}")
    print(f"   ⭐ ESPECIAL 3+3: {len(df_reglas[df_reglas['tipo_regla'] == 'ESPECIAL_3_3'])}")
    
    return df_reglas

def calcular_distancia_haversine(lat1, lon1, lat2, lon2):
    """Calcular distancia en km usando fórmula Haversine"""
    try:
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        R = 6371
        return R * c
    except Exception:
        return float('inf')

def mapear_por_coordenadas_y_fechas(df_ops, df_seg, df_reglas):
    """Mapear submissions sin location usando coordenadas + fechas coincidentes"""
    
    print(f"\n🗺️ MAPEO POR COORDENADAS + FECHAS COINCIDENTES")
    print("=" * 60)
    
    # Submissions sin location que necesitan mapeo
    ops_sin_location = df_ops[~df_ops['tiene_location']].copy()
    seg_sin_location = df_seg[~df_seg['tiene_location']].copy()
    
    print(f"📊 SUBMISSIONS SIN LOCATION:")
    print(f"   🏗️ Operativas sin location: {len(ops_sin_location)}")
    print(f"   🛡️ Seguridad sin location: {len(seg_sin_location)}")
    
    # Mapear seguridad sin location (el problema principal según Roberto)
    if len(seg_sin_location) > 0:
        print(f"\n🛡️ MAPEANDO SEGURIDAD SIN LOCATION:")
        print(f"{'#':<3} {'Fecha':<12} {'Usuario':<15} {'Coords':<8} {'Estrategia'}")
        print("-" * 60)
        
        for i, (idx, row) in enumerate(seg_sin_location.iterrows(), 1):
            fecha_str = row['fecha_str']
            usuario = row['submitted_by']
            tiene_coords = "SÍ" if row['tiene_coordenadas'] else "NO"
            
            estrategia = ""
            if row['tiene_coordenadas']:
                estrategia = "COORDENADAS"
            elif pd.notna(row['sucursal_campo']):
                estrategia = "CAMPO_SUCURSAL"
            elif pd.notna(row['location_map']):
                estrategia = "LOCATION_MAP"
            else:
                estrategia = "FECHA_COINCIDENTE"
            
            print(f"{i:<3} {fecha_str:<12} {usuario:<15} {tiene_coords:<8} {estrategia}")
    
    return ops_sin_location, seg_sin_location

def main():
    """Función principal"""
    
    print("🚀 ETL LIMPIO DESDE API ZENPUT V3")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Objetivo: Extracción completa y limpia desde API")
    print("📋 Target: 238 operativas + 238 seguridad = 476 submissions")
    print("⚖️ Reglas: LOCAL 4+4, FORÁNEA 2+2, ESPECIALES 3+3")
    print("=" * 80)
    
    # 1. Configurar API
    config = configurar_api()
    
    # 2. Extraer operativas
    submissions_operativas = extraer_submissions_api(
        config, 
        config['form_templates']['operativa'], 
        'operativa'
    )
    
    # 3. Extraer seguridad
    submissions_seguridad = extraer_submissions_api(
        config, 
        config['form_templates']['seguridad'], 
        'seguridad'
    )
    
    # 4. Procesar operativas
    df_ops = procesar_submissions_extraidas(submissions_operativas, 'operativa')
    
    # 5. Procesar seguridad
    df_seg = procesar_submissions_extraidas(submissions_seguridad, 'seguridad')
    
    # 6. Cargar reglas de sucursales
    df_reglas = cargar_sucursales_master()
    
    # 7. Mapear submissions sin location
    ops_sin_location, seg_sin_location = mapear_por_coordenadas_y_fechas(df_ops, df_seg, df_reglas)
    
    # 8. Guardar datos extraídos
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    df_ops.to_csv(f"OPERATIVAS_API_V3_{timestamp}.csv", index=False, encoding='utf-8')
    df_seg.to_csv(f"SEGURIDAD_API_V3_{timestamp}.csv", index=False, encoding='utf-8')
    df_reglas.to_csv(f"SUCURSALES_REGLAS_{timestamp}.csv", index=False, encoding='utf-8')
    
    print(f"\n📁 DATOS GUARDADOS:")
    print(f"   ✅ Operativas: OPERATIVAS_API_V3_{timestamp}.csv ({len(df_ops)} registros)")
    print(f"   ✅ Seguridad: SEGURIDAD_API_V3_{timestamp}.csv ({len(df_seg)} registros)")
    print(f"   ✅ Reglas: SUCURSALES_REGLAS_{timestamp}.csv ({len(df_reglas)} sucursales)")
    
    print(f"\n🎯 RESUMEN INICIAL:")
    print(f"   📊 Operativas extraídas: {len(df_ops)}")
    print(f"   📊 Seguridad extraídas: {len(df_seg)}")
    print(f"   📊 Total submissions: {len(df_ops) + len(df_seg)}")
    print(f"   🛡️ Seguridad sin location: {len(seg_sin_location)} (problema principal)")
    
    print(f"\n💡 PRÓXIMO PASO:")
    print(f"   🔄 Implementar mapeo completo por coordenadas + fechas")
    print(f"   🎯 Resolver dudas con Roberto en casos específicos")
    
    return df_ops, df_seg, df_reglas, ops_sin_location, seg_sin_location

if __name__ == "__main__":
    main()