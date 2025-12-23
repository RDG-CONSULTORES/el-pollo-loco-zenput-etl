#!/usr/bin/env python3
"""
🚀 ETL COMPLETO MAPEO API REAL
Mapear 85 submissions seguridad sin location usando:
1. Coordenadas + distancia Haversine
2. Fechas coincidentes con operativas  
3. Campo Sucursal manual
4. Aplicar reglas de negocio confirmadas por Roberto
"""

import pandas as pd
import numpy as np
import math
import re
from datetime import datetime

def cargar_datos_api_extraidos():
    """Cargar datos ya extraídos del API"""
    
    print("📁 CARGAR DATOS API EXTRAÍDOS")
    print("=" * 50)
    
    # Cargar operativas y seguridad del API
    df_ops = pd.read_csv("OPERATIVAS_API_2025_20251218_153746.csv")
    df_seg = pd.read_csv("SEGURIDAD_API_2025_20251218_153746.csv")
    
    # Cargar catálogo sucursales
    df_sucursales = pd.read_csv('SUCURSALES_MASTER_20251218_110913.csv')
    
    print(f"✅ DATOS CARGADOS:")
    print(f"   🏗️ Operativas API 2025: {len(df_ops)}")
    print(f"   🛡️ Seguridad API 2025: {len(df_seg)}")
    print(f"   🏢 Sucursales master: {len(df_sucursales)}")
    
    return df_ops, df_seg, df_sucursales

def crear_catalogo_con_reglas():
    """Crear catálogo sucursales con reglas confirmadas por Roberto"""
    
    print(f"\n⚖️ CREAR CATÁLOGO CON REGLAS CONFIRMADAS")
    print("=" * 50)
    
    df_sucursales = pd.read_csv('SUCURSALES_MASTER_20251218_110913.csv')
    
    # Reglas especiales confirmadas por Roberto
    reglas_especiales = {
        '1 - Pino Suarez': {'ops': 3, 'seg': 3, 'total': 6, 'tipo': 'ESPECIAL_3_3'},
        '5 - Felix U. Gomez': {'ops': 3, 'seg': 3, 'total': 6, 'tipo': 'ESPECIAL_3_3'},
        '2 - Madero': {'ops': 3, 'seg': 3, 'total': 6, 'tipo': 'ESPECIAL_3_3'},
        '3 - Matamoros': {'ops': 3, 'seg': 3, 'total': 6, 'tipo': 'ESPECIAL_3_3'}
    }
    
    sucursales_con_reglas = []
    
    for _, row in df_sucursales.iterrows():
        if pd.notna(row['numero']) and pd.notna(row['nombre']):
            numero = int(row['numero'])
            nombre = row['nombre']
            location_key = f"{numero} - {nombre}"
            
            # Aplicar reglas específicas
            if location_key in reglas_especiales:
                regla = reglas_especiales[location_key]
                ops_esperadas = regla['ops']
                seg_esperadas = regla['seg'] 
                total_esperado = regla['total']
                tipo_regla = regla['tipo']
            else:
                # Reglas por tipo
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
                'tipo_original': row.get('tipo', 'LOCAL'),
                'grupo': row.get('grupo', ''),
                'ops_esperadas': ops_esperadas,
                'seg_esperadas': seg_esperadas,
                'total_esperado': total_esperado,
                'tipo_regla': tipo_regla
            })
    
    df_catalogo = pd.DataFrame(sucursales_con_reglas)
    
    print(f"📊 CATÁLOGO CREADO:")
    print(f"   📋 Total sucursales: {len(df_catalogo)}")
    print(f"   ⭐ ESPECIAL 3+3: {len(df_catalogo[df_catalogo['tipo_regla'] == 'ESPECIAL_3_3'])}")
    print(f"   🏢 LOCAL 4+4: {len(df_catalogo[df_catalogo['tipo_regla'] == 'LOCAL_4_4'])}")
    print(f"   🌍 FORÁNEA 2+2: {len(df_catalogo[df_catalogo['tipo_regla'] == 'FORANEA_2_2'])}")
    
    return df_catalogo

def calcular_distancia_haversine(lat1, lon1, lat2, lon2):
    """Calcular distancia en km usando fórmula Haversine"""
    try:
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        R = 6371  # Radio de la Tierra en km
        return R * c
    except Exception:
        return float('inf')

def extraer_coordenadas_location_map(location_map_text):
    """Extraer coordenadas de Location Map"""
    if pd.isna(location_map_text):
        return None, None
    
    texto = str(location_map_text)
    patterns = [
        r'@(-?\d+\.\d+),(-?\d+\.\d+)',
        r'(-?\d+\.\d+),(-?\d+\.\d+)',
        r'lat[=:]?\s*(-?\d+\.\d+).*lon[=:]?\s*(-?\d+\.\d+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, texto, re.IGNORECASE)
        if match:
            try:
                return float(match.group(1)), float(match.group(2))
            except:
                continue
    return None, None

def mapear_por_coordenadas(submission, df_catalogo, umbral_km=3.0):
    """Mapear submission por coordenadas usando distancia Haversine"""
    
    lat_submission = submission.get('lat_entrega')
    lon_submission = submission.get('lon_entrega')
    
    if pd.isna(lat_submission) or pd.isna(lon_submission):
        # Intentar extraer de location_map
        location_map = submission.get('location_map')
        if pd.notna(location_map):
            lat_map, lon_map = extraer_coordenadas_location_map(location_map)
            if lat_map and lon_map:
                lat_submission, lon_submission = lat_map, lon_map
            else:
                return None, float('inf'), "sin_coordenadas"
        else:
            return None, float('inf'), "sin_coordenadas"
    
    # Calcular distancias a todas las sucursales
    distancias = []
    for _, sucursal in df_catalogo.iterrows():
        if pd.notna(sucursal['lat']) and pd.notna(sucursal['lon']):
            distancia = calcular_distancia_haversine(
                lat_submission, lon_submission,
                sucursal['lat'], sucursal['lon']
            )
            distancias.append({
                'location_key': sucursal['location_key'],
                'distancia': distancia,
                'sucursal': sucursal
            })
    
    # Ordenar por distancia
    distancias.sort(key=lambda x: x['distancia'])
    
    # Verificar si la más cercana está dentro del umbral
    if distancias and distancias[0]['distancia'] <= umbral_km:
        return distancias[0]['location_key'], distancias[0]['distancia'], "coordenadas"
    
    return None, distancias[0]['distancia'] if distancias else float('inf'), "fuera_umbral"

def mapear_por_fechas_coincidentes(submission, df_ops):
    """Mapear por fechas coincidentes con operativas del mismo día"""
    
    fecha_submission = submission.get('fecha_str')
    if pd.isna(fecha_submission):
        return None, "sin_fecha"
    
    # Buscar operativas del mismo día
    ops_mismo_dia = df_ops[df_ops['fecha_str'] == fecha_submission]
    
    if len(ops_mismo_dia) > 0:
        # Tomar la location más frecuente del mismo día
        locations = ops_mismo_dia['location_asignado'].value_counts()
        if len(locations) > 0:
            return locations.index[0], "fecha_coincidente"
    
    return None, "sin_coincidencias"

def mapear_por_campo_sucursal(submission):
    """Mapear usando campo Sucursal manual"""
    
    sucursal_campo = submission.get('sucursal_campo')
    if pd.notna(sucursal_campo):
        return sucursal_campo, "campo_manual"
    
    return None, "sin_campo"

def mapear_submissions_sin_location(df_seg, df_ops, df_catalogo):
    """Mapear todas las submissions sin location usando estrategia combinada"""
    
    print(f"\n🗺️ MAPEAR SUBMISSIONS SIN LOCATION")
    print("=" * 60)
    
    seg_sin_location = df_seg[df_seg['necesita_mapeo'] == True].copy()
    
    print(f"📊 SUBMISSIONS A MAPEAR:")
    print(f"   🛡️ Seguridad sin location: {len(seg_sin_location)}")
    
    resultados_mapeo = []
    
    print(f"\n📋 PROCESO DE MAPEO:")
    print(f"{'#':<3} {'Fecha':<12} {'Estrategia':<15} {'Sucursal':<25} {'Distancia':<10}")
    print("-" * 80)
    
    for i, (idx, submission) in enumerate(seg_sin_location.iterrows(), 1):
        resultado = {
            'submission_id': submission['submission_id'],
            'fecha_str': submission['fecha_str'],
            'usuario': submission['usuario'],
            'lat_entrega': submission['lat_entrega'],
            'lon_entrega': submission['lon_entrega'],
            'sucursal_original': None,
            'sucursal_asignada': None,
            'estrategia': None,
            'distancia_km': None,
            'confianza': None
        }
        
        # Estrategia 1: Coordenadas (prioridad alta)
        sucursal_coords, distancia, motivo_coords = mapear_por_coordenadas(submission, df_catalogo)
        
        if sucursal_coords and motivo_coords == "coordenadas":
            resultado['sucursal_asignada'] = sucursal_coords
            resultado['estrategia'] = 'COORDENADAS'
            resultado['distancia_km'] = distancia
            resultado['confianza'] = 'ALTA'
            
        # Estrategia 2: Fechas coincidentes (prioridad media)
        elif sucursal_coords is None:
            sucursal_fecha, motivo_fecha = mapear_por_fechas_coincidentes(submission, df_ops)
            
            if sucursal_fecha:
                resultado['sucursal_asignada'] = sucursal_fecha
                resultado['estrategia'] = 'FECHA_COINCIDENTE'
                resultado['confianza'] = 'MEDIA'
            
            # Estrategia 3: Campo Sucursal (prioridad baja)
            else:
                sucursal_campo, motivo_campo = mapear_por_campo_sucursal(submission)
                
                if sucursal_campo:
                    resultado['sucursal_asignada'] = sucursal_campo
                    resultado['estrategia'] = 'CAMPO_MANUAL'
                    resultado['confianza'] = 'BAJA'
                else:
                    resultado['estrategia'] = 'NO_MAPEADO'
                    resultado['confianza'] = 'REVISAR'
        else:
            # Coordenadas fuera de umbral
            resultado['estrategia'] = 'FUERA_UMBRAL'
            resultado['distancia_km'] = distancia
            resultado['confianza'] = 'REVISAR'
        
        resultados_mapeo.append(resultado)
        
        # Mostrar progreso
        estrategia_display = resultado['estrategia'][:14]
        sucursal_display = (resultado['sucursal_asignada'] or 'NO_ASIGNADA')[:24]
        distancia_display = f"{resultado['distancia_km']:.1f}km" if resultado['distancia_km'] else "N/A"
        
        print(f"{i:<3} {submission['fecha_str']:<12} {estrategia_display:<15} {sucursal_display:<25} {distancia_display:<10}")
    
    df_mapeo = pd.DataFrame(resultados_mapeo)
    
    # Resumen de mapeo
    print(f"\n📊 RESUMEN MAPEO:")
    estrategias = df_mapeo['estrategia'].value_counts()
    for estrategia, count in estrategias.items():
        print(f"   {estrategia}: {count}")
    
    mapeadas = len(df_mapeo[df_mapeo['sucursal_asignada'].notna()])
    no_mapeadas = len(df_mapeo[df_mapeo['sucursal_asignada'].isna()])
    
    print(f"\n✅ MAPEADAS: {mapeadas}/{len(df_mapeo)} ({mapeadas/len(df_mapeo)*100:.1f}%)")
    print(f"❌ NO MAPEADAS: {no_mapeadas}")
    
    return df_mapeo

def aplicar_mapeo_y_crear_dataset_final(df_ops, df_seg, df_mapeo, df_catalogo):
    """Aplicar mapeo y crear dataset final con reglas de negocio"""
    
    print(f"\n🔄 APLICAR MAPEO Y CREAR DATASET FINAL")
    print("=" * 50)
    
    # Aplicar mapeo a seguridad
    df_seg_final = df_seg.copy()
    
    for _, mapeo in df_mapeo.iterrows():
        if pd.notna(mapeo['sucursal_asignada']):
            # Actualizar location en seguridad
            mask = df_seg_final['submission_id'] == mapeo['submission_id']
            df_seg_final.loc[mask, 'location_asignado'] = mapeo['sucursal_asignada']
            df_seg_final.loc[mask, 'tiene_location'] = True
            df_seg_final.loc[mask, 'necesita_mapeo'] = False
    
    # Combinar operativas y seguridad
    df_final = pd.concat([df_ops, df_seg_final], ignore_index=True)
    
    # Analizar cumplimiento de reglas
    ops_por_sucursal = df_final[df_final['tipo'] == 'operativas']['location_asignado'].value_counts()
    seg_por_sucursal = df_final[df_final['tipo'] == 'seguridad']['location_asignado'].value_counts()
    
    analisis_cumplimiento = []
    
    for _, sucursal in df_catalogo.iterrows():
        location_key = sucursal['location_key']
        ops_actuales = ops_por_sucursal.get(location_key, 0)
        seg_actuales = seg_por_sucursal.get(location_key, 0)
        total_actual = ops_actuales + seg_actuales
        
        ops_esperadas = sucursal['ops_esperadas']
        seg_esperadas = sucursal['seg_esperadas']
        total_esperado = sucursal['total_esperado']
        
        # Estado de cumplimiento
        if total_actual == total_esperado:
            estado = "✅ PERFECTO"
        elif total_actual > total_esperado:
            estado = f"⚠️ EXCESO (+{total_actual - total_esperado})"
        else:
            estado = f"❌ DEFICIT (-{total_esperado - total_actual})"
        
        analisis_cumplimiento.append({
            'location_key': location_key,
            'tipo_regla': sucursal['tipo_regla'],
            'ops_actuales': ops_actuales,
            'seg_actuales': seg_actuales,
            'total_actual': total_actual,
            'ops_esperadas': ops_esperadas,
            'seg_esperadas': seg_esperadas,
            'total_esperado': total_esperado,
            'diferencia': total_actual - total_esperado,
            'estado': estado
        })
    
    df_cumplimiento = pd.DataFrame(analisis_cumplimiento)
    
    # Resumen final
    perfectas = len(df_cumplimiento[df_cumplimiento['diferencia'] == 0])
    con_exceso = len(df_cumplimiento[df_cumplimiento['diferencia'] > 0])
    con_deficit = len(df_cumplimiento[df_cumplimiento['diferencia'] < 0])
    
    print(f"📊 CUMPLIMIENTO REGLAS DE NEGOCIO:")
    print(f"   ✅ Perfectas: {perfectas}")
    print(f"   ⚠️ Con exceso: {con_exceso}")
    print(f"   ❌ Con déficit: {con_deficit}")
    
    return df_final, df_cumplimiento

def main():
    """Función principal"""
    
    print("🚀 ETL COMPLETO MAPEO API REAL")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Objetivo: Mapear 85 submissions sin location + aplicar reglas")
    print("📋 Estrategia: Coordenadas → Fechas → Campo Sucursal")
    print("⚖️ Reglas: LOCAL 4+4, FORÁNEA 2+2, ESPECIALES 3+3")
    print("=" * 80)
    
    # 1. Cargar datos API extraídos
    df_ops, df_seg, df_sucursales = cargar_datos_api_extraidos()
    
    # 2. Crear catálogo con reglas
    df_catalogo = crear_catalogo_con_reglas()
    
    # 3. Mapear submissions sin location
    df_mapeo = mapear_submissions_sin_location(df_seg, df_ops, df_catalogo)
    
    # 4. Aplicar mapeo y crear dataset final
    df_final, df_cumplimiento = aplicar_mapeo_y_crear_dataset_final(df_ops, df_seg, df_mapeo, df_catalogo)
    
    # 5. Guardar resultados
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    df_mapeo.to_csv(f"MAPEO_SUBMISSIONS_{timestamp}.csv", index=False, encoding='utf-8')
    df_final.to_csv(f"DATASET_FINAL_API_{timestamp}.csv", index=False, encoding='utf-8')
    df_cumplimiento.to_csv(f"CUMPLIMIENTO_REGLAS_{timestamp}.csv", index=False, encoding='utf-8')
    
    print(f"\n📁 ARCHIVOS GENERADOS:")
    print(f"   ✅ Mapeo: MAPEO_SUBMISSIONS_{timestamp}.csv")
    print(f"   ✅ Dataset final: DATASET_FINAL_API_{timestamp}.csv")
    print(f"   ✅ Cumplimiento: CUMPLIMIENTO_REGLAS_{timestamp}.csv")
    
    print(f"\n🎯 RESUMEN FINAL:")
    print(f"   📊 Total submissions procesadas: {len(df_final)}")
    print(f"   🏗️ Operativas: {len(df_final[df_final['tipo'] == 'operativas'])}")
    print(f"   🛡️ Seguridad: {len(df_final[df_final['tipo'] == 'seguridad'])}")
    
    sin_location_final = df_final[df_final['necesita_mapeo'] == True]
    print(f"   ❓ Sin mapear final: {len(sin_location_final)}")
    
    if len(sin_location_final) > 0:
        print(f"   📋 Revisar manualmente: {list(sin_location_final['submission_id'])}")
    
    print(f"\n✅ ETL COMPLETO FINALIZADO EXITOSAMENTE")
    print(f"🔧 Próximo: Revisar casos no mapeados y aplicar correcciones")
    
    return df_final, df_mapeo, df_cumplimiento

if __name__ == "__main__":
    main()