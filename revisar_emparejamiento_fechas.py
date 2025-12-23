#!/usr/bin/env python3
"""
🔍 REVISAR EMPAREJAMIENTO POR FECHAS
Pedro Cardenas: emparejar por fechas exactas
Centrito Valle: buscar pares para supervisiones sobrantes
"""

import pandas as pd
import math
from datetime import datetime

def cargar_dataset():
    """Cargar dataset normalizado"""
    df = pd.read_csv("DATASET_NORMALIZADO_20251218_155659.csv")
    return df

def cargar_catalogo_sucursales():
    """Cargar catálogo para coordenadas de Matamoros"""
    df_sucursales = pd.read_csv('SUCURSALES_MASTER_20251218_110913.csv')
    
    # Filtrar sucursales de Matamoros
    matamoros_sucursales = []
    for _, row in df_sucursales.iterrows():
        if pd.notna(row['numero']) and pd.notna(row['nombre']):
            numero = int(row['numero'])
            nombre = row['nombre']
            location_key = f"{numero} - {nombre}"
            ciudad = str(row.get('grupo', '')).lower()
            
            if 'matamoros' in ciudad or 'matamoros' in nombre.lower():
                matamoros_sucursales.append({
                    'location_key': location_key,
                    'lat': float(row['lat']) if pd.notna(row['lat']) else None,
                    'lon': float(row['lon']) if pd.notna(row['lon']) else None,
                    'grupo': row.get('grupo', ''),
                    'numero': numero
                })
    
    return matamoros_sucursales

def calcular_distancia_haversine(lat1, lon1, lat2, lon2):
    """Calcular distancia en km"""
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

def analizar_pedro_cardenas_fechas(df, matamoros_sucursales):
    """Analizar Pedro Cardenas emparejamiento por fechas exactas"""
    
    print(f"🔍 PEDRO CARDENAS - EMPAREJAMIENTO POR FECHAS")
    print("=" * 70)
    
    pedro = df[df['location_asignado'] == '65 - Pedro Cardenas'].copy()
    ops = pedro[pedro['tipo'] == 'operativas'].copy()
    seg = pedro[pedro['tipo'] == 'seguridad'].copy()
    
    print(f"📊 SUPERVISIONES ACTUALES:")
    print(f"   🏗️ Operativas: {len(ops)}")
    print(f"   🛡️ Seguridad: {len(seg)}")
    
    print(f"\n📅 OPERATIVAS (fechas exactas):")
    fechas_ops = []
    for _, row in ops.iterrows():
        fecha = row['fecha_str']
        fechas_ops.append(fecha)
        print(f"   ✅ {fecha} - {row['submission_id'][:15]}")
    
    print(f"\n📅 SEGURIDAD (fechas exactas):")
    fechas_seg = []
    supervisiones_seg = []
    for _, row in seg.iterrows():
        fecha = row['fecha_str']
        fechas_seg.append(fecha)
        supervisiones_seg.append(row)
        tiene_par = "✅" if fecha in fechas_ops else "❌"
        lat = row['lat_entrega']
        lon = row['lon_entrega']
        coords_str = f"{lat:.4f}, {lon:.4f}" if pd.notna(lat) and pd.notna(lon) else "Sin coords"
        print(f"   {tiene_par} {fecha} - {row['submission_id'][:15]} - {coords_str}")
    
    # Identificar supervisión sin par
    fechas_ops_set = set(fechas_ops)
    fechas_seg_set = set(fechas_seg)
    
    fechas_sin_par = fechas_seg_set - fechas_ops_set
    
    print(f"\n🎯 ANÁLISIS DE EMPAREJAMIENTO:")
    print(f"   ✅ Fechas con par: {sorted(fechas_ops_set.intersection(fechas_seg_set))}")
    print(f"   ❌ Seguridad sin par operativa: {sorted(fechas_sin_par)}")
    
    # Analizar supervisión sin par
    if fechas_sin_par:
        fecha_sin_par = list(fechas_sin_par)[0]
        supervision_sin_par = seg[seg['fecha_str'] == fecha_sin_par].iloc[0]
        
        print(f"\n🚨 SUPERVISIÓN SIN PAR IDENTIFICADA:")
        print(f"   📅 Fecha: {fecha_sin_par}")
        print(f"   🆔 ID: {supervision_sin_par['submission_id']}")
        print(f"   📍 Coordenadas: {supervision_sin_par['lat_entrega']}, {supervision_sin_par['lon_entrega']}")
        
        # Si no tiene coordenadas, buscar en otras sucursales de Matamoros por fecha
        if pd.isna(supervision_sin_par['lat_entrega']):
            print(f"\n🔍 BUSCAR EN SUCURSALES DE MATAMOROS (sin coordenadas):")
            print(f"   📅 Fecha objetivo: {fecha_sin_par}")
            
            # Buscar operativas del mismo día en otras sucursales de Matamoros
            operativas_mismo_dia = df[(df['tipo'] == 'operativas') & 
                                    (df['fecha_str'] == fecha_sin_par) & 
                                    (df['location_asignado'] != '65 - Pedro Cardenas')]
            
            print(f"   🎯 Operativas del {fecha_sin_par} en otras sucursales:")
            for _, ops_row in operativas_mismo_dia.iterrows():
                location = ops_row['location_asignado']
                # Verificar si es de Matamoros
                for mat_suc in matamoros_sucursales:
                    if location == mat_suc['location_key']:
                        print(f"      ✅ {location} (Matamoros) - {ops_row['submission_id'][:15]}")
                        break
                else:
                    print(f"      📍 {location} - {ops_row['submission_id'][:15]}")
        
        return supervision_sin_par
    
    return None

def buscar_pares_centrito_sobrantes(df):
    """Buscar pares para supervisiones sobrantes de Centrito Valle"""
    
    print(f"\n🔍 CENTRITO VALLE - BUSCAR PARES PARA SOBRANTES")
    print("=" * 70)
    
    # Supervisiones sobrantes identificadas
    sobrantes = [
        {'fecha': '2025-06-24', 'id': '685b2812604e495', 'tipo': 'seguridad'},
        {'fecha': '2025-08-22', 'id': '68a8b8ece1c1b03', 'tipo': 'seguridad'}
    ]
    
    print(f"🚨 SUPERVISIONES SOBRANTES DE CENTRITO VALLE:")
    for sob in sobrantes:
        print(f"   📅 {sob['fecha']} - {sob['tipo']} - {sob['id'][:15]}")
    
    # Buscar operativas del mismo día en otras sucursales
    for sobrante in sobrantes:
        fecha_target = sobrante['fecha']
        print(f"\n🔍 BUSCAR PARES PARA {fecha_target}:")
        
        # Buscar operativas del mismo día
        ops_mismo_dia = df[(df['tipo'] == 'operativas') & 
                          (df['fecha_str'] == fecha_target) & 
                          (df['location_asignado'] != '71 - Centrito Valle')]
        
        if len(ops_mismo_dia) > 0:
            print(f"   🏗️ OPERATIVAS del {fecha_target} en otras sucursales:")
            for _, ops_row in ops_mismo_dia.iterrows():
                location = ops_row['location_asignado']
                print(f"      📍 {location} - {ops_row['submission_id'][:15]}")
                
                # Verificar si esa sucursal tiene seguridad del mismo día
                seg_mismo_dia = df[(df['tipo'] == 'seguridad') & 
                                  (df['fecha_str'] == fecha_target) & 
                                  (df['location_asignado'] == location)]
                
                if len(seg_mismo_dia) == 0:
                    print(f"         ✅ NO tiene seguridad del {fecha_target} - CANDIDATO para recibir")
                else:
                    print(f"         ❌ Ya tiene seguridad del {fecha_target}")
        else:
            print(f"   ❌ No hay operativas del {fecha_target} en otras sucursales")
        
        # Buscar también sucursales que tengan operativa sin seguridad
        print(f"\n   🔍 SUCURSALES CON OPERATIVA SIN SEGURIDAD del {fecha_target}:")
        
        # Todas las supervisiones del día
        todas_del_dia = df[df['fecha_str'] == fecha_target]
        ops_del_dia = todas_del_dia[todas_del_dia['tipo'] == 'operativas']
        seg_del_dia = todas_del_dia[todas_del_dia['tipo'] == 'seguridad']
        
        sucursales_ops = set(ops_del_dia['location_asignado'])
        sucursales_seg = set(seg_del_dia['location_asignado'])
        
        sucursales_sin_par = sucursales_ops - sucursales_seg
        
        if sucursales_sin_par:
            for sucursal in sucursales_sin_par:
                if sucursal != '71 - Centrito Valle':  # Excluir Centrito
                    print(f"      ✅ {sucursal} - tiene operativa sin seguridad")
        else:
            print(f"      ❌ Todas las sucursales con operativa ya tienen seguridad")

def analizar_conteos_actuales(df):
    """Mostrar conteos actuales para context"""
    
    print(f"\n📊 CONTEOS ACTUALES RELEVANTES")
    print("=" * 50)
    
    # Sucursales problema
    sucursales_problema = [
        '65 - Pedro Cardenas',
        '71 - Centrito Valle'
    ]
    
    for sucursal in sucursales_problema:
        suc_data = df[df['location_asignado'] == sucursal]
        ops = len(suc_data[suc_data['tipo'] == 'operativas'])
        seg = len(suc_data[suc_data['tipo'] == 'seguridad'])
        total = ops + seg
        
        tipo_regla = "FORANEA 2+2" if sucursal == '65 - Pedro Cardenas' else "LOCAL 4+4"
        esperado = 4 if sucursal == '65 - Pedro Cardenas' else 8
        diferencia = total - esperado
        
        estado = "✅ PERFECTO" if diferencia == 0 else f"⚠️ EXCESO (+{diferencia})" if diferencia > 0 else f"❌ DEFICIT (-{abs(diferencia)})"
        
        print(f"📍 {sucursal} ({tipo_regla}):")
        print(f"   🏗️ Operativas: {ops}")
        print(f"   🛡️ Seguridad: {seg}")
        print(f"   📊 Total: {total} (esperado {esperado}) - {estado}")

def main():
    """Función principal"""
    
    print("🔍 REVISAR EMPAREJAMIENTO POR FECHAS")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Objetivo: Emparejar por fechas exactas - Pedro Cardenas y Centrito Valle")
    print("=" * 80)
    
    # 1. Cargar datos
    df = cargar_dataset()
    matamoros_sucursales = cargar_catalogo_sucursales()
    
    print(f"📍 SUCURSALES DE MATAMOROS ENCONTRADAS:")
    for mat_suc in matamoros_sucursales:
        print(f"   {mat_suc['location_key']} - {mat_suc['grupo']}")
    
    # 2. Analizar Pedro Cardenas por fechas exactas
    supervision_sin_par = analizar_pedro_cardenas_fechas(df, matamoros_sucursales)
    
    # 3. Buscar pares para Centrito Valle sobrantes
    buscar_pares_centrito_sobrantes(df)
    
    # 4. Mostrar conteos actuales
    analizar_conteos_actuales(df)
    
    print(f"\n🎯 RESUMEN DE HALLAZGOS")
    print("=" * 50)
    
    if supervision_sin_par is not None:
        print(f"📍 PEDRO CARDENAS:")
        print(f"   🚨 Supervisión sin par: {supervision_sin_par['submission_id'][:15]}")
        print(f"   📅 Fecha: {supervision_sin_par['fecha_str']}")
        print(f"   💡 Probablemente mal asignada a Pedro Cardenas")
    
    print(f"\n📍 CENTRITO VALLE:")
    print(f"   🚨 2 supervisiones sobrantes necesitan reubicación")
    print(f"   📅 2025-06-24 y 2025-08-22")
    
    print(f"\n✅ ANÁLISIS DE EMPAREJAMIENTO COMPLETADO")

if __name__ == "__main__":
    main()