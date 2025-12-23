#!/usr/bin/env python3
"""
🔍 ANALIZAR SUPERVISIONES DETALLADO
Analizar casos específicos: Linda Vista, Pablo Livas, Pedro Cardenas, Centrito Valle
"""

import pandas as pd
import math
import re
from datetime import datetime

def cargar_dataset_normalizado():
    """Cargar dataset normalizado"""
    
    print("📁 CARGAR DATASET NORMALIZADO")
    print("=" * 40)
    
    df = pd.read_csv("DATASET_NORMALIZADO_20251218_155659.csv")
    
    print(f"✅ Dataset cargado: {len(df)} registros")
    
    return df

def cargar_catalogo_sucursales():
    """Cargar catálogo de sucursales para coordenadas"""
    
    df_sucursales = pd.read_csv('SUCURSALES_MASTER_20251218_110913.csv')
    
    catalogo = {}
    for _, row in df_sucursales.iterrows():
        if pd.notna(row['numero']) and pd.notna(row['nombre']):
            numero = int(row['numero'])
            nombre = row['nombre']
            location_key = f"{numero} - {nombre}"
            catalogo[location_key] = {
                'lat': float(row['lat']) if pd.notna(row['lat']) else None,
                'lon': float(row['lon']) if pd.notna(row['lon']) else None,
                'tipo': row.get('tipo', 'LOCAL')
            }
    
    return catalogo

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

def analizar_linda_vista(df):
    """Analizar Linda Vista - necesita +1 seguridad"""
    
    print(f"\n🔍 ANÁLISIS DETALLADO: LINDA VISTA")
    print("=" * 60)
    
    # Obtener supervisiones de Linda Vista
    linda_vista = df[df['location_asignado'] == '18 - Linda Vista'].copy()
    
    ops = linda_vista[linda_vista['tipo'] == 'operativas']
    seg = linda_vista[linda_vista['tipo'] == 'seguridad']
    
    print(f"📊 ESTADO ACTUAL:")
    print(f"   🏗️ Operativas: {len(ops)} (necesita 4)")
    print(f"   🛡️ Seguridad: {len(seg)} (necesita 4, faltan 1)")
    
    print(f"\n📅 FECHAS OPERATIVAS:")
    for _, row in ops.iterrows():
        print(f"   {row['fecha_str']} - {row['submission_id'][:15]}")
    
    print(f"\n📅 FECHAS SEGURIDAD:")
    for _, row in seg.iterrows():
        print(f"   {row['fecha_str']} - {row['submission_id'][:15]}")
    
    # Encontrar fecha sin par
    fechas_ops = set(ops['fecha_str'])
    fechas_seg = set(seg['fecha_str'])
    
    fechas_sin_par_seg = fechas_ops - fechas_seg
    fechas_sin_par_ops = fechas_seg - fechas_ops
    
    print(f"\n🔍 ANÁLISIS DE PARES:")
    print(f"   📅 Fechas operativas sin par seguridad: {fechas_sin_par_seg}")
    print(f"   📅 Fechas seguridad sin par operativas: {fechas_sin_par_ops}")
    
    return linda_vista, fechas_sin_par_seg

def analizar_pablo_livas(df):
    """Analizar Pablo Livas - tiene +1 seguridad sobrante"""
    
    print(f"\n🔍 ANÁLISIS DETALLADO: PABLO LIVAS")
    print("=" * 60)
    
    pablo_livas = df[df['location_asignado'] == '29 - Pablo Livas'].copy()
    
    ops = pablo_livas[pablo_livas['tipo'] == 'operativas']
    seg = pablo_livas[pablo_livas['tipo'] == 'seguridad']
    
    print(f"📊 ESTADO ACTUAL:")
    print(f"   🏗️ Operativas: {len(ops)} (necesita 4)")
    print(f"   🛡️ Seguridad: {len(seg)} (necesita 4, sobra 1)")
    
    print(f"\n📅 FECHAS Y TIEMPOS OPERATIVAS:")
    for _, row in ops.iterrows():
        submitted_at = pd.to_datetime(row['date_submitted']) if pd.notna(row['date_submitted']) else None
        hora = submitted_at.strftime('%H:%M') if submitted_at else 'N/A'
        print(f"   {row['fecha_str']} {hora} - {row['submission_id'][:15]}")
    
    print(f"\n📅 FECHAS Y TIEMPOS SEGURIDAD:")
    for _, row in seg.iterrows():
        submitted_at = pd.to_datetime(row['date_submitted']) if pd.notna(row['date_submitted']) else None
        hora = submitted_at.strftime('%H:%M') if submitted_at else 'N/A'
        print(f"   {row['fecha_str']} {hora} - {row['submission_id'][:15]}")
    
    # Encontrar fechas sin par
    fechas_ops = set(ops['fecha_str'])
    fechas_seg = set(seg['fecha_str'])
    
    fechas_sin_par_seg = fechas_seg - fechas_ops
    fechas_sin_par_ops = fechas_ops - fechas_seg
    
    print(f"\n🔍 ANÁLISIS DE PARES:")
    print(f"   📅 Fechas seguridad sin par operativas: {fechas_sin_par_seg}")
    print(f"   📅 Fechas operativas sin par seguridad: {fechas_sin_par_ops}")
    
    # Identificar supervisión sobrante
    if fechas_sin_par_seg:
        fecha_sobrante = list(fechas_sin_par_seg)[0]
        supervision_sobrante = seg[seg['fecha_str'] == fecha_sobrante].iloc[0]
        print(f"\n🎯 SUPERVISIÓN SOBRANTE IDENTIFICADA:")
        print(f"   📅 Fecha: {fecha_sobrante}")
        print(f"   🆔 ID: {supervision_sobrante['submission_id']}")
        print(f"   🌐 Coordenadas: {supervision_sobrante['lat_entrega']}, {supervision_sobrante['lon_entrega']}")
        
        return pablo_livas, supervision_sobrante
    
    return pablo_livas, None

def analizar_pedro_cardenas(df, catalogo_sucursales):
    """Analizar Pedro Cardenas - FORÁNEA 2+2, tiene +1 seguridad"""
    
    print(f"\n🔍 ANÁLISIS DETALLADO: PEDRO CARDENAS")
    print("=" * 60)
    
    pedro_cardenas = df[df['location_asignado'] == '65 - Pedro Cardenas'].copy()
    
    ops = pedro_cardenas[pedro_cardenas['tipo'] == 'operativas']
    seg = pedro_cardenas[pedro_cardenas['tipo'] == 'seguridad']
    
    print(f"📊 ESTADO ACTUAL:")
    print(f"   🏗️ Operativas: {len(ops)} (FORÁNEA necesita 2)")
    print(f"   🛡️ Seguridad: {len(seg)} (FORÁNEA necesita 2, sobra 1)")
    
    print(f"\n📅 FECHAS OPERATIVAS:")
    for _, row in ops.iterrows():
        print(f"   {row['fecha_str']} - {row['submission_id'][:15]}")
    
    print(f"\n📅 FECHAS SEGURIDAD:")
    for i, (_, row) in enumerate(seg.iterrows(), 1):
        lat = row['lat_entrega']
        lon = row['lon_entrega']
        sucursal_campo = row['sucursal_campo']
        print(f"   {i}. {row['fecha_str']} - {row['submission_id'][:15]}")
        print(f"      📍 Coords: {lat}, {lon}")
        print(f"      📝 Campo Sucursal: '{sucursal_campo}'")
        
        # Buscar coordenadas alternativas en location_map
        if pd.isna(lat) or pd.isna(lon):
            location_map = row['location_map']
            if pd.notna(location_map):
                lat_map, lon_map = extraer_coordenadas_location_map(location_map)
                if lat_map and lon_map:
                    print(f"      🗺️ Location Map: {lat_map}, {lon_map}")
                    lat, lon = lat_map, lon_map
        
        # Calcular distancias a otras sucursales
        if pd.notna(lat) and pd.notna(lon):
            print(f"      🔍 Distancias a sucursales cercanas:")
            distancias = []
            for sucursal_key, sucursal_data in catalogo_sucursales.items():
                if sucursal_data['lat'] and sucursal_data['lon']:
                    distancia = calcular_distancia_haversine(lat, lon, sucursal_data['lat'], sucursal_data['lon'])
                    if distancia < 10:  # Menos de 10km
                        distancias.append((sucursal_key, distancia))
            
            distancias.sort(key=lambda x: x[1])
            for sucursal, dist in distancias[:5]:
                print(f"         {sucursal}: {dist:.1f}km")
        print()
    
    return pedro_cardenas

def analizar_centrito_valle(df):
    """Analizar Centrito Valle - LOCAL 4+4, tiene 5+7=12 (sobra 4)"""
    
    print(f"\n🔍 ANÁLISIS DETALLADO: CENTRITO VALLE")
    print("=" * 60)
    
    centrito = df[df['location_asignado'] == '71 - Centrito Valle'].copy()
    
    ops = centrito[centrito['tipo'] == 'operativas']
    seg = centrito[centrito['tipo'] == 'seguridad']
    
    print(f"📊 ESTADO ACTUAL:")
    print(f"   🏗️ Operativas: {len(ops)} (LOCAL necesita 4, sobra 1)")
    print(f"   🛡️ Seguridad: {len(seg)} (LOCAL necesita 4, sobran 3)")
    
    print(f"\n📅 SUPERVISIONES OPERATIVAS:")
    fechas_ops = []
    for i, (_, row) in enumerate(ops.iterrows(), 1):
        fecha = row['fecha_str']
        fechas_ops.append(fecha)
        submitted_at = pd.to_datetime(row['date_submitted']) if pd.notna(row['date_submitted']) else None
        hora = submitted_at.strftime('%H:%M') if submitted_at else 'N/A'
        print(f"   {i}. {fecha} {hora} - {row['submission_id'][:15]}")
    
    print(f"\n📅 SUPERVISIONES SEGURIDAD:")
    fechas_seg = []
    for i, (_, row) in enumerate(seg.iterrows(), 1):
        fecha = row['fecha_str']
        fechas_seg.append(fecha)
        submitted_at = pd.to_datetime(row['date_submitted']) if pd.notna(row['date_submitted']) else None
        hora = submitted_at.strftime('%H:%M') if submitted_at else 'N/A'
        print(f"   {i}. {fecha} {hora} - {row['submission_id'][:15]}")
    
    # Análisis de pares
    fechas_ops_set = set(fechas_ops)
    fechas_seg_set = set(fechas_seg)
    
    fechas_comunes = fechas_ops_set.intersection(fechas_seg_set)
    fechas_ops_sin_par = fechas_ops_set - fechas_seg_set
    fechas_seg_sin_par = fechas_seg_set - fechas_ops_set
    
    print(f"\n🔍 ANÁLISIS DE PARES:")
    print(f"   ✅ Fechas con par (ops+seg): {sorted(fechas_comunes)}")
    print(f"   ❌ Operativas sin par seguridad: {sorted(fechas_ops_sin_par)}")
    print(f"   ❌ Seguridad sin par operativas: {sorted(fechas_seg_sin_par)}")
    
    # Supervisiones que tienen su par (keeper)
    supervisiones_con_par = []
    for fecha in fechas_comunes:
        ops_fecha = ops[ops['fecha_str'] == fecha]
        seg_fecha = seg[seg['fecha_str'] == fecha]
        supervisiones_con_par.extend(ops_fecha['submission_id'].tolist())
        supervisiones_con_par.extend(seg_fecha['submission_id'].tolist())
    
    # Supervisiones sobrantes
    supervisiones_sobrantes = []
    
    # Operativas sin par
    for fecha in fechas_ops_sin_par:
        ops_sin_par = ops[ops['fecha_str'] == fecha]
        supervisiones_sobrantes.extend([{'id': row['submission_id'], 'tipo': 'operativa', 'fecha': fecha, 'motivo': 'sin_par_seg'} for _, row in ops_sin_par.iterrows()])
    
    # Seguridad sin par
    for fecha in fechas_seg_sin_par:
        seg_sin_par = seg[seg['fecha_str'] == fecha]
        supervisiones_sobrantes.extend([{'id': row['submission_id'], 'tipo': 'seguridad', 'fecha': fecha, 'motivo': 'sin_par_ops'} for _, row in seg_sin_par.iterrows()])
    
    print(f"\n🎯 SUPERVISIONES A MANTENER (con par):")
    print(f"   📊 Total con par: {len(supervisiones_con_par)} supervisiones")
    print(f"   ✅ Fechas con pares completos: {len(fechas_comunes)} fechas")
    
    print(f"\n🚨 SUPERVISIONES SOBRANTES A REDISTRIBUIR:")
    for sup in supervisiones_sobrantes:
        print(f"   📅 {sup['fecha']} - {sup['tipo']} - {sup['id'][:15]} ({sup['motivo']})")
    
    return centrito, supervisiones_sobrantes

def main():
    """Función principal"""
    
    print("🔍 ANALIZAR SUPERVISIONES DETALLADO")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Casos: Linda Vista, Pablo Livas, Pedro Cardenas, Centrito Valle")
    print("=" * 80)
    
    # 1. Cargar datos
    df = cargar_dataset_normalizado()
    catalogo_sucursales = cargar_catalogo_sucursales()
    
    # 2. Analizar cada caso específico
    linda_vista_data, fechas_sin_par_linda = analizar_linda_vista(df)
    pablo_livas_data, supervision_sobrante_pablo = analizar_pablo_livas(df)
    pedro_cardenas_data = analizar_pedro_cardenas(df, catalogo_sucursales)
    centrito_data, supervisiones_sobrantes_centrito = analizar_centrito_valle(df)
    
    # 3. Resumen de hallazgos
    print(f"\n🎯 RESUMEN DE HALLAZGOS")
    print("=" * 60)
    
    print(f"📍 LINDA VISTA:")
    if fechas_sin_par_linda:
        print(f"   ❓ Necesita seguridad para fecha: {list(fechas_sin_par_linda)}")
    
    print(f"\n📍 PABLO LIVAS:")
    if supervision_sobrante_pablo is not None:
        print(f"   ⚠️ Supervisión sobrante: {supervision_sobrante_pablo['submission_id'][:15]}")
        print(f"   📅 Fecha: {supervision_sobrante_pablo['fecha_str']}")
    
    print(f"\n📍 PEDRO CARDENAS:")
    print(f"   🔍 Revisar coordenadas y campos sucursal de las 3 supervisiones")
    
    print(f"\n📍 CENTRITO VALLE:")
    print(f"   🚨 {len(supervisiones_sobrantes_centrito)} supervisiones sobrantes para redistribuir")
    
    print(f"\n✅ ANÁLISIS DETALLADO COMPLETADO")
    
    return {
        'linda_vista': (linda_vista_data, fechas_sin_par_linda),
        'pablo_livas': (pablo_livas_data, supervision_sobrante_pablo),
        'pedro_cardenas': pedro_cardenas_data,
        'centrito_valle': (centrito_data, supervisiones_sobrantes_centrito)
    }

if __name__ == "__main__":
    main()