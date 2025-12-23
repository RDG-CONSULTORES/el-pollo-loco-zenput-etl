#!/usr/bin/env python3
"""
🔍 VERIFICAR ORIGEN SUPERVISIONES SIN COORDENADAS
Determinar si las supervisiones de Gómez Morín y Centrito Valle tienen Location Map o fueron asignadas manualmente
"""

import pandas as pd
from datetime import datetime
import re

def extraer_coordenadas_google_maps(location_map_text):
    """Extraer coordenadas de texto de Google Maps"""
    if pd.isna(location_map_text):
        return None, None
    
    texto = str(location_map_text)
    
    # Patrones para coordenadas
    patterns = [
        r'@(-?\d+\.\d+),(-?\d+\.\d+)',
        r'(-?\d+\.\d+),(-?\d+\.\d+)',
        r'lat[=:]?\s*(-?\d+\.\d+).*lon[=:]?\s*(-?\d+\.\d+)',
        r'latitude[=:]?\s*(-?\d+\.\d+).*longitude[=:]?\s*(-?\d+\.\d+)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, texto, re.IGNORECASE)
        if match:
            try:
                lat, lon = float(match.group(1)), float(match.group(2))
                return lat, lon
            except:
                continue
    
    return None, None

def verificar_supervisiones_operativas():
    """Verificar origen de supervisiones operativas sin coordenadas"""
    
    print("🏗️ VERIFICAR SUPERVISIONES OPERATIVAS")
    print("=" * 60)
    
    df_ops = pd.read_excel("SUPERVISION_OPERATIVA_CAS_11_REV_250125_Submissions-2025-12-18__1228CST-1766141816.xlsx")
    
    # Operativas de interés
    ops_interes = df_ops[df_ops['Location'].isin(['38 - Gomez Morin', '71 - Centrito Valle'])].copy()
    
    print(f"📊 Operativas Gómez Morín + Centrito Valle: {len(ops_interes)}")
    print(f"\n{'#':<3} {'Location':<20} {'Fecha':<12} {'Usuario':<15} {'¿Location Map?':<15} {'Coords Map'}")
    print("-" * 90)
    
    resultados_ops = []
    
    for i, (idx, row) in enumerate(ops_interes.iterrows(), 1):
        location = row['Location']
        fecha_dt = pd.to_datetime(row['Date Submitted'])
        fecha_str = fecha_dt.strftime('%Y-%m-%d')
        usuario = row['Submitted By']
        
        # Verificar si tiene Location Map
        location_map = row.get('Location Map', None)
        
        if pd.notna(location_map):
            # Intentar extraer coordenadas
            lat_map, lon_map = extraer_coordenadas_google_maps(location_map)
            if lat_map and lon_map:
                tiene_map = "✅ SÍ"
                coords_str = f"{lat_map:.4f},{lon_map:.4f}"
            else:
                tiene_map = "⚠️ Texto"
                coords_str = "No extraíbles"
        else:
            tiene_map = "❌ NO"
            coords_str = "N/A"
            lat_map, lon_map = None, None
        
        print(f"{i:<3} {location[:19]:<20} {fecha_str:<12} {usuario:<15} {tiene_map:<15} {coords_str}")
        
        resultados_ops.append({
            'index': idx,
            'location': location,
            'fecha': fecha_dt,
            'usuario': usuario,
            'tiene_location_map': pd.notna(location_map),
            'lat_map': lat_map,
            'lon_map': lon_map,
            'tipo': 'OPERATIVA'
        })
    
    return resultados_ops

def verificar_supervisiones_seguridad():
    """Verificar origen de supervisiones seguridad sin coordenadas"""
    
    print(f"\n🛡️ VERIFICAR SUPERVISIONES SEGURIDAD")
    print("=" * 60)
    
    df_seg = pd.read_excel("CONTROL_OPERATIVO_DE_SEGURIDAD_CAS_11_REV_25012025_Submissions-2025-12-18__1232CST-1766104009.xlsx")
    
    # Seguridad de interés (incluyendo candidatos a mover)
    seg_interes = df_seg[
        (df_seg['Location'].isin(['38 - Gomez Morin', '71 - Centrito Valle'])) |
        (df_seg.index.isin([103, 145]))
    ].copy()
    
    print(f"📊 Seguridad Gómez Morín + Centrito Valle + candidatos: {len(seg_interes)}")
    print(f"\n{'#':<3} {'Index':<6} {'Location':<20} {'Fecha':<12} {'Usuario':<15} {'¿Location Map?':<15} {'Coords Map'}")
    print("-" * 105)
    
    resultados_seg = []
    
    for i, (idx, row) in enumerate(seg_interes.iterrows(), 1):
        location = row['Location']
        fecha_dt = pd.to_datetime(row['Date Submitted'])
        fecha_str = fecha_dt.strftime('%Y-%m-%d')
        usuario = row['Submitted By']
        
        # Verificar si tiene Location Map
        location_map = row.get('Location Map', None)
        
        if pd.notna(location_map):
            # Intentar extraer coordenadas
            lat_map, lon_map = extraer_coordenadas_google_maps(location_map)
            if lat_map and lon_map:
                tiene_map = "✅ SÍ"
                coords_str = f"{lat_map:.4f},{lon_map:.4f}"
            else:
                tiene_map = "⚠️ Texto"
                coords_str = "No extraíbles"
        else:
            tiene_map = "❌ NO"
            coords_str = "N/A"
            lat_map, lon_map = None, None
        
        print(f"{i:<3} {idx:<6} {location[:19]:<20} {fecha_str:<12} {usuario:<15} {tiene_map:<15} {coords_str}")
        
        resultados_seg.append({
            'index': idx,
            'location': location,
            'fecha': fecha_dt,
            'usuario': usuario,
            'tiene_location_map': pd.notna(location_map),
            'lat_map': lat_map,
            'lon_map': lon_map,
            'tipo': 'SEGURIDAD',
            'candidato_mover': idx in [103, 145]
        })
    
    return resultados_seg

def verificar_en_asignaciones_google_maps():
    """Verificar si alguna está en nuestras asignaciones por Google Maps"""
    
    print(f"\n🗺️ VERIFICAR EN ASIGNACIONES GOOGLE MAPS")
    print("=" * 60)
    
    try:
        df_asignaciones = pd.read_csv("ASIGNACIONES_FINALES_CORREGIDAS_20251218_140924.csv")
        
        # Filtrar asignaciones a Gómez Morín y Centrito Valle
        asig_gomez_centrito = df_asignaciones[
            df_asignaciones['sucursal_asignada'].isin(['38 - Gomez Morin', '71 - Centrito Valle'])
        ]
        
        print(f"📊 Asignaciones Google Maps a Gómez Morín + Centrito Valle: {len(asig_gomez_centrito)}")
        
        if len(asig_gomez_centrito) > 0:
            print(f"\n{'Index':<6} {'Sucursal':<20} {'Fecha':<12} {'Usuario':<15} {'Método'}")
            print("-" * 70)
            
            for _, row in asig_gomez_centrito.iterrows():
                fecha_str = pd.to_datetime(row['fecha']).strftime('%Y-%m-%d')
                print(f"{row['index_original']:<6} {row['sucursal_asignada'][:19]:<20} {fecha_str:<12} {row['usuario']:<15} {row['metodo']}")
        else:
            print("❌ No hay asignaciones Google Maps a estas sucursales")
            
        return asig_gomez_centrito
    except FileNotFoundError:
        print("❌ Archivo de asignaciones no encontrado")
        return pd.DataFrame()

def analizar_balance_fechas_final(resultados_ops, resultados_seg):
    """Analizar el balance final de fechas coincidentes"""
    
    print(f"\n📅 ANÁLISIS BALANCE FECHAS COINCIDENTES")
    print("=" * 60)
    
    # Agrupar por sucursal
    for sucursal in ['38 - Gomez Morin', '71 - Centrito Valle']:
        print(f"\n🏢 {sucursal}:")
        
        # Operativas de esta sucursal
        ops_sucursal = [r for r in resultados_ops if r['location'] == sucursal]
        seg_sucursal = [r for r in resultados_seg if r['location'] == sucursal]
        
        # Incluir candidatos si aplica
        if sucursal == '38 - Gomez Morin':
            # Agregar Index 103 que se movería a Gómez Morín
            candidato_103 = [r for r in resultados_seg if r['index'] == 103]
            seg_sucursal.extend(candidato_103)
        
        if sucursal == '71 - Centrito Valle':
            # Quitar Index 103 que se movería a Gómez Morín
            seg_sucursal = [r for r in seg_sucursal if r['index'] != 103]
        
        fechas_ops = [r['fecha'].date() for r in ops_sucursal]
        fechas_seg = [r['fecha'].date() for r in seg_sucursal]
        
        coincidencias = set(fechas_ops) & set(fechas_seg)
        
        print(f"   📊 Operativas: {len(ops_sucursal)} | Seguridad: {len(seg_sucursal)}")
        print(f"   📅 Fechas operativas: {sorted(fechas_ops)}")
        print(f"   📅 Fechas seguridad: {sorted(fechas_seg)}")
        print(f"   ✅ Coincidencias: {sorted(coincidencias)} ({len(coincidencias)} días)")
        
        # Evaluar balance ideal 4+4
        if len(ops_sucursal) == 4 and len(seg_sucursal) == 4 and len(coincidencias) == 4:
            print(f"   🎯 ESTADO: ✅ PERFECTO 4+4 con 4 coincidencias")
        else:
            print(f"   🎯 ESTADO: ⚠️ {len(ops_sucursal)}+{len(seg_sucursal)}={len(ops_sucursal)+len(seg_sucursal)} con {len(coincidencias)} coincidencias")

def main():
    """Función principal"""
    
    print("🔍 VERIFICAR ORIGEN SUPERVISIONES SIN COORDENADAS")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Determinar origen de supervisiones sin smetadata.lat/lon")
    print("=" * 80)
    
    # 1. Verificar operativas
    resultados_ops = verificar_supervisiones_operativas()
    
    # 2. Verificar seguridad
    resultados_seg = verificar_supervisiones_seguridad()
    
    # 3. Verificar en asignaciones Google Maps
    asignaciones = verificar_en_asignaciones_google_maps()
    
    # 4. Analizar balance final de fechas
    analizar_balance_fechas_final(resultados_ops, resultados_seg)
    
    print(f"\n🎯 CONCLUSIONES:")
    print(f"   📍 Supervisiones sin smetadata.lat/lon verificadas")
    print(f"   🗺️ Origen Location Map evaluado")
    print(f"   📅 Balance final de fechas analizado")
    print(f"   ✅ Si solo tenemos campo Sucursal y fechas, usamos esa validación")
    
    return resultados_ops, resultados_seg, asignaciones

if __name__ == "__main__":
    main()