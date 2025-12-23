#!/usr/bin/env python3
"""
📋 LISTADO COMPLETO SUPERVISIONES POR SUCURSAL
Mostrar TODAS las supervisiones (operativas + seguridad) por sucursal
con fechas y validación de coordenadas vs coordenadas normalizadas
"""

import pandas as pd
import numpy as np
import math
from datetime import datetime
import re

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

def extraer_coordenadas_google_maps(location_map_text):
    """Extraer coordenadas de texto de Google Maps"""
    if pd.isna(location_map_text):
        return None, None
    
    texto = str(location_map_text)
    
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

def cargar_coordenadas_normalizadas():
    """Cargar coordenadas normalizadas de sucursales"""
    
    print("📍 CARGANDO COORDENADAS NORMALIZADAS")
    print("=" * 50)
    
    df_sucursales = pd.read_csv('SUCURSALES_MASTER_20251218_110913.csv')
    
    coordenadas_normalizadas = {}
    
    for _, row in df_sucursales.iterrows():
        if pd.notna(row['numero']) and pd.notna(row['lat']) and pd.notna(row['lon']):
            numero = int(row['numero'])
            nombre = row['nombre']
            location_key = f"{numero} - {nombre}"
            
            coordenadas_normalizadas[location_key] = {
                'lat': float(row['lat']),
                'lon': float(row['lon']),
                'numero': numero,
                'nombre': nombre,
                'tipo': row.get('tipo', 'DESCONOCIDO')
            }
    
    print(f"✅ {len(coordenadas_normalizadas)} sucursales con coordenadas cargadas")
    return coordenadas_normalizadas

def procesar_operativas(coordenadas_normalizadas):
    """Procesar todas las operativas con validación de coordenadas"""
    
    print(f"\n🏗️ PROCESANDO OPERATIVAS")
    print("=" * 40)
    
    df_ops = pd.read_excel("SUPERVISION_OPERATIVA_CAS_11_REV_250125_Submissions-2025-12-18__1228CST-1766141816.xlsx")
    
    operativas_procesadas = []
    
    for idx, row in df_ops.iterrows():
        if pd.notna(row['Location']):
            location = row['Location']
            fecha_dt = pd.to_datetime(row['Date Submitted'])
            usuario = row['Submitted By']
            
            # Obtener coordenadas de entrega
            lat_entrega = row.get('smetadata.lat', None)
            lon_entrega = row.get('smetadata.lon', None)
            location_map = row.get('Location Map', None)
            
            # Si no hay smetadata, intentar extraer de Location Map
            if pd.isna(lat_entrega) or pd.isna(lon_entrega):
                lat_map, lon_map = extraer_coordenadas_google_maps(location_map)
                if lat_map and lon_map:
                    lat_entrega, lon_entrega = lat_map, lon_map
                    coord_origen = "LOCATION_MAP"
                else:
                    coord_origen = "SIN_COORDENADAS"
            else:
                coord_origen = "SMETADATA"
            
            # Validar contra coordenadas normalizadas
            distancia_validacion = float('inf')
            coords_validas = False
            
            if pd.notna(lat_entrega) and pd.notna(lon_entrega):
                coords_sucursal = coordenadas_normalizadas.get(location, {})
                if coords_sucursal:
                    distancia_validacion = calcular_distancia_haversine(
                        lat_entrega, lon_entrega,
                        coords_sucursal['lat'], coords_sucursal['lon']
                    )
                    coords_validas = distancia_validacion < 3.0
            
            operativas_procesadas.append({
                'index_excel': idx,
                'location': location,
                'fecha': fecha_dt,
                'fecha_str': fecha_dt.strftime('%Y-%m-%d'),
                'usuario': usuario,
                'lat_entrega': lat_entrega,
                'lon_entrega': lon_entrega,
                'coord_origen': coord_origen,
                'distancia_validacion': distancia_validacion,
                'coords_validas': coords_validas,
                'tipo': 'OPERATIVA'
            })
    
    print(f"✅ {len(operativas_procesadas)} operativas procesadas")
    return operativas_procesadas

def procesar_seguridad_excel(coordenadas_normalizadas):
    """Procesar seguridad del Excel original con coordenadas"""
    
    print(f"\n🛡️ PROCESANDO SEGURIDAD EXCEL")
    print("=" * 40)
    
    df_seg = pd.read_excel("CONTROL_OPERATIVO_DE_SEGURIDAD_CAS_11_REV_25012025_Submissions-2025-12-18__1232CST-1766104009.xlsx")
    
    seguridad_procesada = []
    
    for idx, row in df_seg.iterrows():
        if pd.notna(row['Location']):
            location = row['Location']
            fecha_dt = pd.to_datetime(row['Date Submitted'])
            usuario = row['Submitted By']
            
            # Obtener coordenadas de entrega
            lat_entrega = row.get('smetadata.lat', None)
            lon_entrega = row.get('smetadata.lon', None)
            location_map = row.get('Location Map', None)
            
            # Si no hay smetadata, intentar extraer de Location Map
            if pd.isna(lat_entrega) or pd.isna(lon_entrega):
                lat_map, lon_map = extraer_coordenadas_google_maps(location_map)
                if lat_map and lon_map:
                    lat_entrega, lon_entrega = lat_map, lon_map
                    coord_origen = "LOCATION_MAP"
                else:
                    coord_origen = "SIN_COORDENADAS"
            else:
                coord_origen = "SMETADATA"
            
            # Validar contra coordenadas normalizadas
            distancia_validacion = float('inf')
            coords_validas = False
            
            if pd.notna(lat_entrega) and pd.notna(lon_entrega):
                coords_sucursal = coordenadas_normalizadas.get(location, {})
                if coords_sucursal:
                    distancia_validacion = calcular_distancia_haversine(
                        lat_entrega, lon_entrega,
                        coords_sucursal['lat'], coords_sucursal['lon']
                    )
                    coords_validas = distancia_validacion < 3.0
            
            seguridad_procesada.append({
                'index_excel': idx,
                'location': location,
                'fecha': fecha_dt,
                'fecha_str': fecha_dt.strftime('%Y-%m-%d'),
                'usuario': usuario,
                'lat_entrega': lat_entrega,
                'lon_entrega': lon_entrega,
                'coord_origen': coord_origen,
                'distancia_validacion': distancia_validacion,
                'coords_validas': coords_validas,
                'tipo': 'SEGURIDAD_EXCEL'
            })
    
    print(f"✅ {len(seguridad_procesada)} supervisiones seguridad Excel procesadas")
    return seguridad_procesada

def procesar_seguridad_asignaciones():
    """Procesar seguridad de asignaciones Google Maps"""
    
    print(f"\n🗺️ PROCESANDO SEGURIDAD ASIGNACIONES")
    print("=" * 40)
    
    try:
        df_asignaciones = pd.read_csv("ASIGNACIONES_NORMALIZADAS_TEMP.csv")
        
        seguridad_asignada = []
        
        for _, row in df_asignaciones.iterrows():
            seguridad_asignada.append({
                'index_excel': row['index_original'],
                'location': row['sucursal_asignada'],
                'fecha': pd.to_datetime(row['fecha']),
                'fecha_str': pd.to_datetime(row['fecha']).strftime('%Y-%m-%d'),
                'usuario': row['usuario'],
                'lat_entrega': row.get('lat_google_maps', None),
                'lon_entrega': row.get('lon_google_maps', None),
                'coord_origen': "GOOGLE_MAPS_ASIGNACION",
                'distancia_validacion': row.get('distancia_km', float('inf')),
                'coords_validas': row.get('distancia_km', float('inf')) < 3.0,
                'metodo': row.get('metodo', 'ASIGNACION'),
                'tipo': 'SEGURIDAD_ASIGNADA'
            })
        
        print(f"✅ {len(seguridad_asignada)} asignaciones procesadas")
        return seguridad_asignada
        
    except FileNotFoundError:
        print("⚠️ Archivo de asignaciones no encontrado")
        return []

def generar_listado_por_sucursal(operativas, seguridad_excel, seguridad_asignada):
    """Generar listado completo por sucursal"""
    
    print(f"\n📋 GENERANDO LISTADO POR SUCURSAL")
    print("=" * 60)
    
    # Combinar todas las supervisiones
    todas_supervisiones = operativas + seguridad_excel + seguridad_asignada
    
    # Agrupar por sucursal
    por_sucursal = {}
    
    for supervision in todas_supervisiones:
        location = supervision['location']
        if location not in por_sucursal:
            por_sucursal[location] = {
                'operativas': [],
                'seguridad_excel': [],
                'seguridad_asignada': []
            }
        
        if supervision['tipo'] == 'OPERATIVA':
            por_sucursal[location]['operativas'].append(supervision)
        elif supervision['tipo'] == 'SEGURIDAD_EXCEL':
            por_sucursal[location]['seguridad_excel'].append(supervision)
        elif supervision['tipo'] == 'SEGURIDAD_ASIGNADA':
            por_sucursal[location]['seguridad_asignada'].append(supervision)
    
    return por_sucursal

def mostrar_listado_detallado(por_sucursal, sucursales_focus=None):
    """Mostrar listado detallado de supervisiones por sucursal"""
    
    print(f"\n📊 LISTADO DETALLADO POR SUCURSAL")
    print("=" * 80)
    
    # Si no se especifican sucursales, mostrar todas ordenadas
    if sucursales_focus is None:
        sucursales_mostrar = sorted(por_sucursal.keys())
    else:
        sucursales_mostrar = sucursales_focus
    
    for sucursal in sucursales_mostrar:
        if sucursal not in por_sucursal:
            print(f"\n❌ {sucursal}: No encontrada")
            continue
            
        data = por_sucursal[sucursal]
        ops = data['operativas']
        seg_excel = data['seguridad_excel']
        seg_asig = data['seguridad_asignada']
        
        total = len(ops) + len(seg_excel) + len(seg_asig)
        
        print(f"\n🏢 {sucursal}")
        print(f"📊 Total: {len(ops)} ops + {len(seg_excel)} seg_excel + {len(seg_asig)} seg_asig = {total}")
        print("=" * 80)
        
        # Mostrar operativas
        if ops:
            print(f"🏗️ OPERATIVAS ({len(ops)}):")
            print(f"{'Index':<6} {'Fecha':<12} {'Usuario':<15} {'Coords':<8} {'Dist':<6} {'✓'}")
            print("-" * 60)
            
            for op in sorted(ops, key=lambda x: x['fecha']):
                coords_str = "SÍ" if op['coords_validas'] else "NO"
                dist_str = f"{op['distancia_validacion']:.2f}" if op['distancia_validacion'] < float('inf') else "--"
                valid_str = "✅" if op['coords_validas'] else "❌"
                
                print(f"{op['index_excel']:<6} {op['fecha_str']:<12} {op['usuario']:<15} {coords_str:<8} {dist_str:<6} {valid_str}")
        
        # Mostrar seguridad Excel
        if seg_excel:
            print(f"\n🛡️ SEGURIDAD EXCEL ({len(seg_excel)}):")
            print(f"{'Index':<6} {'Fecha':<12} {'Usuario':<15} {'Coords':<8} {'Dist':<6} {'✓'}")
            print("-" * 60)
            
            for seg in sorted(seg_excel, key=lambda x: x['fecha']):
                coords_str = "SÍ" if seg['coords_validas'] else "NO"
                dist_str = f"{seg['distancia_validacion']:.2f}" if seg['distancia_validacion'] < float('inf') else "--"
                valid_str = "✅" if seg['coords_validas'] else "❌"
                
                print(f"{seg['index_excel']:<6} {seg['fecha_str']:<12} {seg['usuario']:<15} {coords_str:<8} {dist_str:<6} {valid_str}")
        
        # Mostrar seguridad asignada
        if seg_asig:
            print(f"\n🗺️ SEGURIDAD ASIGNADA ({len(seg_asig)}):")
            print(f"{'Index':<6} {'Fecha':<12} {'Usuario':<15} {'Método':<15} {'Dist':<6} {'✓'}")
            print("-" * 70)
            
            for seg in sorted(seg_asig, key=lambda x: x['fecha']):
                metodo = seg.get('metodo', 'ASIGNACION')[:14]
                dist_str = f"{seg['distancia_validacion']:.2f}" if seg['distancia_validacion'] < float('inf') else "--"
                valid_str = "✅" if seg['coords_validas'] else "❌"
                
                print(f"{seg['index_excel']:<6} {seg['fecha_str']:<12} {seg['usuario']:<15} {metodo:<15} {dist_str:<6} {valid_str}")
        
        # Análisis de coincidencias
        fechas_ops = {op['fecha_str'] for op in ops}
        fechas_seg_excel = {seg['fecha_str'] for seg in seg_excel}
        fechas_seg_asig = {seg['fecha_str'] for seg in seg_asig}
        fechas_seg_todas = fechas_seg_excel | fechas_seg_asig
        
        coincidencias = fechas_ops & fechas_seg_todas
        
        print(f"\n📅 ANÁLISIS FECHAS:")
        print(f"   Ops: {sorted(fechas_ops)}")
        print(f"   Seg: {sorted(fechas_seg_todas)}")
        print(f"   ✅ Coincidencias: {sorted(coincidencias)} ({len(coincidencias)} días)")

def main():
    """Función principal"""
    
    print("📋 LISTADO COMPLETO SUPERVISIONES POR SUCURSAL")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Mostrar TODAS las supervisiones con validación de coordenadas")
    print("=" * 80)
    
    # 1. Cargar coordenadas normalizadas
    coordenadas_normalizadas = cargar_coordenadas_normalizadas()
    
    # 2. Procesar operativas
    operativas = procesar_operativas(coordenadas_normalizadas)
    
    # 3. Procesar seguridad Excel
    seguridad_excel = procesar_seguridad_excel(coordenadas_normalizadas)
    
    # 4. Procesar seguridad asignaciones
    seguridad_asignada = procesar_seguridad_asignaciones()
    
    # 5. Generar listado por sucursal
    por_sucursal = generar_listado_por_sucursal(operativas, seguridad_excel, seguridad_asignada)
    
    # 6. Mostrar sucursales de interés (Santa Catarina y Garcia para empezar)
    sucursales_focus = [
        '4 - Santa Catarina',
        '6 - Garcia',
        '38 - Gomez Morin',
        '71 - Centrito Valle'
    ]
    
    mostrar_listado_detallado(por_sucursal, sucursales_focus)
    
    print(f"\n💡 PRÓXIMO PASO:")
    print(f"   🎯 Roberto puede revisar fechas y coordenadas")
    print(f"   📅 Validar coincidencias reales")
    print(f"   📍 Confirmar si las coordenadas validan las asignaciones")
    
    return por_sucursal, operativas, seguridad_excel, seguridad_asignada

if __name__ == "__main__":
    main()