#!/usr/bin/env python3
"""
🔍 VALIDAR COORDENADAS Y BALANCE GÓMEZ MORÍN - CENTRITO VALLE
Verificar coordenadas de entrega vs sucursales reales y balance 4+4 coincidente
"""

import pandas as pd
import numpy as np
import math
from datetime import datetime

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

def obtener_coordenadas_sucursales():
    """Obtener coordenadas reales de sucursales"""
    
    print("📍 COORDENADAS REALES SUCURSALES")
    print("=" * 50)
    
    df_sucursales = pd.read_csv('SUCURSALES_MASTER_20251218_110913.csv')
    
    coordenadas = {}
    
    for _, row in df_sucursales.iterrows():
        if pd.notna(row['numero']) and pd.notna(row['lat']) and pd.notna(row['lon']):
            numero = int(row['numero'])
            nombre = row['nombre']
            location_key = f"{numero} - {nombre}"
            
            if numero in [71, 38]:  # Centrito Valle y Gómez Morín
                coordenadas[location_key] = {
                    'lat': float(row['lat']),
                    'lon': float(row['lon']),
                    'numero': numero,
                    'nombre': nombre
                }
    
    print(f"📊 Sucursales encontradas: {len(coordenadas)}")
    for location, coords in coordenadas.items():
        print(f"   📍 {location}: {coords['lat']:.6f}, {coords['lon']:.6f}")
    
    return coordenadas

def validar_coordenadas_operativas(coordenadas_sucursales):
    """Validar coordenadas de entrega en operativas"""
    
    print(f"\n🏗️ VALIDACIÓN COORDENADAS OPERATIVAS")
    print("=" * 60)
    
    df_ops = pd.read_excel("SUPERVISION_OPERATIVA_CAS_11_REV_250125_Submissions-2025-12-18__1228CST-1766141816.xlsx")
    
    # Filtrar Gómez Morín y Centrito Valle
    ops_interes = df_ops[df_ops['Location'].isin(['38 - Gomez Morin', '71 - Centrito Valle'])].copy()
    
    print(f"📊 Operativas Gómez Morín + Centrito Valle: {len(ops_interes)}")
    
    validaciones = []
    
    print(f"\n{'#':<3} {'Location':<20} {'Fecha':<12} {'Usuario':<15} {'Coords Entrega':<20} {'Dist':<6} {'✓'}")
    print("-" * 90)
    
    for i, (idx, row) in enumerate(ops_interes.iterrows(), 1):
        location = row['Location']
        fecha_dt = pd.to_datetime(row['Date Submitted'])
        fecha_str = fecha_dt.strftime('%Y-%m-%d')
        usuario = row['Submitted By']
        
        # Obtener coordenadas de entrega (smetadata.lat/lon)
        lat_entrega = row.get('smetadata.lat', None)
        lon_entrega = row.get('smetadata.lon', None)
        
        if pd.notna(lat_entrega) and pd.notna(lon_entrega):
            lat_entrega = float(lat_entrega)
            lon_entrega = float(lon_entrega)
            coords_str = f"{lat_entrega:.4f},{lon_entrega:.4f}"
            
            # Calcular distancia a sucursal asignada
            sucursal_coords = coordenadas_sucursales.get(location, {})
            if sucursal_coords:
                distancia = calcular_distancia_haversine(
                    lat_entrega, lon_entrega,
                    sucursal_coords['lat'], sucursal_coords['lon']
                )
                
                # Verificar si está cerca (< 3km)
                valido = distancia < 3.0
                valido_str = "✅" if valido else "❌"
                dist_str = f"{distancia:.2f}km"
                
                validaciones.append({
                    'index': idx,
                    'location': location,
                    'fecha': fecha_dt,
                    'usuario': usuario,
                    'lat_entrega': lat_entrega,
                    'lon_entrega': lon_entrega,
                    'distancia': distancia,
                    'valido': valido,
                    'tipo': 'OPERATIVA'
                })
                
                print(f"{i:<3} {location[:19]:<20} {fecha_str:<12} {usuario:<15} {coords_str:<20} {dist_str:<6} {valido_str}")
            else:
                print(f"{i:<3} {location[:19]:<20} {fecha_str:<12} {usuario:<15} {'SIN COORDS REF':<20} {'--':<6} ❓")
        else:
            print(f"{i:<3} {location[:19]:<20} {fecha_str:<12} {usuario:<15} {'SIN COORDS':<20} {'--':<6} ❓")
    
    return validaciones

def validar_coordenadas_seguridad(coordenadas_sucursales):
    """Validar coordenadas de entrega en seguridad"""
    
    print(f"\n🛡️ VALIDACIÓN COORDENADAS SEGURIDAD")
    print("=" * 60)
    
    df_seg = pd.read_excel("CONTROL_OPERATIVO_DE_SEGURIDAD_CAS_11_REV_25012025_Submissions-2025-12-18__1232CST-1766104009.xlsx")
    
    # Filtrar Gómez Morín y Centrito Valle (incluyendo los que se van a mover)
    seg_interes = df_seg[
        (df_seg['Location'].isin(['38 - Gomez Morin', '71 - Centrito Valle'])) |
        (df_seg.index.isin([103, 145]))  # Los candidatos a mover
    ].copy()
    
    print(f"📊 Seguridad Gómez Morín + Centrito Valle + candidatos: {len(seg_interes)}")
    
    validaciones = []
    
    print(f"\n{'#':<3} {'Index':<6} {'Location':<20} {'Fecha':<12} {'Usuario':<15} {'Coords':<20} {'Dist':<6} {'✓'}")
    print("-" * 100)
    
    for i, (idx, row) in enumerate(seg_interes.iterrows(), 1):
        location = row['Location']
        fecha_dt = pd.to_datetime(row['Date Submitted'])
        fecha_str = fecha_dt.strftime('%Y-%m-%d')
        usuario = row['Submitted By']
        
        # Obtener coordenadas de entrega
        lat_entrega = row.get('smetadata.lat', None)
        lon_entrega = row.get('smetadata.lon', None)
        
        # Para candidatos específicos, evaluar contra ambas sucursales
        sucursales_evaluar = []
        if idx in [103, 145]:  # Candidatos de Centrito que dicen "Gómez Morín"
            sucursales_evaluar = ['38 - Gomez Morin', '71 - Centrito Valle']
        else:
            sucursales_evaluar = [location]
        
        mejor_match = None
        mejor_distancia = float('inf')
        
        if pd.notna(lat_entrega) and pd.notna(lon_entrega):
            lat_entrega = float(lat_entrega)
            lon_entrega = float(lon_entrega)
            coords_str = f"{lat_entrega:.4f},{lon_entrega:.4f}"
            
            # Evaluar contra todas las sucursales candidatas
            for sucursal_evaluar in sucursales_evaluar:
                sucursal_coords = coordenadas_sucursales.get(sucursal_evaluar, {})
                if sucursal_coords:
                    distancia = calcular_distancia_haversine(
                        lat_entrega, lon_entrega,
                        sucursal_coords['lat'], sucursal_coords['lon']
                    )
                    
                    if distancia < mejor_distancia:
                        mejor_distancia = distancia
                        mejor_match = sucursal_evaluar
            
            # Validar resultado
            valido = mejor_distancia < 3.0
            valido_str = "✅" if valido else "❌"
            dist_str = f"{mejor_distancia:.2f}km"
            
            # Si es candidato, mostrar a qué sucursal realmente pertenece
            if idx in [103, 145]:
                location_real = mejor_match if mejor_match else "AMBIGUO"
                print(f"{i:<3} {idx:<6} {location[:19]:<20} {fecha_str:<12} {usuario:<15} {coords_str:<20} {dist_str:<6} {valido_str} → {location_real}")
            else:
                print(f"{i:<3} {idx:<6} {location[:19]:<20} {fecha_str:<12} {usuario:<15} {coords_str:<20} {dist_str:<6} {valido_str}")
            
            validaciones.append({
                'index': idx,
                'location_original': location,
                'location_real': mejor_match,
                'fecha': fecha_dt,
                'usuario': usuario,
                'lat_entrega': lat_entrega,
                'lon_entrega': lon_entrega,
                'distancia': mejor_distancia,
                'valido': valido,
                'tipo': 'SEGURIDAD'
            })
        else:
            print(f"{i:<3} {idx:<6} {location[:19]:<20} {fecha_str:<12} {usuario:<15} {'SIN COORDS':<20} {'--':<6} ❓")
    
    return validaciones

def analizar_balance_4_4_con_coordenadas(validaciones_ops, validaciones_seg):
    """Analizar balance 4+4 considerando validación de coordenadas"""
    
    print(f"\n⚖️ ANÁLISIS BALANCE 4+4 CON VALIDACIÓN COORDENADAS")
    print("=" * 70)
    
    # Filtrar solo validaciones correctas (coordenadas válidas)
    ops_validas = [v for v in validaciones_ops if v.get('valido', False)]
    seg_validas = [v for v in validaciones_seg if v.get('valido', False)]
    
    print(f"📊 OPERATIVAS VÁLIDAS (coordenadas < 3km): {len(ops_validas)}")
    print(f"📊 SEGURIDAD VÁLIDAS (coordenadas < 3km): {len(seg_validas)}")
    
    # Agrupar por sucursal real (según coordenadas)
    balance_gomez = {'ops': [], 'seg': []}
    balance_centrito = {'ops': [], 'seg': []}
    
    # Operativas
    for op in ops_validas:
        if 'Gomez Morin' in op['location']:
            balance_gomez['ops'].append(op)
        elif 'Centrito Valle' in op['location']:
            balance_centrito['ops'].append(op)
    
    # Seguridad (usar location_real si está disponible)
    for seg in seg_validas:
        location_real = seg.get('location_real', seg['location_original'])
        if 'Gomez Morin' in location_real:
            balance_gomez['seg'].append(seg)
        elif 'Centrito Valle' in location_real:
            balance_centrito['seg'].append(seg)
    
    print(f"\n📊 BALANCE POR COORDENADAS REALES:")
    print(f"   🏢 Gómez Morín: {len(balance_gomez['ops'])} ops + {len(balance_gomez['seg'])} seg = {len(balance_gomez['ops']) + len(balance_gomez['seg'])}")
    print(f"   🏢 Centrito Valle: {len(balance_centrito['ops'])} ops + {len(balance_centrito['seg'])} seg = {len(balance_centrito['ops']) + len(balance_centrito['seg'])}")
    
    # Verificar coincidencias de fechas
    print(f"\n📅 VERIFICAR COINCIDENCIAS DE FECHAS:")
    
    for sucursal_nombre, balance in [('Gómez Morín', balance_gomez), ('Centrito Valle', balance_centrito)]:
        print(f"\n🏢 {sucursal_nombre}:")
        
        fechas_ops = [op['fecha'].date() for op in balance['ops']]
        fechas_seg = [seg['fecha'].date() for seg in balance['seg']]
        
        coincidencias = set(fechas_ops) & set(fechas_seg)
        
        print(f"   📅 Fechas operativas: {sorted(fechas_ops)}")
        print(f"   📅 Fechas seguridad: {sorted(fechas_seg)}")
        print(f"   ✅ Coincidencias: {sorted(coincidencias)} ({len(coincidencias)} días)")
        
        # Balance ideal sería 4+4 con 4 fechas coincidentes
        balance_ops = len(balance['ops'])
        balance_seg = len(balance['seg'])
        balance_total = balance_ops + balance_seg
        
        estado = ""
        if balance_total == 8 and len(coincidencias) == 4:
            estado = "✅ PERFECTO"
        elif balance_total == 8:
            estado = f"⚠️ 8 total pero solo {len(coincidencias)} coincidencias"
        else:
            estado = f"❌ {balance_total}/8 total, {len(coincidencias)} coincidencias"
        
        print(f"   📊 Estado: {estado}")
    
    return balance_gomez, balance_centrito

def main():
    """Función principal"""
    
    print("🔍 VALIDAR COORDENADAS Y BALANCE GÓMEZ MORÍN - CENTRITO VALLE")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Validar coordenadas entrega vs reales + balance 4+4 coincidente")
    print("=" * 80)
    
    # 1. Obtener coordenadas reales de sucursales
    coordenadas_sucursales = obtener_coordenadas_sucursales()
    
    # 2. Validar coordenadas operativas
    validaciones_ops = validar_coordenadas_operativas(coordenadas_sucursales)
    
    # 3. Validar coordenadas seguridad
    validaciones_seg = validar_coordenadas_seguridad(coordenadas_sucursales)
    
    # 4. Analizar balance 4+4 con coordenadas
    balance_gomez, balance_centrito = analizar_balance_4_4_con_coordenadas(validaciones_ops, validaciones_seg)
    
    print(f"\n🎯 CONCLUSIÓN:")
    print(f"   ✅ Coordenadas validadas contra ubicaciones reales")
    print(f"   📊 Balance evaluado según coordenadas, no solo asignaciones")
    print(f"   📅 Coincidencias de fechas verificadas")
    print(f"   💡 Solo proceder con movimientos si coordenadas + fechas coinciden")
    
    return coordenadas_sucursales, validaciones_ops, validaciones_seg, balance_gomez, balance_centrito

if __name__ == "__main__":
    main()