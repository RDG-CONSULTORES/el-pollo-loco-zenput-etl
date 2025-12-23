#!/usr/bin/env python3
"""
🔍 ANÁLISIS DE PATRONES GÓMEZ MORÍN vs CENTRITO VALLE
Comparar fechas y coordenadas para detectar patrones de proximidad
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
    """Obtener coordenadas de Centrito Valle y Gómez Morín"""
    
    print("📍 COORDENADAS DE SUCURSALES")
    print("=" * 40)
    
    df_sucursales = pd.read_csv('SUCURSALES_MASTER_20251218_110913.csv')
    
    coordenadas = {}
    
    for _, row in df_sucursales.iterrows():
        if pd.notna(row['numero']) and pd.notna(row['lat']) and pd.notna(row['lon']):
            numero = int(row['numero'])
            nombre = row['nombre']
            location_key = f"{numero} - {nombre}"
            
            if numero == 71 or numero == 38:  # Centrito Valle o Gómez Morín
                coordenadas[location_key] = {
                    'numero': numero,
                    'nombre': nombre,
                    'lat': float(row['lat']),
                    'lon': float(row['lon']),
                    'grupo': row.get('grupo', ''),
                    'tipo': row.get('tipo', '')
                }
    
    # Mostrar coordenadas
    for location, coords in coordenadas.items():
        print(f"📍 {location}:")
        print(f"   📍 Lat: {coords['lat']:.6f}")
        print(f"   📍 Lon: {coords['lon']:.6f}")
        print(f"   🏢 Grupo: {coords['grupo']}")
    
    # Calcular distancia entre sucursales
    if len(coordenadas) == 2:
        coords_list = list(coordenadas.values())
        distancia = calcular_distancia_haversine(
            coords_list[0]['lat'], coords_list[0]['lon'],
            coords_list[1]['lat'], coords_list[1]['lon']
        )
        print(f"\n📏 DISTANCIA ENTRE SUCURSALES: {distancia:.3f} km")
        
        if distancia < 1.0:
            print(f"   ✅ MUY CERCANAS (< 1km) - Redistribución geográficamente lógica")
        elif distancia < 3.0:
            print(f"   ⚠️ CERCANAS (< 3km) - Redistribución aceptable")
        else:
            print(f"   ❌ LEJANAS (> 3km) - Revisar redistribución")
    
    return coordenadas

def analizar_fechas_gomez_morin():
    """Analizar fechas de operativas de Gómez Morín"""
    
    print(f"\n🔍 ANÁLISIS FECHAS GÓMEZ MORÍN")
    print("=" * 50)
    
    df_ops = pd.read_excel("SUPERVISION_OPERATIVA_CAS_11_REV_250125_Submissions-2025-12-18__1228CST-1766141816.xlsx")
    
    # Operativas de Gómez Morín
    ops_gomez = df_ops[df_ops['Location'] == '38 - Gomez Morin'].copy()
    
    print(f"🏗️ OPERATIVAS GÓMEZ MORÍN ({len(ops_gomez)}):")
    print(f"{'#':<3} {'Fecha':<12} {'Hora':<8} {'Usuario':<15} {'Index':<8}")
    print("-" * 60)
    
    gomez_fechas = []
    for i, (idx, row) in enumerate(ops_gomez.iterrows(), 1):
        fecha_dt = pd.to_datetime(row['Date Submitted'])
        fecha_str = fecha_dt.strftime('%Y-%m-%d')
        hora_str = fecha_dt.strftime('%H:%M')
        usuario = row['Submitted By']
        
        print(f"{i:<3} {fecha_str:<12} {hora_str:<8} {usuario:<15} {idx:<8}")
        
        gomez_fechas.append({
            'numero': i,
            'fecha': fecha_dt.date(),
            'fecha_completa': fecha_dt,
            'usuario': usuario,
            'index_excel': idx
        })
    
    return gomez_fechas

def comparar_patrones_fechas(gomez_fechas):
    """Comparar patrones de fechas entre Gómez Morín y opciones de Centrito Valle"""
    
    print(f"\n📅 COMPARACIÓN PATRONES DE FECHAS")
    print("=" * 60)
    
    # Opciones de Centrito Valle (solo Israel Garcia)
    opciones_israel = [
        {'id': 'O1', 'fecha': '2025-11-18', 'usuario': 'Israel Garcia', 'index': 30},
        {'id': 'O3', 'fecha': '2025-07-02', 'usuario': 'Israel Garcia', 'index': 145},
        {'id': 'O5', 'fecha': '2025-04-16', 'usuario': 'Israel Garcia', 'index': 201}
    ]
    
    print(f"🔍 OPCIONES ISRAEL GARCIA EN CENTRITO:")
    for opcion in opciones_israel:
        print(f"   {opcion['id']}: {opcion['fecha']} (Index: {opcion['index']})")
    
    print(f"\n📊 ANÁLISIS DE PROXIMIDAD TEMPORAL:")
    
    # Para cada opción de Israel en Centrito, buscar fechas cercanas en Gómez Morín
    for opcion in opciones_israel:
        fecha_centrito = pd.to_datetime(opcion['fecha']).date()
        
        print(f"\n🔸 {opcion['id']} - {opcion['fecha']}:")
        
        # Buscar fechas cercanas en Gómez Morín (±7 días)
        fechas_cercanas = []
        
        for gomez in gomez_fechas:
            diferencia_dias = abs((fecha_centrito - gomez['fecha']).days)
            
            if diferencia_dias <= 7:  # Dentro de 7 días
                fechas_cercanas.append({
                    'fecha_gomez': gomez['fecha'],
                    'usuario_gomez': gomez['usuario'],
                    'diferencia_dias': diferencia_dias,
                    'index_gomez': gomez['index_excel']
                })
        
        if fechas_cercanas:
            print(f"   ✅ Fechas cercanas en Gómez Morín:")
            for cercana in sorted(fechas_cercanas, key=lambda x: x['diferencia_dias']):
                dias_str = f"mismo día" if cercana['diferencia_dias'] == 0 else f"{cercana['diferencia_dias']} días"
                print(f"      📅 {cercana['fecha_gomez']} ({dias_str}) - {cercana['usuario_gomez']}")
        else:
            print(f"   ❌ No hay fechas cercanas en Gómez Morín")
    
    return opciones_israel

def analizar_patrones_usuarios(gomez_fechas):
    """Analizar patrones de usuarios"""
    
    print(f"\n👤 ANÁLISIS PATRONES DE USUARIOS")
    print("=" * 50)
    
    # Usuarios en Gómez Morín
    usuarios_gomez = [f['usuario'] for f in gomez_fechas]
    usuarios_unicos = list(set(usuarios_gomez))
    
    print(f"👥 USUARIOS EN GÓMEZ MORÍN:")
    for usuario in usuarios_unicos:
        count = usuarios_gomez.count(usuario)
        print(f"   👤 {usuario}: {count} operativas")
    
    # Verificar si Israel García trabaja en Gómez Morín
    israel_en_gomez = 'Israel Garcia' in usuarios_unicos
    
    print(f"\n🔍 ANÁLISIS ISRAEL GARCIA:")
    if israel_en_gomez:
        fechas_israel_gomez = [f for f in gomez_fechas if f['usuario'] == 'Israel Garcia']
        print(f"   ✅ Israel Garcia SÍ trabaja en Gómez Morín ({len(fechas_israel_gomez)} operativas)")
        print(f"   📅 Fechas de Israel en Gómez Morín:")
        for fecha in fechas_israel_gomez:
            print(f"      📅 {fecha['fecha']}")
    else:
        print(f"   ❌ Israel Garcia NO trabaja en Gómez Morín")
    
    return israel_en_gomez, usuarios_unicos

def recomendar_redistribucion_optima(opciones_israel, gomez_fechas, israel_en_gomez):
    """Recomendar la redistribución óptima basada en patrones"""
    
    print(f"\n🎯 RECOMENDACIÓN ÓPTIMA DE REDISTRIBUCIÓN")
    print("=" * 60)
    
    print(f"📊 CRITERIOS DE EVALUACIÓN:")
    print(f"   ✅ Sucursales cercanas geográficamente")
    print(f"   👤 Usuario: Debe ser Israel Garcia")
    print(f"   📅 Proximidad temporal con operativas Gómez Morín")
    print(f"   🔧 Facilidad de justificación del cambio")
    
    # Evaluar cada opción
    evaluaciones = []
    
    for opcion in opciones_israel:
        fecha_centrito = pd.to_datetime(opcion['fecha']).date()
        
        # Calcular proximidad temporal mínima
        min_diferencia = float('inf')
        fecha_mas_cercana = None
        
        for gomez in gomez_fechas:
            diferencia = abs((fecha_centrito - gomez['fecha']).days)
            if diferencia < min_diferencia:
                min_diferencia = diferencia
                fecha_mas_cercana = gomez['fecha']
        
        # Evaluar opción
        score = 0
        criterios = []
        
        # Usuario Israel Garcia (+20 puntos)
        if opcion['usuario'] == 'Israel Garcia':
            score += 20
            criterios.append("✅ Israel Garcia")
        
        # Proximidad temporal
        if min_diferencia == 0:
            score += 15
            criterios.append("✅ Mismo día en Gómez Morín")
        elif min_diferencia <= 3:
            score += 10
            criterios.append(f"✅ {min_diferencia} días de diferencia")
        elif min_diferencia <= 7:
            score += 5
            criterios.append(f"⚠️ {min_diferencia} días de diferencia")
        else:
            criterios.append(f"❌ {min_diferencia} días de diferencia")
        
        # Recencia (fechas más recientes son mejores para justificar)
        if '2025-11' in opcion['fecha']:
            score += 10
            criterios.append("✅ Muy reciente (Nov)")
        elif '2025-07' in opcion['fecha']:
            score += 5
            criterios.append("⚠️ Reciente (Jul)")
        else:
            criterios.append("❌ Menos reciente")
        
        evaluaciones.append({
            'opcion': opcion,
            'score': score,
            'min_diferencia': min_diferencia,
            'fecha_mas_cercana': fecha_mas_cercana,
            'criterios': criterios
        })
    
    # Ordenar por score
    evaluaciones.sort(key=lambda x: x['score'], reverse=True)
    
    print(f"\n📋 EVALUACIÓN DE OPCIONES:")
    print(f"{'Opción':<6} {'Fecha':<12} {'Score':<6} {'Criterios'}")
    print("-" * 80)
    
    for eval in evaluaciones:
        opcion = eval['opcion']
        criterios_str = '; '.join(eval['criterios'][:2])  # Primeros 2 criterios
        print(f"{opcion['id']:<6} {opcion['fecha']:<12} {eval['score']:<6} {criterios_str}")
    
    # Recomendación final
    mejor_opcion = evaluaciones[0]
    
    print(f"\n🏆 RECOMENDACIÓN FINAL:")
    print(f"   🎯 Opción: {mejor_opcion['opcion']['id']} - {mejor_opcion['opcion']['fecha']}")
    print(f"   👤 Usuario: {mejor_opcion['opcion']['usuario']}")
    print(f"   📊 Score: {mejor_opcion['score']}/45")
    print(f"   📅 Fecha más cercana en Gómez Morín: {mejor_opcion['fecha_mas_cercana']} ({mejor_opcion['min_diferencia']} días)")
    print(f"   📋 Criterios cumplidos:")
    for criterio in mejor_opcion['criterios']:
        print(f"      {criterio}")
    
    return mejor_opcion

def main():
    """Función principal"""
    
    print("🔍 ANÁLISIS PATRONES GÓMEZ MORÍN vs CENTRITO VALLE")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Objetivo: Detectar patrones para redistribución óptima")
    print("=" * 80)
    
    # 1. Obtener coordenadas y distancia
    coordenadas = obtener_coordenadas_sucursales()
    
    # 2. Analizar fechas de Gómez Morín
    gomez_fechas = analizar_fechas_gomez_morin()
    
    # 3. Comparar patrones de fechas
    opciones_israel = comparar_patrones_fechas(gomez_fechas)
    
    # 4. Analizar patrones de usuarios
    israel_en_gomez, usuarios_gomez = analizar_patrones_usuarios(gomez_fechas)
    
    # 5. Recomendar redistribución óptima
    mejor_opcion = recomendar_redistribucion_optima(opciones_israel, gomez_fechas, israel_en_gomez)
    
    # 6. Guardar análisis
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    import json
    with open(f"ANALISIS_PATRONES_REDISTRIBUCION_{timestamp}.json", 'w', encoding='utf-8') as f:
        json.dump({
            'timestamp': timestamp,
            'coordenadas_sucursales': coordenadas,
            'fechas_gomez_morin': gomez_fechas,
            'opciones_israel_centrito': opciones_israel,
            'usuarios_gomez_morin': usuarios_gomez,
            'israel_trabaja_gomez': israel_en_gomez,
            'recomendacion_final': mejor_opcion
        }, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"\n📁 ANÁLISIS GUARDADO: ANALISIS_PATRONES_REDISTRIBUCION_{timestamp}.json")
    
    return mejor_opcion, coordenadas

if __name__ == "__main__":
    main()