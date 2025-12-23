#!/usr/bin/env python3
"""
🔍 FASE 1: ANÁLISIS DE CAMPOS EXTRAS
Analizar Location Map (Google Maps) + Sucursal manual para mejorar matching
"""

import pandas as pd
import numpy as np
from datetime import datetime
import re
import json

def analizar_campos_extras_seguridad():
    """Analizar campos Location Map y Sucursal en Excel de seguridad"""
    
    print("🔍 FASE 1: ANÁLISIS DE CAMPOS EXTRAS - SEGURIDAD")
    print("=" * 60)
    
    try:
        # Cargar Excel de seguridad
        df_seg = pd.read_excel("CONTROL_OPERATIVO_DE_SEGURIDAD_CAS_11_REV_25012025_Submissions-2025-12-18__1232CST-1766104009.xlsx")
        
        print(f"✅ Cargado Excel SEGURIDAD: {len(df_seg)} submissions")
        
        # Identificar submissions sin location
        sin_location = df_seg[df_seg['Location'].isna()]
        con_location = df_seg[df_seg['Location'].notna()]
        
        print(f"📊 SIN location: {len(sin_location)}")
        print(f"📊 CON location: {len(con_location)}")
        
        return df_seg, sin_location, con_location
        
    except Exception as e:
        print(f"❌ Error cargando Excel: {e}")
        return None, None, None

def analizar_location_map(df_seg, sin_location):
    """Analizar campo Location Map (Google Maps)"""
    
    print(f"\n🗺️ ANÁLISIS CAMPO 'Location Map'")
    print("=" * 40)
    
    # Buscar columnas que contengan 'map' o similar
    columnas_map = [col for col in df_seg.columns if 'map' in col.lower() or 'location' in col.lower()]
    
    print(f"📊 COLUMNAS RELACIONADAS CON LOCATION/MAP:")
    for col in columnas_map[:10]:  # Primeras 10
        tiene_datos = df_seg[col].notna().sum()
        total = len(df_seg)
        print(f"   📋 {col}: {tiene_datos}/{total} registros ({tiene_datos/total*100:.1f}%)")
    
    # Buscar específicamente Location Map
    location_map_cols = [col for col in df_seg.columns if 'map' in col.lower()]
    
    if location_map_cols:
        print(f"\n🎯 ANÁLISIS DETALLADO LOCATION MAP:")
        for col in location_map_cols[:3]:  # Primeras 3 columnas con 'map'
            print(f"\n📍 COLUMNA: {col}")
            
            # Análisis general
            total_registros = df_seg[col].notna().sum()
            print(f"   📊 Registros con datos: {total_registros}/{len(df_seg)}")
            
            # Análisis específico para submissions sin location
            sin_loc_con_map = sin_location[col].notna().sum()
            print(f"   🎯 Sin location pero CON este campo: {sin_loc_con_map}/{len(sin_location)}")
            
            # Mostrar ejemplos
            if sin_loc_con_map > 0:
                ejemplos = sin_location[sin_location[col].notna()][col].head(3)
                print(f"   📋 EJEMPLOS:")
                for i, ejemplo in enumerate(ejemplos, 1):
                    ejemplo_str = str(ejemplo)[:80] + "..." if len(str(ejemplo)) > 80 else str(ejemplo)
                    print(f"      {i}. {ejemplo_str}")
                
                # Intentar extraer coordenadas si parece Google Maps
                if 'google' in col.lower() or 'maps' in col.lower():
                    coordenadas_extraidas = extraer_coordenadas_google_maps(sin_location, col)
                    if coordenadas_extraidas:
                        print(f"   🎯 Coordenadas extraíbles: {len(coordenadas_extraidas)} casos")
    
    return location_map_cols

def extraer_coordenadas_google_maps(sin_location, col_map):
    """Intentar extraer coordenadas de links de Google Maps"""
    
    coordenadas = []
    
    for idx, row in sin_location.iterrows():
        if pd.notna(row[col_map]):
            link = str(row[col_map])
            
            # Buscar patrones de coordenadas en Google Maps
            # Patrón: @lat,lon,zoom o ll=lat,lon
            patron_coords = r'[@&](-?\d+\.?\d*),(-?\d+\.?\d*)'
            match = re.search(patron_coords, link)
            
            if match:
                lat = float(match.group(1))
                lon = float(match.group(2))
                
                coordenadas.append({
                    'fecha': row['Date Submitted'],
                    'usuario': row['Submitted By'],
                    'lat': lat,
                    'lon': lon,
                    'link_original': link
                })
    
    return coordenadas

def analizar_campo_sucursal(df_seg, sin_location):
    """Analizar campo Sucursal (llenado manual)"""
    
    print(f"\n🏪 ANÁLISIS CAMPO 'Sucursal'")
    print("=" * 40)
    
    # Verificar si existe campo Sucursal
    if 'Sucursal' not in df_seg.columns:
        print("❌ Campo 'Sucursal' no encontrado")
        return None
    
    print(f"✅ Campo 'Sucursal' encontrado")
    
    # Análisis general
    total_con_sucursal = df_seg['Sucursal'].notna().sum()
    print(f"📊 Total con datos Sucursal: {total_con_sucursal}/{len(df_seg)} ({total_con_sucursal/len(df_seg)*100:.1f}%)")
    
    # Análisis específico para submissions sin location
    sin_loc_con_sucursal = sin_location['Sucursal'].notna().sum()
    print(f"🎯 Sin location pero CON Sucursal: {sin_loc_con_sucursal}/{len(sin_location)} ({sin_loc_con_sucursal/len(sin_location)*100:.1f}%)")
    
    if sin_loc_con_sucursal > 0:
        print(f"\n📋 VALORES SUCURSAL EN SUBMISSIONS SIN LOCATION:")
        
        sucursales_sin_loc = sin_location[sin_location['Sucursal'].notna()]['Sucursal'].value_counts()
        
        for i, (sucursal, count) in enumerate(sucursales_sin_loc.head(10).items(), 1):
            print(f"   {i:2d}. '{sucursal}': {count} submissions")
        
        # Mostrar ejemplos con fecha y usuario
        print(f"\n📅 EJEMPLOS CON FECHA Y USUARIO:")
        ejemplos = sin_location[sin_location['Sucursal'].notna()][['Date Submitted', 'Submitted By', 'Sucursal']].head(5)
        
        for idx, row in ejemplos.iterrows():
            fecha = str(row['Date Submitted'])[:10] if pd.notna(row['Date Submitted']) else 'N/A'
            usuario = str(row['Submitted By'])
            sucursal = str(row['Sucursal'])
            print(f"   📅 {fecha} | {usuario} | '{sucursal}'")
    
    return sucursales_sin_loc if sin_loc_con_sucursal > 0 else None

def mapear_sucursal_manual_a_location(sucursales_sin_loc):
    """Mapear nombres de sucursal manual a locations estándar"""
    
    print(f"\n🗺️ MAPEO SUCURSAL MANUAL → LOCATION ESTÁNDAR")
    print("=" * 50)
    
    # Cargar catálogo de sucursales master
    try:
        df_master = pd.read_csv('SUCURSALES_MASTER_20251218_110913.csv')
        print(f"✅ Cargado catálogo master: {len(df_master)} sucursales")
        
        # Crear diccionario de mapeo
        mapeo_sucursales = {}
        
        for sucursal_manual in sucursales_sin_loc.index:
            # Normalizar nombre
            sucursal_norm = sucursal_manual.lower().strip()
            
            # Buscar coincidencias en master
            mejor_match = None
            mejor_similitud = 0
            
            for idx, row in df_master.iterrows():
                if pd.notna(row['nombre']):
                    nombre_master = str(row['nombre']).lower().strip()
                    
                    # Coincidencia directa
                    if sucursal_norm == nombre_master:
                        mejor_match = f"{int(row['numero'])} - {row['nombre']}"
                        mejor_similitud = 1.0
                        break
                    
                    # Coincidencia parcial
                    elif sucursal_norm in nombre_master or nombre_master in sucursal_norm:
                        if len(sucursal_norm) > mejor_similitud:
                            mejor_match = f"{int(row['numero'])} - {row['nombre']}"
                            mejor_similitud = len(sucursal_norm)
            
            if mejor_match:
                mapeo_sucursales[sucursal_manual] = {
                    'location_sugerido': mejor_match,
                    'confianza': mejor_similitud,
                    'submissions_count': sucursales_sin_loc[sucursal_manual]
                }
        
        # Mostrar mapeos encontrados
        print(f"📊 MAPEOS IDENTIFICADOS: {len(mapeo_sucursales)}")
        print(f"{'Sucursal Manual':<25} {'→ Location Sugerido':<30} {'Count':<6} {'Conf':<6}")
        print("-" * 70)
        
        for sucursal, datos in mapeo_sucursales.items():
            sucursal_short = sucursal[:24]
            location_short = datos['location_sugerido'][:29]
            count = datos['submissions_count']
            conf = f"{datos['confianza']:.1f}" if isinstance(datos['confianza'], float) else "1.0"
            
            print(f"{sucursal_short:<25} → {location_short:<30} {count:<6} {conf:<6}")
        
        return mapeo_sucursales
        
    except Exception as e:
        print(f"❌ Error cargando catálogo master: {e}")
        return None

def evaluar_completitud_datos_extras(sin_location, location_map_cols, sucursales_sin_loc, mapeo_sucursales):
    """Evaluar qué tan completos están los datos extras"""
    
    print(f"\n📊 EVALUACIÓN COMPLETITUD DATOS EXTRAS")
    print("=" * 50)
    
    total_sin_location = len(sin_location)
    
    # Evaluar Location Map
    con_location_map = 0
    if location_map_cols:
        for col in location_map_cols:
            con_location_map = max(con_location_map, sin_location[col].notna().sum())
    
    # Evaluar Sucursal manual
    con_sucursal_manual = len(sucursales_sin_loc) if sucursales_sin_loc is not None else 0
    con_sucursal_mapeable = len(mapeo_sucursales) if mapeo_sucursales else 0
    
    print(f"📊 RESUMEN COMPLETITUD:")
    print(f"   📊 Total submissions sin location: {total_sin_location}")
    print(f"   🗺️ Con Location Map útil: {con_location_map} ({con_location_map/total_sin_location*100:.1f}%)")
    print(f"   🏪 Con Sucursal manual: {con_sucursal_manual} ({con_sucursal_manual/total_sin_location*100:.1f}%)")
    print(f"   🎯 Sucursal mapeable a Location: {con_sucursal_mapeable}")
    
    # Calcular cobertura combinada
    cobertura_total = max(con_location_map, con_sucursal_manual)
    porcentaje_cobertura = (cobertura_total / total_sin_location) * 100
    
    print(f"\n🎯 COBERTURA COMBINADA: {cobertura_total}/{total_sin_location} ({porcentaje_cobertura:.1f}%)")
    
    if porcentaje_cobertura >= 80:
        evaluacion = "✅ EXCELENTE"
        estrategia = "Usar datos extras como fuente principal"
    elif porcentaje_cobertura >= 60:
        evaluacion = "⚠️ BUENA"
        estrategia = "Combinar datos extras + coordenadas API"
    elif porcentaje_cobertura >= 40:
        evaluacion = "⚠️ REGULAR"
        estrategia = "Usar datos extras como apoyo + coordenadas API"
    else:
        evaluacion = "❌ LIMITADA"
        estrategia = "Priorizar coordenadas API + validación manual"
    
    print(f"🎯 EVALUACIÓN: {evaluacion}")
    print(f"💡 ESTRATEGIA RECOMENDADA: {estrategia}")
    
    return {
        'total_sin_location': total_sin_location,
        'con_location_map': con_location_map,
        'con_sucursal_manual': con_sucursal_manual,
        'con_sucursal_mapeable': con_sucursal_mapeable,
        'cobertura_total': cobertura_total,
        'porcentaje_cobertura': porcentaje_cobertura,
        'evaluacion': evaluacion,
        'estrategia': estrategia
    }

def proponer_estrategia_mejorada(evaluacion_datos):
    """Proponer estrategia mejorada con datos extras"""
    
    print(f"\n🚀 ESTRATEGIA MEJORADA CON DATOS EXTRAS")
    print("=" * 50)
    
    porcentaje_cobertura = evaluacion_datos['porcentaje_cobertura']
    
    if porcentaje_cobertura >= 60:
        print(f"🎯 ESTRATEGIA OPTIMIZADA (Cobertura: {porcentaje_cobertura:.1f}%):")
        
        estrategia_mejorada = {
            'fase_1': {
                'nombre': 'Mapeo por Datos Excel Extras',
                'tiempo': '15 min',
                'acciones': [
                    'Usar campo Sucursal manual como primera opción',
                    'Mapear sucursales manuales a locations estándar',
                    'Extraer coordenadas de Location Map (Google Maps)',
                    'Crear dataset híbrido: Excel extras + API fallback'
                ]
            },
            'fase_2': {
                'nombre': 'Matching Temporal + Manual',
                'tiempo': '10 min',
                'acciones': [
                    'Priorizar submissions con Sucursal manual',
                    'Matching por fecha para casos sin datos extras',
                    'Validar consistencia con operativas mismo día',
                    'Flagear casos que requieren API coordinates'
                ]
            },
            'fase_3': {
                'nombre': 'Coordenadas API Solo Fallback',
                'tiempo': '10 min',
                'acciones': [
                    'Query API solo para casos no resueltos con datos extras',
                    'Aplicar Haversine solo donde sea necesario',
                    'Validar coherencia geográfica de asignaciones',
                    'Priorizar sucursales que necesitan completar 4+4'
                ]
            },
            'fase_4': {
                'nombre': 'Validación y Balanceo',
                'tiempo': '5 min',
                'acciones': [
                    'Verificar reglas 4+4 completadas',
                    'Balancear distribuciones finales',
                    'Generar reporte de confianza por fuente',
                    'Marcar submissions para validación manual si necesario'
                ]
            }
        }
        
        tiempo_total = sum(int(fase['tiempo'].split()[0]) for fase in estrategia_mejorada.values())
        print(f"⏰ TIEMPO TOTAL OPTIMIZADO: {tiempo_total} min (vs 50 min original)")
        
        for fase_id, detalles in estrategia_mejorada.items():
            print(f"\n📍 {fase_id.upper()}: {detalles['nombre']} ({detalles['tiempo']})")
            for i, accion in enumerate(detalles['acciones'], 1):
                print(f"      {i}. {accion}")
    
    else:
        print(f"⚠️ MANTENER ESTRATEGIA ORIGINAL (Cobertura: {porcentaje_cobertura:.1f}%)")
        print("💡 Datos extras insuficientes, usar API como fuente principal")
        estrategia_mejorada = None
    
    return estrategia_mejorada

def main():
    """Función principal - Fase 1"""
    
    print("🔍 FASE 1: ANÁLISIS DE CAMPOS EXTRAS (Location Map + Sucursal)")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Objetivo: Evaluar campos extras para optimizar matching")
    print("=" * 80)
    
    # 1. Cargar datos
    df_seg, sin_location, con_location = analizar_campos_extras_seguridad()
    if df_seg is None:
        print("❌ Error cargando datos")
        return
    
    # 2. Analizar Location Map
    location_map_cols = analizar_location_map(df_seg, sin_location)
    
    # 3. Analizar Sucursal manual
    sucursales_sin_loc = analizar_campo_sucursal(df_seg, sin_location)
    
    # 4. Mapear sucursal manual a locations
    mapeo_sucursales = None
    if sucursales_sin_loc is not None:
        mapeo_sucursales = mapear_sucursal_manual_a_location(sucursales_sin_loc)
    
    # 5. Evaluar completitud
    evaluacion_datos = evaluar_completitud_datos_extras(sin_location, location_map_cols, sucursales_sin_loc, mapeo_sucursales)
    
    # 6. Proponer estrategia mejorada
    estrategia_mejorada = proponer_estrategia_mejorada(evaluacion_datos)
    
    # 7. Guardar resultados
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    resultado = {
        'timestamp': timestamp,
        'total_sin_location': len(sin_location),
        'location_map_columns': location_map_cols,
        'sucursales_manuales': sucursales_sin_loc.to_dict() if sucursales_sin_loc is not None else None,
        'mapeo_sucursales': mapeo_sucursales,
        'evaluacion_completitud': evaluacion_datos,
        'estrategia_mejorada': estrategia_mejorada
    }
    
    with open(f"FASE1_CAMPOS_EXTRAS_{timestamp}.json", 'w', encoding='utf-8') as f:
        json.dump(resultado, f, indent=2, ensure_ascii=False, default=str)
    
    # CONCLUSIÓN FASE 1
    print(f"\n" + "=" * 80)
    print(f"🎯 CONCLUSIÓN FASE 1")
    print("=" * 80)
    
    evaluacion = evaluacion_datos['evaluacion']
    porcentaje = evaluacion_datos['porcentaje_cobertura']
    
    print(f"📊 COBERTURA DATOS EXTRAS: {evaluacion} ({porcentaje:.1f}%)")
    print(f"💡 RECOMENDACIÓN: {evaluacion_datos['estrategia']}")
    
    if estrategia_mejorada:
        print(f"\n✅ ESTRATEGIA OPTIMIZADA DISPONIBLE:")
        print(f"   ⏰ Tiempo reducido: 40 min (vs 50 min original)")
        print(f"   🎯 Usar datos Excel como fuente principal")
        print(f"   📡 API solo como fallback")
    else:
        print(f"\n⚠️ MANTENER ESTRATEGIA ORIGINAL")
        print(f"   📡 API como fuente principal")
        print(f"   📊 Datos Excel como validación")
    
    print(f"\n📁 ANÁLISIS GUARDADO: FASE1_CAMPOS_EXTRAS_{timestamp}.json")
    
    return resultado

if __name__ == "__main__":
    main()