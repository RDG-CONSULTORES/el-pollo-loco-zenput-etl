#!/usr/bin/env python3
"""
🚀 ETL LIMPIO DESDE EXCELES CON METODOLOGÍA API
Partir de cero usando los Exceles pero con metodología limpia como si fuera API
238 operativas + 238 seguridad = 476 submissions
Reglas: LOCAL 4+4, FORÁNEA 2+2, ESPECIALES 3+3 (Pino Suarez, Felix U Gomez, Madero, Matamoros)
"""

import pandas as pd
import numpy as np
import math
import re
from datetime import datetime

def configurar_reglas_negocio():
    """Configurar reglas de negocio confirmadas por Roberto"""
    
    print("⚖️ CONFIGURAR REGLAS DE NEGOCIO")
    print("=" * 50)
    
    # Reglas especiales confirmadas por Roberto
    reglas_especiales = {
        '1 - Pino Suarez': {'ops': 3, 'seg': 3, 'total': 6, 'tipo': 'ESPECIAL_3_3'},
        '5 - Felix U. Gomez': {'ops': 3, 'seg': 3, 'total': 6, 'tipo': 'ESPECIAL_3_3'},
        '2 - Madero': {'ops': 3, 'seg': 3, 'total': 6, 'tipo': 'ESPECIAL_3_3'},
        '3 - Matamoros': {'ops': 3, 'seg': 3, 'total': 6, 'tipo': 'ESPECIAL_3_3'}
    }
    
    print(f"📋 REGLAS CONFIRMADAS POR ROBERTO:")
    print(f"   ⭐ ESPECIALES (3+3): {list(reglas_especiales.keys())}")
    print(f"   🏢 LOCALES (4+4): Todas las demás sucursales LOCAL")
    print(f"   🌍 FORÁNEAS (2+2): Sucursales marcadas como FORÁNEA")
    
    return reglas_especiales

def cargar_exceles_limpios():
    """Cargar Exceles como fuente de datos limpia"""
    
    print(f"\n📁 CARGAR EXCELES COMO FUENTE LIMPIA")
    print("=" * 50)
    
    # Operativas
    df_ops_raw = pd.read_excel("SUPERVISION_OPERATIVA_CAS_11_REV_250125_Submissions-2025-12-18__1228CST-1766141816.xlsx")
    
    # Seguridad  
    df_seg_raw = pd.read_excel("CONTROL_OPERATIVO_DE_SEGURIDAD_CAS_11_REV_25012025_Submissions-2025-12-18__1232CST-1766104009.xlsx")
    
    # Sucursales master
    df_sucursales = pd.read_csv('SUCURSALES_MASTER_20251218_110913.csv')
    
    print(f"✅ DATOS CARGADOS:")
    print(f"   🏗️ Operativas: {len(df_ops_raw)} registros")
    print(f"   🛡️ Seguridad: {len(df_seg_raw)} registros") 
    print(f"   🏢 Sucursales: {len(df_sucursales)} sucursales")
    
    return df_ops_raw, df_seg_raw, df_sucursales

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

def procesar_operativas_limpio(df_ops_raw):
    """Procesar operativas con estructura limpia"""
    
    print(f"\n🏗️ PROCESAR OPERATIVAS LIMPIO")
    print("=" * 40)
    
    operativas_limpias = []
    
    for idx, row in df_ops_raw.iterrows():
        # Datos básicos
        submission_id = f"ops_{idx}"
        fecha_dt = pd.to_datetime(row['Date Submitted']) if pd.notna(row['Date Submitted']) else None
        usuario = row['Submitted By'] if pd.notna(row['Submitted By']) else 'DESCONOCIDO'
        location_asignado = row['Location'] if pd.notna(row['Location']) else None
        
        # Coordenadas de entrega
        lat_entrega = row.get('smetadata.lat')
        lon_entrega = row.get('smetadata.lon')
        
        # Si no hay smetadata, intentar Location Map
        if pd.isna(lat_entrega) or pd.isna(lon_entrega):
            location_map = row.get('Location Map')
            if pd.notna(location_map):
                lat_map, lon_map = extraer_coordenadas_location_map(location_map)
                if lat_map and lon_map:
                    lat_entrega, lon_entrega = lat_map, lon_map
        
        # Campo Sucursal manual (si existe)
        sucursal_campo = row.get('Sucursal') if pd.notna(row.get('Sucursal')) else None
        
        operativas_limpias.append({
            'index_original': idx,
            'submission_id': submission_id,
            'fecha': fecha_dt,
            'fecha_str': fecha_dt.strftime('%Y-%m-%d') if fecha_dt else None,
            'usuario': usuario,
            'location_asignado': location_asignado,
            'lat_entrega': lat_entrega,
            'lon_entrega': lon_entrega,
            'sucursal_campo': sucursal_campo,
            'tipo': 'OPERATIVA',
            'tiene_location': location_asignado is not None,
            'tiene_coordenadas': lat_entrega is not None and lon_entrega is not None,
            'necesita_mapeo': location_asignado is None
        })
    
    df_ops_limpio = pd.DataFrame(operativas_limpias)
    
    print(f"📊 OPERATIVAS PROCESADAS:")
    print(f"   ✅ Total: {len(df_ops_limpio)}")
    print(f"   📍 Con location: {len(df_ops_limpio[df_ops_limpio['tiene_location']])}")
    print(f"   🌐 Con coordenadas: {len(df_ops_limpio[df_ops_limpio['tiene_coordenadas']])}")
    print(f"   ❓ Necesitan mapeo: {len(df_ops_limpio[df_ops_limpio['necesita_mapeo']])}")
    
    return df_ops_limpio

def procesar_seguridad_limpio(df_seg_raw):
    """Procesar seguridad con estructura limpia"""
    
    print(f"\n🛡️ PROCESAR SEGURIDAD LIMPIO")
    print("=" * 40)
    
    seguridad_limpia = []
    
    for idx, row in df_seg_raw.iterrows():
        # Datos básicos
        submission_id = f"seg_{idx}"
        fecha_dt = pd.to_datetime(row['Date Submitted']) if pd.notna(row['Date Submitted']) else None
        usuario = row['Submitted By'] if pd.notna(row['Submitted By']) else 'DESCONOCIDO'
        location_asignado = row['Location'] if pd.notna(row['Location']) else None
        
        # Coordenadas de entrega
        lat_entrega = row.get('smetadata.lat')
        lon_entrega = row.get('smetadata.lon')
        
        # Si no hay smetadata, intentar Location Map
        if pd.isna(lat_entrega) or pd.isna(lon_entrega):
            location_map = row.get('Location Map')
            if pd.notna(location_map):
                lat_map, lon_map = extraer_coordenadas_location_map(location_map)
                if lat_map and lon_map:
                    lat_entrega, lon_entrega = lat_map, lon_map
        
        # Campo Sucursal manual (si existe)
        sucursal_campo = row.get('Sucursal') if pd.notna(row.get('Sucursal')) else None
        
        seguridad_limpia.append({
            'index_original': idx,
            'submission_id': submission_id,
            'fecha': fecha_dt,
            'fecha_str': fecha_dt.strftime('%Y-%m-%d') if fecha_dt else None,
            'usuario': usuario,
            'location_asignado': location_asignado,
            'lat_entrega': lat_entrega,
            'lon_entrega': lon_entrega,
            'sucursal_campo': sucursal_campo,
            'tipo': 'SEGURIDAD',
            'tiene_location': location_asignado is not None,
            'tiene_coordenadas': lat_entrega is not None and lon_entrega is not None,
            'necesita_mapeo': location_asignado is None
        })
    
    df_seg_limpio = pd.DataFrame(seguridad_limpia)
    
    print(f"📊 SEGURIDAD PROCESADA:")
    print(f"   ✅ Total: {len(df_seg_limpio)}")
    print(f"   📍 Con location: {len(df_seg_limpio[df_seg_limpio['tiene_location']])}")
    print(f"   🌐 Con coordenadas: {len(df_seg_limpio[df_seg_limpio['tiene_coordenadas']])}")
    print(f"   ❓ Necesitan mapeo: {len(df_seg_limpio[df_seg_limpio['necesita_mapeo']])}")
    
    return df_seg_limpio

def crear_catalogo_sucursales_con_reglas(df_sucursales, reglas_especiales):
    """Crear catálogo de sucursales con reglas de negocio"""
    
    print(f"\n📋 CREAR CATÁLOGO CON REGLAS")
    print("=" * 40)
    
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

def analizar_estado_actual_limpio(df_ops, df_seg, df_catalogo):
    """Analizar estado actual con datos limpios"""
    
    print(f"\n📊 ANÁLISIS ESTADO ACTUAL LIMPIO")
    print("=" * 50)
    
    # Contar por sucursal
    ops_por_sucursal = df_ops[df_ops['tiene_location']]['location_asignado'].value_counts()
    seg_por_sucursal = df_seg[df_seg['tiene_location']]['location_asignado'].value_counts()
    
    # Analizar cumplimiento de reglas
    analisis_sucursales = []
    
    for _, sucursal in df_catalogo.iterrows():
        location_key = sucursal['location_key']
        ops_actuales = ops_por_sucursal.get(location_key, 0)
        seg_actuales = seg_por_sucursal.get(location_key, 0)
        total_actual = ops_actuales + seg_actuales
        
        ops_esperadas = sucursal['ops_esperadas']
        seg_esperadas = sucursal['seg_esperadas']
        total_esperado = sucursal['total_esperado']
        tipo_regla = sucursal['tipo_regla']
        
        # Estado
        if total_actual == total_esperado:
            estado = "✅ PERFECTO"
        elif total_actual > total_esperado:
            estado = f"⚠️ EXCESO (+{total_actual - total_esperado})"
        elif total_actual < total_esperado:
            estado = f"❌ DEFICIT (-{total_esperado - total_actual})"
        else:
            estado = f"❓ REVISAR"
        
        analisis_sucursales.append({
            'location_key': location_key,
            'tipo_regla': tipo_regla,
            'ops_actuales': ops_actuales,
            'seg_actuales': seg_actuales,
            'total_actual': total_actual,
            'ops_esperadas': ops_esperadas,
            'seg_esperadas': seg_esperadas,
            'total_esperado': total_esperado,
            'diferencia': total_actual - total_esperado,
            'estado': estado
        })
    
    df_analisis = pd.DataFrame(analisis_sucursales)
    
    # Resumen
    perfectas = len(df_analisis[df_analisis['diferencia'] == 0])
    con_exceso = len(df_analisis[df_analisis['diferencia'] > 0])
    con_deficit = len(df_analisis[df_analisis['diferencia'] < 0])
    sin_supervisiones = len(df_analisis[df_analisis['total_actual'] == 0])
    
    print(f"📊 RESUMEN CUMPLIMIENTO:")
    print(f"   ✅ Perfectas: {perfectas}")
    print(f"   ⚠️ Con exceso: {con_exceso}")
    print(f"   ❌ Con déficit: {con_deficit}")
    print(f"   ⭕ Sin supervisiones: {sin_supervisiones}")
    
    # Supervisiones sin location (el problema principal)
    ops_sin_location = df_ops[df_ops['necesita_mapeo']]
    seg_sin_location = df_seg[df_seg['necesita_mapeo']]
    
    print(f"\n❓ SUPERVISIONES SIN LOCATION (PROBLEMA PRINCIPAL):")
    print(f"   🏗️ Operativas sin location: {len(ops_sin_location)}")
    print(f"   🛡️ Seguridad sin location: {len(seg_sin_location)}")
    
    return df_analisis, ops_sin_location, seg_sin_location

def main():
    """Función principal"""
    
    print("🚀 ETL LIMPIO DESDE EXCELES CON METODOLOGÍA API")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Objetivo: Datos limpios con metodología estructurada")
    print("📋 Target: 238 operativas + 238 seguridad = 476 submissions")
    print("⚖️ Reglas: LOCAL 4+4, FORÁNEA 2+2, ESPECIALES 3+3")
    print("=" * 80)
    
    # 1. Configurar reglas de negocio
    reglas_especiales = configurar_reglas_negocio()
    
    # 2. Cargar exceles como fuente limpia
    df_ops_raw, df_seg_raw, df_sucursales = cargar_exceles_limpios()
    
    # 3. Procesar operativas con estructura limpia
    df_ops_limpio = procesar_operativas_limpio(df_ops_raw)
    
    # 4. Procesar seguridad con estructura limpia
    df_seg_limpio = procesar_seguridad_limpio(df_seg_raw)
    
    # 5. Crear catálogo con reglas
    df_catalogo = crear_catalogo_sucursales_con_reglas(df_sucursales, reglas_especiales)
    
    # 6. Analizar estado actual
    df_analisis, ops_sin_location, seg_sin_location = analizar_estado_actual_limpio(
        df_ops_limpio, df_seg_limpio, df_catalogo
    )
    
    # 7. Guardar datos limpios
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    df_ops_limpio.to_csv(f"OPERATIVAS_LIMPIO_{timestamp}.csv", index=False, encoding='utf-8')
    df_seg_limpio.to_csv(f"SEGURIDAD_LIMPIO_{timestamp}.csv", index=False, encoding='utf-8')
    df_catalogo.to_csv(f"CATALOGO_REGLAS_{timestamp}.csv", index=False, encoding='utf-8')
    df_analisis.to_csv(f"ANALISIS_ESTADO_{timestamp}.csv", index=False, encoding='utf-8')
    
    print(f"\n📁 DATOS LIMPIOS GUARDADOS:")
    print(f"   ✅ Operativas: OPERATIVAS_LIMPIO_{timestamp}.csv")
    print(f"   ✅ Seguridad: SEGURIDAD_LIMPIO_{timestamp}.csv")
    print(f"   ✅ Catálogo: CATALOGO_REGLAS_{timestamp}.csv")
    print(f"   ✅ Análisis: ANALISIS_ESTADO_{timestamp}.csv")
    
    print(f"\n🎯 PRÓXIMOS PASOS:")
    print(f"   1. Mapear {len(seg_sin_location)} supervisiones seguridad sin location")
    print(f"   2. Usar: fechas coincidentes → coordenadas → campo Sucursal")
    print(f"   3. Consultar dudas específicas con Roberto")
    print(f"   4. Aplicar reglas de negocio 4+4, 2+2, 3+3")
    
    return df_ops_limpio, df_seg_limpio, df_catalogo, df_analisis, ops_sin_location, seg_sin_location

if __name__ == "__main__":
    main()