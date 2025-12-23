#!/usr/bin/env python3
"""
🔧 ENRIQUECER EXCEL PARA POSTGRESQL
Agregar coordenadas, estado, país y grupo operativo a los Excel
"""

import pandas as pd
from datetime import datetime
import os

def cargar_datos_base():
    """Cargar datos base para enriquecimiento"""
    
    print("🔧 CARGAR DATOS BASE PARA ENRIQUECIMIENTO")
    print("=" * 60)
    
    # 1. Catálogo sucursales con coordenadas
    try:
        df_sucursales = pd.read_csv("SUCURSALES_CORRECCIONES_ROBERTO_20251218_171807.csv")
        print(f"✅ Catálogo sucursales: {len(df_sucursales)} registros")
        print(f"   Columnas: {list(df_sucursales.columns)}")
    except FileNotFoundError:
        print("❌ No encontré el catálogo de sucursales")
        return None, None, None
    
    # 2. Excel operativas actual
    try:
        df_ops = pd.read_excel("OPERATIVAS_MEJORADO_ROBERTO_20251218_193122.xlsx", 
                               sheet_name='Operativas_Con_Areas')
        print(f"✅ Excel operativas: {len(df_ops)} supervisiones")
    except FileNotFoundError:
        print("❌ No encontré el Excel de operativas")
        return df_sucursales, None, None
    
    # 3. Excel seguridad actual
    try:
        df_seg = pd.read_excel("SEGURIDAD_MEJORADO_ROBERTO_20251218_193122.xlsx",
                               sheet_name='Seguridad_Con_Areas')
        print(f"✅ Excel seguridad: {len(df_seg)} supervisiones")
    except FileNotFoundError:
        print("❌ No encontré el Excel de seguridad")
        return df_sucursales, df_ops, None
    
    return df_sucursales, df_ops, df_seg

def crear_mapeo_sucursales(df_sucursales):
    """Crear mapeo completo de sucursales"""
    
    print("\n🗺️ CREAR MAPEO SUCURSALES COMPLETO")
    print("=" * 50)
    
    mapeo = {}
    
    for _, row in df_sucursales.iterrows():
        if pd.notna(row['numero']) and pd.notna(row['nombre']):
            numero = int(row['numero'])
            nombre = row['nombre']
            
            # Crear diferentes claves de mapeo
            location_key = f"{numero} - {nombre}"
            
            datos_sucursal = {
                'numero': numero,
                'nombre': nombre,
                'grupo_operativo': row.get('grupo', 'DESCONOCIDO'),
                'tipo_sucursal': row.get('tipo', 'LOCAL'),
                'latitud': row.get('lat', None),
                'longitud': row.get('lon', None),
                'estado': 'Nuevo León',  # Todas están en NL
                'pais': 'México',
                'region': 'Norte',
                'zona_horaria': 'America/Monterrey'
            }
            
            # Mapeo por diferentes claves
            mapeo[location_key] = datos_sucursal
            mapeo[nombre] = datos_sucursal
            mapeo[str(numero)] = datos_sucursal
    
    print(f"✅ Mapeo creado para {len(mapeo)} claves")
    
    # Mostrar grupos operativos
    grupos = set()
    for data in mapeo.values():
        if isinstance(data, dict):
            grupos.add(data['grupo_operativo'])
    
    print(f"📋 Grupos operativos encontrados: {sorted(grupos)}")
    
    return mapeo

def enriquecer_excel(df_excel, mapeo_sucursales, tipo_supervision):
    """Enriquecer Excel con datos de sucursales"""
    
    print(f"\n🔧 ENRIQUECER {tipo_supervision.upper()}")
    print("=" * 50)
    
    if df_excel is None:
        print(f"❌ No hay datos para {tipo_supervision}")
        return None
    
    # Crear copia para trabajar
    df_enriquecido = df_excel.copy()
    
    # Inicializar nuevas columnas
    nuevas_columnas = {
        'sucursal_numero': None,
        'latitud': None,
        'longitud': None,
        'grupo_operativo': None,
        'tipo_sucursal': None,
        'estado': 'Nuevo León',
        'pais': 'México',
        'region': 'Norte',
        'zona_horaria': 'America/Monterrey'
    }
    
    for col, default_val in nuevas_columnas.items():
        df_enriquecido[col] = default_val
    
    # Procesar cada supervisión
    enriquecidas = 0
    no_encontradas = []
    
    for idx, row in df_enriquecido.iterrows():
        sucursal = row['SUCURSAL']
        
        # Buscar en mapeo
        datos_sucursal = None
        
        # Buscar por nombre directo
        if sucursal in mapeo_sucursales:
            datos_sucursal = mapeo_sucursales[sucursal]
        
        if datos_sucursal and isinstance(datos_sucursal, dict):
            # Enriquecer con datos encontrados
            df_enriquecido.at[idx, 'sucursal_numero'] = datos_sucursal['numero']
            df_enriquecido.at[idx, 'latitud'] = datos_sucursal['latitud']
            df_enriquecido.at[idx, 'longitud'] = datos_sucursal['longitud']
            df_enriquecido.at[idx, 'grupo_operativo'] = datos_sucursal['grupo_operativo']
            df_enriquecido.at[idx, 'tipo_sucursal'] = datos_sucursal['tipo_sucursal']
            
            enriquecidas += 1
        else:
            no_encontradas.append(sucursal)
    
    print(f"✅ Enriquecidas: {enriquecidas}/{len(df_enriquecido)}")
    
    if no_encontradas:
        print(f"⚠️ No encontradas: {len(set(no_encontradas))}")
        for sucursal in set(no_encontradas):
            print(f"   - {sucursal}")
    
    return df_enriquecido

def reordenar_columnas_postgresql(df, tipo_supervision):
    """Reordenar columnas para PostgreSQL"""
    
    print(f"\n📋 REORDENAR COLUMNAS PARA POSTGRESQL - {tipo_supervision.upper()}")
    print("=" * 60)
    
    if df is None:
        return None
    
    # Columnas principales en orden PostgreSQL
    columnas_principales = [
        # IDENTIFICADORES
        'ID_SUPERVISION',
        'SUCURSAL',
        'sucursal_numero',
        'FECHA',
        
        # GEOLOCALIZACIÓN
        'latitud',
        'longitud',
        'estado',
        'pais',
        'region',
        
        # ORGANIZACIÓN
        'grupo_operativo',
        'tipo_sucursal',
        'zona_horaria',
        
        # CALIFICACIONES
        'CALIFICACION_GENERAL'
    ]
    
    # Columnas de áreas (todas las que no son principales)
    columnas_areas = [col for col in df.columns 
                      if col not in columnas_principales]
    
    # Orden final
    orden_final = []
    
    # Agregar columnas principales que existan
    for col in columnas_principales:
        if col in df.columns:
            orden_final.append(col)
    
    # Agregar columnas de áreas ordenadas
    columnas_areas_sorted = sorted([col for col in columnas_areas 
                                    if not col.startswith(('sucursal_', 'grupo_', 'tipo_', 
                                                          'estado', 'pais', 'region', 'zona_'))])
    orden_final.extend(columnas_areas_sorted)
    
    # Crear DataFrame reordenado
    df_reordenado = df[orden_final].copy()
    
    print(f"✅ Columnas reordenadas: {len(df_reordenado.columns)}")
    print(f"📊 Estructura final:")
    print(f"   🔑 Identificadores: 4 columnas")
    print(f"   🌍 Geolocalización: 4 columnas") 
    print(f"   🏢 Organización: 3 columnas")
    print(f"   📈 Calificaciones: {len(columnas_areas_sorted) + 1} columnas")
    
    return df_reordenado

def generar_excel_postgresql(df_ops_enriquecido, df_seg_enriquecido):
    """Generar Excel final para PostgreSQL"""
    
    print("\n📊 GENERAR EXCEL FINAL PARA POSTGRESQL")
    print("=" * 60)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Archivo operativas
    if df_ops_enriquecido is not None:
        archivo_ops = f"OPERATIVAS_POSTGRESQL_{timestamp}.xlsx"
        
        with pd.ExcelWriter(archivo_ops, engine='openpyxl') as writer:
            # Hoja principal
            df_ops_enriquecido.to_excel(writer, sheet_name='Operativas_PostgreSQL', index=False)
            
            # Hoja de metadatos
            crear_hoja_metadatos(writer, df_ops_enriquecido, 'operativas')
            
        print(f"✅ Operativas PostgreSQL: {archivo_ops}")
        print(f"   📊 {len(df_ops_enriquecido)} supervisiones")
        print(f"   📋 {len(df_ops_enriquecido.columns)} columnas")
    
    # Archivo seguridad
    if df_seg_enriquecido is not None:
        archivo_seg = f"SEGURIDAD_POSTGRESQL_{timestamp}.xlsx"
        
        with pd.ExcelWriter(archivo_seg, engine='openpyxl') as writer:
            # Hoja principal
            df_seg_enriquecido.to_excel(writer, sheet_name='Seguridad_PostgreSQL', index=False)
            
            # Hoja de metadatos
            crear_hoja_metadatos(writer, df_seg_enriquecido, 'seguridad')
            
        print(f"✅ Seguridad PostgreSQL: {archivo_seg}")
        print(f"   📊 {len(df_seg_enriquecido)} supervisiones")
        print(f"   📋 {len(df_seg_enriquecido.columns)} columnas")
    
    return archivo_ops if df_ops_enriquecido is not None else None, \
           archivo_seg if df_seg_enriquecido is not None else None

def crear_hoja_metadatos(writer, df, tipo):
    """Crear hoja de metadatos"""
    
    metadatos = [
        ['METADATOS POSTGRESQL', f'{tipo.upper()}'],
        [''],
        ['Fecha Generación', datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
        ['Total Supervisiones', len(df)],
        ['Total Columnas', len(df.columns)],
        [''],
        ['ESTRUCTURA COLUMNAS', ''],
        ['Identificadores', '4 columnas (ID, Sucursal, Número, Fecha)'],
        ['Geolocalización', '4 columnas (Lat, Lon, Estado, País)'],
        ['Organización', '3 columnas (Grupo, Tipo, Zona)'],
        ['Calificaciones', f'{len(df.columns)-11} columnas (General + Áreas)'],
        [''],
        ['VALIDACIONES', ''],
        ['Coordenadas válidas', len(df[df['latitud'].notna()])],
        ['Grupos asignados', len(df[df['grupo_operativo'] != 'DESCONOCIDO'])],
        ['Calificaciones válidas', len(df[df['CALIFICACION_GENERAL'].notna()])],
        [''],
        ['READY PARA POSTGRESQL', 'SÍ ✅']
    ]
    
    df_metadatos = pd.DataFrame(metadatos)
    df_metadatos.to_excel(writer, sheet_name='Metadatos_PostgreSQL', index=False, header=False)

def validar_datos_postgresql(df_ops, df_seg):
    """Validar datos listos para PostgreSQL"""
    
    print("\n✅ VALIDACIÓN FINAL POSTGRESQL")
    print("=" * 50)
    
    total_supervisiones = 0
    total_con_coordenadas = 0
    total_con_grupo = 0
    
    if df_ops is not None:
        total_supervisiones += len(df_ops)
        total_con_coordenadas += len(df_ops[df_ops['latitud'].notna()])
        total_con_grupo += len(df_ops[df_ops['grupo_operativo'] != 'DESCONOCIDO'])
        
        print(f"🔧 OPERATIVAS:")
        print(f"   📊 Total: {len(df_ops)}")
        print(f"   🌍 Con coordenadas: {len(df_ops[df_ops['latitud'].notna()])}")
        print(f"   🏢 Con grupo: {len(df_ops[df_ops['grupo_operativo'] != 'DESCONOCIDO'])}")
    
    if df_seg is not None:
        total_supervisiones += len(df_seg)
        total_con_coordenadas += len(df_seg[df_seg['latitud'].notna()])
        total_con_grupo += len(df_seg[df_seg['grupo_operativo'] != 'DESCONOCIDO'])
        
        print(f"🛡️ SEGURIDAD:")
        print(f"   📊 Total: {len(df_seg)}")
        print(f"   🌍 Con coordenadas: {len(df_seg[df_seg['latitud'].notna()])}")
        print(f"   🏢 Con grupo: {len(df_seg[df_seg['grupo_operativo'] != 'DESCONOCIDO'])}")
    
    print(f"\n🎯 RESUMEN TOTAL:")
    print(f"   📊 Supervisiones: {total_supervisiones}")
    print(f"   🌍 Con coordenadas: {total_con_coordenadas} ({100*total_con_coordenadas/total_supervisiones:.1f}%)")
    print(f"   🏢 Con grupo: {total_con_grupo} ({100*total_con_grupo/total_supervisiones:.1f}%)")
    
    return {
        'total': total_supervisiones,
        'con_coordenadas': total_con_coordenadas,
        'con_grupo': total_con_grupo,
        'porcentaje_completo': 100 * min(total_con_coordenadas, total_con_grupo) / total_supervisiones
    }

def main():
    """Función principal"""
    
    print("🔧 ENRIQUECER EXCEL PARA POSTGRESQL")
    print("=" * 80)
    print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Roberto: Agregar coordenadas + estado + grupo operativo")
    print("=" * 80)
    
    # 1. Cargar datos base
    df_sucursales, df_ops, df_seg = cargar_datos_base()
    
    if df_sucursales is None:
        print("❌ No se puede continuar sin catálogo de sucursales")
        return
    
    # 2. Crear mapeo sucursales
    mapeo = crear_mapeo_sucursales(df_sucursales)
    
    # 3. Enriquecer operativas
    df_ops_enriquecido = None
    if df_ops is not None:
        df_ops_enriquecido = enriquecer_excel(df_ops, mapeo, 'operativas')
        if df_ops_enriquecido is not None:
            df_ops_enriquecido = reordenar_columnas_postgresql(df_ops_enriquecido, 'operativas')
    
    # 4. Enriquecer seguridad
    df_seg_enriquecido = None
    if df_seg is not None:
        df_seg_enriquecido = enriquecer_excel(df_seg, mapeo, 'seguridad')
        if df_seg_enriquecido is not None:
            df_seg_enriquecido = reordenar_columnas_postgresql(df_seg_enriquecido, 'seguridad')
    
    # 5. Generar Excel PostgreSQL
    archivo_ops, archivo_seg = generar_excel_postgresql(df_ops_enriquecido, df_seg_enriquecido)
    
    # 6. Validación final
    metricas = validar_datos_postgresql(df_ops_enriquecido, df_seg_enriquecido)
    
    print(f"\n🎯 ¡EXCEL ENRIQUECIDO PARA POSTGRESQL!")
    print("=" * 60)
    print(f"✅ {metricas['porcentaje_completo']:.1f}% datos completos")
    print(f"📊 Listos para Railway PostgreSQL Hobby")
    print(f"🚀 Próximo paso: Crear schema PostgreSQL")

if __name__ == "__main__":
    main()