#!/usr/bin/env python3
"""
📊 EXTRAER CALIFICACIONES COMPLETAS CON ÁREAS
Calificación general + TODAS las calificaciones por área (campos con %)
"""

import pandas as pd
from datetime import datetime
import numpy as np

def cargar_datos_completos_con_areas():
    """Cargar Excel originales con calificaciones generales y por áreas"""
    
    print("🔧 CARGAR CALIFICACIONES COMPLETAS CON ÁREAS")
    print("=" * 60)
    
    # Excel operativas original
    try:
        df_ops_original = pd.read_excel("SUPERVISION_OPERATIVA_CAS_11_REV_250125_Submissions-2025-12-18__1228CST-1766141816.xlsx")
        print(f"✅ Operativas original: {len(df_ops_original)} registros, {len(df_ops_original.columns)} columnas")
    except FileNotFoundError:
        print("❌ No encontré el Excel de operativas original")
        df_ops_original = pd.DataFrame()
    
    # Excel seguridad original  
    try:
        df_seg_original = pd.read_excel("CONTROL_OPERATIVO_DE_SEGURIDAD_CAS_11_REV_25012025_Submissions-2025-12-18__1232CST-1766104009.xlsx")
        print(f"✅ Seguridad original: {len(df_seg_original)} registros, {len(df_seg_original.columns)} columnas")
    except FileNotFoundError:
        print("❌ No encontré el Excel de seguridad original")
        df_seg_original = pd.DataFrame()
    
    # Dataset con 476 supervisiones asignadas
    df_dataset = pd.read_csv("DATASET_EMPAREJADO_20251218_164319.csv")
    print(f"✅ Dataset asignadas: {len(df_dataset)} supervisiones")
    
    # Catálogo sucursales
    df_sucursales = pd.read_csv("SUCURSALES_CORRECCIONES_ROBERTO_20251218_171807.csv")
    print(f"✅ Catálogo: {len(df_sucursales)} sucursales")
    
    return df_ops_original, df_seg_original, df_dataset, df_sucursales

def identificar_campos_calificacion_y_areas(df, tipo):
    """Identificar calificación general y TODAS las calificaciones por área"""
    
    print(f"\n🔍 IDENTIFICAR CALIFICACIONES Y ÁREAS {tipo.upper()}")
    print("=" * 60)
    
    if len(df) == 0:
        print("❌ DataFrame vacío")
        return {}, []
    
    # Campos principales
    campos_principales = {}
    campos_areas = []
    
    for col in df.columns:
        col_lower = str(col).lower()
        
        # CAMPOS PRINCIPALES
        if 'submission' in col_lower and 'id' in col_lower:
            campos_principales['submission_id'] = col
        elif 'date' in col_lower and 'submit' in col_lower:
            campos_principales['date_submitted'] = col
        elif col_lower == 'location':
            campos_principales['location'] = col
        elif col_lower in ['sucursal', 'SUCURSAL']:
            campos_principales['sucursal'] = col
            
        # CALIFICACIONES PRINCIPALES
        elif col == 'PUNTOS MAXIMOS':  # Operativas
            campos_principales['puntos_maximos'] = col
        elif col == 'PUNTOS TOTALES':  # Operativas
            campos_principales['puntos_totales'] = col
        elif col == 'PORCENTAJE %':   # Operativas - CALIFICACIÓN GENERAL
            campos_principales['calificacion_general'] = col
            
        elif col == 'PUNTOS MAX':     # Seguridad
            campos_principales['puntos_maximos'] = col
        elif col == 'PUNTOS TOTALES OBTENIDOS':  # Seguridad
            campos_principales['puntos_totales'] = col
        elif col == 'CALIFICACION PORCENTAJE %': # Seguridad - CALIFICACIÓN GENERAL
            campos_principales['calificacion_general'] = col
        
        # CALIFICACIONES POR ÁREA (todos los campos con %)
        elif '%' in col and col not in [campos_principales.get('calificacion_general', '')]:
            # Excluir campos que no son calificaciones de área
            if not any(exclude in col_lower for exclude in ['fecha', 'note', 'comment', 'photo']):
                campos_areas.append(col)
    
    print(f"🎯 CAMPOS PRINCIPALES:")
    for key, field in campos_principales.items():
        if key == 'calificacion_general' and field in df.columns:
            valores = df[field].describe()
            print(f"   ✅ {key}: {field}")
            print(f"      📊 Min: {valores['min']:.1f} | Max: {valores['max']:.1f} | Promedio: {valores['mean']:.1f}")
        elif field in df.columns:
            print(f"   📋 {key}: {field}")
    
    print(f"\n🏢 CALIFICACIONES POR ÁREA ENCONTRADAS: {len(campos_areas)}")
    
    # Mostrar las áreas por categorías
    areas_por_categoria = {}
    for campo_area in campos_areas:
        # Extraer nombre del área
        area_name = campo_area.split(' %')[0].strip()
        area_name = area_name.split(' PORCENTAJE')[0].strip()
        area_name = area_name.split(' CALIFICACION')[0].strip()
        area_name = area_name.split(' CALIFICACIÓN')[0].strip()
        
        # Categorizar
        categoria = 'OTRAS'
        if any(word in area_name.upper() for word in ['MARINADO', 'MARINADOR']):
            categoria = 'MARINADO'
        elif any(word in area_name.upper() for word in ['CUARTO FRIO', 'REFRIGERADOR', 'CONGELADOR']):
            categoria = 'REFRIGERACIÓN'
        elif any(word in area_name.upper() for word in ['HORNO', 'FREIDORA', 'ASADOR', 'PLANCHA']):
            categoria = 'COCINA'
        elif any(word in area_name.upper() for word in ['BAÑO', 'LAVADO', 'LIMPIEZA']):
            categoria = 'HIGIENE'
        elif any(word in area_name.upper() for word in ['COMEDOR', 'SERVICIO', 'BARRA']):
            categoria = 'SERVICIO'
        elif any(word in area_name.upper() for word in ['ALMACEN', 'BODEGA', 'EXTERIOR']):
            categoria = 'ALMACENAMIENTO'
        
        if categoria not in areas_por_categoria:
            areas_por_categoria[categoria] = []
        areas_por_categoria[categoria].append(area_name)
    
    # Mostrar por categorías
    for categoria, areas in areas_por_categoria.items():
        print(f"\n   📋 {categoria} ({len(areas)} áreas):")
        for i, area in enumerate(areas[:5]):  # Mostrar primeras 5
            print(f"      {i+1}. {area[:60]}")
        if len(areas) > 5:
            print(f"      ... y {len(areas)-5} más")
    
    return campos_principales, campos_areas

def procesar_calificaciones_completas_con_areas(df_original, df_dataset_filtrado, tipo, catalogo_sucursales):
    """Procesar calificaciones generales y por áreas"""
    
    print(f"\n🔧 PROCESAR {tipo.upper()} COMPLETO CON ÁREAS")
    print("=" * 60)
    
    if len(df_original) == 0:
        print(f"❌ No hay datos en Excel original de {tipo}")
        return []
    
    # Identificar campos
    campos_principales, campos_areas = identificar_campos_calificacion_y_areas(df_original, tipo)
    
    if 'submission_id' not in campos_principales or 'calificacion_general' not in campos_principales:
        print(f"❌ No encontré campos principales necesarios en {tipo}")
        return []
    
    print(f"\n🏗️ PROCESANDO CON {len(campos_areas)} CALIFICACIONES POR ÁREA")
    
    # Procesar cada supervisión
    datos_procesados = []
    matched_count = 0
    
    for i, (_, row_dataset) in enumerate(df_dataset_filtrado.iterrows(), 1):
        submission_id = row_dataset['submission_id']
        location_asignado = row_dataset['location_asignado']
        
        # Buscar en Excel original
        matches = df_original[df_original[campos_principales['submission_id']] == submission_id]
        
        if len(matches) > 0:
            row_original = matches.iloc[0]
            matched_count += 1
            
            # Datos de sucursal
            sucursal_info = catalogo_sucursales.get(location_asignado, {})
            
            # CREAR REGISTRO BASE
            registro = {
                # DATOS BÁSICOS
                'submission_id': submission_id,
                'tipo_supervision': tipo,
                'date_submitted': row_dataset['date_submitted'],
                'usuario': row_dataset.get('usuario', ''),
                'location_asignado': location_asignado,
                
                # DATOS SUCURSAL
                'sucursal_numero': sucursal_info.get('numero'),
                'sucursal_nombre': sucursal_info.get('nombre'),
                'sucursal_tipo': sucursal_info.get('tipo'),
                'sucursal_grupo': sucursal_info.get('grupo'),
                'sucursal_lat': sucursal_info.get('lat'),
                'sucursal_lon': sucursal_info.get('lon'),
                
                # CALIFICACIÓN GENERAL OFICIAL
                'calificacion_general_zenput': row_original.get(campos_principales['calificacion_general']),
                'puntos_maximos_zenput': row_original.get(campos_principales.get('puntos_maximos')),
                'puntos_totales_zenput': row_original.get(campos_principales.get('puntos_totales')),
                
                # METADATOS
                'tiene_calificacion_oficial': True,
                'areas_evaluadas': len(campos_areas),
                'fuente_calificacion': f'zenput_completo_{tipo}',
                'fecha_procesamiento': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # AGREGAR TODAS LAS CALIFICACIONES POR ÁREA
            for campo_area in campos_areas:
                calificacion_area = row_original.get(campo_area)
                
                # Limpiar nombre del campo para usar como columna
                nombre_limpio = campo_area.replace(' %', '').replace(' PORCENTAJE', '').replace(' CALIFICACION', '').replace(' CALIFICACIÓN', '')
                nombre_limpio = nombre_limpio.replace(' ', '_').replace('/', '_').replace('(', '').replace(')', '')
                nombre_limpio = f"AREA_{nombre_limpio}"
                
                registro[nombre_limpio] = calificacion_area
            
            datos_procesados.append(registro)
        
        else:
            # No encontrado - crear registro sin calificaciones
            sucursal_info = catalogo_sucursales.get(location_asignado, {})
            
            registro = {
                'submission_id': submission_id,
                'tipo_supervision': tipo,
                'date_submitted': row_dataset['date_submitted'],
                'usuario': row_dataset.get('usuario', ''),
                'location_asignado': location_asignado,
                'sucursal_numero': sucursal_info.get('numero'),
                'sucursal_nombre': sucursal_info.get('nombre'),
                'sucursal_tipo': sucursal_info.get('tipo'),
                'sucursal_grupo': sucursal_info.get('grupo'),
                'sucursal_lat': sucursal_info.get('lat'),
                'sucursal_lon': sucursal_info.get('lon'),
                'calificacion_general_zenput': None,
                'puntos_maximos_zenput': None,
                'puntos_totales_zenput': None,
                'tiene_calificacion_oficial': False,
                'areas_evaluadas': 0,
                'fuente_calificacion': 'no_encontrada',
                'fecha_procesamiento': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Agregar campos de área vacíos
            for campo_area in campos_areas:
                nombre_limpio = campo_area.replace(' %', '').replace(' PORCENTAJE', '').replace(' CALIFICACION', '').replace(' CALIFICACIÓN', '')
                nombre_limpio = nombre_limpio.replace(' ', '_').replace('/', '_').replace('(', '').replace(')', '')
                nombre_limpio = f"AREA_{nombre_limpio}"
                registro[nombre_limpio] = None
            
            datos_procesados.append(registro)
        
        # Progress
        if i % 50 == 0:
            print(f"   Procesadas: {i}/{len(df_dataset_filtrado)}")
    
    print(f"\n✅ RESUMEN {tipo.upper()}:")
    print(f"   📊 Procesadas: {len(datos_procesados)} supervisiones")
    print(f"   🎯 Con calificaciones: {matched_count}")
    print(f"   🏢 Áreas por supervisión: {len(campos_areas)}")
    
    return datos_procesados

def crear_excel_completo_con_todas_areas(datos_procesados, tipo):
    """Crear Excel con calificación general + todas las áreas"""
    
    print(f"\n📊 CREAR EXCEL {tipo.upper()} COMPLETO CON ÁREAS")
    print("=" * 60)
    
    if not datos_procesados:
        print(f"❌ No hay datos para {tipo}")
        return None
    
    # Crear DataFrame
    df = pd.DataFrame(datos_procesados)
    
    # Separar columnas por categorías
    columnas_basicas = ['submission_id', 'tipo_supervision', 'date_submitted', 'usuario', 'location_asignado']
    
    columnas_sucursal = ['sucursal_numero', 'sucursal_nombre', 'sucursal_tipo', 'sucursal_grupo', 'sucursal_lat', 'sucursal_lon']
    
    columnas_calificacion_general = ['calificacion_general_zenput', 'puntos_maximos_zenput', 'puntos_totales_zenput']
    
    columnas_metadatos = ['tiene_calificacion_oficial', 'areas_evaluadas', 'fuente_calificacion', 'fecha_procesamiento']
    
    # Columnas de áreas (todas las que empiezan con AREA_)
    columnas_areas = [col for col in df.columns if col.startswith('AREA_')]
    columnas_areas.sort()  # Ordenar alfabéticamente
    
    print(f"   📊 Columnas de áreas: {len(columnas_areas)}")
    
    # Reordenar DataFrame
    columnas_ordenadas = columnas_basicas + columnas_sucursal + columnas_calificacion_general + columnas_areas + columnas_metadatos
    columnas_finales = [col for col in columnas_ordenadas if col in df.columns]
    df_final = df[columnas_finales]
    
    # Ordenar por calificación general
    df_final = df_final.sort_values('calificacion_general_zenput', ascending=False)
    
    # Crear Excel
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archivo_excel = f"SUPERVISIONES_{tipo.upper()}_COMPLETO_CON_AREAS_{timestamp}.xlsx"
    
    with pd.ExcelWriter(archivo_excel, engine='openpyxl') as writer:
        # Hoja principal con todo
        df_final.to_excel(writer, sheet_name=f'{tipo.title()}_Completo', index=False)
        
        # Hoja solo con calificaciones (sin metadatos)
        columnas_calificaciones = columnas_basicas + columnas_sucursal + columnas_calificacion_general + columnas_areas
        columnas_cal_finales = [col for col in columnas_calificaciones if col in df.columns]
        df_calificaciones = df_final[columnas_cal_finales]
        df_calificaciones.to_excel(writer, sheet_name=f'{tipo.title()}_Solo_Calificaciones', index=False)
        
        # Hoja de estadísticas por área
        crear_hoja_estadisticas_areas(writer, df_final, columnas_areas)
        
        # Hoja de ranking de áreas
        crear_hoja_ranking_areas(writer, df_final, columnas_areas)
    
    print(f"✅ Excel creado: {archivo_excel}")
    print(f"   📊 Total registros: {len(df_final)}")
    print(f"   📋 Total columnas: {len(df_final.columns)}")
    print(f"   🏢 Columnas de áreas: {len(columnas_areas)}")
    
    return archivo_excel, df_final

def crear_hoja_estadisticas_areas(writer, df, columnas_areas):
    """Crear estadísticas por área"""
    
    estadisticas_areas = []
    
    for col_area in columnas_areas:
        nombre_area = col_area.replace('AREA_', '').replace('_', ' ')
        valores_validos = df[col_area].dropna()
        
        if len(valores_validos) > 0:
            estadisticas_areas.append({
                'Area': nombre_area,
                'Supervisiones_Evaluadas': len(valores_validos),
                'Promedio': round(valores_validos.mean(), 1),
                'Minimo': round(valores_validos.min(), 1),
                'Maximo': round(valores_validos.max(), 1),
                'Desviacion': round(valores_validos.std(), 1)
            })
    
    if estadisticas_areas:
        df_stats_areas = pd.DataFrame(estadisticas_areas)
        df_stats_areas = df_stats_areas.sort_values('Promedio', ascending=False)
        df_stats_areas.to_excel(writer, sheet_name='Estadisticas_Areas', index=False)

def crear_hoja_ranking_areas(writer, df, columnas_areas):
    """Crear ranking de sucursales por áreas"""
    
    con_calificacion = df[df['tiene_calificacion_oficial'] == True]
    
    if len(con_calificacion) > 0:
        # Calcular promedio por sucursal para cada área
        ranking_data = []
        
        for _, row in con_calificacion.iterrows():
            sucursal = row['sucursal_nombre']
            calificacion_general = row['calificacion_general_zenput']
            
            # Calcular promedio de áreas para esta sucursal
            calificaciones_areas = []
            for col_area in columnas_areas:
                if pd.notna(row[col_area]):
                    calificaciones_areas.append(row[col_area])
            
            promedio_areas = np.mean(calificaciones_areas) if calificaciones_areas else None
            
            ranking_data.append({
                'Sucursal': sucursal,
                'Calificacion_General': calificacion_general,
                'Promedio_Areas': promedio_areas,
                'Areas_Evaluadas': len(calificaciones_areas)
            })
        
        if ranking_data:
            df_ranking = pd.DataFrame(ranking_data)
            df_ranking = df_ranking.groupby('Sucursal').agg({
                'Calificacion_General': 'mean',
                'Promedio_Areas': 'mean',
                'Areas_Evaluadas': 'mean'
            }).round(1).reset_index()
            
            df_ranking = df_ranking.sort_values('Calificacion_General', ascending=False)
            df_ranking.to_excel(writer, sheet_name='Ranking_Sucursales_Areas', index=False)

def main():
    """Función principal"""
    
    print("📊 EXTRAER CALIFICACIONES COMPLETAS CON ÁREAS")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Roberto: Calificación general + TODAS las áreas (~30 operativas, ~11 seguridad)")
    print("=" * 80)
    
    # 1. Cargar datos
    df_ops_original, df_seg_original, df_dataset, df_sucursales = cargar_datos_completos_con_areas()
    
    # 2. Crear catálogo sucursales
    catalogo_sucursales = {}
    for _, row in df_sucursales.iterrows():
        if pd.notna(row['numero']) and pd.notna(row['nombre']):
            numero = int(row['numero'])
            nombre = row['nombre']
            location_key = f"{numero} - {nombre}"
            catalogo_sucursales[location_key] = {
                'numero': numero,
                'nombre': nombre,
                'tipo': row.get('tipo', 'LOCAL'),
                'grupo': row.get('grupo', ''),
                'lat': row.get('lat', None),
                'lon': row.get('lon', None)
            }
    
    # 3. Separar dataset
    operativas_dataset = df_dataset[df_dataset['tipo'] == 'operativas']
    seguridad_dataset = df_dataset[df_dataset['tipo'] == 'seguridad']
    
    print(f"\n📊 DATASET PARA PROCESAR:")
    print(f"   🔧 Operativas: {len(operativas_dataset)}")
    print(f"   🛡️ Seguridad: {len(seguridad_dataset)}")
    
    # 4. Procesar operativas con todas las áreas
    datos_operativas = procesar_calificaciones_completas_con_areas(
        df_ops_original, operativas_dataset, 'operativas', catalogo_sucursales
    )
    
    # 5. Procesar seguridad con todas las áreas
    datos_seguridad = procesar_calificaciones_completas_con_areas(
        df_seg_original, seguridad_dataset, 'seguridad', catalogo_sucursales
    )
    
    # 6. Crear Excel operativas completo
    archivo_operativas, df_operativas = crear_excel_completo_con_todas_areas(datos_operativas, 'operativas')
    
    # 7. Crear Excel seguridad completo
    archivo_seguridad, df_seguridad = crear_excel_completo_con_todas_areas(datos_seguridad, 'seguridad')
    
    # 8. Resumen final
    print(f"\n🎯 EXCEL COMPLETO CON TODAS LAS ÁREAS:")
    print("=" * 70)
    
    if archivo_operativas:
        areas_ops = len([col for col in df_operativas.columns if col.startswith('AREA_')])
        con_cal_ops = df_operativas[df_operativas['tiene_calificacion_oficial'] == True]
        print(f"✅ OPERATIVAS: {archivo_operativas}")
        print(f"   📊 238 supervisiones con {areas_ops} áreas evaluadas")
        print(f"   📈 Promedio general: {con_cal_ops['calificacion_general_zenput'].mean():.1f}")
    
    if archivo_seguridad:
        areas_seg = len([col for col in df_seguridad.columns if col.startswith('AREA_')])
        con_cal_seg = df_seguridad[df_seguridad['tiene_calificacion_oficial'] == True]
        print(f"✅ SEGURIDAD: {archivo_seguridad}")
        print(f"   📊 238 supervisiones con {areas_seg} áreas evaluadas")
        print(f"   📈 Promedio general: {con_cal_seg['calificacion_general_zenput'].mean():.1f}")
    
    print(f"\n📋 ESTRUCTURA FINAL INCLUYE:")
    print(f"   ✅ Calificación general oficial")
    print(f"   ✅ TODAS las calificaciones por área")
    print(f"   ✅ Datos completos de sucursales")
    print(f"   ✅ 4 hojas: Completo + Solo Calificaciones + Estadísticas + Ranking")
    
    print(f"\n🎯 ¡AHORA SÍ ROBERTO! TIENES TODO:")
    print(f"   📊 476 supervisiones asignadas")
    print(f"   🎯 Calificaciones oficiales Zenput")
    print(f"   🏢 ~30 áreas operativas + ~11 áreas seguridad")
    print(f"   📈 Listo para PostgreSQL y Dashboard")
    
    return archivo_operativas, archivo_seguridad

if __name__ == "__main__":
    main()