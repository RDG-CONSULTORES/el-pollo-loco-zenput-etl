#!/usr/bin/env python3
"""
📊 EXTRAER CALIFICACIONES DESDE EXCEL ORIGINALES
Calcular calificaciones para TODAS las 476 supervisiones usando los Excel originales
"""

import pandas as pd
from datetime import datetime
import numpy as np

def cargar_exceles_originales():
    """Cargar los Excel originales con todas las respuestas"""
    
    print("🔧 CARGAR EXCEL ORIGINALES CON RESPUESTAS COMPLETAS")
    print("=" * 60)
    
    # Excel operativas original
    try:
        df_ops_original = pd.read_excel("SUPERVISION_OPERATIVA_CAS_11_REV_250125_Submissions-2025-12-18__1228CST-1766141816.xlsx")
        print(f"✅ Operativas original: {len(df_ops_original)} registros")
    except FileNotFoundError:
        print("❌ No encontré el Excel de operativas original")
        df_ops_original = pd.DataFrame()
    
    # Excel seguridad original  
    try:
        df_seg_original = pd.read_excel("CONTROL_OPERATIVO_DE_SEGURIDAD_CAS_11_REV_25012025_Submissions-2025-12-18__1232CST-1766104009.xlsx")
        print(f"✅ Seguridad original: {len(df_seg_original)} registros")
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

def analizar_estructura_excel_original(df, tipo):
    """Analizar estructura del Excel original"""
    
    print(f"\n🔍 ANALIZAR ESTRUCTURA {tipo.upper()}")
    print("=" * 50)
    
    if len(df) == 0:
        print("❌ DataFrame vacío")
        return {}
    
    print(f"📊 Columnas totales: {len(df.columns)}")
    print(f"📋 Registros: {len(df)}")
    
    # Buscar columnas clave
    columnas_clave = {}
    
    for col in df.columns:
        col_lower = str(col).lower()
        if 'submission' in col_lower and 'id' in col_lower:
            columnas_clave['submission_id'] = col
        elif 'date' in col_lower and 'submit' in col_lower:
            columnas_clave['date_submitted'] = col
        elif 'location' in col_lower:
            columnas_clave['location'] = col
    
    print(f"🔑 Columnas clave encontradas: {list(columnas_clave.keys())}")
    
    # Mostrar muestra de primeras columnas
    print(f"\n📋 PRIMERAS 10 COLUMNAS:")
    for i, col in enumerate(df.columns[:10]):
        valores_unicos = df[col].nunique()
        print(f"   {i+1:2}. {col[:50]:<50} | {valores_unicos} únicos")
    
    return columnas_clave

def calcular_calificacion_desde_respuestas(row, columnas_respuestas):
    """Calcular calificación desde las respuestas del formulario"""
    
    total_preguntas = 0
    respuestas_conformes = 0
    respuestas_no_conformes = 0
    puntos_criticos = []
    
    for col in columnas_respuestas:
        valor = row.get(col, '')
        
        # Verificar si es una respuesta válida
        if pd.notna(valor) and str(valor).strip() != '':
            total_preguntas += 1
            
            valor_str = str(valor).lower().strip()
            
            # Clasificar respuestas
            if any(word in valor_str for word in ['sí', 'si', 'yes', 'true', '1', 'correcto', 'bien', 'cumple']):
                respuestas_conformes += 1
            elif any(word in valor_str for word in ['no', 'false', '0', 'incorrecto', 'mal', 'falta', 'no cumple']):
                respuestas_no_conformes += 1
                # Agregar punto crítico
                pregunta_nombre = col.replace('_', ' ')[:50]
                puntos_criticos.append(pregunta_nombre)
    
    # Calcular métricas
    total_evaluables = respuestas_conformes + respuestas_no_conformes
    
    if total_evaluables > 0:
        porcentaje_conformidad = (respuestas_conformes / total_evaluables) * 100
        calificacion_general = porcentaje_conformidad  # Usar conformidad como calificación
    else:
        porcentaje_conformidad = 0
        calificacion_general = 0
    
    return {
        'total_preguntas': total_preguntas,
        'total_evaluables': total_evaluables,
        'respuestas_conformes': respuestas_conformes,
        'respuestas_no_conformes': respuestas_no_conformes,
        'porcentaje_conformidad': round(porcentaje_conformidad, 1),
        'calificacion_general': round(calificacion_general, 1),
        'puntos_criticos_count': len(puntos_criticos),
        'puntos_criticos_top5': '; '.join(puntos_criticos[:5]) if puntos_criticos else ''
    }

def procesar_excel_original_completo(df_original, df_dataset_filtrado, tipo, catalogo_sucursales):
    """Procesar Excel original para extraer calificaciones"""
    
    print(f"\n🔧 PROCESAR {tipo.upper()} COMPLETO")
    print("=" * 50)
    
    if len(df_original) == 0:
        print(f"❌ No hay datos en Excel original de {tipo}")
        return []
    
    # Analizar estructura
    columnas_clave = analizar_estructura_excel_original(df_original, tipo)
    
    # Identificar columnas de respuestas (excluir metadata)
    columnas_metadata = ['submission_id', 'date_submitted', 'location', 'user', 'created', 'smetadata']
    columnas_respuestas = []
    
    for col in df_original.columns:
        col_lower = str(col).lower()
        es_metadata = any(meta in col_lower for meta in columnas_metadata)
        if not es_metadata and len(str(col)) > 5:  # Probablemente pregunta del formulario
            columnas_respuestas.append(col)
    
    print(f"📝 Columnas de respuestas identificadas: {len(columnas_respuestas)}")
    
    # Crear índice por submission_id
    submission_col = columnas_clave.get('submission_id', 'Submission ID')
    if submission_col not in df_original.columns:
        print(f"❌ No encontré columna submission_id en {tipo}")
        return []
    
    # Procesar cada supervisión asignada
    datos_procesados = []
    matched_count = 0
    
    for _, row_dataset in df_dataset_filtrado.iterrows():
        submission_id = row_dataset['submission_id']
        location_asignado = row_dataset['location_asignado']
        
        # Buscar en Excel original
        matches = df_original[df_original[submission_col] == submission_id]
        
        if len(matches) > 0:
            row_original = matches.iloc[0]
            matched_count += 1
            
            # Calcular calificaciones desde respuestas
            kpis = calcular_calificacion_desde_respuestas(row_original, columnas_respuestas)
            
            # Datos de sucursal
            sucursal_info = catalogo_sucursales.get(location_asignado, {})
            
            # Crear registro completo
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
                
                # CALIFICACIONES CALCULADAS
                'calificacion_general': kpis['calificacion_general'],
                'total_preguntas': kpis['total_preguntas'],
                'total_evaluables': kpis['total_evaluables'],
                'respuestas_conformes': kpis['respuestas_conformes'],
                'respuestas_no_conformes': kpis['respuestas_no_conformes'],
                'porcentaje_conformidad': kpis['porcentaje_conformidad'],
                'puntos_criticos_count': kpis['puntos_criticos_count'],
                'puntos_criticos_principales': kpis['puntos_criticos_top5'],
                
                # METADATOS
                'tiene_calificaciones': True,
                'fuente_calificaciones': f'excel_original_{tipo}',
                'fecha_procesamiento': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            datos_procesados.append(registro)
        
        else:
            # No encontrado en Excel original
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
                'calificacion_general': None,
                'total_preguntas': 0,
                'total_evaluables': 0,
                'respuestas_conformes': 0,
                'respuestas_no_conformes': 0,
                'porcentaje_conformidad': 0,
                'puntos_criticos_count': 0,
                'puntos_criticos_principales': '',
                'tiene_calificaciones': False,
                'fuente_calificaciones': 'no_encontrada',
                'fecha_procesamiento': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            datos_procesados.append(registro)
    
    print(f"✅ Procesadas: {len(datos_procesados)} supervisiones")
    print(f"🎯 Con calificaciones: {matched_count}")
    print(f"📊 Promedio calificación: {np.mean([d['calificacion_general'] for d in datos_procesados if d['calificacion_general'] is not None]):.1f}")
    
    return datos_procesados

def crear_excel_completo_con_todas_calificaciones(datos_procesados, tipo):
    """Crear Excel con TODAS las calificaciones"""
    
    print(f"\n📊 CREAR EXCEL {tipo.upper()} COMPLETO")
    print("=" * 50)
    
    if not datos_procesados:
        print(f"❌ No hay datos para {tipo}")
        return None
    
    # Crear DataFrame
    df = pd.DataFrame(datos_procesados)
    
    # Ordenar por calificación (mejores primero)
    df = df.sort_values('calificacion_general', ascending=False)
    
    # Crear Excel
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archivo_excel = f"SUPERVISIONES_{tipo.upper()}_TODAS_CALIFICACIONES_{timestamp}.xlsx"
    
    with pd.ExcelWriter(archivo_excel, engine='openpyxl') as writer:
        # Hoja principal
        df.to_excel(writer, sheet_name=f'{tipo.title()}', index=False)
        
        # Hoja de estadísticas
        crear_hoja_estadisticas_completas(writer, df, tipo)
        
        # Hoja de ranking
        crear_hoja_ranking_completo(writer, df)
    
    print(f"✅ Excel creado: {archivo_excel}")
    print(f"📊 Total registros: {len(df)}")
    print(f"🎯 Con calificaciones: {len(df[df['tiene_calificaciones'] == True])}")
    
    return archivo_excel, df

def crear_hoja_estadisticas_completas(writer, df, tipo):
    """Crear hoja de estadísticas completas"""
    
    with_calificaciones = df[df['tiene_calificaciones'] == True]
    sin_calificaciones = df[df['tiene_calificaciones'] == False]
    
    estadisticas = {
        'Métrica': [
            'Total Supervisiones',
            'Con Calificaciones Calculadas',
            'Sin Calificaciones',
            'Porcentaje Cobertura',
            'Calificación Promedio',
            'Calificación Máxima',
            'Calificación Mínima',
            'Conformidad Promedio',
            'Total Preguntas Promedio',
            'Puntos Críticos Promedio'
        ],
        'Valor': [
            len(df),
            len(with_calificaciones),
            len(sin_calificaciones),
            f"{len(with_calificaciones)/len(df)*100:.1f}%",
            f"{with_calificaciones['calificacion_general'].mean():.1f}" if len(with_calificaciones) > 0 else 'N/A',
            f"{with_calificaciones['calificacion_general'].max():.1f}" if len(with_calificaciones) > 0 else 'N/A',
            f"{with_calificaciones['calificacion_general'].min():.1f}" if len(with_calificaciones) > 0 else 'N/A',
            f"{with_calificaciones['porcentaje_conformidad'].mean():.1f}%" if len(with_calificaciones) > 0 else 'N/A',
            f"{with_calificaciones['total_preguntas'].mean():.1f}" if len(with_calificaciones) > 0 else 'N/A',
            f"{with_calificaciones['puntos_criticos_count'].mean():.1f}" if len(with_calificaciones) > 0 else 'N/A'
        ]
    }
    
    df_stats = pd.DataFrame(estadisticas)
    df_stats.to_excel(writer, sheet_name='Estadisticas', index=False)

def crear_hoja_ranking_completo(writer, df):
    """Crear ranking completo por sucursal"""
    
    with_calificaciones = df[df['tiene_calificaciones'] == True]
    
    if len(with_calificaciones) > 0:
        ranking = with_calificaciones.groupby('sucursal_nombre').agg({
            'calificacion_general': 'mean',
            'porcentaje_conformidad': 'mean',
            'total_preguntas': 'mean',
            'puntos_criticos_count': 'mean',
            'submission_id': 'count'
        }).round(1)
        
        ranking = ranking.rename(columns={'submission_id': 'total_supervisiones'})
        ranking = ranking.sort_values('calificacion_general', ascending=False)
        ranking = ranking.reset_index()
        
        ranking.to_excel(writer, sheet_name='Ranking_Sucursales', index=False)

def main():
    """Función principal"""
    
    print("📊 EXTRAER CALIFICACIONES DESDE EXCEL ORIGINALES")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Roberto: TODAS las 476 supervisiones con calificaciones completas")
    print("=" * 80)
    
    # 1. Cargar datos
    df_ops_original, df_seg_original, df_dataset, df_sucursales = cargar_exceles_originales()
    
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
    
    # 3. Separar dataset por tipo
    operativas_dataset = df_dataset[df_dataset['tipo'] == 'operativas']
    seguridad_dataset = df_dataset[df_dataset['tipo'] == 'seguridad']
    
    print(f"\n📊 DATASET ASIGNADAS:")
    print(f"   🔧 Operativas: {len(operativas_dataset)}")
    print(f"   🛡️ Seguridad: {len(seguridad_dataset)}")
    
    # 4. Procesar operativas
    datos_operativas = procesar_excel_original_completo(
        df_ops_original, operativas_dataset, 'operativas', catalogo_sucursales
    )
    
    # 5. Procesar seguridad
    datos_seguridad = procesar_excel_original_completo(
        df_seg_original, seguridad_dataset, 'seguridad', catalogo_sucursales
    )
    
    # 6. Crear Excel operativas
    archivo_operativas, df_operativas = crear_excel_completo_con_todas_calificaciones(datos_operativas, 'operativas')
    
    # 7. Crear Excel seguridad
    archivo_seguridad, df_seguridad = crear_excel_completo_con_todas_calificaciones(datos_seguridad, 'seguridad')
    
    # 8. Resumen final
    print(f"\n🎯 EXCEL CON TODAS LAS CALIFICACIONES:")
    print("=" * 60)
    if archivo_operativas:
        print(f"✅ OPERATIVAS: {archivo_operativas}")
        print(f"   📊 {len(df_operativas)} supervisiones")
        print(f"   🎯 {len(df_operativas[df_operativas['tiene_calificaciones'] == True])} con calificaciones")
    
    if archivo_seguridad:
        print(f"✅ SEGURIDAD: {archivo_seguridad}")
        print(f"   📊 {len(df_seguridad)} supervisiones")
        print(f"   🎯 {len(df_seguridad[df_seguridad['tiene_calificaciones'] == True])} con calificaciones")
    
    print(f"\n✅ AHORA SÍ TIENES TODAS LAS CALIFICACIONES ROBERTO!")
    
    return archivo_operativas, archivo_seguridad

if __name__ == "__main__":
    main()