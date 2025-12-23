#!/usr/bin/env python3
"""
📊 EXTRAER EXCEL CON CALIFICACIONES COMPLETAS
Combinar las 476 supervisiones asignadas con las calificaciones y KPIs ya calculados
"""

import pandas as pd
from datetime import datetime
import json

def cargar_datos_completos():
    """Cargar dataset, catálogo y calificaciones"""
    
    print("🔧 CARGAR DATOS COMPLETOS CON CALIFICACIONES")
    print("=" * 60)
    
    # Dataset con 476 supervisiones asignadas
    df_dataset = pd.read_csv("DATASET_EMPAREJADO_20251218_164319.csv")
    
    # Catálogo de sucursales
    df_sucursales = pd.read_csv("SUCURSALES_CORRECCIONES_ROBERTO_20251218_171807.csv")
    
    # Calificaciones y KPIs ya calculados
    with open("data/analysis_238_supervisiones_20251217_150829.json", 'r', encoding='utf-8') as f:
        calificaciones_data = json.load(f)
    
    print(f"✅ Dataset: {len(df_dataset)} supervisiones")
    print(f"✅ Catálogo: {len(df_sucursales)} sucursales")
    print(f"✅ Calificaciones: {calificaciones_data.get('total_submissions', 0)} con KPIs")
    
    return df_dataset, df_sucursales, calificaciones_data

def crear_indice_calificaciones(calificaciones_data):
    """Crear índice de calificaciones por submission_id"""
    
    calificaciones_index = {}
    
    # Procesar operativas
    if 'forms_data' in calificaciones_data and '877138' in calificaciones_data['forms_data']:
        operativas_data = calificaciones_data['forms_data']['877138']
        if 'submissions_detail' in operativas_data:
            for submission in operativas_data['submissions_detail']:
                submission_id = submission.get('submission_id')
                if submission_id:
                    calificaciones_index[submission_id] = {
                        'tipo': 'operativas',
                        'calificacion_general': submission.get('calificacion_general'),
                        'areas_kpis': submission.get('areas_kpis', {}),
                        'supervisor': submission.get('supervisor', ''),
                        'sucursal': submission.get('sucursal', '')
                    }
    
    # Procesar seguridad
    if 'forms_data' in calificaciones_data and '877139' in calificaciones_data['forms_data']:
        seguridad_data = calificaciones_data['forms_data']['877139']
        if 'submissions_detail' in seguridad_data:
            for submission in seguridad_data['submissions_detail']:
                submission_id = submission.get('submission_id')
                if submission_id:
                    calificaciones_index[submission_id] = {
                        'tipo': 'seguridad',
                        'calificacion_general': submission.get('calificacion_general'),
                        'areas_kpis': submission.get('areas_kpis', {}),
                        'supervisor': submission.get('supervisor', ''),
                        'sucursal': submission.get('sucursal', '')
                    }
    
    print(f"✅ Índice calificaciones: {len(calificaciones_index)} submissions")
    
    return calificaciones_index

def crear_catalogo_sucursales_lookup(df_sucursales):
    """Crear diccionario de sucursales"""
    
    catalogo = {}
    for _, row in df_sucursales.iterrows():
        if pd.notna(row['numero']) and pd.notna(row['nombre']):
            numero = int(row['numero'])
            nombre = row['nombre']
            location_key = f"{numero} - {nombre}"
            
            catalogo[location_key] = {
                'numero': numero,
                'nombre': nombre,
                'tipo': row.get('tipo', 'LOCAL'),
                'grupo': row.get('grupo', ''),
                'lat': row.get('lat', None),
                'lon': row.get('lon', None)
            }
    
    return catalogo

def extraer_kpis_areas(areas_kpis):
    """Extraer KPIs consolidados de todas las áreas"""
    
    kpis_consolidados = {
        'total_elementos_evaluados': 0,
        'total_elementos_conformes': 0,
        'total_elementos_no_conformes': 0,
        'total_elementos_criticos_fallidos': 0,
        'porcentaje_conformidad': 0,
        'areas_evaluadas': 0,
        'puntos_criticos_principales': [],
        'areas_con_problemas': []
    }
    
    if not areas_kpis:
        return kpis_consolidados
    
    total_evaluados = 0
    total_conformes = 0
    total_no_conformes = 0
    areas_count = 0
    todos_puntos_criticos = []
    areas_con_problemas = []
    
    for area_key, area_data in areas_kpis.items():
        if isinstance(area_data, dict):
            areas_count += 1
            
            evaluados = area_data.get('elementos_evaluados', 0)
            conformes = area_data.get('elementos_conformes', 0)
            no_conformes = area_data.get('elementos_no_conformes', 0)
            criticos_fallidos = area_data.get('elementos_criticos_fallidos', [])
            
            total_evaluados += evaluados
            total_conformes += conformes
            total_no_conformes += no_conformes
            
            # Agregar puntos críticos
            for critico in criticos_fallidos:
                if isinstance(critico, dict):
                    elemento = critico.get('elemento', '')
                    if elemento:
                        todos_puntos_criticos.append(elemento[:100])  # Primeros 100 chars
            
            # Identificar áreas con problemas
            if no_conformes > 0:
                area_title = area_data.get('area_title', area_key)
                areas_con_problemas.append(f"{area_title} ({no_conformes} NC)")
    
    # Calcular porcentaje de conformidad
    if total_evaluados > 0:
        porcentaje_conformidad = (total_conformes / total_evaluados) * 100
    else:
        porcentaje_conformidad = 0
    
    kpis_consolidados.update({
        'total_elementos_evaluados': total_evaluados,
        'total_elementos_conformes': total_conformes,
        'total_elementos_no_conformes': total_no_conformes,
        'total_elementos_criticos_fallidos': len(todos_puntos_criticos),
        'porcentaje_conformidad': round(porcentaje_conformidad, 2),
        'areas_evaluadas': areas_count,
        'puntos_criticos_principales': '; '.join(todos_puntos_criticos[:5]),  # Top 5
        'areas_con_problemas': '; '.join(areas_con_problemas[:3])  # Top 3
    })
    
    return kpis_consolidados

def enriquecer_supervision_completa(row, catalogo_sucursales, calificaciones_index):
    """Enriquecer supervision con sucursal y calificaciones"""
    
    submission_id = row['submission_id']
    location_asignado = row['location_asignado']
    
    # Datos de sucursal
    sucursal_info = catalogo_sucursales.get(location_asignado, {})
    
    # Datos de calificaciones
    calif_info = calificaciones_index.get(submission_id, {})
    areas_kpis = calif_info.get('areas_kpis', {})
    kpis_consolidados = extraer_kpis_areas(areas_kpis)
    
    # Crear registro completo
    registro = {
        # DATOS BÁSICOS
        'submission_id': submission_id,
        'tipo_supervision': row['tipo'],
        'date_submitted': row['date_submitted'],
        'date_created': row.get('date_created'),
        'usuario': row.get('usuario', ''),
        'supervisor_calificaciones': calif_info.get('supervisor', ''),
        'año': row.get('año'),
        'hora': row.get('hora'),
        
        # UBICACIÓN Y ASIGNACIÓN
        'lat_entrega': row.get('lat_entrega'),
        'lon_entrega': row.get('lon_entrega'),
        'location_field': row.get('location_map'),
        'sucursal_campo': row.get('sucursal_campo'),
        'location_asignado': location_asignado,
        'tiene_coordenadas': row.get('tiene_coordenadas', False),
        
        # DATOS SUCURSAL
        'sucursal_numero': sucursal_info.get('numero'),
        'sucursal_nombre': sucursal_info.get('nombre'),
        'sucursal_tipo': sucursal_info.get('tipo'),
        'sucursal_grupo': sucursal_info.get('grupo'),
        'sucursal_lat': sucursal_info.get('lat'),
        'sucursal_lon': sucursal_info.get('lon'),
        
        # CALIFICACIONES Y KPIS COMPLETOS
        'calificacion_general': calif_info.get('calificacion_general'),
        'total_elementos_evaluados': kpis_consolidados['total_elementos_evaluados'],
        'total_elementos_conformes': kpis_consolidados['total_elementos_conformes'],
        'total_elementos_no_conformes': kpis_consolidados['total_elementos_no_conformes'],
        'total_elementos_criticos_fallidos': kpis_consolidados['total_elementos_criticos_fallidos'],
        'porcentaje_conformidad': kpis_consolidados['porcentaje_conformidad'],
        'areas_evaluadas': kpis_consolidados['areas_evaluadas'],
        'puntos_criticos_principales': kpis_consolidados['puntos_criticos_principales'],
        'areas_con_problemas': kpis_consolidados['areas_con_problemas'],
        
        # METADATOS
        'tiene_calificaciones': bool(calif_info.get('calificacion_general') is not None),
        'fecha_procesamiento': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'fuente_calificaciones': 'analysis_238_supervisiones' if calif_info else 'pendiente'
    }
    
    return registro

def crear_excel_supervision_con_calificaciones(datos_procesados, tipo_supervision):
    """Crear Excel con calificaciones completas"""
    
    print(f"\n📊 CREAR EXCEL {tipo_supervision.upper()} CON CALIFICACIONES")
    print("=" * 60)
    
    if not datos_procesados:
        print(f"❌ No hay datos para {tipo_supervision}")
        return None
    
    # Crear DataFrame
    df = pd.DataFrame(datos_procesados)
    
    # Ordenar por calificación (mejores primero)
    df = df.sort_values('calificacion_general', ascending=False)
    
    # Reordenar columnas por categorías
    columnas_basicas = [
        'submission_id', 'tipo_supervision', 'date_submitted', 'date_created',
        'usuario', 'supervisor_calificaciones', 'año', 'hora'
    ]
    
    columnas_ubicacion = [
        'lat_entrega', 'lon_entrega', 'location_field', 'sucursal_campo',
        'location_asignado', 'tiene_coordenadas'
    ]
    
    columnas_sucursal = [
        'sucursal_numero', 'sucursal_nombre', 'sucursal_tipo', 'sucursal_grupo',
        'sucursal_lat', 'sucursal_lon'
    ]
    
    columnas_calificaciones = [
        'calificacion_general', 'total_elementos_evaluados', 'total_elementos_conformes',
        'total_elementos_no_conformes', 'total_elementos_criticos_fallidos',
        'porcentaje_conformidad', 'areas_evaluadas', 'puntos_criticos_principales',
        'areas_con_problemas'
    ]
    
    columnas_metadatos = [
        'tiene_calificaciones', 'fecha_procesamiento', 'fuente_calificaciones'
    ]
    
    # Todas las columnas en orden
    columnas_ordenadas = (columnas_basicas + columnas_ubicacion + columnas_sucursal + 
                         columnas_calificaciones + columnas_metadatos)
    
    # Filtrar columnas existentes
    columnas_finales = [col for col in columnas_ordenadas if col in df.columns]
    df_final = df[columnas_finales]
    
    # Generar archivo Excel
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archivo_excel = f"SUPERVISIONES_{tipo_supervision.upper()}_CALIFICACIONES_COMPLETAS_{timestamp}.xlsx"
    
    with pd.ExcelWriter(archivo_excel, engine='openpyxl') as writer:
        # Hoja principal
        df_final.to_excel(writer, sheet_name=f'{tipo_supervision.title()}', index=False)
        
        # Hoja de resumen con calificaciones
        crear_hoja_resumen_calificaciones(writer, df_final, tipo_supervision)
        
        # Hoja de ranking por calificación
        crear_hoja_ranking_sucursales(writer, df_final)
        
        # Hoja de KPIs consolidados
        crear_hoja_kpis_consolidados(writer, df_final)
    
    print(f"✅ Excel creado: {archivo_excel}")
    print(f"📊 Registros: {len(df_final)}")
    print(f"📋 Columnas: {len(df_final.columns)}")
    print(f"🎯 Con calificaciones: {len(df_final[df_final['tiene_calificaciones'] == True])}")
    print(f"📈 Calificación promedio: {df_final['calificacion_general'].mean():.1f}")
    
    return archivo_excel, df_final

def crear_hoja_resumen_calificaciones(writer, df, tipo_supervision):
    """Crear hoja de resumen con calificaciones"""
    
    with_calificaciones = df[df['tiene_calificaciones'] == True]
    
    resumen_data = {
        'Métrica': [
            'Total Supervisiones',
            'Con Calificaciones',
            'Sin Calificaciones',
            'Calificación Promedio',
            'Calificación Máxima',
            'Calificación Mínima',
            'Conformidad Promedio',
            'Total Elementos Evaluados',
            'Total Elementos Conformes',
            'Total No Conformes',
            'Sucursales Evaluadas',
            'Áreas Evaluadas Promedio'
        ],
        'Valor': [
            len(df),
            len(with_calificaciones),
            len(df) - len(with_calificaciones),
            f"{with_calificaciones['calificacion_general'].mean():.1f}" if len(with_calificaciones) > 0 else 'N/A',
            f"{with_calificaciones['calificacion_general'].max():.1f}" if len(with_calificaciones) > 0 else 'N/A',
            f"{with_calificaciones['calificacion_general'].min():.1f}" if len(with_calificaciones) > 0 else 'N/A',
            f"{with_calificaciones['porcentaje_conformidad'].mean():.1f}%" if len(with_calificaciones) > 0 else 'N/A',
            with_calificaciones['total_elementos_evaluados'].sum(),
            with_calificaciones['total_elementos_conformes'].sum(),
            with_calificaciones['total_elementos_no_conformes'].sum(),
            with_calificaciones['sucursal_nombre'].nunique(),
            f"{with_calificaciones['areas_evaluadas'].mean():.1f}" if len(with_calificaciones) > 0 else 'N/A'
        ]
    }
    
    df_resumen = pd.DataFrame(resumen_data)
    df_resumen.to_excel(writer, sheet_name='Resumen_Calificaciones', index=False)

def crear_hoja_ranking_sucursales(writer, df):
    """Crear ranking de sucursales por calificación"""
    
    with_calificaciones = df[df['tiene_calificaciones'] == True]
    
    if len(with_calificaciones) > 0:
        ranking = with_calificaciones.groupby(['sucursal_nombre', 'sucursal_tipo']).agg({
            'calificacion_general': 'mean',
            'porcentaje_conformidad': 'mean',
            'total_elementos_evaluados': 'sum',
            'total_elementos_no_conformes': 'sum',
            'submission_id': 'count'
        }).round(1)
        
        ranking = ranking.rename(columns={'submission_id': 'total_supervisiones'})
        ranking = ranking.sort_values('calificacion_general', ascending=False)
        ranking = ranking.reset_index()
        
        ranking.to_excel(writer, sheet_name='Ranking_Sucursales', index=False)

def crear_hoja_kpis_consolidados(writer, df):
    """Crear hoja de KPIs consolidados"""
    
    with_calificaciones = df[df['tiene_calificaciones'] == True]
    
    if len(with_calificaciones) > 0:
        kpis_data = with_calificaciones[[
            'submission_id', 'sucursal_nombre', 'calificacion_general',
            'total_elementos_evaluados', 'total_elementos_conformes',
            'total_elementos_no_conformes', 'porcentaje_conformidad',
            'areas_evaluadas', 'puntos_criticos_principales'
        ]].copy()
        
        kpis_data = kpis_data.sort_values('calificacion_general', ascending=False)
        kpis_data.to_excel(writer, sheet_name='KPIs_Detallados', index=False)

def main():
    """Función principal"""
    
    print("📊 EXTRAER EXCEL CON CALIFICACIONES COMPLETAS")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Roberto: Excel con 476 supervisiones + calificaciones + KPIs completos")
    print("=" * 80)
    
    # 1. Cargar datos
    df_dataset, df_sucursales, calificaciones_data = cargar_datos_completos()
    
    # 2. Crear índices
    catalogo_sucursales = crear_catalogo_sucursales_lookup(df_sucursales)
    calificaciones_index = crear_indice_calificaciones(calificaciones_data)
    
    # 3. Separar por tipo
    operativas = df_dataset[df_dataset['tipo'] == 'operativas'].copy()
    seguridad = df_dataset[df_dataset['tipo'] == 'seguridad'].copy()
    
    print(f"\n📊 DISTRIBUCIÓN:")
    print(f"   🔧 Operativas: {len(operativas)}")
    print(f"   🛡️ Seguridad: {len(seguridad)}")
    
    # 4. Enriquecer operativas
    print(f"\n🔧 ENRIQUECIENDO OPERATIVAS CON CALIFICACIONES")
    datos_operativas = []
    for _, row in operativas.iterrows():
        registro = enriquecer_supervision_completa(row, catalogo_sucursales, calificaciones_index)
        datos_operativas.append(registro)
    
    # 5. Enriquecer seguridad
    print(f"\n🛡️ ENRIQUECIENDO SEGURIDAD CON CALIFICACIONES")
    datos_seguridad = []
    for _, row in seguridad.iterrows():
        registro = enriquecer_supervision_completa(row, catalogo_sucursales, calificaciones_index)
        datos_seguridad.append(registro)
    
    # 6. Crear Excel operativas
    archivo_operativas, df_operativas = crear_excel_supervision_con_calificaciones(datos_operativas, 'operativas')
    
    # 7. Crear Excel seguridad
    archivo_seguridad, df_seguridad = crear_excel_supervision_con_calificaciones(datos_seguridad, 'seguridad')
    
    # 8. Resumen final
    print(f"\n🎯 EXCEL CON CALIFICACIONES COMPLETAS:")
    print("=" * 60)
    if archivo_operativas:
        print(f"✅ OPERATIVAS: {archivo_operativas}")
        print(f"   📊 {len(df_operativas)} supervisiones")
        print(f"   🎯 {len(df_operativas[df_operativas['tiene_calificaciones'] == True])} con calificaciones")
        print(f"   📈 Promedio: {df_operativas['calificacion_general'].mean():.1f}")
    
    if archivo_seguridad:
        print(f"✅ SEGURIDAD: {archivo_seguridad}")
        print(f"   📊 {len(df_seguridad)} supervisiones")
        print(f"   🎯 {len(df_seguridad[df_seguridad['tiene_calificaciones'] == True])} con calificaciones")
        print(f"   📈 Promedio: {df_seguridad['calificacion_general'].mean():.1f}")
    
    print(f"\n📋 ESTRUCTURA COMPLETA INCLUYE:")
    print(f"   📊 476 supervisiones asignadas")
    print(f"   🎯 Calificaciones generales")
    print(f"   📈 KPIs por área consolidados")
    print(f"   🏢 Datos completos de sucursales")
    print(f"   📊 Ranking y análisis de conformidad")
    print(f"   📖 4 hojas: Datos + Resumen + Ranking + KPIs")
    
    print(f"\n✅ EXCEL COMPLETO CON CALIFICACIONES LISTO")
    
    return archivo_operativas, archivo_seguridad

if __name__ == "__main__":
    main()