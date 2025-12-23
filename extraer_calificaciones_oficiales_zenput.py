#!/usr/bin/env python3
"""
📊 EXTRAER CALIFICACIONES OFICIALES ZENPUT
Usar las calificaciones REALES que están al principio de los Excel originales
"""

import pandas as pd
from datetime import datetime
import numpy as np

def cargar_datos_con_calificaciones_oficiales():
    """Cargar Excel originales y extraer calificaciones oficiales"""
    
    print("🔧 CARGAR CALIFICACIONES OFICIALES ZENPUT")
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

def identificar_campos_calificacion_oficial(df, tipo):
    """Identificar los campos oficiales de calificación al principio del Excel"""
    
    print(f"\n🔍 IDENTIFICAR CAMPOS OFICIALES {tipo.upper()}")
    print("=" * 50)
    
    if len(df) == 0:
        print("❌ DataFrame vacío")
        return {}
    
    # Mostrar primeras 15 columnas para identificar structure
    print(f"📋 PRIMERAS 15 COLUMNAS:")
    for i, col in enumerate(df.columns[:15]):
        valores_unicos = df[col].nunique()
        if pd.api.types.is_numeric_dtype(df[col]):
            rango = f"{df[col].min():.1f} - {df[col].max():.1f}"
        else:
            rango = "texto"
        print(f"   {i+1:2}. {col[:50]:<50} | {valores_unicos:3} únicos | {rango}")
    
    # Identificar campos clave de calificación
    campos_calificacion = {}
    
    for col in df.columns:
        col_lower = str(col).lower()
        
        # Submission ID
        if 'submission' in col_lower and 'id' in col_lower:
            campos_calificacion['submission_id'] = col
        
        # Date submitted
        elif 'date' in col_lower and 'submit' in col_lower:
            campos_calificacion['date_submitted'] = col
        
        # Location
        elif col_lower == 'location':
            campos_calificacion['location'] = col
        
        # Sucursal
        elif col_lower == 'sucursal':
            campos_calificacion['sucursal'] = col
            
        # CALIFICACIONES OFICIALES AL PRINCIPIO
        elif col == 'PUNTOS MAXIMOS':  # Operativas
            campos_calificacion['puntos_maximos'] = col
        elif col == 'PUNTOS TOTALES':  # Operativas
            campos_calificacion['puntos_totales'] = col
        elif col == 'PORCENTAJE %':   # Operativas - CALIFICACIÓN GENERAL
            campos_calificacion['calificacion_general'] = col
            
        elif col == 'PUNTOS MAX':     # Seguridad
            campos_calificacion['puntos_maximos'] = col
        elif col == 'PUNTOS TOTALES OBTENIDOS':  # Seguridad
            campos_calificacion['puntos_totales'] = col
        elif col == 'CALIFICACION PORCENTAJE %': # Seguridad - CALIFICACIÓN GENERAL
            campos_calificacion['calificacion_general'] = col
    
    print(f"\n🎯 CAMPOS OFICIALES IDENTIFICADOS:")
    for key, field in campos_calificacion.items():
        if key == 'calificacion_general':
            valores = df[field].describe()
            print(f"   ✅ {key}: {field}")
            print(f"      📊 Min: {valores['min']:.1f} | Max: {valores['max']:.1f} | Promedio: {valores['mean']:.1f}")
        else:
            print(f"   📋 {key}: {field}")
    
    return campos_calificacion

def procesar_calificaciones_oficiales(df_original, df_dataset_filtrado, tipo, catalogo_sucursales):
    """Procesar usando calificaciones oficiales de Zenput"""
    
    print(f"\n🔧 PROCESAR {tipo.upper()} CON CALIFICACIONES OFICIALES")
    print("=" * 60)
    
    if len(df_original) == 0:
        print(f"❌ No hay datos en Excel original de {tipo}")
        return []
    
    # Identificar campos oficiales
    campos = identificar_campos_calificacion_oficial(df_original, tipo)
    
    if 'submission_id' not in campos or 'calificacion_general' not in campos:
        print(f"❌ No encontré campos necesarios en {tipo}")
        return []
    
    # Procesar cada supervisión asignada
    datos_procesados = []
    matched_count = 0
    calificaciones_encontradas = []
    
    for _, row_dataset in df_dataset_filtrado.iterrows():
        submission_id = row_dataset['submission_id']
        location_asignado = row_dataset['location_asignado']
        
        # Buscar en Excel original por Submission ID
        matches = df_original[df_original[campos['submission_id']] == submission_id]
        
        if len(matches) > 0:
            row_original = matches.iloc[0]
            matched_count += 1
            
            # Extraer calificaciones oficiales
            calificacion_general = row_original.get(campos['calificacion_general'])
            puntos_maximos = row_original.get(campos.get('puntos_maximos'))
            puntos_totales = row_original.get(campos.get('puntos_totales'))
            sucursal_excel = row_original.get(campos.get('sucursal', ''))
            
            calificaciones_encontradas.append(calificacion_general)
            
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
                
                # CALIFICACIONES OFICIALES ZENPUT
                'calificacion_general_zenput': calificacion_general,
                'puntos_maximos_zenput': puntos_maximos,
                'puntos_totales_zenput': puntos_totales,
                'sucursal_excel': sucursal_excel,
                
                # METADATOS
                'tiene_calificacion_oficial': True,
                'fuente_calificacion': f'zenput_oficial_{tipo}',
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
                'calificacion_general_zenput': None,
                'puntos_maximos_zenput': None,
                'puntos_totales_zenput': None,
                'sucursal_excel': '',
                'tiene_calificacion_oficial': False,
                'fuente_calificacion': 'no_encontrada',
                'fecha_procesamiento': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            datos_procesados.append(registro)
    
    # Estadísticas de calificaciones
    calificaciones_validas = [c for c in calificaciones_encontradas if pd.notna(c)]
    
    print(f"✅ Procesadas: {len(datos_procesados)} supervisiones")
    print(f"🎯 Con calificación oficial: {matched_count}")
    if calificaciones_validas:
        print(f"📊 Calificación promedio OFICIAL: {np.mean(calificaciones_validas):.1f}")
        print(f"📈 Rango calificaciones: {min(calificaciones_validas):.1f} - {max(calificaciones_validas):.1f}")
    
    return datos_procesados

def crear_excel_calificaciones_oficiales(datos_procesados, tipo):
    """Crear Excel con calificaciones oficiales de Zenput"""
    
    print(f"\n📊 CREAR EXCEL {tipo.upper()} CON CALIFICACIONES OFICIALES")
    print("=" * 60)
    
    if not datos_procesados:
        print(f"❌ No hay datos para {tipo}")
        return None
    
    # Crear DataFrame
    df = pd.DataFrame(datos_procesados)
    
    # Ordenar por calificación oficial (mejores primero)
    df = df.sort_values('calificacion_general_zenput', ascending=False)
    
    # Crear Excel
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archivo_excel = f"SUPERVISIONES_{tipo.upper()}_CALIFICACIONES_OFICIALES_ZENPUT_{timestamp}.xlsx"
    
    with pd.ExcelWriter(archivo_excel, engine='openpyxl') as writer:
        # Hoja principal
        df.to_excel(writer, sheet_name=f'{tipo.title()}', index=False)
        
        # Hoja de estadísticas oficiales
        crear_hoja_estadisticas_oficiales(writer, df, tipo)
        
        # Hoja de ranking oficial
        crear_hoja_ranking_oficial(writer, df)
        
        # Hoja de resumen por sucursal
        crear_hoja_resumen_sucursales(writer, df)
    
    # Estadísticas
    con_calificacion = df[df['tiene_calificacion_oficial'] == True]
    
    print(f"✅ Excel creado: {archivo_excel}")
    print(f"📊 Total registros: {len(df)}")
    print(f"🎯 Con calificación oficial: {len(con_calificacion)}")
    if len(con_calificacion) > 0:
        print(f"📈 Promedio oficial: {con_calificacion['calificacion_general_zenput'].mean():.1f}")
    
    return archivo_excel, df

def crear_hoja_estadisticas_oficiales(writer, df, tipo):
    """Crear hoja de estadísticas con calificaciones oficiales"""
    
    con_calificacion = df[df['tiene_calificacion_oficial'] == True]
    
    estadisticas = {
        'Métrica': [
            'Total Supervisiones',
            'Con Calificación Oficial Zenput',
            'Sin Calificación',
            'Cobertura %',
            'Calificación Promedio Oficial',
            'Calificación Máxima',
            'Calificación Mínima',
            'Puntos Promedio Obtenidos',
            'Puntos Máximos Promedio',
            'Eficiencia Promedio %'
        ],
        'Valor': [
            len(df),
            len(con_calificacion),
            len(df) - len(con_calificacion),
            f"{len(con_calificacion)/len(df)*100:.1f}%",
            f"{con_calificacion['calificacion_general_zenput'].mean():.1f}" if len(con_calificacion) > 0 else 'N/A',
            f"{con_calificacion['calificacion_general_zenput'].max():.1f}" if len(con_calificacion) > 0 else 'N/A',
            f"{con_calificacion['calificacion_general_zenput'].min():.1f}" if len(con_calificacion) > 0 else 'N/A',
            f"{con_calificacion['puntos_totales_zenput'].mean():.1f}" if len(con_calificacion) > 0 else 'N/A',
            f"{con_calificacion['puntos_maximos_zenput'].mean():.1f}" if len(con_calificacion) > 0 else 'N/A',
            f"{(con_calificacion['puntos_totales_zenput']/con_calificacion['puntos_maximos_zenput']*100).mean():.1f}%" if len(con_calificacion) > 0 else 'N/A'
        ]
    }
    
    df_stats = pd.DataFrame(estadisticas)
    df_stats.to_excel(writer, sheet_name='Estadisticas_Oficiales', index=False)

def crear_hoja_ranking_oficial(writer, df):
    """Crear ranking oficial por sucursal"""
    
    con_calificacion = df[df['tiene_calificacion_oficial'] == True]
    
    if len(con_calificacion) > 0:
        ranking = con_calificacion.groupby(['sucursal_nombre', 'sucursal_tipo']).agg({
            'calificacion_general_zenput': 'mean',
            'puntos_totales_zenput': 'mean',
            'puntos_maximos_zenput': 'mean',
            'submission_id': 'count'
        }).round(1)
        
        ranking['eficiencia_promedio'] = (ranking['puntos_totales_zenput'] / ranking['puntos_maximos_zenput'] * 100).round(1)
        ranking = ranking.rename(columns={'submission_id': 'total_supervisiones'})
        ranking = ranking.sort_values('calificacion_general_zenput', ascending=False)
        ranking = ranking.reset_index()
        
        ranking.to_excel(writer, sheet_name='Ranking_Oficial', index=False)

def crear_hoja_resumen_sucursales(writer, df):
    """Crear resumen detallado por sucursal"""
    
    resumen = df.groupby('sucursal_nombre').agg({
        'calificacion_general_zenput': ['count', 'mean', 'min', 'max'],
        'tiene_calificacion_oficial': 'sum',
        'sucursal_tipo': 'first',
        'sucursal_grupo': 'first'
    }).round(1)
    
    # Aplanar columnas
    resumen.columns = ['total_supervisiones', 'promedio_calificacion', 'min_calificacion', 'max_calificacion', 
                      'con_calificacion', 'tipo', 'grupo']
    resumen = resumen.reset_index()
    resumen = resumen.sort_values('promedio_calificacion', ascending=False)
    
    resumen.to_excel(writer, sheet_name='Resumen_Sucursales', index=False)

def main():
    """Función principal"""
    
    print("📊 EXTRAER CALIFICACIONES OFICIALES ZENPUT")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Roberto: Usar calificaciones OFICIALES de Zenput (al principio de Excel)")
    print("=" * 80)
    
    # 1. Cargar datos
    df_ops_original, df_seg_original, df_dataset, df_sucursales = cargar_datos_con_calificaciones_oficiales()
    
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
    
    # 4. Procesar operativas con calificaciones oficiales
    datos_operativas = procesar_calificaciones_oficiales(
        df_ops_original, operativas_dataset, 'operativas', catalogo_sucursales
    )
    
    # 5. Procesar seguridad con calificaciones oficiales
    datos_seguridad = procesar_calificaciones_oficiales(
        df_seg_original, seguridad_dataset, 'seguridad', catalogo_sucursales
    )
    
    # 6. Crear Excel operativas
    archivo_operativas, df_operativas = crear_excel_calificaciones_oficiales(datos_operativas, 'operativas')
    
    # 7. Crear Excel seguridad
    archivo_seguridad, df_seguridad = crear_excel_calificaciones_oficiales(datos_seguridad, 'seguridad')
    
    # 8. Resumen final
    print(f"\n🎯 EXCEL CON CALIFICACIONES OFICIALES ZENPUT:")
    print("=" * 70)
    if archivo_operativas:
        con_cal_ops = df_operativas[df_operativas['tiene_calificacion_oficial'] == True]
        print(f"✅ OPERATIVAS: {archivo_operativas}")
        print(f"   📊 {len(df_operativas)} supervisiones")
        print(f"   🎯 {len(con_cal_ops)} con calificación oficial")
        if len(con_cal_ops) > 0:
            print(f"   📈 Promedio OFICIAL: {con_cal_ops['calificacion_general_zenput'].mean():.1f}")
    
    if archivo_seguridad:
        con_cal_seg = df_seguridad[df_seguridad['tiene_calificacion_oficial'] == True]
        print(f"✅ SEGURIDAD: {archivo_seguridad}")
        print(f"   📊 {len(df_seguridad)} supervisiones")
        print(f"   🎯 {len(con_cal_seg)} con calificación oficial")
        if len(con_cal_seg) > 0:
            print(f"   📈 Promedio OFICIAL: {con_cal_seg['calificacion_general_zenput'].mean():.1f}")
    
    print(f"\n✅ AHORA SÍ: CALIFICACIONES OFICIALES DE ZENPUT")
    print(f"📊 Ya no son mis cálculos inventados - son las calificaciones reales")
    
    return archivo_operativas, archivo_seguridad

if __name__ == "__main__":
    main()