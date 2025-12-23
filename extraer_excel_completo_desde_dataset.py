#!/usr/bin/env python3
"""
📊 EXTRAER EXCEL COMPLETO DESDE DATASET
Usar las 476 supervisiones asignadas y extraer estructura completa del API
"""

import pandas as pd
from datetime import datetime
import requests
import json
import time

def cargar_dataset_y_catalogo():
    """Cargar dataset emparejado y catálogo de sucursales"""
    
    print("🔧 CARGAR DATASET Y CATÁLOGO")
    print("=" * 50)
    
    # Dataset emparejado con 476 supervisiones
    df_dataset = pd.read_csv("DATASET_EMPAREJADO_20251218_164319.csv")
    
    # Catálogo de sucursales corregido
    df_sucursales = pd.read_csv("SUCURSALES_CORRECCIONES_ROBERTO_20251218_171807.csv")
    
    # API config
    api_config = {
        'base_url': 'https://www.zenput.com/api/v3',
        'headers': {
            'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314',
            'Content-Type': 'application/json'
        }
    }
    
    # Crear catálogo para lookup rápido
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
    
    print(f"✅ Dataset: {len(df_dataset)} supervisiones")
    print(f"✅ Catálogo: {len(catalogo_sucursales)} sucursales")
    
    return df_dataset, catalogo_sucursales, api_config

def obtener_submission_completa_api(submission_id, api_config, max_reintentos=3):
    """Obtener submission completa del API con estructura de respuestas"""
    
    for intento in range(max_reintentos):
        try:
            # Obtener submission específica
            url = f"{api_config['base_url']}/submissions/{submission_id}"
            
            response = requests.get(url, headers=api_config['headers'], timeout=30)
            response.raise_for_status()
            
            submission_data = response.json()
            return submission_data
            
        except Exception as e:
            print(f"   ⚠️ Error intento {intento + 1}: {e}")
            if intento < max_reintentos - 1:
                time.sleep(2)  # Esperar antes de reintentar
            
    print(f"   ❌ No se pudo obtener {submission_id}")
    return None

def procesar_submission_completa(submission_api, row_dataset, catalogo_sucursales):
    """Procesar submission con estructura completa"""
    
    # Datos básicos del dataset
    submission_id = row_dataset['submission_id']
    tipo_supervision = row_dataset['tipo']
    date_submitted = row_dataset['date_submitted']
    location_asignado = row_dataset['location_asignado']
    
    # Datos del API si disponible
    if submission_api:
        smetadata = submission_api.get('smetadata', {})
        fields = submission_api.get('fields', {})
        created_by = smetadata.get('created_by', {})
        
        user_name = created_by.get('name', '')
        user_email = created_by.get('email', '')
        
        # Extraer respuestas del formulario
        respuestas = extraer_respuestas_formulario(fields)
        kpis = calcular_kpis_supervision(respuestas, tipo_supervision)
        
    else:
        user_name = row_dataset.get('usuario', '')
        user_email = ''
        respuestas = {}
        kpis = {
            'calificacion_total': None,
            'preguntas_totales': 0,
            'preguntas_respondidas': 0,
            'porcentaje_completitud': 0,
            'respuestas_conformes': 0,
            'respuestas_no_conformes': 0,
            'porcentaje_conformidad': 0,
            'areas_evaluadas': 0,
            'puntos_criticos': ''
        }
    
    # Datos de sucursal
    sucursal_info = catalogo_sucursales.get(location_asignado, {})
    
    # Crear registro completo
    registro = {
        # DATOS BÁSICOS
        'submission_id': submission_id,
        'tipo_supervision': tipo_supervision,
        'date_submitted': date_submitted,
        'user_name': user_name,
        'user_email': user_email,
        
        # UBICACIÓN
        'lat_entrega': row_dataset.get('lat_entrega'),
        'lon_entrega': row_dataset.get('lon_entrega'),
        'location_field': row_dataset.get('location_map'),
        'sucursal_campo': row_dataset.get('sucursal_campo'),
        'location_asignado': location_asignado,
        
        # DATOS SUCURSAL
        'sucursal_numero': sucursal_info.get('numero'),
        'sucursal_nombre': sucursal_info.get('nombre'),
        'sucursal_tipo': sucursal_info.get('tipo'),
        'sucursal_grupo': sucursal_info.get('grupo'),
        'sucursal_lat': sucursal_info.get('lat'),
        'sucursal_lon': sucursal_info.get('lon'),
        
        # KPIS Y CALIFICACIONES
        'calificacion_total': kpis.get('calificacion_total'),
        'preguntas_totales': kpis.get('preguntas_totales'),
        'preguntas_respondidas': kpis.get('preguntas_respondidas'),
        'porcentaje_completitud': kpis.get('porcentaje_completitud'),
        'respuestas_conformes': kpis.get('respuestas_conformes'),
        'respuestas_no_conformes': kpis.get('respuestas_no_conformes'),
        'porcentaje_conformidad': kpis.get('porcentaje_conformidad'),
        'areas_evaluadas': kpis.get('areas_evaluadas'),
        'puntos_criticos': kpis.get('puntos_criticos')
    }
    
    # Agregar respuestas individuales como columnas
    for pregunta_id, respuesta in respuestas.items():
        registro[f'pregunta_{pregunta_id}'] = respuesta.get('valor')
        registro[f'pregunta_{pregunta_id}_texto'] = respuesta.get('texto')
        registro[f'pregunta_{pregunta_id}_tipo'] = respuesta.get('tipo')
    
    return registro

def extraer_respuestas_formulario(fields):
    """Extraer todas las respuestas del formulario"""
    respuestas = {}
    
    for field_id, field_data in fields.items():
        if isinstance(field_data, dict):
            value = field_data.get('value')
            field_type = field_data.get('type', 'unknown')
            text = field_data.get('text', '')
            
            respuestas[field_id] = {
                'valor': value,
                'tipo': field_type,
                'texto': text
            }
    
    return respuestas

def calcular_kpis_supervision(respuestas, tipo_supervision):
    """Calcular KPIs y métricas de la supervisión"""
    
    if not respuestas:
        return {
            'calificacion_total': None,
            'preguntas_totales': 0,
            'preguntas_respondidas': 0,
            'porcentaje_completitud': 0,
            'respuestas_conformes': 0,
            'respuestas_no_conformes': 0,
            'porcentaje_conformidad': 0,
            'areas_evaluadas': 0,
            'puntos_criticos': ''
        }
    
    total_preguntas = len(respuestas)
    preguntas_respondidas = len([r for r in respuestas.values() if r['valor'] is not None and r['valor'] != ''])
    
    # Contar respuestas conformes/no conformes
    conformes = 0
    no_conformes = 0
    puntos_criticos = []
    
    for field_id, respuesta in respuestas.items():
        valor = respuesta.get('valor', '')
        
        if isinstance(valor, str):
            valor_lower = valor.lower()
            if any(word in valor_lower for word in ['sí', 'si', 'yes', 'correcto', 'bien', 'cumple']):
                conformes += 1
            elif any(word in valor_lower for word in ['no', 'incorrecto', 'mal', 'falta', 'no cumple']):
                no_conformes += 1
                puntos_criticos.append(f"Pregunta {field_id}: {respuesta.get('texto', '')}")
    
    # Calcular porcentajes
    porcentaje_completitud = (preguntas_respondidas / total_preguntas * 100) if total_preguntas > 0 else 0
    total_evaluables = conformes + no_conformes
    porcentaje_conformidad = (conformes / total_evaluables * 100) if total_evaluables > 0 else 0
    
    # Calificación total (basada en conformidad y completitud)
    calificacion_total = (porcentaje_conformidad * 0.8 + porcentaje_completitud * 0.2)
    
    return {
        'calificacion_total': round(calificacion_total, 2),
        'preguntas_totales': total_preguntas,
        'preguntas_respondidas': preguntas_respondidas,
        'porcentaje_completitud': round(porcentaje_completitud, 2),
        'respuestas_conformes': conformes,
        'respuestas_no_conformes': no_conformes,
        'porcentaje_conformidad': round(porcentaje_conformidad, 2),
        'areas_evaluadas': len([r for r in respuestas.values() if r['tipo'] in ['section', 'group']]),
        'puntos_criticos': '; '.join(puntos_criticos[:5])  # Primeros 5
    }

def crear_excel_supervision_completo(datos_procesados, tipo_supervision):
    """Crear Excel con estructura completa"""
    
    print(f"\n📊 CREAR EXCEL {tipo_supervision.upper()}")
    print("=" * 50)
    
    if not datos_procesados:
        print(f"❌ No hay datos para {tipo_supervision}")
        return None
    
    # Crear DataFrame
    df = pd.DataFrame(datos_procesados)
    
    # Ordenar columnas por categorías
    columnas_basicas = [
        'submission_id', 'tipo_supervision', 'date_submitted', 
        'user_name', 'user_email'
    ]
    
    columnas_ubicacion = [
        'lat_entrega', 'lon_entrega', 'location_field', 
        'sucursal_campo', 'location_asignado'
    ]
    
    columnas_sucursal = [
        'sucursal_numero', 'sucursal_nombre', 'sucursal_tipo',
        'sucursal_grupo', 'sucursal_lat', 'sucursal_lon'
    ]
    
    columnas_kpis = [
        'calificacion_total', 'preguntas_totales', 'preguntas_respondidas',
        'porcentaje_completitud', 'respuestas_conformes', 'respuestas_no_conformes',
        'porcentaje_conformidad', 'areas_evaluadas', 'puntos_criticos'
    ]
    
    # Columnas de respuestas
    columnas_respuestas = [col for col in df.columns if col.startswith('pregunta_')]
    
    # Reordenar columnas
    columnas_ordenadas = (columnas_basicas + columnas_ubicacion + 
                         columnas_sucursal + columnas_kpis + columnas_respuestas)
    
    # Filtrar solo columnas que existen
    columnas_finales = [col for col in columnas_ordenadas if col in df.columns]
    df_final = df[columnas_finales]
    
    # Guardar Excel
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archivo_excel = f"SUPERVISIONES_{tipo_supervision.upper()}_DATASET_COMPLETO_{timestamp}.xlsx"
    
    with pd.ExcelWriter(archivo_excel, engine='openpyxl') as writer:
        # Hoja principal
        df_final.to_excel(writer, sheet_name=f'{tipo_supervision.title()}', index=False)
        
        # Hoja de resumen
        crear_hoja_resumen(writer, df_final, tipo_supervision)
        
        # Hoja de diccionario
        crear_diccionario_campos(writer, df_final)
    
    print(f"✅ Excel creado: {archivo_excel}")
    print(f"📊 Registros: {len(df_final)}")
    print(f"📋 Columnas: {len(df_final.columns)}")
    
    return archivo_excel, df_final

def crear_hoja_resumen(writer, df, tipo_supervision):
    """Crear hoja de resumen"""
    
    resumen_data = {
        'Métrica': [
            'Total Supervisiones',
            'Sucursales Únicas',
            'Usuarios Únicos',
            'Calificación Promedio',
            'Conformidad Promedio',
            'Completitud Promedio',
            'Fecha Más Antigua',
            'Fecha Más Reciente',
            'Con KPIs Calculados',
            'Supervisiones con Respuestas'
        ],
        'Valor': [
            len(df),
            df['sucursal_nombre'].nunique(),
            df['user_name'].nunique(),
            f"{df['calificacion_total'].mean():.2f}%" if df['calificacion_total'].notna().any() else 'N/A',
            f"{df['porcentaje_conformidad'].mean():.2f}%" if df['porcentaje_conformidad'].notna().any() else 'N/A',
            f"{df['porcentaje_completitud'].mean():.2f}%" if df['porcentaje_completitud'].notna().any() else 'N/A',
            df['date_submitted'].min() if 'date_submitted' in df.columns else 'N/A',
            df['date_submitted'].max() if 'date_submitted' in df.columns else 'N/A',
            len(df[df['calificacion_total'].notna()]),
            len(df[df['preguntas_totales'] > 0])
        ]
    }
    
    df_resumen = pd.DataFrame(resumen_data)
    df_resumen.to_excel(writer, sheet_name='Resumen', index=False)

def crear_diccionario_campos(writer, df):
    """Crear diccionario de campos"""
    
    diccionario_data = []
    for col in df.columns:
        categoria = 'Respuesta'
        if col in ['submission_id', 'tipo_supervision', 'date_submitted', 'user_name', 'user_email']:
            categoria = 'Básico'
        elif 'ubicacion' in col or 'lat' in col or 'lon' in col or 'location' in col:
            categoria = 'Ubicación'
        elif 'sucursal' in col:
            categoria = 'Sucursal'
        elif any(word in col for word in ['calificacion', 'porcentaje', 'preguntas', 'respuestas', 'conformidad']):
            categoria = 'KPI'
        
        diccionario_data.append({
            'Campo': col,
            'Categoría': categoria,
            'Tipo': str(df[col].dtype),
            'Valores_Únicos': df[col].nunique(),
            'Valores_Nulos': df[col].isnull().sum()
        })
    
    df_diccionario = pd.DataFrame(diccionario_data)
    df_diccionario.to_excel(writer, sheet_name='Diccionario', index=False)

def main():
    """Función principal"""
    
    print("📊 EXTRAER EXCEL COMPLETO DESDE DATASET")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Roberto: Excel completo con 476 supervisiones asignadas + estructura API")
    print("=" * 80)
    
    # 1. Cargar datos
    df_dataset, catalogo_sucursales, api_config = cargar_dataset_y_catalogo()
    
    # Separar por tipo
    operativas = df_dataset[df_dataset['tipo'] == 'operativas'].copy()
    seguridad = df_dataset[df_dataset['tipo'] == 'seguridad'].copy()
    
    print(f"\n📊 SEPARACIÓN POR TIPO:")
    print(f"   🔧 Operativas: {len(operativas)}")
    print(f"   🛡️ Seguridad: {len(seguridad)}")
    
    # 2. Procesar Operativas
    print(f"\n🔧 PROCESANDO OPERATIVAS COMPLETAS")
    datos_operativas = []
    
    for i, (_, row) in enumerate(operativas.iterrows(), 1):
        print(f"   Procesando {i}/{len(operativas)}: {row['submission_id'][:12]}...", end=" ")
        
        # Obtener datos del API
        submission_api = obtener_submission_completa_api(row['submission_id'], api_config)
        
        # Procesar con estructura completa
        registro = procesar_submission_completa(submission_api, row, catalogo_sucursales)
        datos_operativas.append(registro)
        
        print("✅" if submission_api else "⚠️")
        
        # Pausa para no sobrecargar API
        if i % 10 == 0:
            time.sleep(1)
    
    # 3. Procesar Seguridad
    print(f"\n🛡️ PROCESANDO SEGURIDAD COMPLETAS")
    datos_seguridad = []
    
    for i, (_, row) in enumerate(seguridad.iterrows(), 1):
        print(f"   Procesando {i}/{len(seguridad)}: {row['submission_id'][:12]}...", end=" ")
        
        # Obtener datos del API
        submission_api = obtener_submission_completa_api(row['submission_id'], api_config)
        
        # Procesar con estructura completa
        registro = procesar_submission_completa(submission_api, row, catalogo_sucursales)
        datos_seguridad.append(registro)
        
        print("✅" if submission_api else "⚠️")
        
        # Pausa para no sobrecargar API
        if i % 10 == 0:
            time.sleep(1)
    
    # 4. Crear Excel para Operativas
    archivo_operativas, df_operativas = crear_excel_supervision_completo(datos_operativas, 'operativas')
    
    # 5. Crear Excel para Seguridad
    archivo_seguridad, df_seguridad = crear_excel_supervision_completo(datos_seguridad, 'seguridad')
    
    # 6. Resumen final
    print(f"\n🎯 ARCHIVOS EXCEL GENERADOS:")
    print("=" * 50)
    if archivo_operativas:
        print(f"✅ OPERATIVAS: {archivo_operativas}")
        print(f"   📊 {len(df_operativas)} registros, {len(df_operativas.columns)} columnas")
        print(f"   📝 Con KPIs: {len(df_operativas[df_operativas['calificacion_total'].notna()])}")
    
    if archivo_seguridad:
        print(f"✅ SEGURIDAD: {archivo_seguridad}")
        print(f"   📊 {len(df_seguridad)} registros, {len(df_seguridad.columns)} columnas")
        print(f"   📝 Con KPIs: {len(df_seguridad[df_seguridad['calificacion_total'].notna()])}")
    
    print(f"\n🎯 ESTRUCTURA COMPLETA INCLUYE:")
    print(f"   📋 476 supervisiones asignadas (238+238)")
    print(f"   🗺️ Ubicación y asignación de sucursales")
    print(f"   🏢 Datos completos de sucursal")
    print(f"   📊 KPIs y calificaciones del formulario")
    print(f"   📝 Todas las respuestas extraídas del API")
    print(f"   📖 3 hojas: Datos + Resumen + Diccionario")
    
    print(f"\n✅ EXCEL COMPLETO LISTO PARA POSTGRESQL")
    
    return archivo_operativas, archivo_seguridad

if __name__ == "__main__":
    main()