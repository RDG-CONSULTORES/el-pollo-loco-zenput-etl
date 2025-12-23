#!/usr/bin/env python3
"""
📊 EXTRAER ESTRUCTURA COMPLETA EXCEL
Crear 2 Excel: Operativas y Seguridad con toda la estructura, KPIs y calificaciones
"""

import pandas as pd
from datetime import datetime
import requests
import json

def cargar_configuracion():
    """Cargar configuración y catálogo de sucursales"""
    
    print("🔧 CARGAR CONFIGURACIÓN")
    print("=" * 50)
    
    # Configuración API
    api_config = {
        'base_url': 'https://www.zenput.com/api/v3',
        'headers': {
            'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314',
            'Content-Type': 'application/json'
        },
        'templates': {
            'operativas': '877138',
            'seguridad': '877139'
        }
    }
    
    # Cargar catálogo de sucursales corregido
    df_sucursales = pd.read_csv("SUCURSALES_CORRECCIONES_ROBERTO_20251218_171807.csv")
    
    # Crear diccionario de sucursales
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
    
    print(f"✅ API configurado: {api_config['base_url']}")
    print(f"✅ Catálogo cargado: {len(catalogo_sucursales)} sucursales")
    
    return api_config, catalogo_sucursales

def extraer_submissions_detalladas(api_config, template_id, tipo_supervision):
    """Extraer submissions con estructura completa del API"""
    
    print(f"\n📊 EXTRAER {tipo_supervision.upper()} COMPLETAS")
    print("=" * 50)
    
    url = f"{api_config['base_url']}/submissions"
    params = {
        'form_template_id': template_id,
        'limit': 500  # Máximo permitido
    }
    
    try:
        response = requests.get(url, headers=api_config['headers'], params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        submissions = data.get('data', [])
        print(f"✅ {len(submissions)} submissions {tipo_supervision} extraídas")
        
        return submissions
        
    except Exception as e:
        print(f"❌ Error extrayendo {tipo_supervision}: {e}")
        return []

def procesar_submission_completa(submission, tipo_supervision, catalogo_sucursales):
    """Procesar submission completa con todos los campos"""
    
    # Datos básicos
    submission_id = submission.get('id', '')
    
    # Metadatos
    smetadata = submission.get('smetadata', {})
    date_submitted = smetadata.get('date_submitted')
    created_by = smetadata.get('created_by', {})
    location_data = smetadata.get('location', {})
    
    # Usuario que creó
    user_name = created_by.get('name', '')
    user_email = created_by.get('email', '')
    
    # Coordenadas de entrega
    lat = location_data.get('lat')
    lon = location_data.get('lon')
    
    # Campo location (si existe)
    location_field = submission.get('location')
    
    # Campos de formulario
    fields = submission.get('fields', {})
    
    # Buscar campo Sucursal
    sucursal_campo = None
    for field_id, field_data in fields.items():
        if isinstance(field_data, dict):
            value = field_data.get('value')
            if value and ('sucursal' in str(value).lower() or len(str(value)) < 20):
                sucursal_campo = str(value)
                break
    
    # Asignar a sucursal usando estrategia
    location_asignado = asignar_sucursal_automatica(
        lat, lon, location_field, sucursal_campo, catalogo_sucursales
    )
    
    # Obtener datos de sucursal
    sucursal_info = catalogo_sucursales.get(location_asignado, {})
    
    # Extraer todas las respuestas del formulario
    respuestas = extraer_respuestas_formulario(fields)
    
    # Calcular KPIs y calificaciones
    kpis = calcular_kpis_supervision(respuestas, tipo_supervision)
    
    # Crear registro completo
    registro = {
        # DATOS BÁSICOS
        'submission_id': submission_id,
        'tipo_supervision': tipo_supervision,
        'date_submitted': date_submitted,
        'user_name': user_name,
        'user_email': user_email,
        
        # UBICACIÓN
        'lat_entrega': lat,
        'lon_entrega': lon,
        'location_field': location_field,
        'sucursal_campo': sucursal_campo,
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

def asignar_sucursal_automatica(lat, lon, location_field, sucursal_campo, catalogo):
    """Asignar sucursal usando estrategia automática"""
    
    # PASO 1: Coordenadas geográficas
    if lat and lon:
        try:
            lat_num = float(lat)
            lon_num = float(lon)
            
            distancias = []
            for location_key, info in catalogo.items():
                if info.get('lat') and info.get('lon'):
                    dist = calcular_distancia_haversine(
                        lat_num, lon_num, 
                        float(info['lat']), float(info['lon'])
                    )
                    distancias.append({'location_key': location_key, 'distancia': dist})
            
            if distancias:
                mas_cercana = min(distancias, key=lambda x: x['distancia'])
                if mas_cercana['distancia'] < 3:  # Menos de 3km
                    return mas_cercana['location_key']
        except:
            pass
    
    # PASO 2: Campo Sucursal
    if sucursal_campo:
        # Normalizar nombre
        nombre_normalizado = normalizar_nombre_sucursal(sucursal_campo)
        
        for location_key, info in catalogo.items():
            if (nombre_normalizado.lower() in info['nombre'].lower() or 
                info['nombre'].lower() in nombre_normalizado.lower()):
                return location_key
    
    # PASO 3: Location field
    if location_field:
        for location_key in catalogo.keys():
            if location_key == location_field:
                return location_key
    
    # PASO 4: Asignar por defecto a primera sucursal disponible
    return list(catalogo.keys())[0] if catalogo else 'SIN_ASIGNAR'

def calcular_distancia_haversine(lat1, lon1, lat2, lon2):
    """Calcular distancia en km usando fórmula Haversine"""
    import math
    
    R = 6371  # Radio de la Tierra en km
    
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    
    a = (math.sin(dlat/2)**2 + 
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * 
         math.sin(dlon/2)**2)
    
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    return R * c

def normalizar_nombre_sucursal(nombre):
    """Normalizar nombres de sucursales"""
    normalizaciones = {
        'SC': 'Santa Catarina',
        'LH': 'La Huasteca',
        'GC': 'Garcia'
    }
    
    for abrev, completo in normalizaciones.items():
        if abrev in nombre:
            return nombre.replace(abrev, completo)
    
    return nombre

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
            if any(word in valor_lower for word in ['sí', 'si', 'yes', 'correcto', 'bien']):
                conformes += 1
            elif any(word in valor_lower for word in ['no', 'incorrecto', 'mal', 'falta']):
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

def crear_excel_supervision(datos_procesados, tipo_supervision):
    """Crear Excel con estructura completa de supervisión"""
    
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
    archivo_excel = f"SUPERVISIONES_{tipo_supervision.upper()}_COMPLETAS_{timestamp}.xlsx"
    
    with pd.ExcelWriter(archivo_excel, engine='openpyxl') as writer:
        # Hoja principal con datos
        df_final.to_excel(writer, sheet_name=f'{tipo_supervision.title()}', index=False)
        
        # Hoja de resumen
        crear_hoja_resumen(writer, df_final, tipo_supervision)
        
        # Hoja de diccionario de campos
        crear_diccionario_campos(writer, df_final)
    
    print(f"✅ Excel creado: {archivo_excel}")
    print(f"📊 Registros: {len(df_final)}")
    print(f"📋 Columnas: {len(df_final.columns)}")
    
    return archivo_excel, df_final

def crear_hoja_resumen(writer, df, tipo_supervision):
    """Crear hoja de resumen con estadísticas"""
    
    resumen_data = {
        'Métrica': [
            'Total Supervisiones',
            'Sucursales Únicas',
            'Usuarios Únicos',
            'Calificación Promedio',
            'Conformidad Promedio',
            'Completitud Promedio',
            'Fecha Más Antigua',
            'Fecha Más Reciente'
        ],
        'Valor': [
            len(df),
            df['sucursal_nombre'].nunique(),
            df['user_name'].nunique(),
            f"{df['calificacion_total'].mean():.2f}%" if 'calificacion_total' in df.columns else 'N/A',
            f"{df['porcentaje_conformidad'].mean():.2f}%" if 'porcentaje_conformidad' in df.columns else 'N/A',
            f"{df['porcentaje_completitud'].mean():.2f}%" if 'porcentaje_completitud' in df.columns else 'N/A',
            df['date_submitted'].min() if 'date_submitted' in df.columns else 'N/A',
            df['date_submitted'].max() if 'date_submitted' in df.columns else 'N/A'
        ]
    }
    
    df_resumen = pd.DataFrame(resumen_data)
    df_resumen.to_excel(writer, sheet_name='Resumen', index=False)

def crear_diccionario_campos(writer, df):
    """Crear diccionario de campos para referencia"""
    
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
    
    print("📊 EXTRAER ESTRUCTURA COMPLETA EXCEL")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Roberto: 2 Excel completos con estructura, KPIs y calificaciones")
    print("=" * 80)
    
    # 1. Cargar configuración
    api_config, catalogo_sucursales = cargar_configuracion()
    
    # 2. Extraer supervisiones operativas
    print(f"\n🔧 PROCESANDO SUPERVISIONES OPERATIVAS")
    submissions_operativas = extraer_submissions_detalladas(
        api_config, api_config['templates']['operativas'], 'operativas'
    )
    
    datos_operativas = []
    for submission in submissions_operativas:
        registro = procesar_submission_completa(submission, 'operativas', catalogo_sucursales)
        datos_operativas.append(registro)
    
    # 3. Extraer supervisiones de seguridad
    print(f"\n🛡️ PROCESANDO SUPERVISIONES DE SEGURIDAD")
    submissions_seguridad = extraer_submissions_detalladas(
        api_config, api_config['templates']['seguridad'], 'seguridad'
    )
    
    datos_seguridad = []
    for submission in submissions_seguridad:
        registro = procesar_submission_completa(submission, 'seguridad', catalogo_sucursales)
        datos_seguridad.append(registro)
    
    # 4. Crear Excel para Operativas
    archivo_operativas, df_operativas = crear_excel_supervision(datos_operativas, 'operativas')
    
    # 5. Crear Excel para Seguridad
    archivo_seguridad, df_seguridad = crear_excel_supervision(datos_seguridad, 'seguridad')
    
    # 6. Resumen final
    print(f"\n🎯 ARCHIVOS EXCEL GENERADOS:")
    print("=" * 50)
    if archivo_operativas:
        print(f"✅ OPERATIVAS: {archivo_operativas}")
        print(f"   📊 {len(df_operativas)} registros, {len(df_operativas.columns)} columnas")
    
    if archivo_seguridad:
        print(f"✅ SEGURIDAD: {archivo_seguridad}")
        print(f"   📊 {len(df_seguridad)} registros, {len(df_seguridad.columns)} columnas")
    
    print(f"\n🎯 ESTRUCTURA INCLUYE:")
    print(f"   📋 Datos básicos (ID, fecha, usuario)")
    print(f"   🗺️ Ubicación completa (coordenadas, asignación)")
    print(f"   🏢 Datos sucursal (número, nombre, tipo, grupo, coordenadas)")
    print(f"   📊 KPIs y calificaciones (conformidad, completitud, puntos críticos)")
    print(f"   📝 Todas las respuestas del formulario")
    print(f"   📖 3 hojas: Datos + Resumen + Diccionario de campos")
    
    print(f"\n✅ SÁBANAS COMPLETAS LISTAS PARA POSTGRESQL")
    
    return archivo_operativas, archivo_seguridad

if __name__ == "__main__":
    main()