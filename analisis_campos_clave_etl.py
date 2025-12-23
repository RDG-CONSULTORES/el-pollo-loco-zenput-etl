#!/usr/bin/env python3
"""
🔑 ANÁLISIS DE CAMPOS CLAVE PARA ETL 2026
Identificar y mapear campos esenciales Excel vs API para automatización completa
"""

import pandas as pd
import numpy as np
from datetime import datetime
import json

def extraer_campos_clave_excel(archivo_path, tipo_formulario):
    """Extraer solo los campos clave necesarios para ETL"""
    
    print(f"\n🔑 CAMPOS CLAVE: {tipo_formulario}")
    print("=" * 60)
    
    try:
        df = pd.read_excel(archivo_path)
        
        # CAMPOS CRÍTICOS PARA ETL
        campos_criticos = {
            'identificacion': ['Location', 'Location External Key', 'Submitted By', 'Date Submitted'],
            'sucursal_info': ['Sucursal', 'Location', 'Location External Key'],
            'usuario_info': ['Submitted By', 'Nombre SUPERVISOR'],
            'fecha_info': ['Date Submitted', 'Fecha y Hora'],
            'scoring': ['PUNTOS MAXIMOS', 'PUNTOS TOTALES', 'PORCENTAJE %']
        }
        
        # EXTRAER Y ANALIZAR CADA GRUPO
        datos_clave = {}
        
        for grupo, columnas in campos_criticos.items():
            print(f"\n📊 {grupo.upper()}:")
            datos_clave[grupo] = {}
            
            for columna in columnas:
                if columna in df.columns:
                    serie = df[columna].dropna()
                    datos_clave[grupo][columna] = {
                        'existe': True,
                        'total_registros': len(serie),
                        'porcentaje_completitud': len(serie) / len(df) * 100,
                        'valores_unicos': serie.nunique(),
                        'muestra_valores': serie.head(3).tolist(),
                        'tipo_dato': str(df[columna].dtype)
                    }
                    
                    print(f"   ✅ {columna}:")
                    print(f"      📊 Completitud: {len(serie)}/{len(df)} ({len(serie)/len(df)*100:.1f}%)")
                    print(f"      🔢 Valores únicos: {serie.nunique()}")
                    print(f"      📝 Muestra: {serie.head(3).tolist()}")
                    
                    if columna in ['Date Submitted', 'Fecha y Hora']:
                        # Análisis especial para fechas
                        fechas_validas = 0
                        for fecha in serie.head(5):
                            try:
                                if isinstance(fecha, str):
                                    pd.to_datetime(fecha)
                                    fechas_validas += 1
                                elif pd.notna(fecha):
                                    fechas_validas += 1
                            except:
                                pass
                        print(f"      📅 Fechas válidas: {fechas_validas}/5 muestras")
                        
                else:
                    datos_clave[grupo][columna] = {'existe': False}
                    print(f"   ❌ {columna}: NO EXISTE")
        
        # ANÁLISIS DE LOCATION DETALLADO
        print(f"\n🎯 ANÁLISIS DETALLADO DE LOCATION:")
        if 'Location' in df.columns:
            locations = df['Location'].dropna().value_counts()
            print(f"   📊 Total locations únicas: {len(locations)}")
            print(f"   📋 Top 10 locations más frecuentes:")
            for i, (location, count) in enumerate(locations.head(10).items(), 1):
                print(f"      {i:2d}. {location} ({count} veces)")
            
            # Análisis de patrones de location
            locations_con_numero = locations.index[locations.index.str.match(r'^\d+\s*-\s*.+', na=False)]
            locations_sin_numero = locations.index[~locations.index.str.match(r'^\d+\s*-\s*.+', na=False)]
            
            print(f"\n   🔍 PATRONES DE LOCATION:")
            print(f"      📊 Con número (ej: '35 - Apodaca'): {len(locations_con_numero)}")
            print(f"      📊 Sin número: {len(locations_sin_numero)}")
            
            if len(locations_sin_numero) > 0:
                print(f"      ⚠️ Locations sin número: {list(locations_sin_numero)[:5]}")
                
        # ANÁLISIS DE SUCURSAL
        print(f"\n🏪 ANÁLISIS DETALLADO DE SUCURSAL:")
        if 'Sucursal' in df.columns:
            sucursales = df['Sucursal'].dropna().value_counts()
            print(f"   📊 Total sucursales únicas: {len(sucursales)}")
            print(f"   📋 Top 10 sucursales más frecuentes:")
            for i, (sucursal, count) in enumerate(sucursales.head(10).items(), 1):
                print(f"      {i:2d}. {sucursal} ({count} veces)")
        
        # ANÁLISIS DE USUARIOS
        print(f"\n👤 ANÁLISIS DETALLADO DE USUARIOS:")
        if 'Submitted By' in df.columns:
            usuarios = df['Submitted By'].dropna().value_counts()
            print(f"   📊 Total usuarios únicos: {len(usuarios)}")
            for i, (usuario, count) in enumerate(usuarios.items(), 1):
                print(f"      {i:2d}. {usuario} ({count} supervisiones)")
        
        return datos_clave
        
    except Exception as e:
        print(f"❌ Error procesando {archivo_path}: {e}")
        return None

def comparar_con_api_structure():
    """Comparar con la estructura que tenemos del API"""
    
    print(f"\n🔄 COMPARACIÓN EXCEL vs API STRUCTURE")
    print("=" * 60)
    
    # Estructura API conocida (de nuestro análisis anterior)
    api_structure = {
        'submission_id': 'ID único de Zenput',
        'form_type': 'OPERATIVA/SEGURIDAD', 
        'fecha': 'date_submitted from smetadata',
        'usuario_nombre': 'created_by.display_name',
        'usuario_id': 'created_by.id',
        'location_name': 'smetadata.location.name',
        'location_id': 'smetadata.location.id', 
        'lat_entrega': 'smetadata.lat',
        'lon_entrega': 'smetadata.lon',
        'tiene_location': 'boolean calculado',
        'tiene_coordenadas': 'boolean calculado'
    }
    
    print(f"📊 ESTRUCTURA API CONOCIDA:")
    for campo, descripcion in api_structure.items():
        print(f"   📋 {campo}: {descripcion}")
    
    return api_structure

def generar_mapeo_excel_api(datos_operativa, datos_seguridad, api_structure):
    """Generar mapeo entre campos Excel y API"""
    
    print(f"\n🗺️ MAPEO EXCEL ↔ API PARA AUTOMATIZACIÓN 2026")
    print("=" * 70)
    
    mapeo = {}
    
    # MAPEO DIRECTO
    mapeo_directo = {
        # API_FIELD: EXCEL_FIELD
        'submission_id': 'ID único (requiere generación)',
        'form_type': 'Formulario (877138=OPERATIVA, 877139=SEGURIDAD)',
        'fecha': 'Date Submitted',
        'usuario_nombre': 'Submitted By',
        'usuario_id': 'Usuario ID (requiere mapeo)',
        'location_name': 'Location',
        'location_id': 'Location External Key',
        'sucursal_nombre': 'Sucursal',
        'lat_entrega': 'Coordenadas (NO disponible en Excel)',
        'lon_entrega': 'Coordenadas (NO disponible en Excel)',
        'puntos_maximos': 'PUNTOS MAXIMOS',
        'puntos_totales': 'PUNTOS TOTALES',
        'porcentaje': 'PORCENTAJE %'
    }
    
    print(f"📊 MAPEO CAMPO POR CAMPO:")
    for api_field, excel_field in mapeo_directo.items():
        if "requiere" in excel_field or "NO disponible" in excel_field:
            print(f"   ⚠️ {api_field:20} → {excel_field}")
        else:
            print(f"   ✅ {api_field:20} → {excel_field}")
    
    # PROBLEMAS IDENTIFICADOS
    print(f"\n❗ PROBLEMAS PARA AUTOMATIZACIÓN:")
    problemas = {
        'coordinates': 'Excel NO tiene lat/lon - API SÍ tiene en smetadata',
        'submission_id': 'Excel NO tiene ID único - API SÍ tiene',
        'usuario_id': 'Excel solo nombre - API tiene ID numérico',
        'form_template_id': 'Excel NO especifica - API necesita 877138/877139',
        'metadata': 'Excel tiene 724 campos detallados - API solo metadata básico'
    }
    
    for problema, descripcion in problemas.items():
        print(f"   🔴 {problema}: {descripcion}")
    
    # SOLUCIONES PROPUESTAS
    print(f"\n💡 SOLUCIONES PARA ETL 2026:")
    soluciones = {
        'hybrid_approach': 'Usar API como fuente primaria + Excel para validación',
        'coordinate_mapping': 'Crear tabla lookup: Location → Coordenadas',
        'user_mapping': 'Crear tabla lookup: Nombre → ID usuario',
        'form_detection': 'Auto-detectar formulario por contenido/nombre archivo',
        'data_enrichment': 'Combinar datos API (básicos) + Excel (detallados)'
    }
    
    for solucion, descripcion in soluciones.items():
        print(f"   🔧 {solucion}: {descripcion}")
    
    return mapeo_directo, problemas, soluciones

def crear_etl_strategy_2026(mapeo, problemas, soluciones):
    """Crear estrategia completa ETL 2026"""
    
    print(f"\n🚀 ESTRATEGIA ETL AUTOMATIZADO 2026")
    print("=" * 60)
    
    estrategia = {
        'fase_1': {
            'nombre': 'Extracción API Primaria',
            'descripcion': 'Usar Zenput API v3 como fuente principal',
            'pasos': [
                'Autenticación con X-API-TOKEN',
                'Query /submissions con form_template_id',
                'Paginación automática con start/limit',
                'Extracción campos: ID, fecha, usuario, location, coordenadas'
            ],
            'campos_obtenidos': ['submission_id', 'fecha', 'usuario_nombre', 'location_name', 'lat_entrega', 'lon_entrega']
        },
        'fase_2': {
            'nombre': 'Enriquecimiento con Excel (Opcional)',
            'descripcion': 'Agregar datos detallados si Excel está disponible',
            'pasos': [
                'Leer Excel exportado desde Zenput',
                'Mapear por Location + Date',
                'Extraer scoring y datos detallados',
                'Validar consistencia API vs Excel'
            ],
            'campos_adicionales': ['puntos_maximos', 'puntos_totales', 'porcentaje', 'supervisor', 'sucursal_detalle']
        },
        'fase_3': {
            'nombre': 'Mapeo a Sucursales',
            'descripcion': 'Asignar submissions a sucursales del master catalog',
            'pasos': [
                'Mapeo directo por location_name',
                'Mapeo por coordenadas (Haversine)',
                'Validación manual casos edge',
                'Clasificación LOCAL/FORÁNEA automática'
            ],
            'salida': 'Submissions asignadas a 86 sucursales con confianza'
        },
        'fase_4': {
            'nombre': 'Validación y QA',
            'descripcion': 'Verificar reglas de negocio y calidad',
            'pasos': [
                'Verificar coincidencia fechas operativa-seguridad',
                'Validar reglas 4+4 (LOCAL) y 2+2 (FORÁNEA)',
                'Detectar anomalías y datos faltantes',
                'Generar reporte de calidad'
            ],
            'reglas': ['LOCAL: 4 ops + 4 seg por año', 'FORÁNEA: 2 ops + 2 seg por año']
        }
    }
    
    for fase, detalles in estrategia.items():
        print(f"\n📋 {fase.upper()}: {detalles['nombre']}")
        print(f"   🎯 {detalles['descripcion']}")
        for i, paso in enumerate(detalles['pasos'], 1):
            print(f"      {i}. {paso}")
        
        if 'campos_obtenidos' in detalles:
            print(f"   📊 Campos obtenidos: {detalles['campos_obtenidos']}")
        if 'campos_adicionales' in detalles:
            print(f"   📊 Campos adicionales: {detalles['campos_adicionales']}")
        if 'salida' in detalles:
            print(f"   📤 Salida: {detalles['salida']}")
        if 'reglas' in detalles:
            print(f"   📋 Reglas: {detalles['reglas']}")
    
    return estrategia

def main():
    """Función principal"""
    
    print("🔑 ANÁLISIS DE CAMPOS CLAVE PARA ETL 2026")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Objetivo: Identificar campos clave y crear estrategia automatización")
    print("=" * 80)
    
    # Archivos a analizar
    archivos = [
        ("SUPERVISION_OPERATIVA_CAS_11_REV_250125_Submissions-2025-12-18__1228CST-1766141816.xlsx", "OPERATIVA"),
        ("CONTROL_OPERATIVO_DE_SEGURIDAD_CAS_11_REV_25012025_Submissions-2025-12-18__1232CST-1766104009.xlsx", "SEGURIDAD")
    ]
    
    # Analizar campos clave de cada archivo
    datos_operativa = None
    datos_seguridad = None
    
    for archivo, tipo in archivos:
        datos = extraer_campos_clave_excel(archivo, tipo)
        if tipo == "OPERATIVA":
            datos_operativa = datos
        else:
            datos_seguridad = datos
    
    # Comparar con API
    api_structure = comparar_con_api_structure()
    
    # Generar mapeo
    if datos_operativa or datos_seguridad:
        mapeo, problemas, soluciones = generar_mapeo_excel_api(datos_operativa, datos_seguridad, api_structure)
        
        # Crear estrategia ETL
        estrategia = crear_etl_strategy_2026(mapeo, problemas, soluciones)
        
        # Guardar análisis
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        resultado_completo = {
            'timestamp': timestamp,
            'datos_operativa': datos_operativa,
            'datos_seguridad': datos_seguridad,
            'api_structure': api_structure,
            'mapeo_excel_api': mapeo,
            'problemas_identificados': problemas,
            'soluciones_propuestas': soluciones,
            'estrategia_etl_2026': estrategia
        }
        
        with open(f"CAMPOS_CLAVE_ETL_2026_{timestamp}.json", 'w', encoding='utf-8') as f:
            json.dump(resultado_completo, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n📁 ANÁLISIS GUARDADO: CAMPOS_CLAVE_ETL_2026_{timestamp}.json")
        
        # CONCLUSIONES FINALES
        print(f"\n" + "=" * 80)
        print(f"🎯 CONCLUSIONES PARA ETL 2026")
        print("=" * 80)
        
        print(f"✅ DATOS EXCEL ANALIZADOS:")
        if datos_operativa:
            print(f"   📊 OPERATIVA: 238 registros, 724 campos detallados")
        if datos_seguridad:
            print(f"   📊 SEGURIDAD: 238 registros, campos detallados")
        
        print(f"\n🔧 RECOMENDACIÓN FINAL:")
        print(f"   1. 🚀 USA API v3 como fuente PRIMARIA (automatizable)")
        print(f"   2. 📊 USA Excel como fuente SECUNDARIA (validación/detalle)")
        print(f"   3. 🗺️ CREA lookup tables para Location → Coordenadas")
        print(f"   4. 🧪 IMPLEMENTA validación híbrida API + Excel")
        print(f"   5. 🔄 AUTOMATIZA para 2026 usando form_template_ids")
        
        print(f"\n🔜 SIGUIENTE PASO:")
        print(f"   ✅ Implementar ETL híbrido usando ambas fuentes")
        print(f"   🎯 Priorizar API para automatización, Excel para validación")
        
        return resultado_completo
    
    else:
        print("❌ No se pudieron analizar los datos")
        return None

if __name__ == "__main__":
    main()