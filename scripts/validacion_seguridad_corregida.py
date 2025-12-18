#!/usr/bin/env python3
"""
🛡️ VALIDACIÓN SEGURIDAD CORREGIDA
Valida correctamente las 11 áreas de evaluación (excluyendo firmas)
"""

import json
from datetime import datetime

def validar_seguridad_corregida():
    """Valida correctamente las 11 áreas de evaluación del formulario de seguridad"""
    
    print("🛡️ VALIDACIÓN SEGURIDAD CORREGIDA - 11 ÁREAS")
    print("=" * 55)
    
    # Leer submission real de seguridad
    try:
        with open('/Users/robertodavila/el-pollo-loco-zenput-etl/data/sample_submission_877139_20251217_145554.json', 'r') as f:
            submission = json.load(f)
        print("✅ Submission de seguridad cargada")
    except FileNotFoundError:
        print("❌ No se pudo cargar la submission")
        return None
    
    # Extraer información básica
    form_id = submission.get('form_id', 'N/A')
    answers = submission.get('answers', [])
    
    # Metadatos de submission
    location_info = submission.get('smetadata', {}).get('location', {})
    sucursal_nombre = location_info.get('name', 'N/A')
    
    created_by = submission.get('smetadata', {}).get('created_by', {})
    auditor_nombre = created_by.get('display_name', 'N/A')
    
    fecha_submission = submission.get('smetadata', {}).get('date_submitted', 'N/A')
    
    # 1. EXTRAER CALIFICACIÓN GENERAL (como la quiere Roberto)
    puntos_max = None
    puntos_obtenidos = None
    calificacion_porcentaje = None
    sucursal_field = None
    auditor_field = None
    fecha_field = None
    
    for answer in answers:
        title = answer.get('title', '')
        value = answer.get('value')
        
        if title == 'PUNTOS MAX':
            puntos_max = value
        elif title == 'PUNTOS TOTALES OBTENIDOS':
            puntos_obtenidos = value
        elif title == 'CALIFICACION PORCENTAJE %':
            calificacion_porcentaje = value
        elif title == 'SUCURSAL':
            sucursal_field = value
        elif title == 'AUDITOR':
            auditor_field = value
        elif title == 'Date':
            fecha_field = value
    
    print(f"\n🎯 CALIFICACIÓN GENERAL (FORMATO ROBERTO):")
    print("=" * 45)
    print(f"CONTROL OPERATIVO DE SEGURIDAD")
    print(f"PUNTOS MAX                    {puntos_max}")
    print(f"PUNTOS TOTALES OBTENIDOS      {puntos_obtenidos}")
    print(f"CALIFICACION PORCENTAJE %     {calificacion_porcentaje}")
    print(f"SUCURSAL                      {sucursal_field}")
    print(f"AUDITOR                       {auditor_field}")
    print(f"Date                          {fecha_field}")
    
    # 2. IDENTIFICAR LAS 11 ÁREAS DE EVALUACIÓN (excluyendo firmas)
    areas_evaluacion = [
        'I. AREA COMEDOR',
        'II. AREA ASADORES', 
        'III. AREA DE MARINADO',
        'IV. AREA DE BODEGA',
        'V. AREA DE HORNO',
        'VI. AREA FREIDORAS',
        'VII. CENTRO DE CARGA',
        'VIII. AREA AZOTEA',
        'IX. AREA EXTERIOR',
        'X. PROGRAMA INTERNO PROTECCION CIVIL',
        'XI. BITACORAS'
    ]
    
    areas_data = {}
    current_area = None
    
    for answer in answers:
        title = answer.get('title', '')
        field_type = answer.get('field_type', '')
        value = answer.get('value')
        
        # Identificar secciones de evaluación
        if field_type == 'section' and title.strip() in areas_evaluacion:
            current_area = title.strip()
            if current_area not in areas_data:
                areas_data[current_area] = {
                    'preguntas': [],
                    'puntos_max': 0,
                    'puntos_obtenidos': 0,
                    'porcentaje': 0
                }
        
        # Buscar puntos de cada área
        elif current_area and field_type == 'formula':
            # Buscar campos específicos de cada área
            area_key = current_area.split('.')[0].strip()  # I, II, III, etc.
            
            if 'PUNTOS MAX' in title and (area_key in title.upper() or current_area.split('.')[1].strip().upper() in title.upper()):
                areas_data[current_area]['puntos_max'] = value
            elif 'PUNTOS TOTALES' in title and (area_key in title.upper() or current_area.split('.')[1].strip().upper() in title.upper()):
                areas_data[current_area]['puntos_obtenidos'] = value
            elif 'PORCENTAJE %' in title and (area_key in title.upper() or current_area.split('.')[1].strip().upper() in title.upper()):
                areas_data[current_area]['porcentaje'] = value
        
        # Agregar preguntas de evaluación
        elif current_area and field_type == 'yesno':
            areas_data[current_area]['preguntas'].append({
                'numero': len(areas_data[current_area]['preguntas']) + 1,
                'texto': title,
                'respuesta': value,
                'respuesta_texto': '✅ SÍ' if value == 'true' else '❌ NO' if value == 'false' else '⚪ N/A'
            })
    
    # Buscar puntos específicos por área revisando mejor los títulos
    for answer in answers:
        title = answer.get('title', '')
        value = answer.get('value')
        field_type = answer.get('field_type', '')
        
        if field_type == 'formula' and 'PUNTOS' in title and 'MAX' in title:
            for area in areas_data.keys():
                # Mapear nombres de áreas a campos de puntos
                area_mapping = {
                    'I. AREA COMEDOR': 'COMEDOR',
                    'II. AREA ASADORES': 'ASADORES',
                    'III. AREA DE MARINADO': 'MARINADO',
                    'IV. AREA DE BODEGA': 'BODEGA',
                    'V. AREA DE HORNO': 'HORNOS',
                    'VI. AREA FREIDORAS': 'FREIDORAS',
                    'VII. CENTRO DE CARGA': 'CENTRO DE CARGA',
                    'VIII. AREA AZOTEA': 'AZOTEA',
                    'IX. AREA EXTERIOR': 'EXTERIOR',
                    'X. PROGRAMA INTERNO PROTECCION CIVIL': 'PROGRAMA',
                    'XI. BITACORAS': 'BITACORAS'
                }
                
                area_key = area_mapping.get(area, '')
                if area_key and area_key in title:
                    if 'PUNTOS MAX' in title:
                        areas_data[area]['puntos_max'] = value
                    elif 'PUNTOS TOTALES' in title:
                        areas_data[area]['puntos_obtenidos'] = value
                    elif 'PORCENTAJE %' in title:
                        areas_data[area]['porcentaje'] = value
    
    print(f"\n🏷️ LAS 11 ÁREAS DE EVALUACIÓN:")
    print("=" * 45)
    
    total_puntos_areas = 0
    total_puntos_max_areas = 0
    
    for i, (area_nombre, area_data) in enumerate(areas_data.items(), 1):
        print(f"\nÁREA {i}: {area_nombre}")
        print(f"   • Preguntas: {len(area_data['preguntas'])}")
        print(f"   • Puntos Máximos: {area_data['puntos_max']}")
        print(f"   • Puntos Obtenidos: {area_data['puntos_obtenidos']}")
        print(f"   • Porcentaje: {area_data['porcentaje']}%")
        
        if area_data['puntos_max']:
            total_puntos_max_areas += area_data['puntos_max']
        if area_data['puntos_obtenidos']:
            total_puntos_areas += area_data['puntos_obtenidos']
        
        # Mostrar algunas preguntas
        for pregunta in area_data['preguntas'][:2]:
            print(f"      {pregunta['numero']}. {pregunta['texto']} → {pregunta['respuesta_texto']}")
        if len(area_data['preguntas']) > 2:
            print(f"      ... y {len(area_data['preguntas']) - 2} preguntas más")
    
    # 3. VERIFICAR SUMA DE PUNTOS
    print(f"\n🔍 VERIFICACIÓN PUNTOS:")
    print("=" * 25)
    print(f"   • Suma áreas MAX: {total_puntos_max_areas}")
    print(f"   • General MAX: {puntos_max}")
    print(f"   • Coincide MAX: {'✅' if total_puntos_max_areas == puntos_max else '❌'}")
    
    print(f"   • Suma áreas OBTENIDOS: {total_puntos_areas}")
    print(f"   • General OBTENIDOS: {puntos_obtenidos}")
    print(f"   • Coincide OBTENIDOS: {'✅' if total_puntos_areas == puntos_obtenidos else '❌'}")
    
    # 4. VALIDACIÓN FINAL
    total_areas = len(areas_data)
    estructura_correcta = total_areas == 11
    tiene_calificacion = calificacion_porcentaje is not None
    puntos_coinciden = (total_puntos_max_areas == puntos_max and total_puntos_areas == puntos_obtenidos)
    
    print(f"\n✅ VALIDACIÓN FINAL:")
    print("=" * 25)
    print(f"   • Áreas encontradas: {total_areas}/11")
    print(f"   • Estructura correcta: {'✅' if estructura_correcta else '❌'}")
    print(f"   • Calificación general: {'✅' if tiene_calificacion else '❌'}")
    print(f"   • Puntos coinciden: {'✅' if puntos_coinciden else '❌'}")
    print(f"   • Datos completos: {'✅' if sucursal_field and auditor_field and fecha_field else '❌'}")
    
    validacion_exitosa = estructura_correcta and tiene_calificacion and puntos_coinciden
    
    # 5. GENERAR ESQUEMA ETL PARA RAILWAY
    if validacion_exitosa:
        print(f"\n🚀 FORMULARIO VALIDADO PARA RAILWAY:")
        print("=" * 40)
        print(f"✅ Form 877139: Control Operativo de Seguridad")
        print(f"✅ 11 áreas de evaluación identificadas")
        print(f"✅ Calificación porcentual: {calificacion_porcentaje}%")
        print(f"✅ Formato exacto como solicita Roberto")
        
        # Generar query SQL para Railway
        sql_extract = f"""
        -- ETL Query para Form Seguridad 877139
        SELECT 
            s.id as submission_id,
            l.id as sucursal_id,
            l.name as sucursal_nombre,
            -- Calificación General
            (SELECT value FROM answers WHERE field_title = 'PUNTOS MAX') as puntos_max,
            (SELECT value FROM answers WHERE field_title = 'PUNTOS TOTALES OBTENIDOS') as puntos_obtenidos,
            (SELECT value FROM answers WHERE field_title = 'CALIFICACION PORCENTAJE %') as calificacion_porcentaje,
            (SELECT value FROM answers WHERE field_title = 'AUDITOR') as auditor,
            (SELECT value FROM answers WHERE field_title = 'Date') as fecha_supervision,
            -- Fecha de submission
            s.submitted_at,
            -- Ubicación
            l.lat, l.lon
        FROM submissions s
        JOIN locations l ON s.location_id = l.id
        WHERE s.form_id = '877139'
        """
        
        print(f"\n📊 QUERY SQL GENERADO PARA RAILWAY:")
        print(sql_extract)
        
        # Resultado para guardar
        resultado = {
            'timestamp': datetime.now().isoformat(),
            'validacion_exitosa': True,
            'form_id': form_id,
            'calificacion_general': {
                'puntos_max': puntos_max,
                'puntos_obtenidos': puntos_obtenidos,
                'calificacion_porcentaje': calificacion_porcentaje,
                'sucursal': sucursal_field,
                'auditor': auditor_field,
                'fecha': fecha_field
            },
            'areas_evaluacion': areas_data,
            'sql_query': sql_extract,
            'submission_ejemplo': {
                'sucursal': sucursal_nombre,
                'auditor': auditor_nombre,
                'fecha': fecha_submission
            }
        }
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"data/validacion_seguridad_exitosa_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(resultado, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n💾 VALIDACIÓN EXITOSA GUARDADA: {filename}")
        print(f"🎉 ¡LISTO PARA ETL RAILWAY!")
        
        return resultado
    else:
        print(f"\n❌ VALIDACIÓN FALLÓ")
        return None

if __name__ == "__main__":
    print("🛡️ EJECUTANDO VALIDACIÓN SEGURIDAD CORREGIDA")
    print("Validando 11 áreas de evaluación (excluyendo firmas)...")
    print()
    
    resultado = validar_seguridad_corregida()
    
    if resultado:
        print(f"\n🎉 ¡VALIDACIÓN COMPLETAMENTE EXITOSA!")
        print(f"✅ Formulario 877139 listo para Railway")
        print(f"📊 11 áreas + calificación general validadas")
        print(f"🎯 Formato exacto como solicita Roberto")
    else:
        print(f"\n❌ Validación falló - revisar datos")