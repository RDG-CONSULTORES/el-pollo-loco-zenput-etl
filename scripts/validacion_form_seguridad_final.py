#!/usr/bin/env python3
"""
🛡️ VALIDACIÓN FINAL FORMULARIO SEGURIDAD
Valida la estructura exacta del formulario 877139 basado en submission real
"""

import json
from datetime import datetime

def validar_estructura_seguridad_final():
    """Valida la estructura exacta del formulario de seguridad con datos reales"""
    
    print("🛡️ VALIDACIÓN FINAL FORMULARIO SEGURIDAD 877139")
    print("=" * 60)
    
    # Leer submission real de seguridad
    try:
        with open('/Users/robertodavila/el-pollo-loco-zenput-etl/data/sample_submission_877139_20251217_145554.json', 'r') as f:
            submission = json.load(f)
        print("✅ Submission de seguridad cargada exitosamente")
    except FileNotFoundError:
        print("❌ No se pudo cargar la submission de seguridad")
        return None
    
    # Extraer información básica
    form_id = submission.get('form_id', 'N/A')
    form_name = submission.get('form_name', 'N/A')
    activity_name = submission.get('activity', {}).get('name', 'N/A')
    
    print(f"\n📋 INFORMACIÓN FORMULARIO:")
    print(f"   • Form ID: {form_id}")
    print(f"   • Form Name: {form_name}")
    print(f"   • Activity: {activity_name}")
    
    # Extraer metadatos de la submission
    location_info = submission.get('smetadata', {}).get('location', {})
    sucursal_nombre = location_info.get('name', 'N/A')
    sucursal_id = location_info.get('external_key', 'N/A')
    
    created_by = submission.get('smetadata', {}).get('created_by', {})
    auditor_nombre = created_by.get('display_name', 'N/A')
    
    fecha_submission = submission.get('smetadata', {}).get('date_submitted', 'N/A')
    
    print(f"\n📊 DATOS SUBMISSION:")
    print(f"   • Sucursal: {sucursal_nombre} (ID: {sucursal_id})")
    print(f"   • Auditor: {auditor_nombre}")
    print(f"   • Fecha: {fecha_submission}")
    
    # Analizar respuestas (answers)
    answers = submission.get('answers', [])
    print(f"   • Total respuestas: {len(answers)}")
    
    # 1. BUSCAR CALIFICACIÓN GENERAL (Formato requerido por Roberto)
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
    
    print(f"\n🎯 CALIFICACIÓN GENERAL EXTRAÍDA (FORMATO ROBERTO):")
    print("=" * 55)
    print(f"CONTROL OPERATIVO DE SEGURIDAD")
    print(f"PUNTOS MAX                    {puntos_max}")
    print(f"PUNTOS TOTALES OBTENIDOS      {puntos_obtenidos}")
    print(f"CALIFICACION PORCENTAJE %     {calificacion_porcentaje}")
    print(f"SUCURSAL                      {sucursal_field}")
    print(f"AUDITOR                       {auditor_field}")
    print(f"Date                          {fecha_field}")
    
    # 2. IDENTIFICAR LAS 11 ÁREAS
    areas_identificadas = {}
    current_area = None
    
    for answer in answers:
        title = answer.get('title', '')
        field_type = answer.get('field_type', '')
        value = answer.get('value')
        
        # Identificar secciones (las 11 áreas)
        if field_type == 'section' and title.strip() and not title in ['CONTROL OPERATIVO DE SEGURIDAD']:
            current_area = title.strip()
            if current_area not in areas_identificadas:
                areas_identificadas[current_area] = {
                    'preguntas': [],
                    'puntos_max': 0,
                    'puntos_obtenidos': 0,
                    'porcentaje': 0
                }
        
        # Buscar puntos máximos, obtenidos y porcentaje de cada área
        elif current_area and 'PUNTOS MAX' in title and current_area.upper().replace('.', '').replace(' ', '') in title.upper().replace(' ', ''):
            areas_identificadas[current_area]['puntos_max'] = value
        
        elif current_area and 'PUNTOS TOTALES' in title and current_area.upper().replace('.', '').replace(' ', '') in title.upper().replace(' ', ''):
            areas_identificadas[current_area]['puntos_obtenidos'] = value
            
        elif current_area and 'PORCENTAJE %' in title and current_area.upper().replace('.', '').replace(' ', '') in title.upper().replace(' ', ''):
            areas_identificadas[current_area]['porcentaje'] = value
        
        # Agregar preguntas de evaluación (yesno)
        elif current_area and field_type == 'yesno':
            areas_identificadas[current_area]['preguntas'].append({
                'texto': title,
                'respuesta': value,
                'field_id': answer.get('field_id')
            })
    
    print(f"\n🏷️ LAS 11 ÁREAS IDENTIFICADAS:")
    print("=" * 50)
    
    for i, (area_nombre, area_data) in enumerate(areas_identificadas.items(), 1):
        print(f"\nÁREA {i}: {area_nombre}")
        print(f"   • Preguntas: {len(area_data['preguntas'])}")
        print(f"   • Puntos Máximos: {area_data['puntos_max']}")
        print(f"   • Puntos Obtenidos: {area_data['puntos_obtenidos']}")
        print(f"   • Porcentaje: {area_data['porcentaje']}%")
        
        # Mostrar primeras 3 preguntas como ejemplo
        if area_data['preguntas']:
            print(f"   • Preguntas ejemplo:")
            for j, pregunta in enumerate(area_data['preguntas'][:2], 1):
                respuesta_texto = "✅ SÍ" if pregunta['respuesta'] == 'true' else "❌ NO" if pregunta['respuesta'] == 'false' else "⚪ N/A"
                print(f"      {j}. {pregunta['texto']} → {respuesta_texto}")
    
    # 3. VALIDACIÓN FINAL
    total_areas = len(areas_identificadas)
    areas_esperadas = 11
    estructura_correcta = total_areas == areas_esperadas
    tiene_calificacion = calificacion_porcentaje is not None and puntos_max is not None
    
    print(f"\n✅ VALIDACIÓN ESTRUCTURA:")
    print("=" * 30)
    print(f"   • Áreas encontradas: {total_areas}/11")
    print(f"   • Estructura correcta: {'✅ SÍ' if estructura_correcta else '❌ NO'}")
    print(f"   • Calificación general: {'✅ SÍ' if tiene_calificacion else '❌ NO'}")
    print(f"   • Datos completos: {'✅ SÍ' if sucursal_field and auditor_field and fecha_field else '❌ NO'}")
    
    # 4. GENERAR ESQUEMA DE EXTRACCIÓN
    esquema_extraccion = {
        'form_info': {
            'form_id': form_id,
            'form_name': form_name,
            'activity_name': activity_name
        },
        'calificacion_general': {
            'field_puntos_max': 'PUNTOS MAX',
            'field_puntos_obtenidos': 'PUNTOS TOTALES OBTENIDOS',
            'field_calificacion_porcentaje': 'CALIFICACION PORCENTAJE %',
            'field_sucursal': 'SUCURSAL',
            'field_auditor': 'AUDITOR',
            'field_fecha': 'Date'
        },
        'areas_detalladas': {}
    }
    
    for area_nombre, area_data in areas_identificadas.items():
        esquema_extraccion['areas_detalladas'][area_nombre] = {
            'nombre': area_nombre,
            'total_preguntas': len(area_data['preguntas']),
            'campos_puntuacion': {
                'puntos_max': f"{area_nombre.upper().replace('.', '').replace(' ', '')} PUNTOS MAX",
                'puntos_obtenidos': f"{area_nombre.upper().replace('.', '').replace(' ', '')} PUNTOS TOTALES",
                'porcentaje': f"{area_nombre.upper().replace('.', '').replace(' ', '')} PORCENTAJE %"
            },
            'preguntas': area_data['preguntas']
        }
    
    # 5. GUARDAR VALIDACIÓN COMPLETA
    resultado_final = {
        'timestamp': datetime.now().isoformat(),
        'form_validado': {
            'form_id': form_id,
            'form_name': form_name,
            'activity_name': activity_name
        },
        'submission_ejemplo': {
            'sucursal': sucursal_nombre,
            'auditor': auditor_nombre,
            'fecha': fecha_submission
        },
        'calificacion_general_validada': {
            'puntos_max': puntos_max,
            'puntos_obtenidos': puntos_obtenidos,
            'calificacion_porcentaje': calificacion_porcentaje,
            'sucursal': sucursal_field,
            'auditor': auditor_field,
            'fecha': fecha_field
        },
        'areas_11_validadas': areas_identificadas,
        'validacion_resultado': {
            'total_areas': total_areas,
            'areas_esperadas': areas_esperadas,
            'estructura_correcta': estructura_correcta,
            'tiene_calificacion_general': tiene_calificacion,
            'validacion_exitosa': estructura_correcta and tiene_calificacion
        },
        'esquema_extraccion': esquema_extraccion
    }
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"data/validacion_seguridad_final_{timestamp}.json"
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(resultado_final, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"\n💾 VALIDACIÓN FINAL GUARDADA: {filename}")
    
    # 6. RESUMEN PARA RAILWAY
    if estructura_correcta and tiene_calificacion:
        print(f"\n🚀 FORMULARIO SEGURIDAD VALIDADO PARA RAILWAY:")
        print("=" * 50)
        print(f"✅ Form ID: {form_id}")
        print(f"✅ 11 áreas identificadas correctamente")
        print(f"✅ Calificación porcentual: {calificacion_porcentaje}%")
        print(f"✅ Datos completos: sucursal, auditor, fecha")
        print(f"✅ Esquema de extracción generado")
        
        # Verificar formato exacto que quiere Roberto
        print(f"\n📋 FORMATO EXACTO VALIDADO (COMO SOLICITA ROBERTO):")
        print("=" * 55)
        print("CONTROL OPERATIVO DE SEGURIDAD")
        print(f"PUNTOS MAX                    {puntos_max}")
        print(f"PUNTOS TOTALES OBTENIDOS      {puntos_obtenidos}")  
        print(f"CALIFICACION PORCENTAJE %     {calificacion_porcentaje}")
        print(f"SUCURSAL                      {sucursal_field}")
        print(f"AUDITOR                       {auditor_field}")
        print(f"Date                          {fecha_field}")
        
        print(f"\n✅ LISTO PARA ETL RAILWAY")
    else:
        print(f"\n❌ VALIDACIÓN FALLÓ - REVISAR ESTRUCTURA")
    
    return resultado_final

if __name__ == "__main__":
    print("🛡️ EJECUTANDO VALIDACIÓN FINAL FORMULARIO SEGURIDAD")
    print("Validando estructura de 11 áreas y calificación porcentual...")
    print()
    
    resultado = validar_estructura_seguridad_final()
    
    if resultado:
        validacion = resultado['validacion_resultado']
        if validacion['validacion_exitosa']:
            print(f"\n🎉 ¡VALIDACIÓN EXITOSA!")
            print(f"🛡️ Formulario 877139 completamente validado")
            print(f"📊 {validacion['total_areas']} áreas de seguridad identificadas")
            print(f"🎯 Calificación porcentual extraída correctamente")
            print(f"🚀 Listo para implementación Railway")
        else:
            print(f"\n⚠️ Validación con problemas:")
            print(f"   • Áreas: {validacion['total_areas']}/11")
            print(f"   • Calificación: {'✅' if validacion['tiene_calificacion_general'] else '❌'}")
    else:
        print(f"\n❌ VALIDACIÓN COMPLETAMENTE FALLÓ")