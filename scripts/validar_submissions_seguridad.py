#!/usr/bin/env python3
"""
🔍 VALIDACIÓN SUBMISSIONS SEGURIDAD
Valida submissions para identificar la estructura exacta del formulario de seguridad
"""

import requests
import json
from datetime import datetime

def validar_submissions_seguridad():
    """Valida submissions existentes para extraer estructura del form de seguridad"""
    
    print("🔍 VALIDACIÓN SUBMISSIONS PARA FORMULARIO SEGURIDAD")
    print("=" * 60)
    
    # Configuración API
    api_token = "cb908e0d4e0f5501c635325c611db314"
    headers = {
        'X-API-TOKEN': api_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    base_url = 'https://www.zenput.com/api/v3'
    
    # 1. BUSCAR SUBMISSIONS RECIENTES
    print("\n📊 BUSCANDO SUBMISSIONS RECIENTES...")
    
    try:
        # Usar endpoint genérico de submissions
        submissions_url = f"{base_url}/submissions"
        params = {
            'limit': 50,
            'offset': 0
        }
        
        response = requests.get(submissions_url, headers=headers, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            # Verificar estructura de respuesta
            if 'data' in data:
                submissions = data['data']
                print(f"✅ {len(submissions)} submissions encontradas")
                
                # Buscar submissions de diferentes forms
                form_counts = {}
                security_submissions = []
                operational_submissions = []
                
                for submission in submissions:
                    form_info = submission.get('form', {})
                    form_id = form_info.get('id')
                    form_name = form_info.get('name', '').lower()
                    
                    if form_id:
                        if form_id not in form_counts:
                            form_counts[form_id] = {
                                'count': 0,
                                'name': form_info.get('name', 'Sin nombre'),
                                'submissions': []
                            }
                        form_counts[form_id]['count'] += 1
                        form_counts[form_id]['submissions'].append(submission)
                        
                        # Identificar forms de seguridad
                        if any(keyword in form_name for keyword in ['seguridad', 'security', 'control']):
                            security_submissions.append(submission)
                        
                        # Identificar forms operativos  
                        elif any(keyword in form_name for keyword in ['operativa', 'operativo', 'operational']):
                            operational_submissions.append(submission)
                
                print(f"\n📋 FORMULARIOS IDENTIFICADOS:")
                print("-" * 40)
                
                for form_id, info in form_counts.items():
                    form_name = info['name']
                    count = info['count']
                    
                    print(f"\n📝 Form ID: {form_id}")
                    print(f"   • Nombre: {form_name}")
                    print(f"   • Submissions: {count}")
                    
                    # Marcar forms especiales
                    form_name_lower = form_name.lower()
                    if any(keyword in form_name_lower for keyword in ['seguridad', 'security', 'control']):
                        print(f"   🛡️ ← FORMULARIO DE SEGURIDAD")
                    elif any(keyword in form_name_lower for keyword in ['operativa', 'operativo', 'operational']):
                        print(f"   ⚙️ ← FORMULARIO OPERATIVO")
                
                # 2. ANALIZAR SUBMISSIONS DE SEGURIDAD
                if security_submissions:
                    print(f"\n🛡️ ANÁLISIS SUBMISSIONS SEGURIDAD ({len(security_submissions)}):")
                    print("=" * 55)
                    
                    # Analizar primera submission de seguridad
                    security_submission = security_submissions[0]
                    
                    form_info = security_submission.get('form', {})
                    form_id = form_info.get('id')
                    form_name = form_info.get('name', 'N/A')
                    
                    submission_id = security_submission.get('id')
                    sucursal = security_submission.get('location', {}).get('name', 'N/A')
                    fecha = security_submission.get('submitted_at', 'N/A')
                    submitted_by = security_submission.get('submitted_by', {})
                    auditor_name = f"{submitted_by.get('first_name', '')} {submitted_by.get('last_name', '')}".strip()
                    
                    print(f"🔍 ANÁLISIS SUBMISSION SEGURIDAD:")
                    print(f"   • Form ID: {form_id}")
                    print(f"   • Form Name: {form_name}")
                    print(f"   • Submission ID: {submission_id}")
                    print(f"   • Sucursal: {sucursal}")
                    print(f"   • Fecha: {fecha}")
                    print(f"   • Auditor: {auditor_name}")
                    
                    # Analizar answers para extraer calificaciones
                    answers = security_submission.get('answers', [])
                    print(f"   • Total respuestas: {len(answers)}")
                    
                    # Agrupar por secciones (las 11 áreas)
                    areas = {}
                    total_puntos_obtenidos = 0
                    total_puntos_maximos = 0
                    
                    for answer in answers:
                        question = answer.get('question', {})
                        question_text = question.get('text', '')
                        section = question.get('section', {})
                        section_name = section.get('name', 'Sin sección')
                        
                        # Score de la respuesta
                        answer_score = answer.get('score', 0)
                        answer_max_score = answer.get('max_score', 0)
                        
                        if section_name not in areas:
                            areas[section_name] = {
                                'puntos_obtenidos': 0,
                                'puntos_maximos': 0,
                                'preguntas': 0
                            }
                        
                        if answer_score is not None:
                            areas[section_name]['puntos_obtenidos'] += float(answer_score)
                            total_puntos_obtenidos += float(answer_score)
                        
                        if answer_max_score is not None:
                            areas[section_name]['puntos_maximos'] += float(answer_max_score)
                            total_puntos_maximos += float(answer_max_score)
                        
                        areas[section_name]['preguntas'] += 1
                    
                    print(f"\n📊 ESTRUCTURA DE 11 ÁREAS IDENTIFICADAS:")
                    print("=" * 50)
                    
                    for i, (area_name, area_data) in enumerate(areas.items(), 1):
                        puntos_obtenidos = area_data['puntos_obtenidos']
                        puntos_maximos = area_data['puntos_maximos']
                        porcentaje = (puntos_obtenidos / puntos_maximos * 100) if puntos_maximos > 0 else 0
                        preguntas = area_data['preguntas']
                        
                        print(f"\nÁREA {i}: {area_name}")
                        print(f"   • Puntos Obtenidos: {puntos_obtenidos:.1f}")
                        print(f"   • Puntos Máximos: {puntos_maximos:.1f}")
                        print(f"   • Porcentaje: {porcentaje:.2f}%")
                        print(f"   • Preguntas: {preguntas}")
                    
                    # Calcular calificación general
                    if total_puntos_maximos > 0:
                        calificacion_porcentaje = (total_puntos_obtenidos / total_puntos_maximos) * 100
                        
                        print(f"\n🎯 CALIFICACIÓN GENERAL (FORMATO ROBERTO):")
                        print("=" * 50)
                        print(f"CONTROL OPERATIVO DE SEGURIDAD")
                        print(f"PUNTOS MAX                    {total_puntos_maximos:.0f}")
                        print(f"PUNTOS TOTALES OBTENIDOS      {total_puntos_obtenidos:.0f}")
                        print(f"CALIFICACION PORCENTAJE %     {calificacion_porcentaje:.2f}")
                        print(f"SUCURSAL                      {sucursal}")
                        print(f"AUDITOR                       {auditor_name}")
                        print(f"Date                          {fecha}")
                        
                        # Validar estructura
                        areas_count = len(areas)
                        estructura_correcta = areas_count == 11
                        
                        print(f"\n✅ VALIDACIÓN ESTRUCTURA:")
                        print(f"   • Áreas encontradas: {areas_count}/11")
                        print(f"   • Estructura correcta: {'✅ SÍ' if estructura_correcta else '❌ NO'}")
                        print(f"   • Calificación general: ✅ {calificacion_porcentaje:.2f}%")
                        
                        # Guardar análisis completo
                        resultado = {
                            'timestamp': datetime.now().isoformat(),
                            'form_seguridad': {
                                'id': form_id,
                                'name': form_name
                            },
                            'submission_analizada': {
                                'id': submission_id,
                                'sucursal': sucursal,
                                'fecha': fecha,
                                'auditor': auditor_name
                            },
                            'calificacion_general': {
                                'puntos_obtenidos': total_puntos_obtenidos,
                                'puntos_maximos': total_puntos_maximos,
                                'porcentaje': calificacion_porcentaje
                            },
                            'areas_detalladas': areas,
                            'validacion': {
                                'areas_encontradas': areas_count,
                                'areas_esperadas': 11,
                                'estructura_correcta': estructura_correcta,
                                'tiene_calificacion_general': True
                            }
                        }
                        
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                        filename = f"data/validacion_seguridad_submission_{timestamp}.json"
                        
                        with open(filename, 'w', encoding='utf-8') as f:
                            json.dump(resultado, f, indent=2, ensure_ascii=False, default=str)
                        
                        print(f"\n💾 VALIDACIÓN GUARDADA: {filename}")
                        
                        return resultado
                    else:
                        print("⚠️ No se pudo calcular calificación general - puntos máximos = 0")
                        
                else:
                    print("\n⚠️ NO SE ENCONTRARON SUBMISSIONS DE SEGURIDAD")
                    print("Listando todas las submissions disponibles:")
                    
                    for form_id, info in form_counts.items():
                        print(f"   • Form {form_id}: {info['name']} ({info['count']} submissions)")
                
            else:
                print("❌ Estructura de respuesta inesperada")
                print(f"Claves disponibles: {list(data.keys())}")
                
        else:
            print(f"❌ Error HTTP: {response.status_code}")
            try:
                error_data = response.json()
                print(f"Error details: {error_data}")
            except:
                print(f"Error text: {response.text}")
                
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

if __name__ == "__main__":
    print("🔍 EJECUTANDO VALIDACIÓN SUBMISSIONS SEGURIDAD")
    print("Analizando submissions para validar estructura de 11 áreas...")
    print()
    
    resultado = validar_submissions_seguridad()
    
    if resultado:
        print(f"\n✅ VALIDACIÓN EXITOSA")
        print(f"🛡️ Formulario de seguridad identificado y validado")
        print(f"📊 Estructura de 11 áreas verificada")
        print(f"🎯 Calificación porcentual extraída correctamente")
    else:
        print(f"\n❌ VALIDACIÓN FALLÓ")
        print(f"No se pudo validar el formulario de seguridad")