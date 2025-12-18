#!/usr/bin/env python3
"""
🔍 VALIDACIÓN DETALLADA FORM SEGURIDAD 877139
Valida estructura exacta del formulario de seguridad y sus 11 áreas
"""

import requests
import json
from datetime import datetime

def validar_form_seguridad_detallado():
    """Valida estructura detallada del formulario de seguridad 877139"""
    
    print("🔍 VALIDACIÓN DETALLADA FORMULARIO SEGURIDAD 877139")
    print("=" * 65)
    
    # Configuración API
    api_token = "cb908e0d4e0f5501c635325c611db314"
    headers = {
        'X-API-TOKEN': api_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    base_url = 'https://www.zenput.com/api/v3'
    
    # 1. OBTENER ESTRUCTURA DEL FORM 877139
    print("\n📋 CONSULTANDO ESTRUCTURA FORM 877139 (SEGURIDAD)...")
    
    try:
        form_url = f"{base_url}/forms/877139"
        form_response = requests.get(form_url, headers=headers, timeout=30)
        
        if form_response.status_code == 200:
            form_data = form_response.json()
            print(f"✅ Form 877139 consultado exitosamente")
            
            # Analizar estructura del form
            form_info = form_data.get('data', {})
            form_name = form_info.get('name', 'N/A')
            form_sections = form_info.get('sections', [])
            
            print(f"\n📝 INFORMACIÓN FORM:")
            print(f"   • Nombre: {form_name}")
            print(f"   • ID: 877139")
            print(f"   • Secciones: {len(form_sections)}")
            
            # Analizar cada sección (las 11 áreas)
            print(f"\n🔍 ANÁLISIS DETALLADO DE LAS 11 ÁREAS:")
            print("=" * 60)
            
            for i, section in enumerate(form_sections, 1):
                section_name = section.get('name', f'Sección {i}')
                section_id = section.get('id', 'N/A')
                questions = section.get('questions', [])
                
                print(f"\n🏷️  ÁREA {i}: {section_name}")
                print(f"   • ID Sección: {section_id}")
                print(f"   • Preguntas: {len(questions)}")
                
                # Analizar preguntas de cada área
                if questions:
                    for j, question in enumerate(questions[:3], 1):  # Mostrar primeras 3
                        q_text = question.get('text', 'Sin texto')[:100]
                        q_type = question.get('question_type', 'N/A')
                        q_id = question.get('id', 'N/A')
                        
                        print(f"      {j}. {q_text}... (Tipo: {q_type}, ID: {q_id})")
                    
                    if len(questions) > 3:
                        print(f"      ... y {len(questions) - 3} preguntas más")
            
            # Buscar información de scoring/calificación
            print(f"\n🎯 BÚSQUEDA DE SISTEMA DE CALIFICACIÓN:")
            print("-" * 45)
            
            scoring_info = form_info.get('scoring', {})
            if scoring_info:
                print(f"✅ Sistema de scoring encontrado:")
                print(f"   • Scoring: {scoring_info}")
            else:
                print("⚠️ No se encontró información de scoring en metadatos")
            
        else:
            print(f"❌ Error al consultar form: {form_response.status_code}")
            return None
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return None
    
    # 2. OBTENER SUBMISSION RECIENTE PARA ANALIZAR ESTRUCTURA
    print("\n📊 CONSULTANDO SUBMISSION RECIENTE PARA ANÁLISIS...")
    
    try:
        # Buscar submissions del form 877139
        submissions_url = f"{base_url}/forms/877139/submissions"
        params = {
            'limit': 5,  # Solo necesitamos unas pocas para análisis
            'offset': 0
        }
        
        submissions_response = requests.get(submissions_url, headers=headers, params=params, timeout=30)
        
        if submissions_response.status_code == 200:
            submissions_data = submissions_response.json()
            submissions_list = submissions_data.get('data', [])
            
            print(f"✅ {len(submissions_list)} submissions encontradas")
            
            if submissions_list:
                # Analizar la submission más reciente
                submission = submissions_list[0]
                
                submission_id = submission.get('id')
                sucursal_name = submission.get('location', {}).get('name', 'N/A')
                submitted_date = submission.get('submitted_at', 'N/A')
                score = submission.get('score', 'N/A')
                
                print(f"\n🔍 ANÁLISIS SUBMISSION MÁS RECIENTE:")
                print(f"   • ID: {submission_id}")
                print(f"   • Sucursal: {sucursal_name}")
                print(f"   • Fecha: {submitted_date}")
                print(f"   • Score: {score}")
                
                # Analizar respuestas para encontrar calificaciones
                answers = submission.get('answers', [])
                print(f"   • Respuestas: {len(answers)}")
                
                # Buscar patrones de calificación
                print(f"\n🎯 ANÁLISIS DE CALIFICACIONES POR ÁREA:")
                print("=" * 50)
                
                area_scores = {}
                total_points = 0
                max_points = 0
                
                for answer in answers:
                    question_info = answer.get('question', {})
                    question_text = question_info.get('text', '')
                    question_id = question_info.get('id')
                    answer_value = answer.get('answer')
                    question_score = answer.get('score', 0)
                    question_max_score = answer.get('max_score', 0)
                    
                    # Buscar información de sección/área
                    section_info = question_info.get('section', {})
                    section_name = section_info.get('name', 'Sin sección')
                    
                    if section_name not in area_scores:
                        area_scores[section_name] = {
                            'puntos_obtenidos': 0,
                            'puntos_maximos': 0,
                            'preguntas': 0
                        }
                    
                    if question_score is not None:
                        area_scores[section_name]['puntos_obtenidos'] += float(question_score)
                        total_points += float(question_score)
                    
                    if question_max_score is not None:
                        area_scores[section_name]['puntos_maximos'] += float(question_max_score)
                        max_points += float(question_max_score)
                    
                    area_scores[section_name]['preguntas'] += 1
                
                # Mostrar resumen por área
                print(f"\n📊 CALIFICACIONES POR ÁREA (11 ÁREAS):")
                print("-" * 55)
                
                for i, (area_name, area_data) in enumerate(area_scores.items(), 1):
                    puntos_obtenidos = area_data['puntos_obtenidos']
                    puntos_maximos = area_data['puntos_maximos']
                    porcentaje = (puntos_obtenidos / puntos_maximos * 100) if puntos_maximos > 0 else 0
                    
                    print(f"ÁREA {i}: {area_name}")
                    print(f"   • Puntos Obtenidos: {puntos_obtenidos:.2f}")
                    print(f"   • Puntos Máximos: {puntos_maximos:.2f}")
                    print(f"   • Porcentaje: {porcentaje:.2f}%")
                    print(f"   • Preguntas: {area_data['preguntas']}")
                    print()
                
                # Mostrar CALIFICACIÓN GENERAL como en el formato
                calificacion_porcentaje = (total_points / max_points * 100) if max_points > 0 else 0
                
                print(f"🎯 CALIFICACIÓN GENERAL (FORMATO REQUERIDO):")
                print("=" * 50)
                print(f"CONTROL OPERATIVO DE SEGURIDAD")
                print(f"PUNTOS MAX                {max_points:.0f}")
                print(f"PUNTOS TOTALES OBTENIDOS  {total_points:.0f}")
                print(f"CALIFICACION PORCENTAJE % {calificacion_porcentaje:.2f}")
                print(f"SUCURSAL                  {sucursal_name}")
                print(f"AUDITOR                   [Extraer de metadatos]")
                print(f"Date                      {submitted_date}")
                
                # Guardar análisis detallado
                resultado_analisis = {
                    'timestamp': datetime.now().isoformat(),
                    'form_id': 877139,
                    'form_name': form_name,
                    'total_secciones': len(form_sections),
                    'estructura_form': form_data,
                    'submission_analizada': {
                        'id': submission_id,
                        'sucursal': sucursal_name,
                        'fecha': submitted_date,
                        'puntos_totales_obtenidos': total_points,
                        'puntos_maximos': max_points,
                        'calificacion_porcentaje': calificacion_porcentaje
                    },
                    'areas_detalladas': area_scores,
                    'validacion': {
                        'areas_encontradas': len(area_scores),
                        'areas_esperadas': 11,
                        'coincide_estructura': len(area_scores) == 11,
                        'tiene_calificacion_general': max_points > 0
                    }
                }
                
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"data/validacion_form_seguridad_{timestamp}.json"
                
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(resultado_analisis, f, indent=2, ensure_ascii=False, default=str)
                
                print(f"\n💾 ANÁLISIS GUARDADO: {filename}")
                
                return resultado_analisis
            else:
                print("⚠️ No se encontraron submissions para analizar")
                return None
                
        else:
            print(f"❌ Error al consultar submissions: {submissions_response.status_code}")
            return None
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

if __name__ == "__main__":
    print("🔍 EJECUTANDO VALIDACIÓN DETALLADA FORM SEGURIDAD")
    print("Validando estructura de 11 áreas y calificación porcentual...")
    print()
    
    resultado = validar_form_seguridad_detallado()
    
    if resultado:
        print(f"\n✅ VALIDACIÓN COMPLETADA")
        print(f"📊 Estructura form analizada y calificaciones verificadas")
        
        # Resumen de validación
        validacion = resultado['validacion']
        print(f"\n🎯 RESUMEN VALIDACIÓN:")
        print(f"   • Áreas encontradas: {validacion['areas_encontradas']}/11")
        print(f"   • Estructura correcta: {'✅' if validacion['coincide_estructura'] else '❌'}")
        print(f"   • Calificación general: {'✅' if validacion['tiene_calificacion_general'] else '❌'}")
    else:
        print(f"\n❌ VALIDACIÓN FALLÓ")
        print(f"No se pudo analizar la estructura del formulario")