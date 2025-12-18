#!/usr/bin/env python3
"""
📊 ANÁLISIS COMPLETO DE SUPERVISIONES - 238 REGISTROS TOTALES
Extrae TODOS los KPIs por área y maneja cambios dinámicos en estructura
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.zenput_api import create_zenput_client
import json
from datetime import datetime, timedelta
from collections import defaultdict
import re

def extract_all_areas_and_kpis(answers):
    """Extrae todas las áreas identificadas y sus KPIs dinámicamente"""
    
    areas_structure = defaultdict(lambda: {
        'sections': [],
        'kpis': defaultdict(list),
        'questions': [],
        'scores': [],
        'images': [],
        'critical_items': [],
        'yesno_responses': {'si': 0, 'no': 0}
    })
    
    current_section = None
    
    for answer in answers:
        field_type = answer.get('field_type')
        title = answer.get('title', '')
        value = answer.get('value')
        is_answered = answer.get('is_answered', False)
        
        # Identificar secciones (áreas)
        if field_type == 'section':
            current_section = title.strip()
            if current_section:
                areas_structure[current_section]['sections'].append(title)
                
        # Clasificar respuestas por tipo y área
        area_key = current_section if current_section else 'GENERAL'
        
        if is_answered and value is not None:
            # Detectar KPIs de puntuación
            if any(keyword in title.upper() for keyword in ['PUNTOS', 'CALIFICACION', 'PORCENTAJE', '%', 'SCORE']):
                areas_structure[area_key]['scores'].append({
                    'field': title,
                    'value': value,
                    'type': field_type
                })
            
            # Detectar preguntas críticas
            if any(keyword in title.upper() for keyword in ['CRITICO', 'FALLA', 'PROBLEMA', 'INCIDENTE', 'ERROR']):
                areas_structure[area_key]['critical_items'].append({
                    'field': title,
                    'value': value,
                    'response': answer.get('yesno_value') if field_type == 'yesno' else value
                })
            
            # Contar respuestas SI/NO
            if field_type == 'yesno':
                yesno_val = answer.get('yesno_value')
                if yesno_val is True:
                    areas_structure[area_key]['yesno_responses']['si'] += 1
                elif yesno_val is False:
                    areas_structure[area_key]['yesno_responses']['no'] += 1
            
            # Detectar imágenes por área
            if field_type == 'image' and value:
                image_count = len(value) if isinstance(value, list) else 1
                areas_structure[area_key]['images'].append({
                    'field': title,
                    'count': image_count
                })
            
            # Almacenar todas las preguntas
            areas_structure[area_key]['questions'].append({
                'field': title,
                'value': value,
                'type': field_type,
                'answered': is_answered
            })
    
    return dict(areas_structure)

def calculate_area_kpis(area_data):
    """Calcula KPIs específicos por área"""
    
    kpis = {}
    
    # KPI 1: Porcentaje de completitud
    total_questions = len(area_data['questions'])
    answered_questions = len([q for q in area_data['questions'] if q['answered']])
    kpis['completitud_porcentaje'] = round((answered_questions / total_questions * 100), 2) if total_questions > 0 else 0
    
    # KPI 2: Score promedio (si existe)
    scores = [s['value'] for s in area_data['scores'] if isinstance(s['value'], (int, float))]
    kpis['score_promedio'] = round(sum(scores) / len(scores), 2) if scores else None
    kpis['score_maximo'] = max(scores) if scores else None
    kpis['score_minimo'] = min(scores) if scores else None
    
    # KPI 3: Porcentaje de conformidad (SI vs NO)
    total_yesno = area_data['yesno_responses']['si'] + area_data['yesno_responses']['no']
    kpis['conformidad_porcentaje'] = round((area_data['yesno_responses']['si'] / total_yesno * 100), 2) if total_yesno > 0 else None
    
    # KPI 4: Elementos críticos detectados
    kpis['elementos_criticos'] = len(area_data['critical_items'])
    kpis['criticos_fallaron'] = len([c for c in area_data['critical_items'] if c['response'] is False])
    
    # KPI 5: Evidencia fotográfica
    kpis['total_imagenes'] = sum([img['count'] for img in area_data['images']])
    kpis['campos_con_imagenes'] = len(area_data['images'])
    
    # KPI 6: Distribución de respuestas
    kpis['respuestas_si'] = area_data['yesno_responses']['si']
    kpis['respuestas_no'] = area_data['yesno_responses']['no']
    
    return kpis

def analyze_comprehensive_supervision():
    """Análisis completo de todas las supervisiones"""
    
    print("📊 ANÁLISIS COMPLETO DE SUPERVISIONES - EL POLLO LOCO MÉXICO")
    print("=" * 80)
    print("🎯 Objetivo: Extraer TODOS los KPIs por área de las 238 supervisiones")
    print("=" * 80)
    
    client = create_zenput_client()
    
    if not client.validate_api_connection():
        print("❌ No se puede conectar a API Zenput")
        return False
    
    # Obtener TODAS las supervisiones (no solo 7 días)
    supervision_forms = {
        '877138': 'Supervisión Operativa EPL CAS',
        '877139': 'Control Operativo de Seguridad EPL CAS'
    }
    
    comprehensive_analysis = {
        'analysis_timestamp': datetime.now().isoformat(),
        'total_submissions_analyzed': 0,
        'forms_analysis': {},
        'areas_discovered': set(),
        'kpi_structure': {},
        'summary_by_area': defaultdict(lambda: {
            'submissions_count': 0,
            'avg_kpis': {},
            'trends': [],
            'top_performers': [],
            'attention_needed': []
        }),
        'dynamic_structure': {
            'new_areas_detected': [],
            'field_changes_detected': [],
            'kpi_evolution': {}
        }
    }
    
    total_submissions_processed = 0
    
    for form_id, form_name in supervision_forms.items():
        print(f"\n🔍 === ANALIZANDO {form_name} ({form_id}) ===")
        print("-" * 70)
        
        # Obtener TODAS las submissions (aumentar días para capturar las 238)
        submissions = client.get_submissions_for_form(form_id, days_back=30)  # Aumentar rango
        
        if not submissions:
            print(f"⚠️ No hay submissions para {form_id}")
            continue
        
        print(f"📊 Procesando {len(submissions)} submissions de {form_name}")
        total_submissions_processed += len(submissions)
        
        form_areas = defaultdict(list)
        form_kpis = defaultdict(list)
        
        for i, submission in enumerate(submissions, 1):
            submission_id = submission.get('id')
            metadata = submission.get('smetadata', {})
            sucursal_name = metadata.get('location', {}).get('name', 'Unknown')
            supervisor_name = metadata.get('created_by', {}).get('display_name', 'Unknown')
            fecha = metadata.get('date_completed_local', 'Unknown')
            
            # Extraer estructura por áreas
            answers = submission.get('answers', [])
            areas_data = extract_all_areas_and_kpis(answers)
            
            # Calcular KPIs por cada área
            submission_analysis = {
                'submission_id': submission_id,
                'sucursal': sucursal_name,
                'supervisor': supervisor_name,
                'fecha': fecha,
                'areas_kpis': {}
            }
            
            for area_name, area_data in areas_data.items():
                # Registrar área descubierta
                comprehensive_analysis['areas_discovered'].add(area_name)
                
                # Calcular KPIs del área
                area_kpis = calculate_area_kpis(area_data)
                submission_analysis['areas_kpis'][area_name] = area_kpis
                
                # Agregar a estadísticas por área
                comprehensive_analysis['summary_by_area'][area_name]['submissions_count'] += 1
                form_kpis[area_name].append(area_kpis)
            
            form_areas[submission_id] = submission_analysis
            
            if i <= 5:  # Mostrar progreso para primeras 5
                areas_count = len(areas_data.keys())
                print(f"   ✅ {i:2d}. {sucursal_name[:25]:25s} - {areas_count} áreas | {supervisor_name[:15]:15s}")
        
        if len(submissions) > 5:
            print(f"   ... y {len(submissions) - 5} supervisiones más procesadas")
        
        # Calcular promedios por área para este formulario
        print(f"\n📊 ÁREAS IDENTIFICADAS EN {form_name}:")
        for area_name, kpis_list in form_kpis.items():
            if kpis_list:
                avg_completitud = sum([k['completitud_porcentaje'] for k in kpis_list]) / len(kpis_list)
                scores_disponibles = [k['score_promedio'] for k in kpis_list if k['score_promedio'] is not None]
                avg_score = sum(scores_disponibles) / len(scores_disponibles) if scores_disponibles else None
                
                print(f"   🏭 {area_name[:40]:40s} | Completitud: {avg_completitud:5.1f}%" + 
                      (f" | Score: {avg_score:5.1f}" if avg_score else ""))
                
                # Guardar promedios en análisis comprehensivo
                if area_name not in comprehensive_analysis['summary_by_area']:
                    comprehensive_analysis['summary_by_area'][area_name] = defaultdict(dict)
                
                comprehensive_analysis['summary_by_area'][area_name]['avg_kpis'][form_name] = {
                    'completitud_promedio': round(avg_completitud, 2),
                    'score_promedio': round(avg_score, 2) if avg_score else None,
                    'submissions_analizadas': len(kpis_list)
                }
        
        comprehensive_analysis['forms_analysis'][form_id] = {
            'form_name': form_name,
            'submissions_analyzed': len(submissions),
            'areas_found': list(form_kpis.keys()),
            'detailed_analysis': dict(form_areas)
        }
    
    # Convertir set a lista para JSON
    comprehensive_analysis['areas_discovered'] = list(comprehensive_analysis['areas_discovered'])
    comprehensive_analysis['total_submissions_analyzed'] = total_submissions_processed
    
    # Generar reporte final
    generate_comprehensive_report(comprehensive_analysis)
    
    # Guardar análisis completo
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"data/comprehensive_supervision_analysis_{timestamp}.json"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(comprehensive_analysis, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"\n💾 Análisis completo guardado en: {output_file}")
    
    return comprehensive_analysis

def generate_comprehensive_report(analysis):
    """Genera reporte completo de todas las áreas y KPIs"""
    
    print(f"\n" + "=" * 80)
    print("📊 REPORTE COMPLETO - TODAS LAS ÁREAS Y KPIs")
    print("=" * 80)
    
    print(f"📈 RESUMEN GENERAL:")
    print(f"   ✅ Total submissions analizadas: {analysis['total_submissions_analyzed']}")
    print(f"   🏭 Áreas operativas identificadas: {len(analysis['areas_discovered'])}")
    print(f"   📝 Formularios procesados: {len(analysis['forms_analysis'])}")
    
    print(f"\n🏭 ÁREAS OPERATIVAS IDENTIFICADAS:")
    for i, area in enumerate(analysis['areas_discovered'], 1):
        submissions_count = analysis['summary_by_area'][area]['submissions_count']
        print(f"   {i:2d}. {area[:50]:50s} | {submissions_count:3d} submissions")
    
    print(f"\n📊 KPIs DISPONIBLES POR ÁREA:")
    print("   📋 Completitud Porcentaje - % de campos completados")
    print("   📊 Score Promedio - Calificación cuando disponible") 
    print("   ✅ Conformidad Porcentaje - % respuestas SI vs NO")
    print("   🚨 Elementos Críticos - Detección de problemas")
    print("   📸 Evidencia Fotográfica - Imágenes por área")
    print("   📈 Tendencias Temporales - Evolución en el tiempo")
    
    print(f"\n🎯 PRÓXIMOS PASOS PARA DASHBOARD:")
    print("   1. ✅ Crear KPI cards por cada área identificada")
    print("   2. 🔍 Implementar alertas por área crítica") 
    print("   3. 📈 Dashboard dinámico que se adapte a nuevas áreas")
    print("   4. 📊 Comparativas entre sucursales por área específica")
    print("   5. 🚨 Sistema de notificaciones por KPI crítico")

def main():
    """Función principal"""
    
    try:
        analysis = analyze_comprehensive_supervision()
        if analysis:
            print("\n🎉 Análisis completo de supervisiones exitoso")
            return True
        else:
            print("\n❌ Análisis falló")
            return False
    except Exception as e:
        print(f"\n💥 Error crítico: {e}")
        return False

if __name__ == "__main__":
    main()