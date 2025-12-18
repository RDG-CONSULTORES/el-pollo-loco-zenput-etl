#!/usr/bin/env python3
"""
🔍 ANÁLISIS DETALLADO DE FORMULARIOS DE SUPERVISIÓN
Analiza el contenido real de los formularios 877138 y 877139
Para diseñar dashboard con los campos correctos
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.zenput_api import create_zenput_client
import json
from datetime import datetime

def analyze_supervision_forms():
    """Analiza en detalle los formularios de supervisión"""
    
    print("🔍 ANÁLISIS DETALLADO DE FORMULARIOS DE SUPERVISIÓN")
    print("=" * 70)
    
    client = create_zenput_client()
    
    if not client.validate_api_connection():
        print("❌ No se puede conectar a API Zenput")
        return
    
    # Formularios de supervisión a analizar
    supervision_forms = {
        '877138': 'Supervisión Operativa EPL CAS',
        '877139': 'Control Operativo de Seguridad EPL CAS'
    }
    
    form_analysis = {}
    
    for form_id, form_name in supervision_forms.items():
        print(f"\n🔍 === ANALIZANDO {form_name} ({form_id}) ===")
        print("-" * 60)
        
        # Obtener submissions recientes para análisis
        submissions = client.get_submissions_for_form(form_id, days_back=7)
        
        if not submissions:
            print(f"❌ No se encontraron submissions para {form_id}")
            continue
        
        print(f"📊 Analizando {len(submissions)} submissions de los últimos 7 días")
        
        # Analizar estructura de una submission típica
        sample_submission = submissions[0]
        
        print(f"\n📋 ESTRUCTURA DE SUBMISSION:")
        print(f"   🆔 ID: {sample_submission.get('id')}")
        print(f"   📅 Fecha: {sample_submission.get('submitted_at')}")
        print(f"   🏪 Location: {sample_submission.get('location', {}).get('name')}")
        print(f"   👤 Usuario: {sample_submission.get('submitter', {}).get('display_name')}")
        
        # Debug: Mostrar estructura completa de la submission
        print(f"\n🔍 ESTRUCTURA RAW DE SUBMISSION:")
        print(f"   📊 Keys disponibles: {list(sample_submission.keys())}")
        
        # Analizar diferentes posibles ubicaciones de datos
        form_data = {}
        
        # Intentar diferentes ubicaciones de datos del formulario
        if 'form_data' in sample_submission:
            form_data = sample_submission.get('form_data', {})
            print(f"   ✅ Datos en 'form_data': {len(form_data)} campos")
        elif 'answers' in sample_submission:
            form_data = sample_submission.get('answers', {})
            print(f"   ✅ Datos en 'answers': {len(form_data)} campos")
        elif 'data' in sample_submission:
            form_data = sample_submission.get('data', {})
            print(f"   ✅ Datos en 'data': {len(form_data)} campos")
        else:
            print(f"   ⚠️ No se encontró estructura de datos conocida")
            # Mostrar muestra de la submission para diagnóstico
            print(f"   📄 Sample submission keys: {sample_submission.keys()}")
            if sample_submission:
                first_key = list(sample_submission.keys())[0] if sample_submission.keys() else None
                if first_key:
                    print(f"   📋 Ejemplo contenido '{first_key}': {sample_submission[first_key]}")
            
            # Usar la submission completa como form_data para análisis
            form_data = sample_submission
        
        if form_data:
            print(f"\n📝 CAMPOS DEL FORMULARIO ({len(form_data)} campos):")
            
            field_analysis = {}
            
            # Manejar tanto diccionarios como listas
            if isinstance(form_data, list):
                print(f"   📋 Datos en formato lista - analizando {len(form_data)} respuestas")
                
                for i, answer in enumerate(form_data[:10]):  # Solo primeras 10 para no saturar
                    if isinstance(answer, dict):
                        # Analizar estructura de respuesta
                        answer_keys = list(answer.keys())
                        question = answer.get('question', f'Pregunta {i+1}')
                        response = answer.get('response', answer.get('answer', 'Sin respuesta'))
                        
                        field_info = {
                            'type': type(response).__name__,
                            'sample_value': response,
                            'question': question,
                            'is_score': False,
                            'is_critical': False,
                            'dashboard_priority': 'low'
                        }
                        
                        # Detectar tipos de campos importantes basado en la pregunta
                        question_lower = str(question).lower()
                        if any(word in question_lower for word in ['score', 'puntuacion', 'calificacion', 'rating']):
                            field_info['is_score'] = True
                            field_info['dashboard_priority'] = 'high'
                        
                        if any(word in question_lower for word in ['critico', 'falla', 'problema', 'alerta', 'danger', 'incidente']):
                            field_info['is_critical'] = True
                            field_info['dashboard_priority'] = 'high'
                        
                        if any(word in question_lower for word in ['temperatura', 'limpieza', 'seguridad', 'calidad', 'higiene']):
                            field_info['dashboard_priority'] = 'medium'
                        
                        field_key = f"pregunta_{i+1}"
                        field_analysis[field_key] = field_info
                        
                        # Mostrar campo
                        priority_icon = "🔴" if field_info['is_critical'] else "🔥" if field_info['is_score'] else "📊" if field_info['dashboard_priority'] == 'medium' else "📋"
                        print(f"      {priority_icon} {question[:60]}: {str(response)[:40]}")
                    
                if len(form_data) > 10:
                    print(f"      ... y {len(form_data) - 10} preguntas más")
                
            elif isinstance(form_data, dict):
                # Análisis tradicional para diccionarios
                for field_key, field_value in form_data.items():
                    field_type = type(field_value).__name__
                    
                    # Analizar el campo en detalle
                    field_info = {
                        'type': field_type,
                        'sample_value': field_value,
                        'is_score': False,
                        'is_critical': False,
                        'dashboard_priority': 'low'
                    }
                    
                    # Detectar tipos de campos importantes
                    if field_key.lower().find('score') != -1 or field_key.lower().find('puntuacion') != -1:
                        field_info['is_score'] = True
                        field_info['dashboard_priority'] = 'high'
                    
                    if any(word in field_key.lower() for word in ['critico', 'falla', 'problema', 'alerta', 'danger']):
                        field_info['is_critical'] = True
                        field_info['dashboard_priority'] = 'high'
                    
                    if any(word in field_key.lower() for word in ['temperatura', 'limpieza', 'seguridad', 'calidad']):
                        field_info['dashboard_priority'] = 'medium'
                    
                    field_analysis[field_key] = field_info
                    
                    # Mostrar campo
                    priority_icon = "🔴" if field_info['is_critical'] else "🔥" if field_info['is_score'] else "📊" if field_info['dashboard_priority'] == 'medium' else "📋"
                    print(f"      {priority_icon} {field_key}: {field_type} = {str(field_value)[:50]}")
            
            else:
                print(f"   ⚠️ Tipo de datos no reconocido: {type(form_data)}")
                field_analysis = {}
            
            # Buscar patrones de scoring
            scoring_fields = [k for k, v in field_analysis.items() if v['is_score']]
            critical_fields = [k for k, v in field_analysis.items() if v['is_critical']]
            
            if scoring_fields:
                print(f"\n🎯 CAMPOS DE PUNTUACIÓN IDENTIFICADOS ({len(scoring_fields)}):")
                for field in scoring_fields:
                    value = form_data.get(field)
                    print(f"      📊 {field}: {value}")
            
            if critical_fields:
                print(f"\n🚨 CAMPOS CRÍTICOS IDENTIFICADOS ({len(critical_fields)}):")
                for field in critical_fields:
                    value = form_data.get(field)
                    print(f"      ⚠️ {field}: {value}")
        
        # Analizar tendencias en múltiples submissions
        if len(submissions) > 1:
            print(f"\n📈 ANÁLISIS DE TENDENCIAS ({len(submissions)} submissions):")
            
            # Análisis de ubicaciones que más reportan
            location_counts = {}
            submitter_counts = {}
            date_distribution = {}
            
            for submission in submissions:
                # Extraer nombre de ubicación de diferentes posibles estructuras
                location_name = 'Unknown'
                if 'location' in submission and submission['location']:
                    location_name = submission['location'].get('name', 'Unknown')
                elif 'location_name' in submission:
                    location_name = submission.get('location_name', 'Unknown')
                
                # Extraer nombre de usuario
                submitter_name = 'Unknown'
                if 'submitter' in submission and submission['submitter']:
                    submitter_name = submission['submitter'].get('display_name', 'Unknown')
                elif 'submitted_by' in submission:
                    submitter_name = submission.get('submitted_by', 'Unknown')
                
                # Extraer fecha
                date = ''
                submitted_at = submission.get('submitted_at', '')
                if submitted_at:
                    date = submitted_at.split('T')[0] if 'T' in submitted_at else submitted_at[:10]
                else:
                    date = 'Unknown'
                
                location_counts[location_name] = location_counts.get(location_name, 0) + 1
                submitter_counts[submitter_name] = submitter_counts.get(submitter_name, 0) + 1
                date_distribution[date] = date_distribution.get(date, 0) + 1
            
            # Top sucursales que más reportan
            top_locations = sorted(location_counts.items(), key=lambda x: x[1], reverse=True)[:5]
            print(f"   🏪 Top 5 sucursales con más supervisiones:")
            for location, count in top_locations:
                print(f"      • {location}: {count} supervisiones")
            
            # Distribución por fecha
            print(f"   📅 Distribución por fecha:")
            for date, count in sorted(date_distribution.items()):
                print(f"      • {date}: {count} supervisiones")
        
        # Guardar análisis completo
        form_analysis[form_id] = {
            'form_name': form_name,
            'total_submissions_analyzed': len(submissions),
            'field_count': len(form_data) if form_data else 0,
            'field_analysis': field_analysis if 'field_analysis' in locals() else {},
            'sample_submission': sample_submission,
            'location_distribution': location_counts if 'location_counts' in locals() else {},
            'dashboard_recommendations': []
        }
        
        # Recomendaciones para dashboard
        recommendations = []
        
        if 'scoring_fields' in locals() and scoring_fields:
            recommendations.append({
                'type': 'KPI_CARD',
                'title': f'Promedio de Puntuación {form_name}',
                'fields': scoring_fields,
                'priority': 'high'
            })
        
        if 'critical_fields' in locals() and critical_fields:
            recommendations.append({
                'type': 'ALERT_PANEL',
                'title': f'Alertas Críticas {form_name}',
                'fields': critical_fields,
                'priority': 'critical'
            })
        
        recommendations.append({
            'type': 'ACTIVITY_CHART',
            'title': f'Actividad de Supervisiones {form_name}',
            'data_source': 'date_distribution',
            'priority': 'medium'
        })
        
        recommendations.append({
            'type': 'LOCATION_RANKING',
            'title': f'Ranking Sucursales {form_name}',
            'data_source': 'location_distribution',
            'priority': 'medium'
        })
        
        form_analysis[form_id]['dashboard_recommendations'] = recommendations
        
        print(f"\n💡 RECOMENDACIONES DASHBOARD:")
        for rec in recommendations:
            print(f"   📊 {rec['type']}: {rec['title']} (Prioridad: {rec['priority']})")
    
    # Guardar análisis completo
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    analysis_file = f"data/supervision_forms_analysis_{timestamp}.json"
    
    with open(analysis_file, 'w', encoding='utf-8') as f:
        json.dump(form_analysis, f, indent=2, ensure_ascii=False, default=str)
    
    # Generar reporte ejecutivo
    generate_dashboard_proposal(form_analysis, analysis_file)
    
    return form_analysis

def generate_dashboard_proposal(analysis, analysis_file):
    """Genera propuesta de dashboard basada en el análisis"""
    
    print(f"\n" + "="*70)
    print("📊 PROPUESTA DE DASHBOARD - SUPERVISIONES EPL")
    print("="*70)
    
    print(f"📁 Análisis completo guardado en: {analysis_file}")
    
    total_forms = len(analysis)
    total_fields = sum(form['field_count'] for form in analysis.values())
    
    print(f"\n📊 RESUMEN DEL ANÁLISIS:")
    print(f"   📝 Formularios analizados: {total_forms}")
    print(f"   📋 Total campos identificados: {total_fields}")
    
    print(f"\n🎯 COMPONENTES RECOMENDADOS PARA DASHBOARD:")
    
    all_recommendations = []
    for form_id, form_data in analysis.items():
        form_name = form_data['form_name']
        recommendations = form_data.get('dashboard_recommendations', [])
        
        print(f"\n   📝 {form_name}:")
        for rec in recommendations:
            print(f"      {rec['type']}: {rec['title']}")
            all_recommendations.append(rec)
    
    # Priorizar componentes
    critical_components = [r for r in all_recommendations if r['priority'] == 'critical']
    high_components = [r for r in all_recommendations if r['priority'] == 'high']
    medium_components = [r for r in all_recommendations if r['priority'] == 'medium']
    
    print(f"\n🚨 COMPONENTES CRÍTICOS ({len(critical_components)}):")
    for comp in critical_components:
        print(f"   • {comp['title']}")
    
    print(f"\n🔥 COMPONENTES ALTA PRIORIDAD ({len(high_components)}):")
    for comp in high_components:
        print(f"   • {comp['title']}")
    
    print(f"\n📊 COMPONENTES MEDIA PRIORIDAD ({len(medium_components)}):")
    for comp in medium_components:
        print(f"   • {comp['title']}")
    
    print(f"\n💡 PRÓXIMOS PASOS:")
    print(f"   1. Revisar análisis detallado en {analysis_file}")
    print(f"   2. Validar campos identificados")
    print(f"   3. Confirmar componentes de dashboard")
    print(f"   4. Diseñar ETL específico para supervisiones")
    print(f"   5. Implementar dashboard con componentes priorizados")

def main():
    """Función principal"""
    analyze_supervision_forms()

if __name__ == "__main__":
    main()