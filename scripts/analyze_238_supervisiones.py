#!/usr/bin/env python3
"""
📊 ANÁLISIS COMPLETO DE 238 SUPERVISIONES POR ÁREAS
Extrae KPIs específicos de cada área operativa identificada
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.zenput_api import create_zenput_client
import json
from datetime import datetime, timedelta
from collections import defaultdict

def extract_area_kpis(answers):
    """Extrae KPIs por área específica basado en estructura real"""
    
    # Áreas identificadas en la estructura real
    AREAS_IDENTIFICADAS = {
        'CONTROL OPERATIVO DE SEGURIDAD': 'HEADER',
        'I. AREA COMEDOR': 'COMEDOR',
        'II. AREA ASADORES': 'ASADORES',
        'III. AREA DE MARINADO': 'MARINADO',
        'IV. AREA DE BODEGA': 'BODEGA',
        'V. AREA DE HORNO': 'HORNO',
        'VI. AREA FREIDORAS': 'FREIDORAS',
        'VII. CENTRO DE CARGA': 'CENTRO_CARGA',
        'VIII. AREA AZOTEA': 'AZOTEA',
        'IX. AREA EXTERIOR': 'EXTERIOR',
        'X. PROGRAMA INTERNO PROTECCION CIVIL': 'PROTECCION_CIVIL',
        'XI. BITACORAS': 'BITACORAS',
        'XII. NOMBRES Y FIRMAS': 'FIRMAS'
    }
    
    areas_data = {}
    current_area = 'HEADER'  # Comenzar con datos generales
    
    # Inicializar todas las áreas
    for area_title, area_key in AREAS_IDENTIFICADAS.items():
        areas_data[area_key] = {
            'area_title': area_title,
            'elementos_evaluados': 0,
            'elementos_conformes': 0,
            'elementos_no_conformes': 0,
            'elementos_criticos_fallidos': [],
            'evidencia_fotografica': 0,
            'respuestas_totales': 0,
            'respuestas_si': 0,
            'respuestas_no': 0,
            'campos_completados': 0,
            'total_campos': 0,
            'score_area': None,
            'elementos_detalle': []
        }
    
    for answer in answers:
        field_type = answer.get('field_type')
        title = answer.get('title', '')
        value = answer.get('value')
        is_answered = answer.get('is_answered', False)
        
        # Detectar cambio de área
        if field_type == 'section' and title in AREAS_IDENTIFICADAS:
            current_area = AREAS_IDENTIFICADAS[title]
            continue
        
        # Procesar campo dentro del área actual
        area = areas_data[current_area]
        area['total_campos'] += 1
        
        if is_answered:
            area['campos_completados'] += 1
            
            # Extraer datos específicos del HEADER (datos generales)
            if current_area == 'HEADER':
                if 'PUNTOS MAX' in title:
                    area['puntos_max'] = value
                elif 'PUNTOS TOTALES OBTENIDOS' in title:
                    area['puntos_obtenidos'] = value
                elif 'CALIFICACION PORCENTAJE' in title:
                    area['calificacion_general'] = value
                elif 'SUCURSAL' in title:
                    area['sucursal'] = value
                elif 'AUDITOR' in title:
                    area['auditor'] = value
            
            # Procesar respuestas SI/NO
            if field_type == 'yesno':
                area['respuestas_totales'] += 1
                area['elementos_evaluados'] += 1
                
                yesno_value = answer.get('yesno_value')
                if yesno_value == 'true' or yesno_value is True:
                    area['respuestas_si'] += 1
                    area['elementos_conformes'] += 1
                elif yesno_value == 'false' or yesno_value is False:
                    area['respuestas_no'] += 1
                    area['elementos_no_conformes'] += 1
                    
                    # Identificar elementos críticos fallidos
                    area['elementos_criticos_fallidos'].append({
                        'elemento': title,
                        'tipo_falla': 'no_conforme'
                    })
                
                # Guardar detalle del elemento
                area['elementos_detalle'].append({
                    'elemento': title,
                    'respuesta': 'SI' if yesno_value in ['true', True] else 'NO',
                    'conforme': yesno_value in ['true', True]
                })
            
            # Contar evidencia fotográfica
            elif field_type == 'image' and value:
                if isinstance(value, list):
                    area['evidencia_fotografica'] += len(value)
                else:
                    area['evidencia_fotografica'] += 1
    
    # Calcular KPIs finales por área
    for area_key, area_data in areas_data.items():
        if area_data['elementos_evaluados'] > 0:
            area_data['conformidad_porcentaje'] = round(
                (area_data['elementos_conformes'] / area_data['elementos_evaluados']) * 100, 2
            )
        else:
            area_data['conformidad_porcentaje'] = None
            
        if area_data['total_campos'] > 0:
            area_data['completitud_porcentaje'] = round(
                (area_data['campos_completados'] / area_data['total_campos']) * 100, 2
            )
        else:
            area_data['completitud_porcentaje'] = 0
    
    return areas_data

def analyze_238_supervisiones():
    """Análiza todas las supervisiones disponibles por área"""
    
    print("📊 ANÁLISIS COMPLETO - 238 SUPERVISIONES POR ÁREAS")
    print("=" * 70)
    print("🎯 Extrayendo KPIs específicos por cada área operativa")
    print("=" * 70)
    
    client = create_zenput_client()
    
    if not client.validate_api_connection():
        print("❌ No se puede conectar a API Zenput")
        return False
    
    supervision_forms = {
        '877138': 'Supervisión Operativa EPL CAS',
        '877139': 'Control Operativo de Seguridad EPL CAS'
    }
    
    comprehensive_analysis = {
        'analysis_timestamp': datetime.now().isoformat(),
        'total_submissions': 0,
        'forms_data': {},
        'areas_summary': defaultdict(lambda: {
            'total_supervisiones': 0,
            'conformidad_promedio': 0,
            'completitud_promedio': 0,
            'elementos_criticos_totales': 0,
            'evidencia_fotografica_total': 0,
            'sucursales_con_problemas': [],
            'tendencia_mensual': []
        }),
        'sucursales_ranking': {},
        'supervisores_performance': {},
        'alertas_por_area': defaultdict(list)
    }
    
    total_supervisiones = 0
    
    for form_id, form_name in supervision_forms.items():
        print(f"\n🔍 === ANALIZANDO {form_name} ({form_id}) ===")
        print("-" * 60)
        
        # Obtener más supervisiones (aumentar rango para capturar más de las 238)
        submissions = client.get_submissions_for_form(form_id, days_back=45)
        
        if not submissions:
            print(f"⚠️ No hay submissions para {form_id}")
            continue
        
        print(f"📊 Procesando {len(submissions)} submissions")
        total_supervisiones += len(submissions)
        
        form_analysis = {
            'form_name': form_name,
            'submissions_analyzed': len(submissions),
            'submissions_detail': []
        }
        
        # Acumuladores por área para este formulario
        areas_accumulator = defaultdict(lambda: {
            'conformidad_total': 0,
            'completitud_total': 0,
            'supervisiones_count': 0,
            'elementos_criticos': 0,
            'evidencia_total': 0
        })
        
        for i, submission in enumerate(submissions, 1):
            submission_id = submission.get('id')
            metadata = submission.get('smetadata', {})
            
            # Datos básicos de la supervisión
            sucursal = metadata.get('location', {}).get('name', 'Unknown')
            supervisor = metadata.get('created_by', {}).get('display_name', 'Unknown')
            fecha = metadata.get('date_completed_local', 'Unknown')
            
            # Extraer KPIs por área
            answers = submission.get('answers', [])
            areas_kpis = extract_area_kpis(answers)
            
            # Datos generales de la supervisión (del HEADER)
            header_data = areas_kpis.get('HEADER', {})
            calificacion_general = header_data.get('calificacion_general')
            
            submission_data = {
                'submission_id': submission_id,
                'sucursal': sucursal,
                'supervisor': supervisor,
                'fecha': fecha,
                'calificacion_general': calificacion_general,
                'areas_kpis': areas_kpis
            }
            
            form_analysis['submissions_detail'].append(submission_data)
            
            # Acumular datos por área
            for area_key, area_data in areas_kpis.items():
                if area_key == 'HEADER':
                    continue
                    
                if area_data['conformidad_porcentaje'] is not None:
                    areas_accumulator[area_key]['conformidad_total'] += area_data['conformidad_porcentaje']
                    areas_accumulator[area_key]['supervisiones_count'] += 1
                
                areas_accumulator[area_key]['completitud_total'] += area_data['completitud_porcentaje']
                areas_accumulator[area_key]['elementos_criticos'] += len(area_data['elementos_criticos_fallidos'])
                areas_accumulator[area_key]['evidencia_total'] += area_data['evidencia_fotografica']
                
                # Detectar problemas por sucursal/área
                if area_data['conformidad_porcentaje'] is not None and area_data['conformidad_porcentaje'] < 80:
                    comprehensive_analysis['alertas_por_area'][area_key].append({
                        'sucursal': sucursal,
                        'conformidad': area_data['conformidad_porcentaje'],
                        'elementos_fallidos': len(area_data['elementos_criticos_fallidos']),
                        'supervisor': supervisor,
                        'fecha': fecha
                    })
            
            # Actualizar ranking de sucursales
            if calificacion_general:
                if sucursal not in comprehensive_analysis['sucursales_ranking']:
                    comprehensive_analysis['sucursales_ranking'][sucursal] = {
                        'calificaciones': [],
                        'supervisor_frecuente': supervisor,
                        'supervisiones_count': 0
                    }
                
                comprehensive_analysis['sucursales_ranking'][sucursal]['calificaciones'].append(calificacion_general)
                comprehensive_analysis['sucursales_ranking'][sucursal]['supervisiones_count'] += 1
            
            # Actualizar performance de supervisores
            if supervisor not in comprehensive_analysis['supervisores_performance']:
                comprehensive_analysis['supervisores_performance'][supervisor] = {
                    'calificaciones_otorgadas': [],
                    'sucursales_supervisadas': set(),
                    'supervisiones_count': 0
                }
            
            if calificacion_general:
                comprehensive_analysis['supervisores_performance'][supervisor]['calificaciones_otorgadas'].append(calificacion_general)
            comprehensive_analysis['supervisores_performance'][supervisor]['sucursales_supervisadas'].add(sucursal)
            comprehensive_analysis['supervisores_performance'][supervisor]['supervisiones_count'] += 1
            
            # Mostrar progreso
            if i <= 5:
                calif_str = f"{calificacion_general:.1f}%" if calificacion_general else "N/A"
                print(f"   ✅ {i:2d}. {sucursal[:25]:25s} - {calif_str:6s} | {supervisor[:15]:15s}")
        
        if len(submissions) > 5:
            print(f"   ... y {len(submissions) - 5} supervisiones más procesadas")
        
        # Calcular promedios por área para este formulario
        print(f"\n📊 KPIS POR ÁREA EN {form_name}:")
        for area_key, area_acc in areas_accumulator.items():
            if area_acc['supervisiones_count'] > 0:
                conformidad_prom = area_acc['conformidad_total'] / area_acc['supervisiones_count']
                completitud_prom = area_acc['completitud_total'] / area_acc['supervisiones_count']
                
                # Actualizar summary general
                comprehensive_analysis['areas_summary'][area_key]['total_supervisiones'] += area_acc['supervisiones_count']
                comprehensive_analysis['areas_summary'][area_key]['conformidad_promedio'] = conformidad_prom
                comprehensive_analysis['areas_summary'][area_key]['completitud_promedio'] = completitud_prom
                comprehensive_analysis['areas_summary'][area_key]['elementos_criticos_totales'] += area_acc['elementos_criticos']
                comprehensive_analysis['areas_summary'][area_key]['evidencia_fotografica_total'] += area_acc['evidencia_total']
                
                # Mostrar resultado
                area_name = areas_kpis.get(area_key, {}).get('area_title', area_key)
                status = "🔴" if conformidad_prom < 70 else "🟡" if conformidad_prom < 85 else "🟢"
                print(f"   {status} {area_name[:35]:35s} | Conformidad: {conformidad_prom:5.1f}% | Completitud: {completitud_prom:5.1f}%")
        
        comprehensive_analysis['forms_data'][form_id] = form_analysis
    
    # Calcular promedios finales para sucursales y supervisores
    for sucursal, data in comprehensive_analysis['sucursales_ranking'].items():
        if data['calificaciones']:
            data['promedio'] = sum(data['calificaciones']) / len(data['calificaciones'])
            data['calificaciones'] = []  # Limpiar para JSON
    
    for supervisor, data in comprehensive_analysis['supervisores_performance'].items():
        if data['calificaciones_otorgadas']:
            data['promedio'] = sum(data['calificaciones_otorgadas']) / len(data['calificaciones_otorgadas'])
            data['calificaciones_otorgadas'] = []  # Limpiar para JSON
        data['sucursales_supervisadas'] = list(data['sucursales_supervisadas'])
    
    comprehensive_analysis['total_submissions'] = total_supervisiones
    
    # Generar reporte final
    generate_final_report(comprehensive_analysis)
    
    # Guardar análisis completo
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"data/analysis_238_supervisiones_{timestamp}.json"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(comprehensive_analysis, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"\n💾 Análisis completo guardado en: {output_file}")
    
    return comprehensive_analysis

def generate_final_report(analysis):
    """Genera reporte final con todos los KPIs"""
    
    print(f"\n" + "=" * 70)
    print("📊 REPORTE FINAL - ANÁLISIS DE 238 SUPERVISIONES")
    print("=" * 70)
    
    print(f"📈 RESUMEN GENERAL:")
    print(f"   ✅ Total supervisiones analizadas: {analysis['total_submissions']}")
    print(f"   🏪 Sucursales evaluadas: {len(analysis['sucursales_ranking'])}")
    print(f"   👥 Supervisores activos: {len(analysis['supervisores_performance'])}")
    print(f"   🏭 Áreas operativas: {len(analysis['areas_summary'])}")
    
    # Top 5 sucursales
    sucursales_sorted = sorted(analysis['sucursales_ranking'].items(), 
                              key=lambda x: x[1].get('promedio', 0), reverse=True)
    
    print(f"\n🏆 TOP 5 SUCURSALES:")
    for i, (sucursal, data) in enumerate(sucursales_sorted[:5], 1):
        promedio = data.get('promedio', 0)
        count = data.get('supervisiones_count', 0)
        print(f"   {i}. {sucursal[:30]:30s} - {promedio:5.1f}% ({count} supervisiones)")
    
    # Áreas con mejor/peor performance
    areas_sorted = sorted(analysis['areas_summary'].items(), 
                         key=lambda x: x[1]['conformidad_promedio'], reverse=True)
    
    print(f"\n🟢 MEJORES ÁREAS:")
    for area_key, area_data in areas_sorted[:3]:
        conformidad = area_data['conformidad_promedio']
        count = area_data['total_supervisiones']
        print(f"   • {area_key[:30]:30s} - {conformidad:5.1f}% ({count} supervisiones)")
    
    print(f"\n🔴 ÁREAS QUE REQUIEREN ATENCIÓN:")
    for area_key, area_data in areas_sorted[-3:]:
        conformidad = area_data['conformidad_promedio']
        count = area_data['total_supervisiones']
        criticos = area_data['elementos_criticos_totales']
        print(f"   • {area_key[:30]:30s} - {conformidad:5.1f}% ({criticos} elementos críticos)")
    
    # Alertas por área
    print(f"\n🚨 ALERTAS ACTIVAS POR ÁREA:")
    for area_key, alertas in analysis['alertas_por_area'].items():
        if alertas:
            print(f"   🔴 {area_key}: {len(alertas)} sucursales con conformidad <80%")
            for alerta in alertas[:2]:  # Mostrar solo 2 ejemplos
                print(f"      • {alerta['sucursal']}: {alerta['conformidad']:.1f}%")

def main():
    """Función principal"""
    
    try:
        result = analyze_238_supervisiones()
        if result:
            print("\n🎉 Análisis de 238 supervisiones completado exitosamente")
            return True
        else:
            print("\n❌ Análisis falló")
            return False
    except Exception as e:
        print(f"\n💥 Error crítico: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    main()