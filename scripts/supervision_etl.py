#!/usr/bin/env python3
"""
🔍 ETL ESPECÍFICO PARA SUPERVISIONES - EL POLLO LOCO MÉXICO
Extrae datos de formularios 877138 y 877139 para dashboard de supervisiones
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.zenput_api import create_zenput_client
import json
from datetime import datetime, timedelta

def extract_supervision_metrics(submission):
    """Extrae métricas clave de una submission de supervisión"""
    
    # Datos básicos
    metrics = {
        'submission_id': submission.get('id'),
        'form_id': submission.get('form_id'),  # Usar form_id directo
        'form_name': submission.get('form_name'),
        'extracted_at': submission.get('extracted_at'),
    }
    
    # Metadatos del supervisor y sucursal
    metadata = submission.get('smetadata', {})
    if metadata:
        # Información del supervisor
        created_by = metadata.get('created_by', {})
        metrics.update({
            'supervisor_id': created_by.get('id'),
            'supervisor_name': created_by.get('display_name'),
            'supervisor_role': metadata.get('user_role', {}).get('name'),
        })
        
        # Información de la sucursal
        location = metadata.get('location', {})
        metrics.update({
            'sucursal_id': location.get('id'),
            'sucursal_name': location.get('name'),
            'sucursal_address': location.get('address', '')[:100],  # Límite de caracteres
        })
        
        # Fechas y tiempos
        metrics.update({
            'fecha_creacion': metadata.get('date_created_local'),
            'fecha_completada': metadata.get('date_completed_local'),
            'fecha_enviada': metadata.get('date_submitted_local'),
            'tiempo_supervision': metadata.get('time_to_complete'),  # en milisegundos
            'zona_horaria': metadata.get('time_zone'),
        })
        
        # Ubicación GPS
        metrics.update({
            'coordenadas_lat': metadata.get('lat'),
            'coordenadas_lon': metadata.get('lon'),
            'distancia_sucursal': metadata.get('distance_to_account'),
        })
        
        # Metadatos técnicos
        metrics.update({
            'plataforma': metadata.get('platform'),
            'ambiente': metadata.get('environment'),
        })
    
    # Extraer campos específicos según el formulario
    answers = submission.get('answers', [])
    
    # Buscar campos específicos de seguridad (Form 877139)
    if submission.get('form_name') == 'Control Operativo de Seguridad EPL CAS':
        for answer in answers:
            title = answer.get('title', '').upper()
            value = answer.get('value')
            
            if 'PUNTOS MAX' in title and value is not None:
                metrics['puntos_max'] = value
            elif 'PUNTOS TOTALES OBTENIDOS' in title and value is not None:
                metrics['puntos_obtenidos'] = value
            elif 'CALIFICACION PORCENTAJE' in title and value is not None:
                metrics['calificacion_porcentaje'] = value
            elif 'SUCURSAL' in title and value is not None:
                metrics['sucursal_formulario'] = value  # Como aparece en el formulario
    
    # Buscar campos de imágenes y evidencia
    image_count = 0
    yesno_responses = {'si': 0, 'no': 0}
    
    for answer in answers:
        field_type = answer.get('field_type')
        value = answer.get('value')
        is_answered = answer.get('is_answered', False)
        
        if field_type == 'image' and is_answered and value:
            # Contar imágenes subidas
            if isinstance(value, list):
                image_count += len(value)
            else:
                image_count += 1
        
        elif field_type == 'yesno' and is_answered:
            # Contar respuestas Sí/No
            if answer.get('yesno_value') is True:
                yesno_responses['si'] += 1
            elif answer.get('yesno_value') is False:
                yesno_responses['no'] += 1
    
    # Agregar estadísticas de respuestas
    metrics.update({
        'total_respuestas': len([a for a in answers if a.get('is_answered')]),
        'total_preguntas': len(answers),
        'imagenes_subidas': image_count,
        'respuestas_si': yesno_responses['si'],
        'respuestas_no': yesno_responses['no'],
        'porcentaje_completado': round((len([a for a in answers if a.get('is_answered')]) / len(answers) * 100), 2) if answers else 0
    })
    
    return metrics

def run_supervision_etl():
    """Ejecuta ETL específico para supervisiones"""
    
    print("🔍 INICIANDO ETL SUPERVISIONES - EL POLLO LOCO MÉXICO")
    print("=" * 70)
    print(f"📅 Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    client = create_zenput_client()
    
    if not client.validate_api_connection():
        print("❌ No se puede conectar a API Zenput")
        return False
    
    # Formularios de supervisión
    supervision_forms = {
        '877138': 'Supervisión Operativa EPL CAS',
        '877139': 'Control Operativo de Seguridad EPL CAS'
    }
    
    all_supervision_data = {
        'extraction_timestamp': datetime.now().isoformat(),
        'forms_processed': [],
        'supervision_metrics': [],
        'summary': {
            'total_submissions': 0,
            'sucursales_supervisadas': set(),
            'supervisores_activos': set(),
            'calificaciones_promedio': {}
        }
    }
    
    for form_id, form_name in supervision_forms.items():
        print(f"\n🔍 === PROCESANDO {form_name} ({form_id}) ===")
        print("-" * 60)
        
        # Obtener submissions de los últimos 7 días (ajustable)
        submissions = client.get_submissions_for_form(form_id, days_back=7)
        
        if not submissions:
            print(f"⚠️ No hay submissions para {form_id}")
            continue
        
        print(f"📊 Procesando {len(submissions)} submissions")
        
        form_metrics = []
        calificaciones = []
        
        for i, submission in enumerate(submissions, 1):
            # Extraer métricas de cada submission
            metrics = extract_supervision_metrics(submission)
            form_metrics.append(metrics)
            
            # Recopilar para estadísticas
            if metrics.get('sucursal_name'):
                all_supervision_data['summary']['sucursales_supervisadas'].add(metrics['sucursal_name'])
            if metrics.get('supervisor_name'):
                all_supervision_data['summary']['supervisores_activos'].add(metrics['supervisor_name'])
            if metrics.get('calificacion_porcentaje'):
                calificaciones.append(metrics['calificacion_porcentaje'])
            
            print(f"   ✅ {i:2d}. {metrics.get('sucursal_name', 'N/A')[:20]:20s} - {metrics.get('supervisor_name', 'N/A')[:15]:15s} - {metrics.get('calificacion_porcentaje', 'N/A')}")
        
        # Estadísticas por formulario
        if calificaciones:
            promedio = sum(calificaciones) / len(calificaciones)
            all_supervision_data['summary']['calificaciones_promedio'][form_name] = round(promedio, 2)
            print(f"\n📊 Promedio de calificaciones {form_name}: {promedio:.2f}%")
            print(f"   📈 Rango: {min(calificaciones):.1f}% - {max(calificaciones):.1f}%")
        
        all_supervision_data['forms_processed'].append({
            'form_id': form_id,
            'form_name': form_name,
            'submissions_count': len(submissions),
            'metrics': form_metrics
        })
        
        all_supervision_data['supervision_metrics'].extend(form_metrics)
    
    # Estadísticas finales
    all_supervision_data['summary']['total_submissions'] = len(all_supervision_data['supervision_metrics'])
    all_supervision_data['summary']['total_sucursales'] = len(all_supervision_data['summary']['sucursales_supervisadas'])
    all_supervision_data['summary']['total_supervisores'] = len(all_supervision_data['summary']['supervisores_activos'])
    
    print(f"\n📊 === RESUMEN FINAL ===")
    print(f"   ✅ Total submissions procesadas: {all_supervision_data['summary']['total_submissions']}")
    print(f"   🏪 Sucursales supervisadas: {all_supervision_data['summary']['total_sucursales']}")
    print(f"   👨‍💼 Supervisores activos: {all_supervision_data['summary']['total_supervisores']}")
    
    for form_name, promedio in all_supervision_data['summary']['calificaciones_promedio'].items():
        print(f"   📊 Promedio {form_name}: {promedio}%")
    
    # Guardar datos procesados
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"data/supervision_etl_data_{timestamp}.json"
    
    # Convertir sets a listas para JSON
    all_supervision_data['summary']['sucursales_supervisadas'] = list(all_supervision_data['summary']['sucursales_supervisadas'])
    all_supervision_data['summary']['supervisores_activos'] = list(all_supervision_data['summary']['supervisores_activos'])
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_supervision_data, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"\n💾 Datos guardados en: {output_file}")
    
    # Generar alerta si hay calificaciones bajas
    check_supervision_alerts(all_supervision_data)
    
    print(f"\n✅ ETL SUPERVISIONES COMPLETADO")
    print("=" * 70)
    
    return True

def check_supervision_alerts(data):
    """Verifica y reporta alertas de supervisiones"""
    
    print(f"\n🚨 === VERIFICACIÓN DE ALERTAS ===")
    
    alerts = {
        'critical': [],    # <70%
        'warning': [],     # 70-79%
        'good': [],        # 80-89%
        'excellent': []    # >90%
    }
    
    for metrics in data['supervision_metrics']:
        calificacion = metrics.get('calificacion_porcentaje')
        if calificacion is None:
            continue
            
        sucursal = metrics.get('sucursal_name', 'N/A')
        supervisor = metrics.get('supervisor_name', 'N/A')
        
        alert_data = {
            'sucursal': sucursal,
            'supervisor': supervisor,
            'calificacion': calificacion,
            'fecha': metrics.get('fecha_completada')
        }
        
        if calificacion < 70:
            alerts['critical'].append(alert_data)
        elif calificacion < 80:
            alerts['warning'].append(alert_data)
        elif calificacion < 90:
            alerts['good'].append(alert_data)
        else:
            alerts['excellent'].append(alert_data)
    
    # Reportar alertas
    if alerts['critical']:
        print(f"   🔴 CRÍTICAS ({len(alerts['critical'])}): Calificaciones <70%")
        for alert in alerts['critical']:
            print(f"      • {alert['sucursal']} - {alert['calificacion']:.1f}% ({alert['supervisor']})")
    
    if alerts['warning']:
        print(f"   🟡 ADVERTENCIAS ({len(alerts['warning'])}): Calificaciones 70-79%")
        for alert in alerts['warning']:
            print(f"      • {alert['sucursal']} - {alert['calificacion']:.1f}% ({alert['supervisor']})")
    
    if not alerts['critical'] and not alerts['warning']:
        print(f"   ✅ Sin alertas críticas - Todas las supervisiones ≥80%")
    
    print(f"   🟢 Excelentes: {len(alerts['excellent'])} supervisiones >90%")

def main():
    """Función principal"""
    
    try:
        success = run_supervision_etl()
        if success:
            print("🎉 ETL Supervisiones ejecutado exitosamente")
            sys.exit(0)
        else:
            print("❌ ETL Supervisiones falló")
            sys.exit(1)
    except Exception as e:
        print(f"💥 Error crítico: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()