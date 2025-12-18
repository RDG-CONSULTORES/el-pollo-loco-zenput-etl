#!/usr/bin/env python3
"""
🔍 INSPECCIÓN DETALLADA DEL CONTENIDO DE SUPERVISIONES
Examina la estructura real de las respuestas para entender qué datos extraer
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.zenput_api import create_zenput_client
import json
from datetime import datetime

def inspect_supervision_content():
    """Inspecciona el contenido real de las supervisiones"""
    
    print("🔍 INSPECCIÓN DETALLADA - CONTENIDO SUPERVISIONES")
    print("=" * 70)
    
    client = create_zenput_client()
    
    if not client.validate_api_connection():
        print("❌ No se puede conectar a API Zenput")
        return
    
    # Formularios de supervisión
    supervision_forms = {
        '877138': 'Supervisión Operativa EPL CAS',
        '877139': 'Control Operativo de Seguridad EPL CAS'
    }
    
    for form_id, form_name in supervision_forms.items():
        print(f"\n🔍 === INSPECCIONANDO {form_name} ({form_id}) ===")
        print("-" * 60)
        
        # Obtener 1 submission para análisis detallado
        submissions = client.get_submissions_for_form(form_id, days_back=7)
        
        if not submissions:
            print(f"❌ No hay submissions para {form_id}")
            continue
        
        # Analizar la primera submission en detalle
        submission = submissions[0]
        
        print(f"📋 ANÁLISIS DETALLADO DE SUBMISSION:")
        print(f"   🆔 ID: {submission.get('id')}")
        print(f"   📝 Form Name: {submission.get('form_name')}")
        print(f"   🔍 Search Text Preview: {submission.get('search_text', '')[:100]}...")
        
        # Analizar estructura de answers
        answers = submission.get('answers', [])
        print(f"\n📊 ESTRUCTURA DE ANSWERS ({len(answers)} respuestas):")
        
        if answers and len(answers) > 0:
            # Mostrar estructura del primer answer
            first_answer = answers[0]
            print(f"   📋 Keys en primer answer: {list(first_answer.keys()) if isinstance(first_answer, dict) else 'No es dict'}")
            
            # Analizar las primeras 5 respuestas para entender patrones
            print(f"\n📝 PRIMERAS 5 RESPUESTAS:")
            for i, answer in enumerate(answers[:5]):
                if isinstance(answer, dict):
                    print(f"\n   {i+1}. {answer.keys()}")
                    for key, value in answer.items():
                        if isinstance(value, str) and len(value) > 100:
                            print(f"      {key}: {value[:80]}...")
                        else:
                            print(f"      {key}: {value}")
                else:
                    print(f"\n   {i+1}. Tipo: {type(answer)} = {answer}")
        
        # Buscar respuestas con contenido interesante
        print(f"\n🔍 BUSCANDO RESPUESTAS CON CONTENIDO RELEVANTE:")
        
        interesting_answers = []
        for i, answer in enumerate(answers):
            if isinstance(answer, dict):
                # Buscar campos que contienen datos útiles
                for key, value in answer.items():
                    if value and str(value).strip() and str(value) != 'null':
                        if any(keyword in str(key).lower() for keyword in ['question', 'response', 'answer', 'value']):
                            interesting_answers.append((i, key, value))
                            if len(interesting_answers) < 10:  # Mostrar solo las primeras 10
                                print(f"   📋 Respuesta {i+1} - {key}: {str(value)[:60]}")
        
        if interesting_answers:
            print(f"\n💡 Se encontraron {len(interesting_answers)} respuestas con contenido")
        else:
            print(f"\n⚠️ No se encontraron respuestas con contenido obvio")
        
        # Buscar metadatos útiles
        metadata = submission.get('smetadata', {})
        if metadata:
            print(f"\n📊 METADATOS DISPONIBLES:")
            for key, value in metadata.items():
                print(f"   📋 {key}: {str(value)[:60]}")
        
        # Guardar submission completa para análisis manual
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        sample_file = f"data/sample_submission_{form_id}_{timestamp}.json"
        
        with open(sample_file, 'w', encoding='utf-8') as f:
            json.dump(submission, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n💾 Submission completa guardada en: {sample_file}")
        print(f"📝 Total keys en submission: {len(submission.keys())}")
        print(f"📊 Total answers: {len(answers)}")

def main():
    """Función principal"""
    inspect_supervision_content()

if __name__ == "__main__":
    main()