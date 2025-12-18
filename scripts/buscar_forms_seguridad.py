#!/usr/bin/env python3
"""
🔍 BÚSQUEDA FORMS SEGURIDAD
Busca todos los formularios disponibles y encuentra el de seguridad
"""

import requests
import json
from datetime import datetime

def buscar_forms_seguridad():
    """Busca formularios de seguridad disponibles"""
    
    print("🔍 BÚSQUEDA FORMULARIOS DE SEGURIDAD")
    print("=" * 45)
    
    # Configuración API
    api_token = "cb908e0d4e0f5501c635325c611db314"
    headers = {
        'X-API-TOKEN': api_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    base_url = 'https://www.zenput.com/api/v3'
    
    # 1. OBTENER TODOS LOS FORMS DISPONIBLES
    print("\n📋 CONSULTANDO TODOS LOS FORMS DISPONIBLES...")
    
    try:
        forms_url = f"{base_url}/forms"
        forms_response = requests.get(forms_url, headers=headers, timeout=30)
        
        if forms_response.status_code == 200:
            forms_data = forms_response.json()
            forms_list = forms_data.get('data', [])
            
            print(f"✅ {len(forms_list)} forms encontrados")
            
            # Buscar forms relacionados con seguridad
            security_forms = []
            operational_forms = []
            
            print(f"\n🔍 ANÁLISIS DE FORMULARIOS:")
            print("=" * 40)
            
            for form in forms_list:
                form_id = form.get('id')
                form_name = form.get('name', '').lower()
                form_status = form.get('status', 'N/A')
                
                print(f"\n📝 Form ID: {form_id}")
                print(f"   • Nombre: {form.get('name', 'N/A')}")
                print(f"   • Status: {form_status}")
                
                # Identificar forms de seguridad
                if any(keyword in form_name for keyword in ['seguridad', 'security', 'control']):
                    security_forms.append(form)
                    print(f"   🛡️ ← FORMULARIO DE SEGURIDAD IDENTIFICADO")
                
                # Identificar forms operativos
                if any(keyword in form_name for keyword in ['operativa', 'operativo', 'operational']):
                    operational_forms.append(form)
                    print(f"   ⚙️ ← FORMULARIO OPERATIVO IDENTIFICADO")
            
            print(f"\n🎯 RESUMEN IDENTIFICACIÓN:")
            print(f"   • Forms de Seguridad: {len(security_forms)}")
            print(f"   • Forms Operativos: {len(operational_forms)}")
            
            # 2. ANALIZAR FORMS DE SEGURIDAD EN DETALLE
            if security_forms:
                print(f"\n🛡️ ANÁLISIS DETALLADO FORMS SEGURIDAD:")
                print("=" * 50)
                
                for i, form in enumerate(security_forms, 1):
                    form_id = form.get('id')
                    form_name = form.get('name', 'N/A')
                    
                    print(f"\n🛡️ FORM SEGURIDAD {i}: {form_name}")
                    print(f"   • ID: {form_id}")
                    
                    # Intentar obtener estructura detallada
                    try:
                        form_detail_url = f"{base_url}/forms/{form_id}"
                        form_detail_response = requests.get(form_detail_url, headers=headers, timeout=15)
                        
                        if form_detail_response.status_code == 200:
                            form_detail = form_detail_response.json()
                            form_info = form_detail.get('data', {})
                            sections = form_info.get('sections', [])
                            
                            print(f"   • Secciones: {len(sections)}")
                            
                            if len(sections) == 11:
                                print(f"   ✅ COINCIDE CON 11 ÁREAS ESPERADAS")
                            else:
                                print(f"   ⚠️ Diferente a 11 áreas esperadas")
                            
                            # Mostrar nombres de secciones
                            if sections:
                                print(f"   📋 Áreas/Secciones:")
                                for j, section in enumerate(sections, 1):
                                    section_name = section.get('name', f'Área {j}')
                                    questions_count = len(section.get('questions', []))
                                    print(f"      {j}. {section_name} ({questions_count} preguntas)")
                            
                            # Buscar submissions recientes
                            submissions_url = f"{base_url}/forms/{form_id}/submissions"
                            submissions_params = {'limit': 3}
                            
                            try:
                                submissions_response = requests.get(submissions_url, headers=headers, params=submissions_params, timeout=15)
                                
                                if submissions_response.status_code == 200:
                                    submissions_data = submissions_response.json()
                                    submissions_list = submissions_data.get('data', [])
                                    
                                    print(f"   • Submissions disponibles: {len(submissions_list)}")
                                    
                                    if submissions_list:
                                        # Analizar submission más reciente
                                        submission = submissions_list[0]
                                        sucursal = submission.get('location', {}).get('name', 'N/A')
                                        fecha = submission.get('submitted_at', 'N/A')
                                        score = submission.get('score')
                                        
                                        print(f"   📊 Submission más reciente:")
                                        print(f"      • Sucursal: {sucursal}")
                                        print(f"      • Fecha: {fecha}")
                                        print(f"      • Score: {score}")
                                        
                                        # Analizar calificaciones
                                        answers = submission.get('answers', [])
                                        total_score = 0
                                        max_score = 0
                                        
                                        for answer in answers:
                                            answer_score = answer.get('score', 0)
                                            answer_max_score = answer.get('max_score', 0)
                                            
                                            if answer_score is not None:
                                                total_score += float(answer_score)
                                            if answer_max_score is not None:
                                                max_score += float(answer_max_score)
                                        
                                        if max_score > 0:
                                            percentage = (total_score / max_score) * 100
                                            
                                            print(f"   🎯 CALIFICACIÓN VALIDADA:")
                                            print(f"      • Puntos obtenidos: {total_score:.0f}")
                                            print(f"      • Puntos máximos: {max_score:.0f}")
                                            print(f"      • Porcentaje: {percentage:.2f}%")
                                            
                                            # Verificar si coincide con formato esperado
                                            print(f"\n   📋 FORMATO VALIDACIÓN:")
                                            print(f"      CONTROL OPERATIVO DE SEGURIDAD")
                                            print(f"      PUNTOS MAX                {max_score:.0f}")
                                            print(f"      PUNTOS TOTALES OBTENIDOS  {total_score:.0f}")
                                            print(f"      CALIFICACION PORCENTAJE % {percentage:.2f}")
                                            print(f"      SUCURSAL                  {sucursal}")
                                            print(f"      Date                      {fecha}")
                                        
                                else:
                                    print(f"   ⚠️ No se pudieron obtener submissions")
                                    
                            except Exception as e:
                                print(f"   ❌ Error submissions: {e}")
                        else:
                            print(f"   ❌ Error detalle form: {form_detail_response.status_code}")
                            
                    except Exception as e:
                        print(f"   ❌ Error: {e}")
            else:
                print("\n⚠️ NO SE ENCONTRARON FORMULARIOS DE SEGURIDAD")
                
                # Mostrar todos los forms para identificación manual
                print(f"\n📋 TODOS LOS FORMULARIOS DISPONIBLES:")
                print("-" * 40)
                
                for form in forms_list:
                    print(f"ID: {form.get('id')} - {form.get('name', 'Sin nombre')}")
            
            # 3. GUARDAR ANÁLISIS COMPLETO
            resultado = {
                'timestamp': datetime.now().isoformat(),
                'total_forms': len(forms_list),
                'security_forms': security_forms,
                'operational_forms': operational_forms,
                'all_forms': forms_list
            }
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"data/analisis_forms_seguridad_{timestamp}.json"
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(resultado, f, indent=2, ensure_ascii=False, default=str)
            
            print(f"\n💾 ANÁLISIS GUARDADO: {filename}")
            
            return resultado
            
        else:
            print(f"❌ Error al consultar forms: {forms_response.status_code}")
            return None
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

if __name__ == "__main__":
    print("🔍 EJECUTANDO BÚSQUEDA FORMS SEGURIDAD")
    print("Identificando formularios de seguridad con 11 áreas...")
    print()
    
    resultado = buscar_forms_seguridad()
    
    if resultado:
        security_count = len(resultado['security_forms'])
        print(f"\n✅ BÚSQUEDA COMPLETADA")
        print(f"🛡️ {security_count} formularios de seguridad identificados")
        
        if security_count > 0:
            print(f"📊 Formularios analizados para validar estructura de 11 áreas")
        else:
            print(f"⚠️ Revisar manualmente listado de formularios")
    else:
        print(f"\n❌ BÚSQUEDA FALLÓ")