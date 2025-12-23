#!/usr/bin/env python3
"""
🚀 EXTRAER API ZENPUT CORREGIDO
Usar formato correcto detectado: data en lugar de submissions
"""

import requests
import pandas as pd
import json
import time
from datetime import datetime

def extraer_submissions_zenput_v3(form_template_id, tipo_supervision):
    """Extraer submissions usando formato correcto detectado"""
    
    print(f"📥 EXTRAYENDO {tipo_supervision.upper()} DESDE ZENPUT API V3")
    print("=" * 60)
    
    url = "https://www.zenput.com/api/v3/submissions"
    headers = {
        'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314',
        'Content-Type': 'application/json'
    }
    
    todas_submissions = []
    start = 0
    limit = 100
    
    while True:
        params = {
            'form_template_id': form_template_id,
            'start': start,
            'limit': limit
        }
        
        print(f"📡 Request: start={start}, limit={limit}")
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                # El formato correcto es data, no submissions
                submissions_batch = data.get('data', [])
                
                if not submissions_batch:
                    print(f"✅ No más datos. Total extraídas: {len(todas_submissions)}")
                    break
                
                # Filtrar por año 2025
                submissions_2025 = []
                for submission in submissions_batch:
                    submitted_at = submission.get('submitted_at', '')
                    if '2025' in submitted_at:
                        submissions_2025.append(submission)
                
                todas_submissions.extend(submissions_2025)
                
                print(f"   ✅ Batch: {len(submissions_2025)} del 2025 (de {len(submissions_batch)} total)")
                
                # Siguiente página
                start += limit
                
                # Pausa para no sobrecargar
                time.sleep(0.5)
                
            else:
                print(f"❌ Error API: {response.status_code}")
                print(f"   Response: {response.text}")
                break
                
        except Exception as e:
            print(f"❌ Error: {e}")
            break
    
    print(f"\n📊 RESUMEN EXTRACCIÓN {tipo_supervision.upper()}:")
    print(f"   ✅ Total submissions 2025: {len(todas_submissions)}")
    
    return todas_submissions

def procesar_submissions_a_dataframe(submissions, tipo):
    """Procesar submissions en DataFrame estructurado"""
    
    print(f"\n🔄 PROCESANDO {tipo.upper()} A DATAFRAME")
    print("=" * 50)
    
    datos_procesados = []
    
    for i, submission in enumerate(submissions, 1):
        try:
            # Datos básicos
            submission_id = submission.get('id')
            submitted_at = submission.get('submitted_at')
            submitted_by_info = submission.get('submitted_by', {})
            submitted_by = submitted_by_info.get('name', 'DESCONOCIDO') if submitted_by_info else 'DESCONOCIDO'
            
            # Location
            location_info = submission.get('location', {})
            location_name = location_info.get('name') if location_info else None
            
            # Coordenadas de entrega (smetadata)
            smetadata = submission.get('smetadata', {})
            lat_entrega = smetadata.get('lat')
            lon_entrega = smetadata.get('lon')
            
            # Respuestas del formulario
            responses = submission.get('responses', [])
            sucursal_campo = None
            location_map = None
            
            # Buscar campo Sucursal y Location Map en respuestas
            for response in responses:
                question = response.get('question', {})
                question_name = question.get('name', '').lower()
                answer = response.get('answer')
                
                if 'sucursal' in question_name:
                    sucursal_campo = answer
                elif 'location' in question_name and 'map' in question_name:
                    location_map = answer
            
            # Procesar fecha
            fecha_dt = pd.to_datetime(submitted_at) if submitted_at else None
            
            datos_procesados.append({
                'submission_id': submission_id,
                'submitted_at': submitted_at,
                'fecha': fecha_dt,
                'fecha_str': fecha_dt.strftime('%Y-%m-%d') if fecha_dt else None,
                'usuario': submitted_by,
                'location_asignado': location_name,
                'lat_entrega': lat_entrega,
                'lon_entrega': lon_entrega,
                'sucursal_campo': sucursal_campo,
                'location_map': location_map,
                'tipo': tipo,
                'tiene_location': location_name is not None,
                'tiene_coordenadas': lat_entrega is not None and lon_entrega is not None,
                'necesita_mapeo': location_name is None,
                'submission_raw': submission  # Datos completos
            })
            
            # Mostrar progreso cada 50
            if i % 50 == 0:
                print(f"   Procesadas: {i}/{len(submissions)}")
                
        except Exception as e:
            print(f"⚠️ Error procesando submission {i}: {e}")
            continue
    
    df = pd.DataFrame(datos_procesados)
    
    print(f"\n📊 PROCESAMIENTO {tipo.upper()} COMPLETADO:")
    print(f"   ✅ Total procesadas: {len(df)}")
    print(f"   📍 Con location: {len(df[df['tiene_location']])}")
    print(f"   🌐 Con coordenadas: {len(df[df['tiene_coordenadas']])}")
    print(f"   ❓ Necesitan mapeo: {len(df[df['necesita_mapeo']])}")
    
    # Muestra de datos
    if len(df) > 0:
        print(f"\n📋 MUESTRA DE DATOS:")
        muestra = df.head(3)[['submission_id', 'fecha_str', 'usuario', 'location_asignado', 'tiene_coordenadas']]
        print(muestra.to_string(index=False))
    
    return df

def main():
    """Función principal"""
    
    print("🚀 EXTRAER API ZENPUT CORREGIDO")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Usar formato correcto: data en lugar de submissions")
    print("=" * 80)
    
    # Form templates
    form_operativas = 877138
    form_seguridad = 877139
    
    # 1. Extraer operativas
    submissions_ops = extraer_submissions_zenput_v3(form_operativas, "operativas")
    
    # 2. Extraer seguridad
    submissions_seg = extraer_submissions_zenput_v3(form_seguridad, "seguridad")
    
    # 3. Procesar operativas
    df_ops = procesar_submissions_a_dataframe(submissions_ops, "operativas")
    
    # 4. Procesar seguridad
    df_seg = procesar_submissions_a_dataframe(submissions_seg, "seguridad")
    
    # 5. Guardar datos
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if len(df_ops) > 0:
        df_ops.to_csv(f"OPERATIVAS_API_ZENPUT_{timestamp}.csv", index=False, encoding='utf-8')
        print(f"✅ Operativas guardadas: OPERATIVAS_API_ZENPUT_{timestamp}.csv")
    
    if len(df_seg) > 0:
        df_seg.to_csv(f"SEGURIDAD_API_ZENPUT_{timestamp}.csv", index=False, encoding='utf-8')
        print(f"✅ Seguridad guardadas: SEGURIDAD_API_ZENPUT_{timestamp}.csv")
    
    # 6. Resumen final
    print(f"\n🎯 RESUMEN FINAL:")
    print(f"   🏗️ Operativas extraídas: {len(df_ops)}")
    print(f"   🛡️ Seguridad extraídas: {len(df_seg)}")
    print(f"   📊 Total submissions: {len(df_ops) + len(df_seg)}")
    
    if len(df_seg) > 0:
        seg_sin_location = df_seg[df_seg['necesita_mapeo']]
        print(f"   ❓ Seguridad sin location: {len(seg_sin_location)} (problema a resolver)")
    
    print(f"\n✅ EXTRACCIÓN API COMPLETADA EXITOSAMENTE")
    
    return df_ops, df_seg

if __name__ == "__main__":
    main()