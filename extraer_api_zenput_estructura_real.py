#!/usr/bin/env python3
"""
🚀 EXTRAER API ZENPUT ESTRUCTURA REAL
Usar estructura real detectada: smetadata campos principales
"""

import requests
import pandas as pd
import json
import time
from datetime import datetime

def extraer_submissions_zenput_estructura_real(form_template_id, tipo_supervision):
    """Extraer submissions usando estructura real detectada"""
    
    print(f"📥 EXTRAYENDO {tipo_supervision.upper()} - ESTRUCTURA REAL")
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
                
                # Usar estructura real: data field
                submissions_batch = data.get('data', [])
                
                if not submissions_batch:
                    print(f"✅ No más datos. Total extraídas: {len(todas_submissions)}")
                    break
                
                # No filtrar por año - extraer todo
                todas_submissions.extend(submissions_batch)
                
                # Analizar fechas de esta batch
                fechas_batch = []
                for submission in submissions_batch:
                    smetadata = submission.get('smetadata', {})
                    date_submitted = smetadata.get('date_submitted')
                    if date_submitted:
                        try:
                            fecha_dt = pd.to_datetime(date_submitted)
                            fechas_batch.append(fecha_dt.year)
                        except:
                            pass
                
                if fechas_batch:
                    años_batch = pd.Series(fechas_batch).value_counts()
                    print(f"   ✅ Batch: {len(submissions_batch)} submissions")
                    print(f"   📅 Años en batch: {dict(años_batch)}")
                else:
                    print(f"   ✅ Batch: {len(submissions_batch)} submissions (sin fechas procesables)")
                
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
    print(f"   ✅ Total submissions extraídas: {len(todas_submissions)}")
    
    # Analizar años totales
    if todas_submissions:
        fechas_todas = []
        for submission in todas_submissions:
            smetadata = submission.get('smetadata', {})
            date_submitted = smetadata.get('date_submitted')
            if date_submitted:
                try:
                    fecha_dt = pd.to_datetime(date_submitted)
                    fechas_todas.append(fecha_dt.year)
                except:
                    pass
        
        if fechas_todas:
            años_totales = pd.Series(fechas_todas).value_counts().sort_index()
            print(f"   📅 Distribución por años: {dict(años_totales)}")
    
    return todas_submissions

def procesar_submissions_estructura_real(submissions, tipo):
    """Procesar submissions con estructura real detectada"""
    
    print(f"\n🔄 PROCESANDO {tipo.upper()} - ESTRUCTURA REAL")
    print("=" * 50)
    
    datos_procesados = []
    
    for i, submission in enumerate(submissions, 1):
        try:
            # Datos básicos de nivel superior
            submission_id = submission.get('id')
            
            # Datos de smetadata (aquí están los datos principales)
            smetadata = submission.get('smetadata', {})
            
            # Fechas
            date_submitted = smetadata.get('date_submitted')
            date_created = smetadata.get('date_created')
            date_completed = smetadata.get('date_completed')
            
            # Usuario - está en created_by dentro de smetadata
            created_by = smetadata.get('created_by', {})
            usuario = created_by.get('name', 'DESCONOCIDO') if created_by else 'DESCONOCIDO'
            
            # Location - está en location dentro de smetadata
            location_info = smetadata.get('location', {})
            location_name = location_info.get('name') if location_info else None
            
            # Coordenadas de entrega - están en smetadata
            lat_entrega = smetadata.get('lat')
            lon_entrega = smetadata.get('lon')
            
            # Respuestas del formulario - están en answers
            answers = submission.get('answers', [])
            sucursal_campo = None
            location_map = None
            
            # Buscar campo Sucursal y Location Map en answers
            for answer in answers:
                question_name = answer.get('question', {}).get('name', '').lower()
                answer_value = answer.get('answer')
                
                if 'sucursal' in question_name:
                    sucursal_campo = answer_value
                elif 'location' in question_name and 'map' in question_name:
                    location_map = answer_value
            
            # Procesar fecha principal
            fecha_dt = pd.to_datetime(date_submitted) if date_submitted else None
            
            datos_procesados.append({
                'submission_id': submission_id,
                'date_submitted': date_submitted,
                'date_created': date_created,
                'date_completed': date_completed,
                'fecha': fecha_dt,
                'fecha_str': fecha_dt.strftime('%Y-%m-%d') if fecha_dt else None,
                'año': fecha_dt.year if fecha_dt else None,
                'usuario': usuario,
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
    
    # Análisis por años
    if len(df) > 0 and 'año' in df.columns:
        años_df = df['año'].value_counts().sort_index()
        print(f"   📅 Por años: {dict(años_df)}")
    
    # Muestra de datos
    if len(df) > 0:
        print(f"\n📋 MUESTRA DE DATOS:")
        muestra = df.head(3)[['submission_id', 'fecha_str', 'año', 'usuario', 'location_asignado', 'tiene_coordenadas']]
        print(muestra.to_string(index=False))
    
    return df

def filtrar_por_year(df, year_target=2025):
    """Filtrar DataFrame por año específico"""
    
    print(f"\n🔍 FILTRAR POR AÑO {year_target}")
    print("=" * 40)
    
    df_year = df[df['año'] == year_target].copy()
    
    print(f"📊 FILTRO AÑO {year_target}:")
    print(f"   ✅ Total {year_target}: {len(df_year)}")
    print(f"   📍 Con location: {len(df_year[df_year['tiene_location']])}")
    print(f"   ❓ Sin location: {len(df_year[df_year['necesita_mapeo']])}")
    
    return df_year

def main():
    """Función principal"""
    
    print("🚀 EXTRAER API ZENPUT ESTRUCTURA REAL")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Usar estructura real del API: smetadata, data, answers")
    print("=" * 80)
    
    # Form templates
    form_operativas = 877138
    form_seguridad = 877139
    
    # 1. Extraer operativas (todas, sin filtrar por año)
    submissions_ops = extraer_submissions_zenput_estructura_real(form_operativas, "operativas")
    
    # 2. Extraer seguridad (todas, sin filtrar por año)  
    submissions_seg = extraer_submissions_zenput_estructura_real(form_seguridad, "seguridad")
    
    # 3. Procesar operativas con estructura real
    df_ops_all = procesar_submissions_estructura_real(submissions_ops, "operativas")
    
    # 4. Procesar seguridad con estructura real
    df_seg_all = procesar_submissions_estructura_real(submissions_seg, "seguridad")
    
    # 5. Filtrar por 2025 (si existen datos 2025)
    if len(df_ops_all) > 0 and df_ops_all['año'].notna().any():
        años_disponibles_ops = sorted(df_ops_all['año'].dropna().unique())
        print(f"\n📅 Años disponibles OPERATIVAS: {años_disponibles_ops}")
        
        if 2025 in años_disponibles_ops:
            df_ops_2025 = filtrar_por_year(df_ops_all, 2025)
        else:
            print(f"⚠️ No hay datos 2025 en operativas. Usar año más reciente: {max(años_disponibles_ops)}")
            df_ops_2025 = filtrar_por_year(df_ops_all, max(años_disponibles_ops))
    else:
        df_ops_2025 = df_ops_all
        
    if len(df_seg_all) > 0 and df_seg_all['año'].notna().any():
        años_disponibles_seg = sorted(df_seg_all['año'].dropna().unique())
        print(f"\n📅 Años disponibles SEGURIDAD: {años_disponibles_seg}")
        
        if 2025 in años_disponibles_seg:
            df_seg_2025 = filtrar_por_year(df_seg_all, 2025)
        else:
            print(f"⚠️ No hay datos 2025 en seguridad. Usar año más reciente: {max(años_disponibles_seg)}")
            df_seg_2025 = filtrar_por_year(df_seg_all, max(años_disponibles_seg))
    else:
        df_seg_2025 = df_seg_all
    
    # 6. Guardar datos
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Guardar datos completos (todos los años)
    if len(df_ops_all) > 0:
        df_ops_all.to_csv(f"OPERATIVAS_API_ALL_YEARS_{timestamp}.csv", index=False, encoding='utf-8')
        print(f"✅ Operativas (todos años): OPERATIVAS_API_ALL_YEARS_{timestamp}.csv")
    
    if len(df_seg_all) > 0:
        df_seg_all.to_csv(f"SEGURIDAD_API_ALL_YEARS_{timestamp}.csv", index=False, encoding='utf-8')
        print(f"✅ Seguridad (todos años): SEGURIDAD_API_ALL_YEARS_{timestamp}.csv")
    
    # Guardar datos filtrados por año
    if len(df_ops_2025) > 0:
        año_ops = df_ops_2025['año'].iloc[0] if len(df_ops_2025) > 0 else 'NA'
        df_ops_2025.to_csv(f"OPERATIVAS_API_{año_ops}_{timestamp}.csv", index=False, encoding='utf-8')
        print(f"✅ Operativas {año_ops}: OPERATIVAS_API_{año_ops}_{timestamp}.csv")
    
    if len(df_seg_2025) > 0:
        año_seg = df_seg_2025['año'].iloc[0] if len(df_seg_2025) > 0 else 'NA'
        df_seg_2025.to_csv(f"SEGURIDAD_API_{año_seg}_{timestamp}.csv", index=False, encoding='utf-8')
        print(f"✅ Seguridad {año_seg}: SEGURIDAD_API_{año_seg}_{timestamp}.csv")
    
    # 7. Resumen final
    print(f"\n🎯 RESUMEN FINAL:")
    print(f"   🏗️ Operativas extraídas (total): {len(df_ops_all)}")
    print(f"   🛡️ Seguridad extraídas (total): {len(df_seg_all)}")
    print(f"   📊 Total submissions: {len(df_ops_all) + len(df_seg_all)}")
    
    if len(df_ops_2025) > 0 or len(df_seg_2025) > 0:
        año_target = df_ops_2025['año'].iloc[0] if len(df_ops_2025) > 0 else df_seg_2025['año'].iloc[0]
        print(f"   🎯 Operativas {año_target}: {len(df_ops_2025)}")
        print(f"   🎯 Seguridad {año_target}: {len(df_seg_2025)}")
        
        if len(df_seg_2025) > 0:
            seg_sin_location = df_seg_2025[df_seg_2025['necesita_mapeo']]
            print(f"   ❓ Seguridad sin location: {len(seg_sin_location)} (problema a resolver)")
    
    print(f"\n✅ EXTRACCIÓN API CON ESTRUCTURA REAL COMPLETADA")
    print(f"🔧 Próximo paso: Resolver mapeo de submissions sin location")
    
    return df_ops_all, df_seg_all, df_ops_2025, df_seg_2025

if __name__ == "__main__":
    main()