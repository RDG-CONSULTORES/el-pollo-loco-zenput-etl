#!/usr/bin/env python3
"""
PRUEBA PEQUEÑA - FORM 877138 SUPERVISIÓN OPERATIVA
Validar estructura exacta antes de Railway PostgreSQL
"""

import json
import os
import sys
from datetime import datetime
import pandas as pd

# Agregar el directorio src al path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from zenput_api import ZenputAPIClient

def test_form_877138_structure():
    """Prueba pequeña para validar estructura Form 877138"""
    
    print("🧪 INICIANDO PRUEBA FORM 877138 - SUPERVISIÓN OPERATIVA")
    print("=" * 60)
    
    # 1. CONECTAR API
    api_key = "cb908e0d4e0f5501c635325c611db314"
    client = ZenputAPIClient(api_key)
    
    print("\n📡 Conectando a Zenput API...")
    
    # 2. OBTENER ALGUNAS SUPERVISIONES 877138
    try:
        print("🔍 Buscando supervisiones Form 877138...")
        submissions = client.get_submissions_for_form(
            form_id="877138", 
            days_back=30  # Últimos 30 días
        )
        
        if not submissions:
            print("❌ No se encontraron supervisiones Form 877138")
            return False
            
        print(f"✅ Encontradas {len(submissions)} supervisiones para análisis")
        
    except Exception as e:
        print(f"❌ Error conectando API: {e}")
        return False
    
    # 3. ANALIZAR ESTRUCTURA DETALLADA
    print(f"\n📊 ANALIZANDO ESTRUCTURA DE {len(submissions)} SUPERVISIONES")
    print("-" * 60)
    
    campos_encontrados = {}
    areas_encontradas = {}
    calificaciones_generales = []
    
    for i, submission in enumerate(submissions, 1):
        print(f"\n🏢 SUPERVISIÓN {i}/{len(submissions)}")
        print(f"ID: {submission.get('id', 'N/A')}")
        print(f"Sucursal: {submission.get('location_name', 'N/A')}")
        print(f"Fecha: {submission.get('submitted_at', 'N/A')}")
        
        # Los datos ya vienen en la submission
        try:
            if 'answers' not in submission:
                print(f"⚠️ Sin respuestas (answers) para supervisión {i}")
                continue
                
            # BUSCAR CALIFICACIÓN GENERAL
            calificacion_general = None
            areas_porcentajes = {}
            
            for answer in submission['answers']:
                title = answer.get('title', '').strip()
                value = answer.get('value')
                field_type = answer.get('field_type', '')
                
                # CALIFICACIÓN GENERAL (Roberto confirmó que viene con %)
                if 'PORCENTAJE %' in title and 'SUPERVISION OPERATIVA' in title:
                    calificacion_general = value
                    print(f"✅ CALIFICACIÓN GENERAL: {title} = {value}%")
                
                # ÁREAS INDIVIDUALES (buscar todas las que terminan en PORCENTAJE %)
                elif 'PORCENTAJE %' in title and 'SUPERVISION OPERATIVA' not in title:
                    area_name = title.replace('PORCENTAJE %', '').replace('CALIFICACION', '').strip()
                    areas_porcentajes[area_name] = value
                    
                    if area_name not in areas_encontradas:
                        areas_encontradas[area_name] = []
                    areas_encontradas[area_name].append(value)
                    
                    print(f"   📊 {area_name}: {value}%")
                
                # GUARDAR TODOS LOS CAMPOS PARA ANÁLISIS
                if title not in campos_encontrados:
                    campos_encontrados[title] = []
                campos_encontrados[title].append({
                    'value': value,
                    'field_type': field_type,
                    'submission': i
                })
            
            if calificacion_general is not None:
                calificaciones_generales.append(calificacion_general)
                print(f"   🎯 Calificación General: {calificacion_general}%")
            else:
                print(f"   ❌ NO se encontró calificación general")
                
            print(f"   📊 Áreas encontradas: {len(areas_porcentajes)}")
            
        except Exception as e:
            print(f"❌ Error procesando supervisión {i}: {e}")
            continue
    
    # 4. GENERAR REPORTE DE ESTRUCTURA
    print(f"\n📋 REPORTE FINAL DE ESTRUCTURA")
    print("=" * 60)
    
    print(f"\n🎯 CALIFICACIONES GENERALES:")
    print(f"   Supervisiones con calificación general: {len(calificaciones_generales)}")
    if calificaciones_generales:
        print(f"   Promedio: {sum(calificaciones_generales)/len(calificaciones_generales):.2f}%")
        print(f"   Rango: {min(calificaciones_generales):.1f}% - {max(calificaciones_generales):.1f}%")
    
    print(f"\n📊 ÁREAS OPERATIVAS ENCONTRADAS:")
    print(f"   Total áreas únicas: {len(areas_encontradas)}")
    
    for area, valores in sorted(areas_encontradas.items()):
        promedio = sum(v for v in valores if v is not None) / len(valores) if valores else 0
        print(f"   {area}: {promedio:.1f}% (en {len(valores)} supervisiones)")
    
    print(f"\n🔍 CAMPOS RELEVANTES ENCONTRADOS:")
    campos_porcentaje = [campo for campo in campos_encontrados.keys() if 'PORCENTAJE' in campo or '%' in campo]
    print(f"   Campos con 'PORCENTAJE' o '%': {len(campos_porcentaje)}")
    
    for campo in sorted(campos_porcentaje)[:10]:  # Mostrar solo primeros 10
        valores = [item['value'] for item in campos_encontrados[campo] if item['value'] is not None]
        if valores:
            promedio = sum(valores) / len(valores)
            print(f"   {campo}: promedio {promedio:.1f}%")
    
    # 5. GENERAR CSV DE PRUEBA
    print(f"\n💾 GENERANDO CSV DE PRUEBA...")
    
    csv_data = []
    for i, submission in enumerate(submissions):
        try:
            if 'answers' not in submission:
                continue
                
            # Crear fila base
            row = {
                'submission_id': submission['id'],
                'sucursal_nombre': submission.get('location_name', ''),
                'fecha_supervision': submission.get('submitted_at', ''),
                'calificacion_general': None
            }
            
            # Extraer calificación general y áreas
            for answer in submission['answers']:
                title = answer.get('title', '').strip()
                value = answer.get('value')
                
                if 'PORCENTAJE %' in title and 'SUPERVISION OPERATIVA' in title:
                    row['calificacion_general'] = value
                elif 'PORCENTAJE %' in title:
                    area_name = title.replace('PORCENTAJE %', '').replace('CALIFICACION', '').strip()
                    # Limpiar nombre de área para CSV
                    area_clean = area_name.replace(' ', '_').replace('/', '_').lower()
                    row[f'area_{area_clean}'] = value
            
            csv_data.append(row)
            
        except Exception as e:
            print(f"⚠️ Error procesando supervisión {i+1} para CSV: {e}")
            continue
    
    # Guardar CSV
    if csv_data:
        df = pd.DataFrame(csv_data)
        csv_path = f"data/test_form_877138_structure_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        # Crear directorio si no existe
        os.makedirs('data', exist_ok=True)
        
        df.to_csv(csv_path, index=False)
        print(f"✅ CSV guardado: {csv_path}")
        print(f"   Filas: {len(df)}")
        print(f"   Columnas: {len(df.columns)}")
        
        # Mostrar muestra de datos
        print(f"\n👀 MUESTRA DE DATOS:")
        if len(df) > 0:
            print(df.head().to_string())
        else:
            print("Sin datos para mostrar")
    
    # 6. RECOMENDACIONES PARA RAILWAY
    print(f"\n🚀 RECOMENDACIONES PARA RAILWAY:")
    print("-" * 40)
    
    if len(areas_encontradas) >= 25:  # Roberto espera ~31 áreas
        print(f"✅ Estructura sólida: {len(areas_encontradas)} áreas encontradas")
        print(f"✅ Calificaciones generales: {len(calificaciones_generales)} supervisiones")
        print(f"✅ LISTO PARA RAILWAY POSTGRESQL")
        
        print(f"\n📋 CAMPOS POSTGRESQL RECOMENDADOS:")
        print("CREATE TABLE supervision_operativa (")
        print("    id SERIAL PRIMARY KEY,")
        print("    submission_id TEXT UNIQUE,")
        print("    sucursal_nombre VARCHAR(100),")
        print("    fecha_supervision TIMESTAMP,")
        print("    calificacion_general DECIMAL(5,2),")
        
        # Generar campos para cada área encontrada
        for i, area in enumerate(sorted(areas_encontradas.keys())[:31], 1):
            area_clean = area.replace(' ', '_').replace('/', '_').lower()
            area_clean = ''.join(c for c in area_clean if c.isalnum() or c == '_')
            print(f"    area_{i:02d}_{area_clean[:20]} DECIMAL(5,2), -- {area}")
        
        print("    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        print(");")
        
        return True
        
    else:
        print(f"⚠️ Pocas áreas encontradas: {len(areas_encontradas)} (esperábamos ~31)")
        print(f"💡 Revisar estructura antes de Railway")
        return False

if __name__ == "__main__":
    success = test_form_877138_structure()
    
    if success:
        print(f"\n🎉 PRUEBA EXITOSA - PROCEDER CON RAILWAY")
    else:
        print(f"\n❌ REVISAR ESTRUCTURA ANTES DE RAILWAY")