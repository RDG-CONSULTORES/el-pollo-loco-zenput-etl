#!/usr/bin/env python3
"""
PRUEBA CORREGIDA - FORM 877138 SUPERVISIÓN OPERATIVA  
Buscar campos HEADER: PUNTOS MAXIMOS, PUNTOS TOTALES, PORCENTAJE %
"""

import json
import os
import sys
from datetime import datetime
import pandas as pd

# Agregar el directorio src al path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from zenput_api import ZenputAPIClient

def test_form_877138_header_structure():
    """Prueba corregida para encontrar campos del HEADER"""
    
    print("🧪 PRUEBA CORREGIDA - FORM 877138 HEADER")
    print("=" * 50)
    
    # 1. CONECTAR API
    api_key = "cb908e0d4e0f5501c635325c611db314"
    client = ZenputAPIClient(api_key)
    
    print("\n📡 Conectando a Zenput API...")
    
    # 2. OBTENER SUPERVISIONES 877138
    try:
        print("🔍 Buscando supervisiones Form 877138...")
        submissions = client.get_submissions_for_form(
            form_id="877138", 
            days_back=15  # Últimos 15 días para más datos
        )
        
        if not submissions:
            print("❌ No se encontraron supervisiones Form 877138")
            return False
            
        print(f"✅ Encontradas {len(submissions)} supervisiones")
        
    except Exception as e:
        print(f"❌ Error conectando API: {e}")
        return False
    
    # 3. BUSCAR CAMPOS HEADER ESPECÍFICOS
    print(f"\n🔍 BUSCANDO CAMPOS HEADER EN {len(submissions)} SUPERVISIONES")
    print("-" * 50)
    
    header_fields_found = {}
    calificaciones_encontradas = []
    
    for i, submission in enumerate(submissions[:5], 1):  # Solo primeras 5 para prueba
        print(f"\n🏢 SUPERVISIÓN {i}/5")
        print(f"ID: {submission.get('id', 'N/A')}")
        
        if 'answers' not in submission:
            print(f"⚠️ Sin respuestas")
            continue
            
        # BUSCAR CAMPOS ESPECÍFICOS DEL HEADER
        puntos_maximos = None
        puntos_totales = None  
        porcentaje = None
        
        print(f"\n🔍 BUSCANDO CAMPOS HEADER:")
        
        for answer in submission['answers']:
            title = answer.get('title', '').strip()
            value = answer.get('value')
            field_type = answer.get('field_type', '')
            
            # Guardar todos los campos para análisis
            if title not in header_fields_found:
                header_fields_found[title] = []
            header_fields_found[title].append({
                'value': value,
                'field_type': field_type,
                'submission': i
            })
            
            # BUSCAR CAMPOS HEADER ESPECÍFICOS (según Roberto)
            if 'PUNTOS MAXIMOS' in title:
                puntos_maximos = value
                print(f"   ✅ PUNTOS MAXIMOS: {title} = {value}")
                
            elif 'PUNTOS TOTALES' in title:
                puntos_totales = value  
                print(f"   ✅ PUNTOS TOTALES: {title} = {value}")
                
            elif title == 'PORCENTAJE %':  # Campo exacto
                porcentaje = value
                print(f"   ✅ PORCENTAJE %: {title} = {value}")
        
        # VALIDAR SI ENCONTRAMOS LOS 3 CAMPOS
        if puntos_maximos and puntos_totales and porcentaje:
            calificacion_completa = {
                'submission_id': submission.get('id'),
                'puntos_maximos': puntos_maximos,
                'puntos_totales': puntos_totales, 
                'porcentaje': porcentaje
            }
            calificaciones_encontradas.append(calificacion_completa)
            print(f"   🎯 CALIFICACIÓN COMPLETA ENCONTRADA:")
            print(f"      Máximos: {puntos_maximos}, Totales: {puntos_totales}, %: {porcentaje}")
        else:
            print(f"   ❌ CALIFICACIÓN INCOMPLETA:")
            print(f"      Máximos: {puntos_maximos}, Totales: {puntos_totales}, %: {porcentaje}")
    
    # 4. ANÁLISIS DE CAMPOS ENCONTRADOS
    print(f"\n📊 ANÁLISIS DE CAMPOS HEADER")
    print("=" * 50)
    
    print(f"\n🎯 CALIFICACIONES COMPLETAS ENCONTRADAS: {len(calificaciones_encontradas)}")
    if calificaciones_encontradas:
        print(f"   Supervisiones exitosas: {len(calificaciones_encontradas)}/5")
        
        for cal in calificaciones_encontradas:
            print(f"   📊 {cal['submission_id'][:8]}: {cal['puntos_totales']}/{cal['puntos_maximos']} = {cal['porcentaje']}%")
    
    # 5. BUSCAR PATRONES ALTERNATIVOS SI NO ENCONTRAMOS LOS 3
    print(f"\n🔍 TODOS LOS CAMPOS CON 'PUNTOS' O 'PORCENTAJE':")
    campos_relevantes = {}
    
    for campo, valores in header_fields_found.items():
        if 'PUNTOS' in campo or 'PORCENTAJE' in campo or campo == '':
            promedios = [v['value'] for v in valores if v['value'] is not None]
            if promedios:
                promedio = sum(promedios) / len(promedios) if isinstance(promedios[0], (int, float)) else 'N/A'
                print(f"   📊 '{campo}': {promedio} (en {len(valores)} supervisiones)")
                campos_relevantes[campo] = {
                    'promedio': promedio,
                    'apariciones': len(valores),
                    'ejemplo_valor': promedios[0] if promedios else None
                }
    
    # 6. GENERAR RECOMENDACIONES
    print(f"\n🚀 RECOMENDACIONES PARA ROBERTO:")
    print("-" * 40)
    
    if len(calificaciones_encontradas) >= 3:
        print(f"✅ ESTRUCTURA HEADER CONFIRMADA:")
        print(f"   • 'PUNTOS MAXIMOS': Campo encontrado")
        print(f"   • 'PUNTOS TOTALES': Campo encontrado") 
        print(f"   • 'PORCENTAJE %': Campo encontrado")
        print(f"✅ LISTO PARA RAILWAY CON ESTRUCTURA CORRECTA")
        
        # Mostrar estructura SQL recomendada
        print(f"\n📋 ESTRUCTURA POSTGRESQL RECOMENDADA:")
        print("CREATE TABLE supervision_operativa (")
        print("    id SERIAL PRIMARY KEY,")
        print("    submission_id TEXT UNIQUE,") 
        print("    sucursal_nombre VARCHAR(100),")
        print("    fecha_supervision TIMESTAMP,")
        print("    -- CALIFICACION GENERAL (HEADER)")
        print("    puntos_maximos INTEGER,")
        print("    puntos_totales INTEGER,")
        print("    calificacion_porcentaje DECIMAL(5,2),")
        print("    -- AREAS OPERATIVAS (28 areas)")
        print("    area_marinado DECIMAL(5,2),")
        print("    area_cuarto_frio_1 DECIMAL(5,2),")
        print("    area_refrigeradores DECIMAL(5,2),")
        print("    -- ... [25 areas más]")
        print("    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        print(");")
        
        return True
        
    else:
        print(f"⚠️ ESTRUCTURA HEADER INCOMPLETA:")
        print(f"   Encontradas {len(calificaciones_encontradas)}/5 calificaciones completas")
        print(f"💡 CAMPOS ALTERNATIVOS DETECTADOS:")
        
        for campo, info in campos_relevantes.items():
            if info['apariciones'] >= 3:
                print(f"   • '{campo}': {info['ejemplo_valor']} (en {info['apariciones']} supervisiones)")
        
        return False

if __name__ == "__main__":
    success = test_form_877138_header_structure()
    
    if success:
        print(f"\n🎉 ESTRUCTURA CONFIRMADA - PROCEDER CON RAILWAY")
    else:
        print(f"\n❌ REVISAR PATRONES ANTES DE RAILWAY")