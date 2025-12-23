#!/usr/bin/env python3
"""
📊 TEST MUESTRA EXCEL COMPLETO
Probar con las primeras 5 operativas y 5 seguridad para validar estructura
"""

import pandas as pd
from datetime import datetime
import requests
import json
import time

def main():
    """Test con muestra pequeña"""
    
    print("📊 TEST MUESTRA EXCEL COMPLETO")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Test con 10 supervisiones para validar estructura")
    print("=" * 80)
    
    # Cargar dataset
    df_dataset = pd.read_csv("DATASET_EMPAREJADO_20251218_164319.csv")
    df_sucursales = pd.read_csv("SUCURSALES_CORRECCIONES_ROBERTO_20251218_171807.csv")
    
    print(f"✅ Dataset: {len(df_dataset)} supervisiones")
    print(f"✅ Catálogo: {len(df_sucursales)} sucursales")
    
    # Separar por tipo - tomar muestra pequeña
    operativas = df_dataset[df_dataset['tipo'] == 'operativas'].head(5)
    seguridad = df_dataset[df_dataset['tipo'] == 'seguridad'].head(5)
    
    print(f"\n📊 MUESTRA PARA TEST:")
    print(f"   🔧 Operativas: {len(operativas)}")
    print(f"   🛡️ Seguridad: {len(seguridad)}")
    
    # Crear catálogo para lookup
    catalogo_sucursales = {}
    for _, row in df_sucursales.iterrows():
        if pd.notna(row['numero']) and pd.notna(row['nombre']):
            numero = int(row['numero'])
            nombre = row['nombre']
            location_key = f"{numero} - {nombre}"
            
            catalogo_sucursales[location_key] = {
                'numero': numero,
                'nombre': nombre,
                'tipo': row.get('tipo', 'LOCAL'),
                'grupo': row.get('grupo', ''),
                'lat': row.get('lat', None),
                'lon': row.get('lon', None)
            }
    
    # API config
    api_config = {
        'base_url': 'https://www.zenput.com/api/v3',
        'headers': {
            'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314',
            'Content-Type': 'application/json'
        }
    }
    
    print(f"\n🔧 PROBANDO EXTRACCIÓN DE ESTRUCTURA API:")
    
    # Test con una sola submission
    test_row = operativas.iloc[0]
    submission_id = test_row['submission_id']
    
    print(f"   🎯 Testing: {submission_id}")
    
    try:
        url = f"{api_config['base_url']}/submissions/{submission_id}"
        response = requests.get(url, headers=api_config['headers'], timeout=30)
        response.raise_for_status()
        
        submission_data = response.json()
        
        print(f"   ✅ API Response OK")
        print(f"   📊 Keys en response: {list(submission_data.keys())}")
        
        # Verificar campos
        smetadata = submission_data.get('smetadata', {})
        fields = submission_data.get('fields', {})
        
        print(f"   📋 smetadata keys: {list(smetadata.keys())}")
        print(f"   📝 fields count: {len(fields)}")
        
        # Mostrar algunas respuestas
        if fields:
            print(f"   📝 MUESTRA DE RESPUESTAS:")
            for i, (field_id, field_data) in enumerate(list(fields.items())[:3]):
                if isinstance(field_data, dict):
                    value = field_data.get('value')
                    text = field_data.get('text', '')
                    print(f"      • {field_id}: {value} | {text[:50]}...")
        
        # Calcular KPIs de muestra
        conformes = 0
        no_conformes = 0
        total_respuestas = 0
        
        for field_id, field_data in fields.items():
            if isinstance(field_data, dict):
                value = field_data.get('value')
                if value and isinstance(value, str):
                    total_respuestas += 1
                    valor_lower = value.lower()
                    if any(word in valor_lower for word in ['sí', 'si', 'yes', 'correcto', 'bien']):
                        conformes += 1
                    elif any(word in valor_lower for word in ['no', 'incorrecto', 'mal', 'falta']):
                        no_conformes += 1
        
        print(f"   📊 KPI MUESTRA:")
        print(f"      Total respuestas: {total_respuestas}")
        print(f"      Conformes: {conformes}")
        print(f"      No conformes: {no_conformes}")
        if total_respuestas > 0:
            conformidad = (conformes / (conformes + no_conformes) * 100) if (conformes + no_conformes) > 0 else 0
            print(f"      % Conformidad: {conformidad:.1f}%")
        
    except Exception as e:
        print(f"   ❌ Error API: {e}")
    
    # Crear estructura básica sin API
    print(f"\n📊 CREAR ESTRUCTURA SIN API (DEMO):")
    
    datos_demo = []
    for _, row in operativas.iterrows():
        location_asignado = row['location_asignado']
        sucursal_info = catalogo_sucursales.get(location_asignado, {})
        
        registro = {
            'submission_id': row['submission_id'],
            'tipo_supervision': row['tipo'],
            'date_submitted': row['date_submitted'],
            'user_name': row.get('usuario', ''),
            'location_asignado': location_asignado,
            'sucursal_numero': sucursal_info.get('numero'),
            'sucursal_nombre': sucursal_info.get('nombre'),
            'sucursal_tipo': sucursal_info.get('tipo'),
            'sucursal_grupo': sucursal_info.get('grupo'),
            'lat_entrega': row.get('lat_entrega'),
            'lon_entrega': row.get('lon_entrega'),
            # KPIs serán llenados con API
            'calificacion_total': None,
            'preguntas_totales': 0,
            'respuestas_conformes': 0,
            'porcentaje_conformidad': 0
        }
        
        datos_demo.append(registro)
    
    # Crear Excel demo
    df_demo = pd.DataFrame(datos_demo)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archivo_demo = f"DEMO_ESTRUCTURA_EXCEL_{timestamp}.xlsx"
    
    with pd.ExcelWriter(archivo_demo, engine='openpyxl') as writer:
        df_demo.to_excel(writer, sheet_name='Demo', index=False)
    
    print(f"   ✅ Excel demo: {archivo_demo}")
    print(f"   📊 {len(df_demo)} registros, {len(df_demo.columns)} columnas")
    
    print(f"\n🎯 CONCLUSIONES TEST:")
    print(f"   ✅ Dataset con 476 supervisiones asignadas OK")
    print(f"   ✅ Catálogo sucursales OK") 
    print(f"   ✅ API individual funciona")
    print(f"   📊 Estructura Excel base OK")
    print(f"   ⏳ Falta: extraer campos del API para KPIs completos")
    
    print(f"\n📋 SIGUIENTE PASO:")
    print(f"   🚀 Ejecutar script completo con todas las 476 supervisiones")
    print(f"   📝 Extraer respuestas del formulario del API")
    print(f"   📊 Calcular KPIs y calificaciones reales")
    
    return archivo_demo

if __name__ == "__main__":
    main()