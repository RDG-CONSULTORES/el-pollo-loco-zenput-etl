#!/usr/bin/env python3
"""
⚡ ETL MUESTRA RÁPIDA
Obtener solo una muestra pequeña para validar que funciona
"""

import requests
import csv
import math
import json
from datetime import datetime
from collections import defaultdict

# Configuración
ZENPUT_CONFIG = {
    'base_url': 'https://www.zenput.com/api/v3',
    'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}
}

FORMULARIOS_2025 = {
    '877138': 'OPERATIVA',
    '877139': 'SEGURIDAD'
}

REGLAS_MAXIMAS = {
    'LOCAL': {'operativas': 4, 'seguridad': 4},
    'FORANEA': {'operativas': 2, 'seguridad': 2}
}

GRUPOS_LOCALES = ['OGAS', 'TEC', 'TEPEYAC', 'PLOG NUEVO LEON', 'GRUPO CENTRITO', 'GRUPO SALTILLO']

def cargar_sucursales_master():
    """Cargar sucursales master"""
    sucursales = {}
    
    with open('data/86_sucursales_master.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['Latitude'] and row['Longitude']:
                sucursales[row['Nombre_Sucursal']] = {
                    'numero': int(row['Numero_Sucursal']),
                    'nombre': row['Nombre_Sucursal'],
                    'grupo': row['Grupo_Operativo'],
                    'lat': float(row['Latitude']),
                    'lon': float(row['Longitude']),
                    'tipo': 'LOCAL' if row['Grupo_Operativo'] in GRUPOS_LOCALES else 'FORANEA'
                }
    
    return sucursales

def obtener_muestra_submissions():
    """Obtener solo una muestra pequeña"""
    
    print("⚡ OBTENIENDO MUESTRA RÁPIDA")
    print("=" * 40)
    
    muestra = []
    
    for form_id, tipo_form in FORMULARIOS_2025.items():
        print(f"\n📋 Form {form_id} ({tipo_form})")
        
        try:
            url = f"{ZENPUT_CONFIG['base_url']}/submissions"
            params = {
                'form_template_id': form_id,
                'created_after': '2025-01-01T00:00:00Z',
                'created_before': '2025-12-31T23:59:59Z',
                'page': 1,
                'page_size': 10  # Solo 10 por formulario
            }
            
            response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                submissions = data.get('data', [])
                
                print(f"✅ {len(submissions)} submissions")
                
                for submission in submissions:
                    submission['form_type'] = tipo_form
                    muestra.append(submission)
                    
            else:
                print(f"❌ Error {response.status_code}")
                
        except Exception as e:
            print(f"💥 Error: {e}")
    
    print(f"\n📊 Total muestra: {len(muestra)}")
    return muestra

def procesar_muestra(muestra, sucursales_master):
    """Procesar la muestra"""
    
    print(f"\n🔄 PROCESANDO MUESTRA")
    print("=" * 40)
    
    resultados = []
    
    for submission in muestra:
        # Extraer datos básicos
        datos = {
            'submission_id': submission.get('id'),
            'form_type': submission.get('form_type'),
            'location_name': None,
            'lat': None,
            'lon': None,
            'usuario': None,
            'fecha': None
        }
        
        # Location
        if 'location' in submission:
            location = submission['location']
            datos['lat'] = location.get('lat')
            datos['lon'] = location.get('lon')
            datos['location_name'] = location.get('name')
        
        # Usuario
        if 'user' in submission:
            datos['usuario'] = submission['user'].get('name')
        
        # Buscar fecha
        for field in ['created_at', 'updated_at', 'submitted_at']:
            if field in submission and submission[field]:
                try:
                    fecha_str = submission[field]
                    if 'T' in fecha_str:
                        datos['fecha'] = datetime.fromisoformat(fecha_str.replace('Z', '+00:00')).date()
                        break
                except:
                    continue
        
        # Mapear a sucursal si tenemos coordenadas
        if datos['lat'] and datos['lon']:
            distancia_minima = float('inf')
            sucursal_mapeada = None
            
            for sucursal_key, sucursal_data in sucursales_master.items():
                try:
                    distancia = math.sqrt((float(datos['lat']) - float(sucursal_data['lat']))**2 + 
                                        (float(datos['lon']) - float(sucursal_data['lon']))**2)
                    
                    if distancia <= 0.01 and distancia < distancia_minima:
                        distancia_minima = distancia
                        sucursal_mapeada = sucursal_data
                        
                except:
                    continue
            
            if sucursal_mapeada:
                datos['sucursal_mapeada'] = sucursal_mapeada['nombre']
                datos['grupo_operativo'] = sucursal_mapeada['grupo']
                datos['tipo_sucursal'] = sucursal_mapeada['tipo']
                datos['distancia_mapeo'] = distancia_minima
        
        resultados.append(datos)
    
    return resultados

def mostrar_resultados(resultados):
    """Mostrar resultados de la muestra"""
    
    print(f"\n📊 RESULTADOS MUESTRA")
    print("=" * 50)
    
    mapeados = [r for r in resultados if 'sucursal_mapeada' in r]
    sin_mapear = [r for r in resultados if 'sucursal_mapeada' not in r]
    
    print(f"✅ Mapeados: {len(mapeados)}")
    print(f"❌ Sin mapear: {len(sin_mapear)}")
    
    if mapeados:
        print(f"\n🎯 SUBMISSIONS MAPEADAS:")
        for r in mapeados:
            print(f"   {r['form_type']} - {r['sucursal_mapeada']} ({r['tipo_sucursal']})")
            print(f"      Usuario: {r['usuario']}, Fecha: {r['fecha']}")
            print(f"      Coordenadas: {r['lat']:.4f}, {r['lon']:.4f}")
            print()
    
    # Agrupar por sucursal
    por_sucursal = defaultdict(lambda: {'operativas': 0, 'seguridad': 0, 'tipo': None})
    
    for r in mapeados:
        if 'sucursal_mapeada' in r:
            sucursal = r['sucursal_mapeada']
            tipo_form = r['form_type'].lower()
            
            por_sucursal[sucursal][tipo_form + 's'] += 1
            por_sucursal[sucursal]['tipo'] = r['tipo_sucursal']
    
    if por_sucursal:
        print(f"📏 CUMPLIMIENTO DE REGLAS:")
        for sucursal, datos in por_sucursal.items():
            tipo = datos['tipo']
            ops = datos['operativas']
            segs = datos['seguridad']
            
            max_ops = REGLAS_MAXIMAS[tipo]['operativas']
            max_segs = REGLAS_MAXIMAS[tipo]['seguridad']
            
            cumple = ops <= max_ops and segs <= max_segs
            status = "✅" if cumple else "❌"
            
            print(f"   {status} {sucursal} ({tipo}): {ops}/{max_ops} Op, {segs}/{max_segs} Seg")
    
    return resultados

def main():
    """Función principal"""
    
    print("⚡ ETL MUESTRA RÁPIDA 2025")
    print("=" * 60)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # 1. Cargar sucursales
    sucursales_master = cargar_sucursales_master()
    print(f"📊 Sucursales master: {len(sucursales_master)}")
    
    # 2. Obtener muestra
    muestra = obtener_muestra_submissions()
    
    if not muestra:
        print("❌ No se obtuvo muestra")
        return
    
    # 3. Procesar
    resultados = procesar_muestra(muestra, sucursales_master)
    
    # 4. Mostrar resultados
    mostrar_resultados(resultados)
    
    # 5. Guardar muestra
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_file = f"muestra_rapida_{timestamp}.json"
    
    with open(json_file, 'w') as f:
        json.dump(resultados, f, indent=2, default=str)
    
    print(f"\n💾 Muestra guardada: {json_file}")
    print("🎉 MUESTRA COMPLETADA")

if __name__ == "__main__":
    main()