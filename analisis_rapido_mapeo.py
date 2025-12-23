#!/usr/bin/env python3
"""
⚡ ANÁLISIS RÁPIDO DE MAPEO DE COORDENADAS
Análisis eficiente de submissions para identificar problemas de mapeo
"""

import requests
import csv
import math
import json
from datetime import datetime, date
from collections import defaultdict
import time

ZENPUT_CONFIG = {
    'base_url': 'https://www.zenput.com/api/v3',
    'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}
}

FORMULARIOS_2025 = {
    '877138': 'OPERATIVA',
    '877139': 'SEGURIDAD'
}

def cargar_sucursales_normalizadas():
    """Cargar sucursales con coordenadas normalizadas"""
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
                    'tipo': 'LOCAL' if row['Grupo_Operativo'] in ['OGAS', 'TEC', 'TEPEYAC', 'PLOG NUEVO LEON', 'GRUPO CENTRITO', 'GRUPO SALTILLO'] else 'FORANEA'
                }
    
    return sucursales

def calcular_distancia_km(lat1, lon1, lat2, lon2):
    """Calcular distancia en km"""
    try:
        from math import radians, sin, cos, sqrt, atan2
        
        lat1, lon1, lat2, lon2 = map(radians, [float(lat1), float(lon1), float(lat2), float(lon2)])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1-a))
        R = 6371  # Radio de la Tierra en km
        return R * c
    except:
        return float('inf')

def contar_submissions_totales():
    """Contar total de submissions por formulario"""
    
    print("📊 CONTANDO SUBMISSIONS TOTALES...")
    
    totales = {}
    
    for form_id, tipo_form in FORMULARIOS_2025.items():
        print(f"\n📋 Contando {tipo_form}...")
        
        try:
            url = f"{ZENPUT_CONFIG['base_url']}/submissions"
            params = {
                'form_template_id': form_id,
                'created_after': '2025-01-01T00:00:00Z',
                'created_before': '2025-12-31T23:59:59Z',
                'page': 1,
                'page_size': 1
            }
            
            response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                total = data.get('count', 0)
                totales[form_id] = {
                    'tipo': tipo_form,
                    'total': total,
                    'paginas': math.ceil(total / 100)
                }
                print(f"✅ {tipo_form}: {total} submissions ({totales[form_id]['paginas']} páginas)")
            else:
                print(f"❌ Error {response.status_code}")
                
        except Exception as e:
            print(f"💥 Error: {e}")
    
    return totales

def analizar_muestra_submissions(sucursales_normalizadas, muestra_size=50):
    """Analizar una muestra de submissions para identificar patrones"""
    
    print(f"\n🔍 ANALIZANDO MUESTRA DE {muestra_size} SUBMISSIONS POR FORMULARIO")
    print("=" * 70)
    
    resultados_muestra = {
        'con_location_correcto': 0,
        'con_location_incorrecto': 0,
        'sin_location': 0,
        'sin_coordenadas_entrega': 0,
        'ejemplos_problemas': [],
        'distancias': []
    }
    
    for form_id, tipo_form in FORMULARIOS_2025.items():
        print(f"\n📋 Analizando muestra {tipo_form}...")
        
        try:
            url = f"{ZENPUT_CONFIG['base_url']}/submissions"
            params = {
                'form_template_id': form_id,
                'created_after': '2025-01-01T00:00:00Z',
                'created_before': '2025-12-31T23:59:59Z',
                'page': 1,
                'page_size': muestra_size
            }
            
            response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                submissions = data.get('data', [])
                
                print(f"   📊 Obtenidas: {len(submissions)} submissions")
                
                for i, submission in enumerate(submissions):
                    if i % 10 == 0:
                        print(f"   🔄 Procesando {i+1}/{len(submissions)}")
                    
                    # Extraer metadatos
                    smetadata = submission.get('smetadata', {})
                    
                    # Coordenadas de entrega
                    lat_entrega = smetadata.get('lat')
                    lon_entrega = smetadata.get('lon')
                    
                    # Location asignada
                    location_zenput = smetadata.get('location', {})
                    location_id = location_zenput.get('id')
                    location_name = location_zenput.get('name')
                    
                    # Usuario y fecha
                    created_by = smetadata.get('created_by', {})
                    usuario = created_by.get('display_name')
                    fecha_submitted = smetadata.get('date_submitted')
                    
                    if not lat_entrega or not lon_entrega:
                        resultados_muestra['sin_coordenadas_entrega'] += 1
                        continue
                    
                    if not location_id:
                        resultados_muestra['sin_location'] += 1
                        
                        # Buscar sucursal más cercana
                        mejor_distancia = float('inf')
                        mejor_sucursal = None
                        
                        for sucursal_key, datos_sucursal in sucursales_normalizadas.items():
                            distancia = calcular_distancia_km(
                                lat_entrega, lon_entrega,
                                datos_sucursal['lat'], datos_sucursal['lon']
                            )
                            
                            if distancia < mejor_distancia:
                                mejor_distancia = distancia
                                mejor_sucursal = datos_sucursal['nombre']
                        
                        if mejor_distancia <= 2.0:  # Menos de 2km
                            resultados_muestra['ejemplos_problemas'].append({
                                'submission_id': submission.get('id'),
                                'tipo_form': tipo_form,
                                'usuario': usuario,
                                'fecha': fecha_submitted,
                                'problema': 'SIN_LOCATION',
                                'lat_entrega': lat_entrega,
                                'lon_entrega': lon_entrega,
                                'sucursal_sugerida': mejor_sucursal,
                                'distancia_km': mejor_distancia
                            })
                    
                    else:
                        # Verificar si location es correcta
                        lat_zenput = location_zenput.get('lat')
                        lon_zenput = location_zenput.get('lon')
                        
                        if lat_zenput and lon_zenput:
                            distancia_location = calcular_distancia_km(
                                lat_entrega, lon_entrega, lat_zenput, lon_zenput
                            )
                            
                            resultados_muestra['distancias'].append(distancia_location)
                            
                            if distancia_location <= 1.0:  # Menos de 1km
                                resultados_muestra['con_location_correcto'] += 1
                            else:
                                resultados_muestra['con_location_incorrecto'] += 1
                                
                                # Buscar mejor sucursal
                                mejor_distancia = float('inf')
                                mejor_sucursal = None
                                
                                for sucursal_key, datos_sucursal in sucursales_normalizadas.items():
                                    distancia = calcular_distancia_km(
                                        lat_entrega, lon_entrega,
                                        datos_sucursal['lat'], datos_sucursal['lon']
                                    )
                                    
                                    if distancia < mejor_distancia:
                                        mejor_distancia = distancia
                                        mejor_sucursal = datos_sucursal['nombre']
                                
                                resultados_muestra['ejemplos_problemas'].append({
                                    'submission_id': submission.get('id'),
                                    'tipo_form': tipo_form,
                                    'usuario': usuario,
                                    'fecha': fecha_submitted,
                                    'problema': 'LOCATION_INCORRECTO',
                                    'location_asignado': location_name,
                                    'lat_entrega': lat_entrega,
                                    'lon_entrega': lon_entrega,
                                    'distancia_location_km': distancia_location,
                                    'sucursal_sugerida': mejor_sucursal,
                                    'distancia_sugerida_km': mejor_distancia
                                })
                        else:
                            resultados_muestra['sin_coordenadas_entrega'] += 1
                            
            else:
                print(f"   ❌ Error {response.status_code}")
                
        except Exception as e:
            print(f"   💥 Error: {e}")
    
    return resultados_muestra

def main():
    """Función principal"""
    
    print("⚡ ANÁLISIS RÁPIDO DE MAPEO DE COORDENADAS")
    print("=" * 80)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # 1. Cargar sucursales
    sucursales_normalizadas = cargar_sucursales_normalizadas()
    print(f"✅ {len(sucursales_normalizadas)} sucursales cargadas")
    
    # 2. Contar submissions totales
    totales = contar_submissions_totales()
    
    total_operativas = totales.get('877138', {}).get('total', 0)
    total_seguridad = totales.get('877139', {}).get('total', 0)
    
    print(f"\n📊 TOTALES:")
    print(f"   📋 OPERATIVAS: {total_operativas}")
    print(f"   🔒 SEGURIDAD: {total_seguridad}")
    print(f"   📈 TOTAL: {total_operativas + total_seguridad}")
    
    # 3. Analizar muestra
    resultados_muestra = analizar_muestra_submissions(sucursales_normalizadas, muestra_size=100)
    
    print(f"\n📊 RESULTADOS DE MUESTRA:")
    print(f"   ✅ Con location correcto: {resultados_muestra['con_location_correcto']}")
    print(f"   ⚠️ Con location incorrecto: {resultados_muestra['con_location_incorrecto']}")
    print(f"   ❌ Sin location: {resultados_muestra['sin_location']}")
    print(f"   💥 Sin coordenadas entrega: {resultados_muestra['sin_coordenadas_entrega']}")
    
    if resultados_muestra['distancias']:
        distancias = resultados_muestra['distancias']
        distancia_promedio = sum(distancias) / len(distancias)
        distancia_max = max(distancias)
        print(f"   📏 Distancia promedio: {distancia_promedio:.2f}km")
        print(f"   📏 Distancia máxima: {distancia_max:.2f}km")
    
    # 4. Mostrar ejemplos de problemas
    if resultados_muestra['ejemplos_problemas']:
        print(f"\n⚠️ EJEMPLOS DE PROBLEMAS ({len(resultados_muestra['ejemplos_problemas'])}):")
        for i, problema in enumerate(resultados_muestra['ejemplos_problemas'][:5]):
            print(f"\n   {i+1}. {problema['tipo_form']} - {problema['problema']}")
            print(f"      ID: {problema['submission_id']}")
            print(f"      Usuario: {problema['usuario']}")
            print(f"      Coordenadas entrega: {problema['lat_entrega']:.6f}, {problema['lon_entrega']:.6f}")
            
            if problema['problema'] == 'SIN_LOCATION':
                print(f"      Sucursal sugerida: {problema['sucursal_sugerida']} ({problema['distancia_km']:.2f}km)")
            else:
                print(f"      Location asignado: {problema['location_asignado']}")
                print(f"      Distancia a location: {problema['distancia_location_km']:.2f}km")
                print(f"      Sucursal sugerida: {problema['sucursal_sugerida']} ({problema['distancia_sugerida_km']:.2f}km)")
    
    # 5. Extrapolación
    if total_operativas > 0 and total_seguridad > 0:
        muestra_total = resultados_muestra['con_location_correcto'] + resultados_muestra['con_location_incorrecto'] + resultados_muestra['sin_location']
        
        if muestra_total > 0:
            print(f"\n🔮 EXTRAPOLACIÓN A TODAS LAS SUBMISSIONS:")
            
            porcentaje_sin_location = (resultados_muestra['sin_location'] / muestra_total) * 100
            porcentaje_incorrecto = (resultados_muestra['con_location_incorrecto'] / muestra_total) * 100
            
            total_submissions = total_operativas + total_seguridad
            estimado_sin_location = int((porcentaje_sin_location / 100) * total_submissions)
            estimado_incorrecto = int((porcentaje_incorrecto / 100) * total_submissions)
            estimado_problemas = estimado_sin_location + estimado_incorrecto
            
            print(f"   📊 Total submissions: {total_submissions}")
            print(f"   ❌ Estimado sin location: {estimado_sin_location} ({porcentaje_sin_location:.1f}%)")
            print(f"   ⚠️ Estimado location incorrecto: {estimado_incorrecto} ({porcentaje_incorrecto:.1f}%)")
            print(f"   🎯 Total estimado necesita mapeo: {estimado_problemas}")
    
    # 6. Guardar resultados
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"analisis_rapido_mapeo_{timestamp}.json"
    
    resultado_final = {
        'timestamp': timestamp,
        'totales': totales,
        'total_operativas': total_operativas,
        'total_seguridad': total_seguridad,
        'resultados_muestra': resultados_muestra,
        'sucursales_disponibles': len(sucursales_normalizadas)
    }
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(resultado_final, f, indent=2, default=str, ensure_ascii=False)
    
    print(f"\n💾 Resultados guardados en: {filename}")
    print(f"🎉 ANÁLISIS COMPLETADO")

if __name__ == "__main__":
    main()