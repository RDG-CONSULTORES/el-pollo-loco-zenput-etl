#!/usr/bin/env python3
"""
🔍 EXTRACCIÓN COMPLETA - ESTRUCTURA ORGANIZACIONAL EPL
Extrae TODAS las 86 sucursales + jerarquía organizacional desde API Zenput
"""

import sys
import os
import json
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from zenput_api import create_zenput_client

def extract_complete_epl_structure():
    """Extrae estructura organizacional completa de El Pollo Loco"""
    
    print("🔍 EXTRACCIÓN COMPLETA - ESTRUCTURA EL POLLO LOCO MÉXICO")
    print("=" * 60)
    
    # 1. CONECTAR API
    client = create_zenput_client()
    
    if not client.validate_api_connection():
        print("❌ No se puede conectar al API Zenput")
        return False
    
    print("✅ Conexión API exitosa")
    
    # 2. EXTRAER TODAS LAS SUCURSALES
    print("\n📍 EXTRAYENDO TODAS LAS SUCURSALES...")
    
    try:
        all_locations = client.get_all_locations()
        
        if not all_locations:
            print("❌ No se encontraron sucursales")
            return False
            
        print(f"✅ {len(all_locations)} sucursales encontradas")
        
    except Exception as e:
        print(f"❌ Error extrayendo sucursales: {e}")
        return False
    
    # 3. ANALIZAR ESTRUCTURA ORGANIZACIONAL
    print(f"\n🏗️ ANALIZANDO ESTRUCTURA ORGANIZACIONAL...")
    
    estructura_completa = {
        'timestamp': datetime.now().isoformat(),
        'total_sucursales': len(all_locations),
        'sucursales': [],
        'jerarquia_detectada': {},
        'campos_disponibles': set(),
        'tags_organizacionales': set(),
        'posibles_directores': set(),
        'posibles_grupos': set()
    }
    
    print(f"\n📊 ANALIZANDO {len(all_locations)} SUCURSALES:")
    print("-" * 50)
    
    for i, location in enumerate(all_locations, 1):
        print(f"\n🏢 SUCURSAL {i}/{len(all_locations)}")
        
        # Extraer número de sucursal
        numero_sucursal = extract_sucursal_number(location.get('name', ''))
        
        sucursal_info = {
            'numero': numero_sucursal,
            'zenput_id': location.get('id'),
            'nombre_zenput': location.get('name', ''),
            'direccion': location.get('address', ''),
            'ciudad': location.get('city', ''),
            'estado': location.get('state', ''),
            'codigo_postal': location.get('postal_code', ''),
            'coordenadas': {
                'lat': location.get('latitude'),
                'lon': location.get('longitude')
            },
            'tags': location.get('tags', []),
            'campos_adicionales': {}
        }
        
        # Buscar campos organizacionales en location
        for key, value in location.items():
            estructura_completa['campos_disponibles'].add(key)
            
            # Guardar campos que pueden indicar jerarquía
            if any(term in key.lower() for term in ['director', 'manager', 'supervisor', 'group', 'region', 'zone']):
                sucursal_info['campos_adicionales'][key] = value
                print(f"   🎯 Campo jerárquico: {key} = {value}")
        
        # Analizar tags
        for tag in location.get('tags', []):
            estructura_completa['tags_organizacionales'].add(tag.get('name', ''))
            
            # Buscar patrones en tags que indiquen jerarquía
            tag_name = tag.get('name', '')
            if any(term in tag_name.lower() for term in ['director', 'group', 'region', 'zone']):
                print(f"   🏷️ Tag organizacional: {tag_name}")
                estructura_completa['posibles_grupos'].add(tag_name)
        
        # Buscar directores en cualquier campo de texto
        for key, value in location.items():
            if isinstance(value, str) and any(term in value.lower() for term in ['director', 'manager']):
                estructura_completa['posibles_directores'].add(value)
                print(f"   👤 Posible director: {value} (campo: {key})")
        
        estructura_completa['sucursales'].append(sucursal_info)
        
        print(f"   📍 {numero_sucursal or 'N/A'} - {location.get('name', 'N/A')}")
        print(f"   🌍 {location.get('city', '')}, {location.get('state', '')}")
        print(f"   📧 Coord: ({location.get('latitude')}, {location.get('longitude')})")
        print(f"   🏷️ Tags: {len(location.get('tags', []))}")
    
    # 4. ANÁLISIS DE JERARQUÍA
    print(f"\n🔍 ANÁLISIS DE JERARQUÍA ORGANIZACIONAL")
    print("=" * 50)
    
    print(f"\n📋 CAMPOS DISPONIBLES EN API ({len(estructura_completa['campos_disponibles'])}):")
    for campo in sorted(estructura_completa['campos_disponibles']):
        print(f"   • {campo}")
    
    print(f"\n🏷️ TAGS ORGANIZACIONALES ({len(estructura_completa['tags_organizacionales'])}):")
    for tag in sorted(estructura_completa['tags_organizacionales']):
        print(f"   • {tag}")
    
    print(f"\n👥 POSIBLES DIRECTORES DETECTADOS ({len(estructura_completa['posibles_directores'])}):")
    for director in sorted(estructura_completa['posibles_directores']):
        print(f"   • {director}")
    
    print(f"\n🏗️ POSIBLES GRUPOS DETECTADOS ({len(estructura_completa['posibles_grupos'])}):")
    for grupo in sorted(estructura_completa['posibles_grupos']):
        print(f"   • {grupo}")
    
    # 5. DETECTAR PATRONES DE AGRUPACIÓN
    print(f"\n🧩 DETECCIÓN DE PATRONES DE AGRUPACIÓN")
    print("-" * 40)
    
    # Agrupar por estado
    por_estado = {}
    for sucursal in estructura_completa['sucursales']:
        estado = sucursal.get('estado', 'Sin estado')
        if estado not in por_estado:
            por_estado[estado] = []
        por_estado[estado].append(sucursal)
    
    print(f"📊 AGRUPACIÓN POR ESTADO:")
    for estado, sucursales in por_estado.items():
        print(f"   {estado}: {len(sucursales)} sucursales")
    
    # Agrupar por ciudad
    por_ciudad = {}
    for sucursal in estructura_completa['sucursales']:
        ciudad = sucursal.get('ciudad', 'Sin ciudad')
        if ciudad not in por_ciudad:
            por_ciudad[ciudad] = []
        por_ciudad[ciudad].append(sucursal)
    
    print(f"\n📊 AGRUPACIÓN POR CIUDAD (Top 10):")
    ciudades_ordenadas = sorted(por_ciudad.items(), key=lambda x: len(x[1]), reverse=True)[:10]
    for ciudad, sucursales in ciudades_ordenadas:
        print(f"   {ciudad}: {len(sucursales)} sucursales")
    
    # Buscar patrones geográficos
    print(f"\n🗺️ ANÁLISIS GEOGRÁFICO:")
    sucursales_con_coords = [s for s in estructura_completa['sucursales'] 
                           if s['coordenadas']['lat'] and s['coordenadas']['lon']]
    
    if sucursales_con_coords:
        lats = [s['coordenadas']['lat'] for s in sucursales_con_coords]
        lons = [s['coordenadas']['lon'] for s in sucursales_con_coords]
        
        lat_min, lat_max = min(lats), max(lats)
        lon_min, lon_max = min(lons), max(lons)
        
        print(f"   Latitud: {lat_min:.3f} a {lat_max:.3f}")
        print(f"   Longitud: {lon_min:.3f} a {lon_max:.3f}")
        print(f"   Sucursales con coordenadas: {len(sucursales_con_coords)}/{len(estructura_completa['sucursales'])}")
        
        # Sugerir agrupación geográfica
        lat_center = (lat_min + lat_max) / 2
        lon_center = (lon_min + lon_max) / 2
        
        grupos_geograficos = {
            'Norte': [],
            'Sur': [],
            'Este': [],
            'Oeste': []
        }
        
        for sucursal in sucursales_con_coords:
            lat = sucursal['coordenadas']['lat']
            lon = sucursal['coordenadas']['lon']
            
            if lat > lat_center:
                if lon > lon_center:
                    grupos_geograficos['Norte'].append(sucursal)
                else:
                    grupos_geograficos['Oeste'].append(sucursal)
            else:
                if lon > lon_center:
                    grupos_geograficos['Este'].append(sucursal)
                else:
                    grupos_geograficos['Sur'].append(sucursal)
        
        print(f"\n📊 SUGERENCIA DE AGRUPACIÓN GEOGRÁFICA:")
        for grupo, sucursales in grupos_geograficos.items():
            if sucursales:
                print(f"   {grupo}: {len(sucursales)} sucursales")
    
    # 6. GUARDAR RESULTADOS
    print(f"\n💾 GUARDANDO ESTRUCTURA COMPLETA...")
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Guardar JSON completo
    json_filename = f"data/estructura_completa_epl_{timestamp}.json"
    os.makedirs('data', exist_ok=True)
    
    # Convertir sets a lists para JSON
    estructura_completa['campos_disponibles'] = sorted(estructura_completa['campos_disponibles'])
    estructura_completa['tags_organizacionales'] = sorted(estructura_completa['tags_organizacionales'])
    estructura_completa['posibles_directores'] = sorted(estructura_completa['posibles_directores'])
    estructura_completa['posibles_grupos'] = sorted(estructura_completa['posibles_grupos'])
    
    with open(json_filename, 'w', encoding='utf-8') as f:
        json.dump(estructura_completa, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"✅ Estructura completa guardada: {json_filename}")
    
    # Generar reporte CSV para Roberto
    csv_filename = f"data/sucursales_para_revision_{timestamp}.csv"
    
    import csv
    with open(csv_filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Numero', 'Nombre_Zenput', 'Ciudad', 'Estado', 'Direccion',
            'Lat', 'Lon', 'Tags', 'Campos_Adicionales'
        ])
        
        for sucursal in estructura_completa['sucursales']:
            tags_str = '; '.join([tag.get('name', '') for tag in sucursal.get('tags', [])])
            campos_str = '; '.join([f"{k}:{v}" for k, v in sucursal.get('campos_adicionales', {}).items()])
            
            writer.writerow([
                sucursal.get('numero', ''),
                sucursal.get('nombre_zenput', ''),
                sucursal.get('ciudad', ''),
                sucursal.get('estado', ''),
                sucursal.get('direccion', ''),
                sucursal['coordenadas'].get('lat', ''),
                sucursal['coordenadas'].get('lon', ''),
                tags_str,
                campos_str
            ])
    
    print(f"✅ CSV para revisión: {csv_filename}")
    
    # 7. RECOMENDACIONES FINALES
    print(f"\n🎯 RECOMENDACIONES PARA ROBERTO")
    print("=" * 40)
    
    print(f"✅ DATOS EXTRAÍDOS EXITOSAMENTE:")
    print(f"   • {len(estructura_completa['sucursales'])} sucursales completas")
    print(f"   • {len(sucursales_con_coords)} con coordenadas GPS")
    print(f"   • {len(estructura_completa['campos_disponibles'])} campos API disponibles")
    print(f"   • {len(estructura_completa['tags_organizacionales'])} tags organizacionales")
    
    if not estructura_completa['posibles_directores'] and not estructura_completa['posibles_grupos']:
        print(f"\n⚠️ JERARQUÍA NO DETECTADA EN API:")
        print(f"   • No se encontraron campos de directores")
        print(f"   • No se encontraron grupos operativos claros")
        print(f"   • RECOMENDACIÓN: Agrupación manual por Roberto")
        
        print(f"\n📋 OPCIONES DE AGRUPACIÓN SUGERIDAS:")
        print(f"   1. Por Estado: NL (41), TM (20), CO (11), otros (14)")
        print(f"   2. Por ciudad: Principales ciudades como centros")
        print(f"   3. Manual: Roberto define grupos según operación")
    
    else:
        print(f"\n✅ POSIBLE JERARQUÍA DETECTADA:")
        if estructura_completa['posibles_directores']:
            print(f"   • Directores: {len(estructura_completa['posibles_directores'])}")
        if estructura_completa['posibles_grupos']:
            print(f"   • Grupos: {len(estructura_completa['posibles_grupos'])}")
    
    print(f"\n🚀 SIGUIENTES PASOS:")
    print(f"   1. Roberto revisar CSV: {csv_filename}")
    print(f"   2. Roberto definir grupos operativos")
    print(f"   3. Roberto asignar directores responsables")
    print(f"   4. Implementar estructura en Railway PostgreSQL")
    
    return True

def extract_sucursal_number(sucursal_name):
    """Extrae número de sucursal del nombre"""
    
    import re
    
    patterns = [
        r'^(\d+)\s*-\s*',        # "53 - Lienzo Charro"
        r'^(\d+)\s+',            # "53 Lienzo Charro"
        r'(\d+)',                # Cualquier número
    ]
    
    for pattern in patterns:
        match = re.search(pattern, sucursal_name)
        if match:
            numero = int(match.group(1))
            if 1 <= numero <= 100:  # Rango válido
                return numero
    
    return None

if __name__ == "__main__":
    print("🔍 INICIANDO EXTRACCIÓN COMPLETA EL POLLO LOCO MÉXICO")
    print("Este proceso extraerá TODAS las 86 sucursales y buscará jerarquía organizacional")
    print()
    
    success = extract_complete_epl_structure()
    
    if success:
        print(f"\n🎉 EXTRACCIÓN COMPLETA EXITOSA")
        print(f"📋 Roberto: Revisar archivos generados y definir grupos operativos")
    else:
        print(f"\n❌ ERROR EN EXTRACCIÓN")
        print(f"💡 Verificar conexión API y permisos")