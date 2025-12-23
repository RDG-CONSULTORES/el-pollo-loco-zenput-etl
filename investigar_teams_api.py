#!/usr/bin/env python3
"""
🔍 INVESTIGAR TEAMS VIA API
Obtener información de teams 114836 y 115095 via Teams API
"""

import requests
import json

# Configuración
ZENPUT_CONFIG = {
    'base_url': 'https://www.zenput.com/api/v3',
    'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}
}

def investigar_team_por_id(team_id):
    """Investigar team específico por ID"""
    
    print(f"\n🔍 INVESTIGANDO TEAM {team_id}")
    print("-" * 40)
    
    # Consulta directa al team
    url = f"{ZENPUT_CONFIG['base_url']}/teams/{team_id}"
    
    try:
        response = requests.get(url, headers=ZENPUT_CONFIG['headers'], timeout=30)
        
        if response.status_code == 200:
            team_data = response.json()
            print(f"✅ Team encontrado:")
            print(json.dumps(team_data, indent=2))
            return team_data
        else:
            print(f"❌ Error {response.status_code}: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def buscar_en_teams_list():
    """Buscar en lista completa de teams"""
    
    print(f"\n📋 BUSCANDO EN LISTA COMPLETA DE TEAMS")
    print("-" * 50)
    
    url = f"{ZENPUT_CONFIG['base_url']}/teams"
    
    try:
        response = requests.get(url, headers=ZENPUT_CONFIG['headers'], timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            teams = data.get('data', [])
            
            print(f"✅ Encontrados {len(teams)} teams totales")
            
            # Buscar nuestros teams específicos
            target_teams = [114836, 115095]
            found_teams = {}
            
            for team in teams:
                team_id = team.get('id')
                if team_id in target_teams:
                    found_teams[team_id] = team
                    print(f"\n🎯 TEAM {team_id} ENCONTRADO:")
                    print(f"   Nombre: {team.get('name', 'Sin nombre')}")
                    print(f"   Parent: {team.get('parent', 'None')}")
                    print(f"   Level: {team.get('level', 'Unknown')}")
                    print(f"   Todos los campos:")
                    print(json.dumps(team, indent=4))
            
            if not found_teams:
                print("❌ Teams objetivo no encontrados en lista")
                
                # Mostrar algunos teams como referencia
                print(f"\n📊 SAMPLE DE TEAMS (primeros 5):")
                for i, team in enumerate(teams[:5]):
                    print(f"   {i+1}. Team {team.get('id')}: '{team.get('name', 'Sin nombre')}'")
            
            return found_teams
        else:
            print(f"❌ Error {response.status_code}: {response.text}")
            return {}
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return {}

def correlacionar_por_coordenadas():
    """Correlacionar usando coordenadas de submissions problemáticas"""
    
    print(f"\n📍 CORRELACIÓN POR COORDENADAS")
    print("-" * 40)
    
    # Las coordenadas de la submission problemática
    lat_problema = 25.665858533486954
    lon_problema = -100.3691519659049
    
    print(f"🎯 Coordenadas problemática: {lat_problema}, {lon_problema}")
    
    # Buscar en nuestras sucursales cuál es la más cercana
    import csv
    import math
    
    def calcular_distancia(lat1, lon1, lat2, lon2):
        """Calcular distancia entre dos puntos"""
        return math.sqrt((lat1 - lat2)**2 + (lon1 - lon2)**2)
    
    sucursales_cercanas = []
    
    with open('data/86_sucursales_master.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['Latitude'] and row['Longitude']:
                lat_sucursal = float(row['Latitude'])
                lon_sucursal = float(row['Longitude'])
                
                distancia = calcular_distancia(lat_problema, lon_problema, lat_sucursal, lon_sucursal)
                
                sucursales_cercanas.append({
                    'sucursal': row['Nombre_Sucursal'],
                    'grupo': row['Grupo_Operativo'],
                    'numero': row['Numero_Sucursal'],
                    'distancia': distancia,
                    'lat': lat_sucursal,
                    'lon': lon_sucursal
                })
    
    # Ordenar por distancia
    sucursales_cercanas.sort(key=lambda x: x['distancia'])
    
    print(f"\n🏪 SUCURSALES MÁS CERCANAS (top 5):")
    for i, sucursal in enumerate(sucursales_cercanas[:5], 1):
        print(f"   {i}. {sucursal['sucursal']} ({sucursal['grupo']})")
        print(f"      Distancia: {sucursal['distancia']:.6f}")
        print(f"      Coordenadas: {sucursal['lat']}, {sucursal['lon']}")
        print()
    
    return sucursales_cercanas[:5]

def generar_mapping_sugerido(investigacion_results, coordenadas_results):
    """Generar mapping sugerido basado en investigación"""
    
    print(f"\n💡 MAPPING SUGERIDO")
    print("=" * 40)
    
    # Basado en coordenadas, el grupo más probable
    if coordenadas_results:
        grupo_probable = coordenadas_results[0]['grupo']
        print(f"🎯 Grupo más probable por coordenadas: {grupo_probable}")
    else:
        grupo_probable = "UNKNOWN"
    
    # Mapping sugerido
    mapping_sugerido = {
        114836: grupo_probable,
        115095: grupo_probable  # Probablemente el mismo grupo
    }
    
    print(f"\n📋 CÓDIGO PARA ACTUALIZAR:")
    print("```python")
    print("# Agregar a TEAMS_TO_GRUPOS:")
    for team_id, grupo in mapping_sugerido.items():
        print(f"{team_id}: \"{grupo}\",  # Teams sin nombre - inferido por coordenadas")
    print("```")
    
    return mapping_sugerido

if __name__ == "__main__":
    print("🔍 INVESTIGACIÓN COMPLETA DE TEAMS PROBLEMÁTICOS")
    print("=" * 60)
    
    # 1. Investigación directa por ID
    results_114836 = investigar_team_por_id(114836)
    results_115095 = investigar_team_por_id(115095)
    
    # 2. Búsqueda en lista completa
    teams_found = buscar_en_teams_list()
    
    # 3. Correlación por coordenadas
    sucursales_cercanas = correlacionar_por_coordenadas()
    
    # 4. Generar mapping sugerido
    mapping_sugerido = generar_mapping_sugerido(teams_found, sucursales_cercanas)
    
    print(f"\n🎯 RESUMEN:")
    print(f"   🔍 Teams investigados: 114836, 115095")
    print(f"   📍 Grupo sugerido por coordenadas: {sucursales_cercanas[0]['grupo'] if sucursales_cercanas else 'Unknown'}")
    print(f"   📊 Total submissions que se recuperarían: 85")
    print(f"\n📝 PRÓXIMO PASO: Actualizar mapping y re-ejecutar ETL")