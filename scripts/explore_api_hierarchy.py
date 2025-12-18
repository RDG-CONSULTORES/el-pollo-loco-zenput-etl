#!/usr/bin/env python3
"""
🔍 EXPLORACIÓN COMPLETA API ZENPUT - JERARQUÍA ORGANIZACIONAL
Explora todos los endpoints para encontrar teams, groups, hierarchy
"""

import sys
import os
import json
import requests
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

def explore_zenput_api_hierarchy():
    """Explora todos los endpoints de jerarquía en Zenput API"""
    
    print("🔍 EXPLORACIÓN COMPLETA API ZENPUT - JERARQUÍA")
    print("=" * 55)
    
    # Configuración API
    api_token = "cb908e0d4e0f5501c635325c611db314"
    headers = {
        'X-API-TOKEN': api_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    base_url = 'https://www.zenput.com/api/v3'
    
    # Endpoints a explorar
    endpoints_to_explore = [
        # Organizacionales
        {'endpoint': '/teams', 'descripcion': 'Teams/Equipos organizacionales'},
        {'endpoint': '/groups', 'descripcion': 'Grupos organizacionales'},
        {'endpoint': '/hierarchy', 'descripcion': 'Jerarquía organizacional'},
        {'endpoint': '/users', 'descripcion': 'Usuarios con roles'},
        {'endpoint': '/roles', 'descripcion': 'Roles organizacionales'},
        {'endpoint': '/company', 'descripcion': 'Información de empresa'},
        {'endpoint': '/organizations', 'descripcion': 'Organizaciones'},
        
        # Relacionados con locations
        {'endpoint': '/locations/tags', 'descripcion': 'Tags de locations'},
        {'endpoint': '/location_groups', 'descripcion': 'Grupos de locations'},
        {'endpoint': '/location_teams', 'descripcion': 'Teams de locations'},
        
        # Alternativas
        {'endpoint': '/departments', 'descripcion': 'Departamentos'},
        {'endpoint': '/regions', 'descripcion': 'Regiones'},
        {'endpoint': '/districts', 'descripcion': 'Distritos'},
        {'endpoint': '/zones', 'descripcion': 'Zonas'},
    ]
    
    resultados = {
        'timestamp': datetime.now().isoformat(),
        'endpoints_explorados': len(endpoints_to_explore),
        'endpoints_exitosos': 0,
        'jerarquia_encontrada': {},
        'teams_data': [],
        'users_data': [],
        'estructura_detectada': {}
    }
    
    print(f"\n📡 EXPLORANDO {len(endpoints_to_explore)} ENDPOINTS...")
    print("-" * 50)
    
    for i, endpoint_info in enumerate(endpoints_to_explore, 1):
        endpoint = endpoint_info['endpoint']
        descripcion = endpoint_info['descripcion']
        
        print(f"\n🔗 ENDPOINT {i}/{len(endpoints_to_explore)}: {endpoint}")
        print(f"   📝 {descripcion}")
        
        try:
            # Probar con diferentes parámetros
            response = requests.get(
                f"{base_url}{endpoint}",
                headers=headers,
                timeout=15
            )
            
            print(f"   📊 Status: {response.status_code}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    resultados['endpoints_exitosos'] += 1
                    
                    # Analizar estructura de la respuesta
                    if isinstance(data, dict):
                        print(f"   📋 Tipo: Objeto con {len(data)} campos")
                        
                        # Mostrar campos principales
                        campos_principales = list(data.keys())[:5]
                        print(f"   🔑 Campos: {', '.join(campos_principales)}")
                        
                        # Buscar datos organizacionales
                        if 'results' in data and isinstance(data['results'], list):
                            items_count = len(data['results'])
                            print(f"   📊 Items: {items_count} resultados")
                            
                            if items_count > 0:
                                primer_item = data['results'][0]
                                if isinstance(primer_item, dict):
                                    campos_item = list(primer_item.keys())
                                    print(f"   🏷️ Campos del item: {', '.join(campos_item[:5])}")
                            
                            # Guardar datos importantes
                            if endpoint == '/teams':
                                resultados['teams_data'] = data['results']
                                analizar_teams_data(data['results'])
                            elif endpoint == '/users':
                                resultados['users_data'] = data['results']
                                analizar_users_data(data['results'])
                            
                        # Detectar campos organizacionales
                        campos_organizacionales = []
                        for campo in data.keys():
                            if any(term in campo.lower() for term in ['team', 'group', 'manager', 'director', 'supervisor', 'hierarchy']):
                                campos_organizacionales.append(campo)
                        
                        if campos_organizacionales:
                            print(f"   🎯 Campos organizacionales: {', '.join(campos_organizacionales)}")
                            resultados['jerarquia_encontrada'][endpoint] = campos_organizacionales
                    
                    elif isinstance(data, list):
                        print(f"   📋 Tipo: Lista con {len(data)} elementos")
                        
                        if len(data) > 0:
                            primer_elemento = data[0]
                            if isinstance(primer_elemento, dict):
                                campos = list(primer_elemento.keys())
                                print(f"   🔑 Campos del elemento: {', '.join(campos[:5])}")
                                
                                # Analizar para organizacionales
                                campos_organizacionales = []
                                for campo in campos:
                                    if any(term in campo.lower() for term in ['team', 'group', 'manager', 'director', 'supervisor']):
                                        campos_organizacionales.append(campo)
                                
                                if campos_organizacionales:
                                    print(f"   🎯 Campos organizacionales: {', '.join(campos_organizacionales)}")
                                    resultados['jerarquia_encontrada'][endpoint] = campos_organizacionales
                        
                        # Guardar datos importantes
                        if endpoint == '/teams':
                            resultados['teams_data'] = data
                            analizar_teams_data(data)
                        elif endpoint == '/users':
                            resultados['users_data'] = data
                            analizar_users_data(data)
                    
                    else:
                        print(f"   📋 Tipo: {type(data)}")
                        print(f"   💾 Valor: {str(data)[:100]}...")
                
                except json.JSONDecodeError as e:
                    print(f"   ❌ Error JSON: {e}")
                    print(f"   📄 Respuesta: {response.text[:200]}...")
                    
            elif response.status_code == 404:
                print(f"   ⚠️ Endpoint no disponible")
            elif response.status_code == 403:
                print(f"   🔒 Sin permisos")
            elif response.status_code == 401:
                print(f"   🚫 No autorizado")
            else:
                print(f"   ❌ Error: {response.text[:100]}")
                
        except requests.exceptions.Timeout:
            print(f"   ⏰ Timeout")
        except requests.exceptions.RequestException as e:
            print(f"   💥 Error: {e}")
    
    # Análisis final
    print(f"\n📊 RESUMEN DE EXPLORACIÓN")
    print("=" * 40)
    
    print(f"✅ Endpoints exitosos: {resultados['endpoints_exitosos']}/{len(endpoints_to_explore)}")
    print(f"🎯 Endpoints con jerarquía: {len(resultados['jerarquia_encontrada'])}")
    
    if resultados['jerarquia_encontrada']:
        print(f"\n🏗️ JERARQUÍA ENCONTRADA:")
        for endpoint, campos in resultados['jerarquia_encontrada'].items():
            print(f"   {endpoint}: {', '.join(campos)}")
    
    if resultados['teams_data']:
        print(f"\n👥 TEAMS ENCONTRADOS: {len(resultados['teams_data'])}")
    
    if resultados['users_data']:
        print(f"\n👤 USUARIOS ENCONTRADOS: {len(resultados['users_data'])}")
    
    # Guardar resultados
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    json_filename = f"data/api_hierarchy_exploration_{timestamp}.json"
    os.makedirs('data', exist_ok=True)
    
    with open(json_filename, 'w', encoding='utf-8') as f:
        json.dump(resultados, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"\n💾 Resultados guardados: {json_filename}")
    
    return resultados

def analizar_teams_data(teams_data):
    """Analiza datos de teams para encontrar estructura organizacional"""
    
    print(f"\n👥 ANÁLISIS DETALLADO DE TEAMS:")
    print("-" * 30)
    
    if not teams_data:
        print("   ❌ No hay datos de teams")
        return
    
    for i, team in enumerate(teams_data[:5], 1):  # Solo primeros 5
        print(f"\n   🏢 TEAM {i}:")
        
        if isinstance(team, dict):
            # Mostrar campos principales
            for key, value in team.items():
                if isinstance(value, (str, int, float, bool)):
                    print(f"      {key}: {value}")
                elif isinstance(value, list):
                    print(f"      {key}: Lista con {len(value)} elementos")
                elif isinstance(value, dict):
                    print(f"      {key}: Objeto con {len(value)} campos")
            
            # Buscar campos relacionados con jerarquía
            campos_jerarquia = []
            for key, value in team.items():
                if any(term in key.lower() for term in ['manager', 'director', 'supervisor', 'parent', 'location', 'member']):
                    campos_jerarquia.append(f"{key}: {value}")
            
            if campos_jerarquia:
                print(f"      🎯 Jerarquía: {'; '.join(campos_jerarquia)}")

def analizar_users_data(users_data):
    """Analiza datos de usuarios para encontrar roles y jerarquía"""
    
    print(f"\n👤 ANÁLISIS DETALLADO DE USUARIOS:")
    print("-" * 35)
    
    if not users_data:
        print("   ❌ No hay datos de usuarios")
        return
    
    # Contadores de roles
    roles_encontrados = {}
    
    for i, user in enumerate(users_data[:10], 1):  # Solo primeros 10
        if isinstance(user, dict):
            # Extraer información relevante
            nombre = user.get('name', user.get('username', f'Usuario {i}'))
            email = user.get('email', '')
            rol = user.get('role', user.get('title', user.get('position', 'Sin rol')))
            
            print(f"   👤 {nombre} ({email})")
            print(f"      🎭 Rol: {rol}")
            
            # Contar roles
            if rol and rol != 'Sin rol':
                if rol not in roles_encontrados:
                    roles_encontrados[rol] = 0
                roles_encontrados[rol] += 1
            
            # Buscar campos de jerarquía
            campos_jerarquia = []
            for key, value in user.items():
                if any(term in key.lower() for term in ['team', 'group', 'manager', 'supervisor', 'location', 'department']):
                    if value:
                        campos_jerarquia.append(f"{key}: {value}")
            
            if campos_jerarquia:
                print(f"      🎯 Jerarquía: {'; '.join(campos_jerarquia)}")
    
    if roles_encontrados:
        print(f"\n   📊 ROLES ENCONTRADOS:")
        for rol, cantidad in sorted(roles_encontrados.items(), key=lambda x: x[1], reverse=True):
            print(f"      {rol}: {cantidad} usuarios")

if __name__ == "__main__":
    print("🔍 INICIANDO EXPLORACIÓN COMPLETA API ZENPUT")
    print("Explorando todos los endpoints de jerarquía organizacional...")
    print()
    
    resultados = explore_zenput_api_hierarchy()
    
    if resultados['endpoints_exitosos'] > 0:
        print(f"\n🎉 EXPLORACIÓN COMPLETADA")
        print(f"📋 Revisar resultados y decidir si usar API o Excel de Roberto")
    else:
        print(f"\n⚠️ NO SE ENCONTRÓ JERARQUÍA EN API")
        print(f"💡 Usar Excel de Roberto con grupos y coordenadas")