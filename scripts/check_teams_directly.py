#!/usr/bin/env python3
"""
🔍 VERIFICACIÓN DIRECTA TEAMS Y USERS
Consulta directa a endpoints que funcionan
"""

import requests
import json
from datetime import datetime

def check_teams_and_users_directly():
    """Consulta directa a teams y users para ver estructura completa"""
    
    print("🔍 VERIFICACIÓN DIRECTA - TEAMS Y USERS")
    print("=" * 45)
    
    # Configuración API
    api_token = "cb908e0d4e0f5501c635325c611db314"
    headers = {
        'X-API-TOKEN': api_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    base_url = 'https://www.zenput.com/api/v3'
    
    # 1. VERIFICAR TEAMS
    print("\n👥 CONSULTANDO /teams...")
    print("-" * 25)
    
    try:
        response = requests.get(f"{base_url}/teams", headers=headers, timeout=15)
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            teams_data = response.json()
            print(f"\n📋 ESTRUCTURA COMPLETA DE TEAMS:")
            print(json.dumps(teams_data, indent=2)[:1000] + "..." if len(json.dumps(teams_data)) > 1000 else json.dumps(teams_data, indent=2))
            
            # Analizar si hay datos útiles
            if isinstance(teams_data, dict):
                if 'data' in teams_data and isinstance(teams_data['data'], list):
                    print(f"\n📊 TEAMS ENCONTRADOS: {len(teams_data['data'])}")
                    
                    for i, team in enumerate(teams_data['data'][:3]):
                        print(f"\n🏢 TEAM {i+1}:")
                        if isinstance(team, dict):
                            for key, value in team.items():
                                print(f"   {key}: {value}")
                else:
                    print(f"\n⚠️ No se encontraron teams en 'data'")
            
        else:
            print(f"❌ Error: {response.status_code}")
            print(f"Respuesta: {response.text}")
            
    except Exception as e:
        print(f"💥 Error: {e}")
    
    # 2. VERIFICAR USERS
    print("\n\n👤 CONSULTANDO /users...")
    print("-" * 25)
    
    try:
        response = requests.get(f"{base_url}/users", headers=headers, timeout=15)
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            users_data = response.json()
            print(f"\n📋 ESTRUCTURA COMPLETA DE USERS (primeros 500 chars):")
            users_json = json.dumps(users_data, indent=2)
            print(users_json[:500] + "..." if len(users_json) > 500 else users_json)
            
            # Analizar usuarios
            if isinstance(users_data, dict):
                if 'data' in users_data and isinstance(users_data['data'], list):
                    print(f"\n📊 USUARIOS ENCONTRADOS: {len(users_data['data'])}")
                    
                    # Buscar supervisores conocidos
                    supervisores_conocidos = ['Israel Garcia', 'Jorge Reynosa']
                    supervisores_encontrados = []
                    
                    for user in users_data['data']:
                        if isinstance(user, dict):
                            nombre = user.get('name', user.get('username', ''))
                            email = user.get('email', '')
                            
                            # Verificar si es supervisor conocido
                            for supervisor in supervisores_conocidos:
                                if supervisor.lower() in nombre.lower():
                                    supervisores_encontrados.append({
                                        'nombre': nombre,
                                        'email': email,
                                        'data_completa': user
                                    })
                    
                    print(f"\n🎯 SUPERVISORES CONOCIDOS ENCONTRADOS: {len(supervisores_encontrados)}")
                    for supervisor in supervisores_encontrados:
                        print(f"\n👤 {supervisor['nombre']} ({supervisor['email']})")
                        user_data = supervisor['data_completa']
                        for key, value in user_data.items():
                            if any(term in key.lower() for term in ['team', 'group', 'role', 'title', 'department', 'location']):
                                print(f"   🎯 {key}: {value}")
                    
                    # Mostrar primeros 3 usuarios como muestra
                    print(f"\n👥 MUESTRA USUARIOS (primeros 3):")
                    for i, user in enumerate(users_data['data'][:3]):
                        print(f"\n👤 USUARIO {i+1}:")
                        if isinstance(user, dict):
                            for key, value in user.items():
                                if isinstance(value, (str, int, float, bool)):
                                    print(f"   {key}: {value}")
                                elif isinstance(value, list):
                                    print(f"   {key}: Lista con {len(value)} elementos")
                else:
                    print(f"\n⚠️ No se encontraron usuarios en 'data'")
            
        else:
            print(f"❌ Error: {response.status_code}")
            print(f"Respuesta: {response.text}")
            
    except Exception as e:
        print(f"💥 Error: {e}")
    
    # 3. BUSCAR OTROS ENDPOINTS RELACIONADOS
    print("\n\n🔗 PROBANDO OTROS ENDPOINTS RELACIONADOS...")
    print("-" * 45)
    
    otros_endpoints = [
        '/forms',
        '/submissions/teams',
        '/locations/teams', 
        '/teams/locations',
        '/users/teams',
        '/teams/users'
    ]
    
    for endpoint in otros_endpoints:
        try:
            response = requests.get(f"{base_url}{endpoint}", headers=headers, timeout=10)
            print(f"{endpoint}: Status {response.status_code}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    if isinstance(data, dict) and 'data' in data:
                        data_length = len(data['data']) if isinstance(data['data'], list) else 'N/A'
                        print(f"   ✅ Datos encontrados: {data_length} elementos")
                    elif isinstance(data, list):
                        print(f"   ✅ Lista con {len(data)} elementos")
                    else:
                        print(f"   ℹ️ Respuesta: {str(data)[:100]}...")
                except:
                    print(f"   ⚠️ JSON inválido")
                    
        except Exception as e:
            print(f"{endpoint}: Error - {e}")
    
    print(f"\n🎯 CONCLUSIÓN:")
    print("=" * 15)
    print("Si no encontramos jerarquía útil en el API,")
    print("procederemos con el Excel de Roberto que contiene:")
    print("- Grupos operativos organizados")
    print("- Coordenadas GPS normalizadas") 
    print("- Estructura jerárquica definida")

if __name__ == "__main__":
    check_teams_and_users_directly()