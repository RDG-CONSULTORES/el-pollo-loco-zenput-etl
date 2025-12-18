#!/usr/bin/env python3
"""
👤 EXTRACCIÓN DIRECTORES Y USUARIOS
Consulta API /users para encontrar directores de grupos operativos
"""

import requests
import json
from datetime import datetime
import pandas as pd

def extract_directors_and_users():
    """Extrae directores y usuarios desde API Zenput"""
    
    print("👤 EXTRACCIÓN DIRECTORES Y USUARIOS ZENPUT")
    print("=" * 50)
    
    # Configuración API
    api_token = "cb908e0d4e0f5501c635325c611db314"
    headers = {
        'X-API-TOKEN': api_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    print("\n🔍 CONSULTANDO API /users...")
    
    try:
        response = requests.get(
            'https://www.zenput.com/api/v3/users',
            headers=headers,
            timeout=30
        )
        
        if response.status_code == 200:
            users_data = response.json()
            print(f"✅ API Users consultado exitosamente")
            
            if isinstance(users_data, dict) and 'data' in users_data:
                users_list = users_data['data']
                print(f"📊 USUARIOS ENCONTRADOS: {len(users_list)}")
                
                # Análisis de usuarios
                analyze_users_structure(users_list)
                
                # Buscar directores conocidos
                find_known_supervisors(users_list)
                
                # Buscar campos organizacionales
                find_organizational_data(users_list)
                
                # Guardar datos para análisis posterior
                save_users_data(users_list)
                
                return users_list
                
            else:
                print("⚠️ Estructura inesperada en respuesta API users")
                return []
                
        else:
            print(f"❌ Error API users: {response.status_code}")
            print(f"Respuesta: {response.text[:200]}...")
            return []
            
    except Exception as e:
        print(f"❌ Error consultando API users: {e}")
        return []

def analyze_users_structure(users_list):
    """Analiza la estructura de usuarios"""
    
    print(f"\n📋 ANÁLISIS ESTRUCTURA DE USUARIOS")
    print("-" * 40)
    
    if not users_list:
        print("❌ No hay usuarios para analizar")
        return
    
    # Muestra de campos disponibles
    print(f"\n🔑 CAMPOS DISPONIBLES (primer usuario):")
    if users_list:
        primer_usuario = users_list[0]
        for key, value in primer_usuario.items():
            if isinstance(value, (str, int, float, bool)):
                print(f"   • {key}: {str(value)[:50]}...")
            elif isinstance(value, list):
                print(f"   • {key}: Lista con {len(value)} elementos")
            elif isinstance(value, dict):
                print(f"   • {key}: Objeto con {len(value)} campos")
            else:
                print(f"   • {key}: {type(value)}")
    
    # Contadores de tipos de usuario
    roles_count = {}
    titles_count = {}
    departments_count = {}
    
    for user in users_list[:20]:  # Solo primeros 20 para análisis
        # Roles
        role = user.get('role', user.get('role_name', 'Sin rol'))
        if role:
            roles_count[role] = roles_count.get(role, 0) + 1
        
        # Títulos
        title = user.get('title', user.get('position', user.get('job_title', 'Sin título')))
        if title:
            titles_count[title] = titles_count.get(title, 0) + 1
        
        # Departamentos
        dept = user.get('department', user.get('team', user.get('group', 'Sin departamento')))
        if dept:
            departments_count[dept] = departments_count.get(dept, 0) + 1
    
    print(f"\n📊 ROLES MÁS COMUNES:")
    for role, count in sorted(roles_count.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"   • {role}: {count} usuarios")
    
    print(f"\n📊 TÍTULOS MÁS COMUNES:")
    for title, count in sorted(titles_count.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"   • {title}: {count} usuarios")
    
    if departments_count and any(dept != 'Sin departamento' for dept in departments_count.keys()):
        print(f"\n📊 DEPARTAMENTOS:")
        for dept, count in sorted(departments_count.items(), key=lambda x: x[1], reverse=True)[:10]:
            if dept != 'Sin departamento':
                print(f"   • {dept}: {count} usuarios")

def find_known_supervisors(users_list):
    """Busca supervisores conocidos en la lista de usuarios"""
    
    print(f"\n🎯 BÚSQUEDA SUPERVISORES CONOCIDOS")
    print("-" * 35)
    
    # Supervisores conocidos de las supervisiones
    supervisores_conocidos = [
        'Israel Garcia',
        'Israel García',
        'Jorge Reynosa',
        'Jorge Arturo Reynosa',
        'Israel',
        'Jorge',
        'García',
        'Reynosa'
    ]
    
    supervisores_encontrados = []
    
    for user in users_list:
        nombre = user.get('name', user.get('full_name', user.get('username', '')))
        email = user.get('email', '')
        
        # Verificar coincidencias
        for supervisor in supervisores_conocidos:
            if supervisor.lower() in nombre.lower():
                supervisores_encontrados.append({
                    'nombre': nombre,
                    'email': email,
                    'supervisor_buscado': supervisor,
                    'usuario_completo': user
                })
                break
    
    print(f"📊 SUPERVISORES ENCONTRADOS: {len(supervisores_encontrados)}")
    
    for supervisor in supervisores_encontrados:
        print(f"\n👤 {supervisor['nombre']} ({supervisor['email']})")
        print(f"   🔍 Coincide con: {supervisor['supervisor_buscado']}")
        
        # Buscar campos organizacionales en este usuario
        user_data = supervisor['usuario_completo']
        campos_org = []
        
        for key, value in user_data.items():
            if any(term in key.lower() for term in ['team', 'group', 'manager', 'supervisor', 'location', 'department', 'role']):
                campos_org.append(f"{key}: {value}")
        
        if campos_org:
            print(f"   🏢 Organización:")
            for campo in campos_org:
                print(f"      • {campo}")
    
    return supervisores_encontrados

def find_organizational_data(users_list):
    """Busca datos organizacionales en usuarios"""
    
    print(f"\n🏗️ ANÁLISIS DATOS ORGANIZACIONALES")
    print("-" * 40)
    
    # Buscar usuarios con datos organizacionales
    usuarios_con_org = []
    
    for user in users_list:
        campos_org = {}
        
        # Buscar campos relevantes
        for key, value in user.items():
            if any(term in key.lower() for term in ['team', 'group', 'manager', 'director', 'supervisor', 'location']):
                if value and str(value).strip() not in ['', 'None', 'null']:
                    campos_org[key] = value
        
        if campos_org:
            usuario_info = {
                'nombre': user.get('name', user.get('username', 'Sin nombre')),
                'email': user.get('email', ''),
                'campos_organizacionales': campos_org
            }
            usuarios_con_org.append(usuario_info)
    
    print(f"📊 USUARIOS CON DATOS ORGANIZACIONALES: {len(usuarios_con_org)}")
    
    # Mostrar primeros 10
    for i, usuario in enumerate(usuarios_con_org[:10], 1):
        print(f"\n👤 {i}. {usuario['nombre']} ({usuario['email']})")
        for campo, valor in usuario['campos_organizacionales'].items():
            print(f"   • {campo}: {valor}")
    
    # Analizar patrones en campos organizacionales
    todos_campos_org = {}
    for usuario in usuarios_con_org:
        for campo, valor in usuario['campos_organizacionales'].items():
            if campo not in todos_campos_org:
                todos_campos_org[campo] = set()
            todos_campos_org[campo].add(str(valor))
    
    print(f"\n📊 PATRONES EN CAMPOS ORGANIZACIONALES:")
    for campo, valores in todos_campos_org.items():
        valores_unicos = len(valores)
        print(f"   • {campo}: {valores_unicos} valores únicos")
        if valores_unicos <= 10:
            print(f"     Valores: {', '.join(sorted(valores))}")

def save_users_data(users_list):
    """Guarda datos de usuarios para análisis posterior"""
    
    print(f"\n💾 GUARDANDO DATOS DE USUARIOS...")
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Guardar JSON completo
    json_filename = f"data/users_data_{timestamp}.json"
    
    users_data = {
        'timestamp': datetime.now().isoformat(),
        'total_usuarios': len(users_list),
        'usuarios': users_list
    }
    
    with open(json_filename, 'w', encoding='utf-8') as f:
        json.dump(users_data, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"✅ Datos usuarios JSON: {json_filename}")
    
    # Crear CSV resumido
    csv_filename = f"data/usuarios_resumen_{timestamp}.csv"
    
    usuarios_resumen = []
    for user in users_list:
        usuario_info = {
            'ID': user.get('id'),
            'Nombre': user.get('name', user.get('username', '')),
            'Email': user.get('email', ''),
            'Rol': user.get('role', user.get('role_name', '')),
            'Titulo': user.get('title', user.get('position', '')),
            'Departamento': user.get('department', user.get('team', '')),
            'Activo': user.get('active', user.get('is_active', ''))
        }
        usuarios_resumen.append(usuario_info)
    
    pd.DataFrame(usuarios_resumen).to_csv(csv_filename, index=False)
    print(f"✅ Resumen usuarios CSV: {csv_filename}")

def correlate_with_teams():
    """Correlaciona usuarios con teams para encontrar directores"""
    
    print(f"\n🔗 CORRELACIÓN USERS Y TEAMS")
    print("-" * 30)
    
    print("Esta función se implementará una vez tengamos los datos de users")
    print("Para correlacionar directores con grupos operativos específicos")

if __name__ == "__main__":
    print("👤 INICIANDO EXTRACCIÓN DIRECTORES")
    print("Consultando API /users de Zenput...")
    print()
    
    users = extract_directors_and_users()
    
    if users:
        print(f"\n🎉 EXTRACCIÓN USUARIOS COMPLETADA")
        print(f"📊 {len(users)} usuarios extraídos")
        print(f"📋 Datos listos para correlación con grupos")
        
        # Próximos pasos
        correlate_with_teams()
    else:
        print(f"\n❌ NO SE PUDIERON EXTRAER USUARIOS")
        print(f"💡 Verificar permisos API o conectividad")