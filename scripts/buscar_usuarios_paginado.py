#!/usr/bin/env python3
"""
🔍 BÚSQUEDA USUARIOS CON PAGINADO + COINCIDENCIAS NOMBRES
Busca todos los usuarios con paginación y correlaciona por nombres parciales
"""

import requests
import json
from datetime import datetime

def buscar_todos_usuarios_paginado():
    """Busca todos los usuarios usando paginación y correlaciona con teams"""
    
    print("🔍 BÚSQUEDA COMPLETA USUARIOS CON PAGINADO")
    print("=" * 55)
    
    # Configuración API
    api_token = "cb908e0d4e0f5501c635325c611db314"
    headers = {
        'X-API-TOKEN': api_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    base_url = 'https://www.zenput.com/api/v3'
    
    # 1. BUSCAR TODOS LOS USUARIOS CON PAGINACIÓN
    print("\n👥 EXTRAYENDO TODOS LOS USUARIOS CON PAGINACIÓN...")
    
    all_users = []
    limit = 50
    offset = 0
    page = 1
    
    while True:
        print(f"📄 Página {page} (offset: {offset}, limit: {limit})...")
        
        try:
            url = f"{base_url}/users?limit={limit}&offset={offset}"
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'data' in data and isinstance(data['data'], list):
                    users_batch = data['data']
                    print(f"   ✅ {len(users_batch)} usuarios en esta página")
                    
                    if len(users_batch) == 0:
                        print("   🔚 No más usuarios, paginación completa")
                        break
                    
                    all_users.extend(users_batch)
                    
                    # Verificar si hay más páginas
                    if len(users_batch) < limit:
                        print("   🔚 Última página alcanzada")
                        break
                    
                    offset += limit
                    page += 1
                else:
                    print(f"   ⚠️ Estructura inesperada en página {page}")
                    break
                    
            else:
                print(f"   ❌ Error HTTP {response.status_code} en página {page}")
                break
                
        except Exception as e:
            print(f"   💥 Error en página {page}: {e}")
            break
    
    print(f"\n✅ TOTAL USUARIOS EXTRAÍDOS: {len(all_users)}")
    
    # 2. CONSULTAR TEAMS PARA CORRELACIÓN
    print("\n🏢 CONSULTANDO TEAMS...")
    
    try:
        response = requests.get(f"{base_url}/teams", headers=headers, timeout=15)
        
        if response.status_code == 200:
            teams_data = response.json()
            main_team = None
            
            for team in teams_data.get('data', []):
                if 'El Pollo Loco México' in team.get('name', ''):
                    main_team = team
                    break
            
            if main_team and 'children' in main_team:
                teams_list = main_team['children']
                print(f"✅ {len(teams_list)} teams encontrados")
            else:
                teams_list = []
                print("⚠️ No se encontró estructura de teams")
        else:
            teams_list = []
            print(f"❌ Error teams: {response.status_code}")
            
    except Exception as e:
        teams_list = []
        print(f"❌ Error: {e}")
    
    # 3. ANALIZAR TODOS LOS USUARIOS
    print(f"\n📊 ANÁLISIS COMPLETO DE {len(all_users)} USUARIOS:")
    print("=" * 50)
    
    # Categorizar usuarios por rol
    usuarios_por_rol = {}
    directores_y_gerentes = []
    
    for i, user in enumerate(all_users, 1):
        nombre_completo = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
        email = user.get('email', '')
        telefono = user.get('sms_number', '')
        rol = user.get('user_role', {}).get('name', 'Sin rol')
        teams = user.get('teams', [])
        default_team = user.get('default_team') or {}
        
        # Contar por rol
        if rol not in usuarios_por_rol:
            usuarios_por_rol[rol] = 0
        usuarios_por_rol[rol] += 1
        
        # Filtrar roles importantes
        if any(palabra in rol for palabra in ['Director', 'Gerente', 'Manager']):
            directores_y_gerentes.append({
                'id': user.get('id'),
                'nombre': nombre_completo,
                'email': email,
                'telefono': telefono,
                'rol': rol,
                'teams': teams,
                'default_team': default_team,
                'user_completo': user
            })
    
    # Mostrar estadísticas de roles
    print(f"📈 USUARIOS POR ROL:")
    for rol, count in sorted(usuarios_por_rol.items(), key=lambda x: x[1], reverse=True):
        print(f"   • {rol}: {count} usuarios")
    
    print(f"\n🎭 DIRECTORES Y GERENTES ENCONTRADOS ({len(directores_y_gerentes)}):")
    print("=" * 55)
    
    for i, usuario in enumerate(directores_y_gerentes, 1):
        print(f"\n👤 {i}. {usuario['nombre']}")
        print(f"   📧 {usuario['email']}")
        print(f"   📱 {usuario['telefono']}")
        print(f"   🎭 {usuario['rol']}")
        print(f"   🏢 Default Team: {usuario['default_team'].get('name', 'N/A') if usuario['default_team'] else 'N/A'}")
        
        if usuario['teams']:
            print(f"   👥 Teams ({len(usuario['teams'])}):")
            for team in usuario['teams']:
                print(f"      • {team.get('name', 'N/A')}")
    
    # 4. CORRELACIÓN POR NOMBRES PARCIALES
    print(f"\n🔗 CORRELACIÓN POR NOMBRES PARCIALES CON TEAMS:")
    print("=" * 55)
    
    correlaciones_nombres = {}
    teams_names = [team.get('name', '') for team in teams_list]
    
    print(f"🎯 BUSCANDO COINCIDENCIAS DE NOMBRES EN TEAMS:")
    
    for team_name in teams_names:
        print(f"\n🏢 TEAM: {team_name}")
        
        # Buscar usuarios que podrían ser directores de este team
        candidatos = []
        
        # Buscar por nombres en el team name
        palabras_team = extraer_palabras_significativas(team_name)
        
        for usuario in directores_y_gerentes:
            nombre_usuario = usuario['nombre']
            palabras_nombre = extraer_palabras_significativas(nombre_usuario)
            
            # Verificar si alguna palabra del team está en el nombre
            coincidencias = set(palabras_team) & set(palabras_nombre)
            
            if coincidencias:
                candidatos.append({
                    'usuario': usuario,
                    'coincidencias': list(coincidencias),
                    'score': len(coincidencias)
                })
        
        # Buscar también en teams asignados
        for usuario in directores_y_gerentes:
            for team_asignado in usuario['teams']:
                if team_name == team_asignado.get('name', ''):
                    if not any(c['usuario']['id'] == usuario['id'] for c in candidatos):
                        candidatos.append({
                            'usuario': usuario,
                            'coincidencias': ['team_asignado'],
                            'score': 10  # Mayor score por team asignado
                        })
            
            # Verificar default team
            if usuario['default_team'] and team_name == usuario['default_team'].get('name', ''):
                if not any(c['usuario']['id'] == usuario['id'] for c in candidatos):
                    candidatos.append({
                        'usuario': usuario,
                        'coincidencias': ['default_team'],
                        'score': 15  # Mayor score por default team
                    })
        
        # Mostrar candidatos ordenados por score
        if candidatos:
            candidatos_ordenados = sorted(candidatos, key=lambda x: x['score'], reverse=True)
            
            for j, candidato in enumerate(candidatos_ordenados[:3], 1):  # Top 3
                usuario = candidato['usuario']
                print(f"   {j}. {usuario['nombre']} - {usuario['rol']}")
                print(f"      📧 {usuario['email']} 📱 {usuario['telefono']}")
                print(f"      🎯 Coincidencias: {', '.join(candidato['coincidencias'])} (Score: {candidato['score']})")
            
            # Guardar mejor candidato
            mejor_candidato = candidatos_ordenados[0]
            correlaciones_nombres[team_name] = mejor_candidato['usuario']
        else:
            print(f"   ❌ No se encontraron candidatos")
            correlaciones_nombres[team_name] = None
    
    # 5. GUARDAR RESULTADOS COMPLETOS
    print(f"\n💾 GUARDANDO RESULTADOS COMPLETOS...")
    
    resultado_completo = {
        'timestamp': datetime.now().isoformat(),
        'total_usuarios': len(all_users),
        'usuarios_completos': all_users,
        'usuarios_por_rol': usuarios_por_rol,
        'directores_y_gerentes': directores_y_gerentes,
        'teams_api': teams_list,
        'correlaciones_por_nombres': correlaciones_nombres,
        'estadisticas': {
            'total_usuarios': len(all_users),
            'directores_gerentes': len(directores_y_gerentes),
            'teams_con_candidato': len([t for t in correlaciones_nombres.values() if t]),
            'teams_sin_candidato': len([t for t in correlaciones_nombres.values() if not t])
        }
    }
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"data/usuarios_completos_paginado_{timestamp}.json"
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(resultado_completo, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"✅ Usuarios completos guardados: {filename}")
    
    # 6. RESUMEN FINAL
    print(f"\n📊 RESUMEN FINAL:")
    print("=" * 25)
    
    print(f"📈 ESTADÍSTICAS:")
    print(f"   • Total usuarios: {len(all_users)}")
    print(f"   • Directores/Gerentes: {len(directores_y_gerentes)}")
    print(f"   • Teams API: {len(teams_list)}")
    print(f"   • Teams con candidato: {len([t for t in correlaciones_nombres.values() if t])}")
    print(f"   • Teams sin candidato: {len([t for t in correlaciones_nombres.values() if not t])}")
    
    print(f"\n✅ CORRELACIÓN FINAL POR NOMBRES:")
    print("-" * 40)
    
    for team_name, candidato in correlaciones_nombres.items():
        if candidato:
            print(f"• {team_name}: {candidato['nombre']} - {candidato['email']} - {candidato['telefono']}")
        else:
            print(f"• {team_name}: [SIN CANDIDATO IDENTIFICADO]")
    
    return resultado_completo

def extraer_palabras_significativas(texto):
    """Extrae palabras significativas de un texto"""
    
    import re
    
    # Limpiar texto
    texto_limpio = re.sub(r'[^\w\s]', ' ', texto.lower())
    palabras = texto_limpio.split()
    
    # Filtrar palabras no significativas
    palabras_no_significativas = {
        'el', 'pollo', 'loco', 'mexico', 'méxico', 'de', 'la', 'del', 'las', 'los',
        'grupo', 'team', 'equipo', 'operativo', 'operaciones', 'director', 'gerente',
        'plog', 'epl', 'cas', 'centro', 'norte', 'sur', 'este', 'oeste'
    }
    
    palabras_significativas = [p for p in palabras if len(p) >= 3 and p not in palabras_no_significativas]
    
    return palabras_significativas

if __name__ == "__main__":
    print("🔍 EJECUTANDO BÚSQUEDA COMPLETA CON PAGINADO")
    print("Extrayendo todos los usuarios y correlacionando por nombres...")
    print()
    
    resultado = buscar_todos_usuarios_paginado()
    
    print(f"\n🎉 BÚSQUEDA COMPLETA FINALIZADA")
    print(f"📊 Usuarios extraídos con paginación y correlacionados por nombres")
    print(f"📧 Candidatos identificados para cada team")