#!/usr/bin/env python3
"""
🎯 CORRELACIÓN COMPLETA DIRECTORES + TEAMS
Correlaciona usuarios "Director de Operaciones" con teams/equipos de API
"""

import json
import requests

def correlacion_completa_directores_teams():
    """Correlaciona directores de operaciones con teams desde API"""
    
    print("🎯 CORRELACIÓN COMPLETA DIRECTORES + TEAMS")
    print("=" * 55)
    
    # 1. CARGAR USUARIOS
    print("\n👥 CARGANDO USUARIOS...")
    
    try:
        with open('/Users/robertodavila/el-pollo-loco-zenput-etl/data/users_data_20251217_182215.json', 'r') as f:
            users_data = json.load(f)
        print(f"✅ {users_data['total_usuarios']} usuarios cargados")
    except FileNotFoundError:
        print("❌ Archivo usuarios no encontrado")
        return
    
    # 2. CONSULTAR TEAMS API FRESH
    print("\n👥 CONSULTANDO TEAMS API...")
    
    api_token = "cb908e0d4e0f5501c635325c611db314"
    headers = {
        'X-API-TOKEN': api_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    try:
        response = requests.get('https://www.zenput.com/api/v3/teams', headers=headers, timeout=15)
        
        if response.status_code == 200:
            teams_data = response.json()
            print(f"✅ Teams API consultado exitosamente")
            
            # Extraer jerarquía de teams
            main_team = None
            for team in teams_data.get('data', []):
                if 'El Pollo Loco México' in team.get('name', ''):
                    main_team = team
                    break
            
            if main_team and 'children' in main_team:
                teams_list = main_team['children']
                print(f"📊 {len(teams_list)} teams encontrados")
            else:
                teams_list = []
                print("⚠️ No se encontró estructura de teams")
                
        else:
            print(f"❌ Error API teams: {response.status_code}")
            teams_list = []
            
    except Exception as e:
        print(f"❌ Error: {e}")
        teams_list = []
    
    # 3. FILTRAR DIRECTORES DE OPERACIONES
    print(f"\n🎯 FILTRANDO DIRECTORES DE OPERACIONES...")
    
    directores_operaciones = []
    
    for user in users_data['usuarios']:
        rol = user.get('user_role', {}).get('name', '')
        
        if 'Director de Operaciones' in rol:
            nombre_completo = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
            email = user.get('email', '')
            telefono = user.get('sms_number', '')
            teams_asignados = user.get('teams', [])
            default_team = user.get('default_team') or {}
            
            directores_operaciones.append({
                'id': user.get('id'),
                'nombre': nombre_completo,
                'email': email,
                'telefono': telefono,
                'rol': rol,
                'teams': teams_asignados,
                'default_team': default_team,
                'user_completo': user
            })
    
    print(f"✅ {len(directores_operaciones)} Directores de Operaciones encontrados:")
    
    for i, director in enumerate(directores_operaciones, 1):
        print(f"\n🎭 DIRECTOR {i}: {director['nombre']}")
        print(f"   📧 {director['email']}")
        print(f"   📱 {director['telefono']}")
        print(f"   🏢 Default Team: {director['default_team'].get('name', 'N/A')}")
        
        if director['teams']:
            print(f"   👥 Teams asignados ({len(director['teams'])}):")
            for team in director['teams']:
                print(f"      • {team.get('name', 'N/A')}")
    
    # 4. MOSTRAR TODOS LOS TEAMS DISPONIBLES
    print(f"\n🏢 TODOS LOS TEAMS DISPONIBLES ({len(teams_list)}):")
    print("=" * 45)
    
    for i, team in enumerate(teams_list, 1):
        team_name = team.get('name', 'N/A')
        team_id = team.get('id', 'N/A')
        has_children = team.get('has_children', False)
        
        print(f"{i:2}. {team_name} (ID: {team_id}) {'🌳' if has_children else '📄'}")
    
    # 5. CORRELACIONAR DIRECTORES CON TEAMS
    print(f"\n🔗 CORRELACIÓN DIRECTORES ↔ TEAMS:")
    print("=" * 40)
    
    correlacion_final = {}
    teams_sin_director = []
    directores_sin_team_principal = []
    
    # Mapear todos los teams por nombre
    teams_dict = {team.get('name', ''): team for team in teams_list}
    
    for director in directores_operaciones:
        director_teams = director['teams']
        default_team_name = director['default_team'].get('name', '') if director['default_team'] else ''
        
        # Buscar correlación con teams principales
        team_principal = None
        
        # 1. Buscar en default team
        if default_team_name in teams_dict:
            team_principal = default_team_name
        else:
            # 2. Buscar en teams asignados
            for team in director_teams:
                team_name = team.get('name', '')
                if team_name in teams_dict:
                    team_principal = team_name
                    break
        
        if team_principal:
            print(f"✅ {director['nombre']} → {team_principal}")
            correlacion_final[team_principal] = {
                'director_nombre': director['nombre'],
                'director_email': director['email'], 
                'director_telefono': director['telefono'],
                'director_id': director['id'],
                'team_info': teams_dict[team_principal]
            }
        else:
            print(f"⚠️ {director['nombre']} → SIN TEAM PRINCIPAL IDENTIFICADO")
            directores_sin_team_principal.append(director)
    
    # 6. IDENTIFICAR TEAMS SIN DIRECTOR
    print(f"\n📋 TEAMS SIN DIRECTOR ASIGNADO:")
    print("-" * 35)
    
    for team_name in teams_dict.keys():
        if team_name not in correlacion_final:
            teams_sin_director.append(team_name)
            print(f"❌ {team_name}")
    
    # 7. MAPEAR TEAMS SIN DIRECTOR A GRUPOS OPERATIVOS
    print(f"\n🔍 MAPEANDO TEAMS A GRUPOS OPERATIVOS:")
    print("=" * 45)
    
    # Cargar grupos operativos del Excel
    import pandas as pd
    excel_path = "/Users/robertodavila/pollo-loco-tracking-gps/grupos_operativos_final_corregido.csv"
    df_excel = pd.read_csv(excel_path)
    grupos_operativos = sorted(df_excel['Grupo_Operativo'].unique())
    
    correlacion_grupos = {}
    
    print(f"📊 GRUPOS OPERATIVOS vs TEAMS API:")
    print(f"   Grupos Excel: {len(grupos_operativos)}")
    print(f"   Teams API: {len(teams_list)}")
    print()
    
    for grupo in grupos_operativos:
        # Buscar team correspondiente (exacto o similar)
        team_encontrado = None
        
        # 1. Búsqueda exacta
        if grupo in teams_dict:
            team_encontrado = grupo
        else:
            # 2. Búsqueda por similitud
            for team_name in teams_dict.keys():
                if mapear_grupo_a_team(grupo, team_name):
                    team_encontrado = team_name
                    break
        
        if team_encontrado:
            director_info = correlacion_final.get(team_encontrado)
            if director_info:
                print(f"✅ {grupo} → {team_encontrado} → {director_info['director_nombre']}")
                correlacion_grupos[grupo] = {
                    'team_name': team_encontrado,
                    'director': director_info
                }
            else:
                print(f"🔶 {grupo} → {team_encontrado} → SIN DIRECTOR")
                correlacion_grupos[grupo] = {
                    'team_name': team_encontrado,
                    'director': None
                }
        else:
            print(f"❌ {grupo} → SIN TEAM IDENTIFICADO")
            correlacion_grupos[grupo] = {
                'team_name': None,
                'director': None
            }
    
    # 8. RESUMEN FINAL
    print(f"\n📊 RESUMEN FINAL CORRELACIÓN:")
    print("=" * 35)
    
    grupos_con_director = len([g for g in correlacion_grupos.values() if g['director']])
    grupos_sin_director = len(correlacion_grupos) - grupos_con_director
    
    print(f"📈 ESTADÍSTICAS:")
    print(f"   • Directores de Operaciones: {len(directores_operaciones)}")
    print(f"   • Teams API: {len(teams_list)}")
    print(f"   • Grupos Operativos: {len(grupos_operativos)}")
    print(f"   • Grupos CON director: {grupos_con_director}")
    print(f"   • Grupos SIN director: {grupos_sin_director}")
    
    print(f"\n✅ CORRELACIÓN FINAL GRUPOS → DIRECTORES:")
    print("-" * 45)
    
    for grupo, info in correlacion_grupos.items():
        if info['director']:
            director = info['director']
            print(f"• {grupo}: {director['director_nombre']} - {director['director_email']} - {director['director_telefono']}")
        else:
            print(f"• {grupo}: [SIN DIRECTOR ASIGNADO]")
    
    # 9. GUARDAR CORRELACIÓN COMPLETA
    print(f"\n💾 GUARDANDO CORRELACIÓN COMPLETA...")
    
    resultado_final = {
        'timestamp': '2025-12-17T19:15:00.000000',
        'directores_operaciones': directores_operaciones,
        'teams_api': teams_list,
        'correlacion_directores_teams': correlacion_final,
        'correlacion_grupos_directores': correlacion_grupos,
        'teams_sin_director': teams_sin_director,
        'directores_sin_team': directores_sin_team_principal,
        'estadisticas': {
            'total_directores': len(directores_operaciones),
            'total_teams': len(teams_list),
            'total_grupos': len(grupos_operativos),
            'grupos_con_director': grupos_con_director,
            'grupos_sin_director': grupos_sin_director
        }
    }
    
    with open('data/correlacion_completa_directores_teams.json', 'w', encoding='utf-8') as f:
        json.dump(resultado_final, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"✅ Correlación completa guardada: data/correlacion_completa_directores_teams.json")
    
    return resultado_final

def mapear_grupo_a_team(grupo_operativo, team_name):
    """Determina si un grupo operativo corresponde a un team"""
    
    # Mapeos directos conocidos
    mapeos_directos = {
        'PLOG NUEVO LEON': ['PLOG Nuevo Leon'],
        'PLOG LAGUNA': ['PLOG Laguna'],
        'PLOG QUERETARO': ['PLOG Queretaro'],
        'OGAS': ['OGAS'],
        'TEPEYAC': ['TEPEYAC'],
        'EXPO': ['EXPO'],
        'EFM': ['EFM'],
        'EPL SO': ['EPL SO'],
        'TEC': ['TEC'],
        'CRR': ['CRR'],
        'RAP': ['RAP']
    }
    
    if grupo_operativo in mapeos_directos:
        return team_name in mapeos_directos[grupo_operativo]
    
    # Mapeos por similitud
    grupo_lower = grupo_operativo.lower()
    team_lower = team_name.lower()
    
    # Buscar palabras clave comunes
    palabras_grupo = grupo_lower.split()
    palabras_team = team_lower.split()
    
    coincidencias = len(set(palabras_grupo) & set(palabras_team))
    
    # Si hay al menos 1 palabra en común significativa
    if coincidencias >= 1:
        # Filtrar palabras comunes no significativas
        palabras_no_significativas = {'grupo', 'el', 'pollo', 'loco', 'de', 'la', 'del'}
        palabras_significativas_grupo = set(palabras_grupo) - palabras_no_significativas
        palabras_significativas_team = set(palabras_team) - palabras_no_significativas
        
        coincidencias_significativas = len(palabras_significativas_grupo & palabras_significativas_team)
        return coincidencias_significativas >= 1
    
    return False

if __name__ == "__main__":
    print("🎯 EJECUTANDO CORRELACIÓN COMPLETA DIRECTORES + TEAMS")
    print("Correlacionando usuarios 'Director de Operaciones' con teams API...")
    print()
    
    resultado = correlacion_completa_directores_teams()
    
    print(f"\n🎉 CORRELACIÓN COMPLETA FINALIZADA")
    print(f"📊 Directores correlacionados con teams y grupos operativos")
    print(f"📧 Emails y teléfonos reales extraídos")