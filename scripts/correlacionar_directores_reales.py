#!/usr/bin/env python3
"""
🔍 CORRELACIÓN DIRECTORES REALES
Busca directores reales desde endpoint /users y correlaciona con grupos operativos
"""

import json
import pandas as pd

def correlacionar_directores_reales():
    """Correlaciona directores reales desde API /users con grupos operativos"""
    
    print("🔍 CORRELACIONANDO DIRECTORES REALES DESDE API /users")
    print("=" * 65)
    
    # 1. LEER DATOS USUARIOS API
    print("\n👥 LEYENDO USUARIOS ZENPUT API...")
    
    try:
        with open('/Users/robertodavila/el-pollo-loco-zenput-etl/data/users_data_20251217_182215.json', 'r') as f:
            users_data = json.load(f)
        
        print(f"✅ {users_data['total_usuarios']} usuarios cargados")
        
    except FileNotFoundError:
        print("❌ Archivo usuarios no encontrado")
        return
    
    # 2. ANALIZAR CADA USUARIO CON SUS TEAMS Y SUCURSALES
    print(f"\n📊 ANÁLISIS DETALLADO USUARIOS CON TEAMS Y SUCURSALES:")
    print("=" * 60)
    
    directores_encontrados = {}
    
    for i, user in enumerate(users_data['usuarios'], 1):
        email = user.get('email', '')
        nombre_completo = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
        telefono = user.get('sms_number', '')
        rol = user.get('user_role', {}).get('name', '')
        
        # Teams asignados
        teams = user.get('teams', [])
        default_team = user.get('default_team') or {}
        
        # Sucursales asignadas (owned_locations)
        sucursales_owned = user.get('owned_locations', [])
        
        print(f"\n👤 USUARIO {i}: {nombre_completo}")
        print(f"   📧 Email: {email}")
        print(f"   📱 Teléfono: {telefono}")
        print(f"   🎭 Rol: {rol}")
        print(f"   🏢 Default Team: {default_team.get('name', 'N/A') if default_team else 'N/A'}")
        
        # Analizar teams
        if teams:
            print(f"   👥 Teams asignados ({len(teams)}):")
            for team in teams:
                team_name = team.get('name', '')
                print(f"      • {team_name}")
                
                # Buscar coincidencias con grupos operativos
                if team_name in ['TEPEYAC', 'OGAS', 'EXPO', 'EFM', 'EPL SO', 'TEC', 'CRR', 'RAP']:
                    if team_name not in directores_encontrados:
                        directores_encontrados[team_name] = []
                    directores_encontrados[team_name].append({
                        'nombre': nombre_completo,
                        'email': email,
                        'telefono': telefono,
                        'rol': rol
                    })
        
        # Buscar en default team
        default_team_name = default_team.get('name', '') if default_team else ''
        if default_team_name and any(grupo in default_team_name for grupo in ['PLOG', 'GRUPO', 'TEPEYAC', 'OGAS']):
            # Mapear nombres de teams a grupos
            grupo_mapeado = mapear_team_a_grupo(default_team_name)
            if grupo_mapeado:
                if grupo_mapeado not in directores_encontrados:
                    directores_encontrados[grupo_mapeado] = []
                directores_encontrados[grupo_mapeado].append({
                    'nombre': nombre_completo,
                    'email': email,
                    'telefono': telefono,
                    'rol': rol,
                    'team': default_team_name
                })
        
        # Analizar sucursales owned
        if sucursales_owned:
            print(f"   🏪 Sucursales owned ({len(sucursales_owned)}):")
            for sucursal in sucursales_owned:
                sucursal_nombre = sucursal.get('name', '')
                sucursal_numero = extraer_numero_sucursal(sucursal_nombre)
                print(f"      • {sucursal_nombre} (#{sucursal_numero})")
    
    # 3. MOSTRAR DIRECTORES ENCONTRADOS POR GRUPO
    print(f"\n🎯 DIRECTORES REALES ENCONTRADOS POR GRUPO:")
    print("=" * 50)
    
    # Leer datos Excel para correlacionar
    excel_path = "/Users/robertodavila/pollo-loco-tracking-gps/grupos_operativos_final_corregido.csv"
    df_excel = pd.read_csv(excel_path)
    grupos_excel = df_excel['Grupo_Operativo'].unique()
    
    directores_finales = {}
    
    for grupo in sorted(grupos_excel):
        print(f"\n🏢 {grupo}:")
        
        # Buscar directores exactos
        if grupo in directores_encontrados:
            directores = directores_encontrados[grupo]
            for director in directores:
                print(f"   ✅ DIRECTOR: {director['nombre']}")
                print(f"      📧 {director['email']}")
                print(f"      📱 {director['telefono']}")
                print(f"      🎭 {director['rol']}")
                
                directores_finales[grupo] = {
                    'nombre': director['nombre'],
                    'email': director['email'],
                    'telefono': director['telefono'],
                    'rol': director['rol']
                }
                break
        else:
            # Buscar por similitud o mapeo
            director_similar = buscar_director_similar(grupo, users_data['usuarios'])
            if director_similar:
                print(f"   🔍 DIRECTOR (por mapeo): {director_similar['nombre']}")
                print(f"      📧 {director_similar['email']}")
                print(f"      📱 {director_similar['telefono']}")
                print(f"      🎭 {director_similar['rol']}")
                print(f"      🏢 Team: {director_similar.get('team', 'N/A')}")
                
                directores_finales[grupo] = director_similar
            else:
                print(f"   ⚠️ Director no identificado automáticamente")
                directores_finales[grupo] = None
    
    # 4. GENERAR LISTADO FINAL CORREGIDO
    print(f"\n📋 LISTADO FINAL DIRECTORES CORREGIDOS")
    print("=" * 45)
    
    print(f"\n✅ DIRECTORES IDENTIFICADOS ({len([d for d in directores_finales.values() if d])} de {len(directores_finales)}):")
    
    for grupo, director in directores_finales.items():
        if director:
            print(f"• {grupo}: {director['nombre']} - {director['email']} - {director['telefono']}")
        else:
            print(f"• {grupo}: [DIRECTOR NO IDENTIFICADO]")
    
    # 5. GUARDAR CORRELACIÓN PARA RAILWAY
    print(f"\n💾 GUARDANDO CORRELACIÓN DIRECTORES...")
    
    correlacion_final = {
        'timestamp': '2025-12-17T18:51:00.000000',
        'total_grupos': len(directores_finales),
        'directores_identificados': len([d for d in directores_finales.values() if d]),
        'correlacion_directores': directores_finales,
        'usuarios_completos': users_data['usuarios']
    }
    
    with open('data/directores_reales_correlacionados.json', 'w', encoding='utf-8') as f:
        json.dump(correlacion_final, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"✅ Correlación guardada: data/directores_reales_correlacionados.json")
    
    return directores_finales

def mapear_team_a_grupo(team_name):
    """Mapea nombre de team API a nombre de grupo operativo"""
    
    mapeos = {
        'PLOG Nuevo Leon': 'PLOG NUEVO LEON',
        'PLOG Laguna': 'PLOG LAGUNA', 
        'PLOG Queretaro': 'PLOG QUERETARO',
        'OGAS': 'OGAS',
        'TEPEYAC': 'TEPEYAC',
        'EXPO': 'EXPO',
        'El Pollo Loco México': None,  # Team principal, no grupo específico
        'All El Pollo Loco México': None
    }
    
    # Mapeo exacto
    if team_name in mapeos:
        return mapeos[team_name]
    
    # Mapeo por similitud
    team_lower = team_name.lower()
    if 'plog' in team_lower and 'nuevo' in team_lower:
        return 'PLOG NUEVO LEON'
    elif 'plog' in team_lower and 'laguna' in team_lower:
        return 'PLOG LAGUNA'
    elif 'plog' in team_lower and 'queretaro' in team_lower:
        return 'PLOG QUERETARO'
    elif 'saltillo' in team_lower:
        return 'GRUPO SALTILLO'
    elif 'tampico' in team_lower:
        return 'OCHTER TAMPICO'
    elif 'matamoros' in team_lower:
        return 'GRUPO MATAMOROS'
    
    return None

def buscar_director_similar(grupo, usuarios):
    """Busca director por similitud de teams o sucursales owned"""
    
    for user in usuarios:
        nombre_completo = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
        email = user.get('email', '')
        telefono = user.get('sms_number', '')
        rol = user.get('user_role', {}).get('name', '')
        
        # Buscar en teams
        teams = user.get('teams', [])
        for team in teams:
            team_name = team.get('name', '')
            grupo_mapeado = mapear_team_a_grupo(team_name)
            if grupo_mapeado == grupo:
                return {
                    'nombre': nombre_completo,
                    'email': email,
                    'telefono': telefono,
                    'rol': rol,
                    'team': team_name
                }
        
        # Buscar en default team
        default_team_obj = user.get('default_team') or {}
        default_team = default_team_obj.get('name', '') if default_team_obj else ''
        grupo_mapeado = mapear_team_a_grupo(default_team)
        if grupo_mapeado == grupo:
            return {
                'nombre': nombre_completo,
                'email': email,
                'telefono': telefono,
                'rol': rol,
                'team': default_team
            }
    
    return None

def extraer_numero_sucursal(sucursal_name):
    """Extrae número de sucursal del nombre"""
    
    import re
    match = re.search(r'^(\d+)\s*-', sucursal_name)
    if match:
        return int(match.group(1))
    return None

if __name__ == "__main__":
    print("🔍 EJECUTANDO CORRELACIÓN DIRECTORES REALES")
    print("Buscando directores desde API /users...")
    print()
    
    resultado = correlacionar_directores_reales()
    
    print(f"\n🎉 CORRELACIÓN COMPLETADA")
    print(f"📊 Directores identificados desde API real")
    print(f"📧 Emails y teléfonos correctos extraídos")