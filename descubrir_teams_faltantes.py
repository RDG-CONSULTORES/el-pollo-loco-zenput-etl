#!/usr/bin/env python3
"""
🔍 DESCUBRIR TEAMS FALTANTES
Identificar teams no mapeados en las 85 supervisiones problemáticas
"""

import requests
import json
from collections import Counter

# Configuración
ZENPUT_CONFIG = {
    'base_url': 'https://www.zenput.com/api/v3',
    'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}
}

# Teams mapping conocido
TEAMS_CONOCIDOS = {
    115097: "TEPEYAC",
    115098: "EXPO", 
    115099: "PLOG NUEVO LEON",
    115100: "OGAS",
    115101: "EFM",
    115102: "RAP",
    115103: "CRR",
    115104: "TEC",
    115105: "EPL SO",
    115106: "PLOG LAGUNA",
    115107: "PLOG QUERETARO",
    115108: "GRUPO SALTILLO",
    115109: "OCHTER TAMPICO",
    115110: "GRUPO CANTERA ROSA (MORELIA)",
    115111: "GRUPO MATAMOROS",
    115112: "GRUPO PIEDRAS NEGRAS", 
    115113: "GRUPO CENTRITO",
    115114: "GRUPO SABINAS HIDALGO",
    115115: "GRUPO RIO BRAVO",
    115116: "GRUPO NUEVO LAREDO (RUELAS)"
}

def extraer_teams_desconocidos():
    """Identificar teams no mapeados en submissions problemáticas"""
    
    print("🔍 IDENTIFICANDO TEAMS NO MAPEADOS")
    print("=" * 50)
    
    # Extraer todas las submissions
    all_submissions = []
    page = 0
    limit = 100
    
    while True:
        url = f"{ZENPUT_CONFIG['base_url']}/submissions"
        params = {
            'form_template_id': '877139',
            'limit': limit,
            'offset': page * limit,
            'created_after': '2025-01-01'
        }
        
        response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            submissions = data.get('data', [])
            
            if not submissions:
                break
                
            all_submissions.extend(submissions)
            page += 1
        else:
            break
    
    print(f"✅ Extraídas {len(all_submissions)} submissions totales")
    
    # Analizar teams en submissions problemáticas
    teams_desconocidos = Counter()
    teams_desconocidos_detalles = {}
    submission_ids_problematicas = []
    
    for i, submission in enumerate(all_submissions):
        if not submission:
            continue
            
        smetadata = submission.get('smetadata', {})
        location = smetadata.get('location')
        teams_data = smetadata.get('teams', [])
        
        # Si no hay location pero sí teams, es problemática
        if not location and teams_data:
            submission_id = submission.get('id')
            submission_ids_problematicas.append(submission_id)
            
            for team_info in teams_data:
                team_id = team_info.get('id')
                team_name = team_info.get('name', 'Sin nombre')
                
                if team_id not in TEAMS_CONOCIDOS:
                    teams_desconocidos[team_id] += 1
                    teams_desconocidos_detalles[team_id] = {
                        'name': team_name,
                        'count': teams_desconocidos[team_id],
                        'sample_submission': submission_id
                    }
    
    print(f"\n📊 RESULTADOS DEL ANÁLISIS")
    print("-" * 40)
    print(f"   🚨 Submissions problemáticas: {len(submission_ids_problematicas)}")
    print(f"   🆔 Teams desconocidos encontrados: {len(teams_desconocidos)}")
    
    if teams_desconocidos:
        print(f"\n🔍 TEAMS DESCONOCIDOS DETECTADOS")
        print("-" * 40)
        
        for team_id, count in teams_desconocidos.most_common():
            details = teams_desconocidos_detalles[team_id]
            print(f"   Team {team_id}: '{details['name']}' ({count} submissions)")
        
        print(f"\n📋 PROPUESTA DE MAPPING AMPLIADO")
        print("-" * 40)
        print("# Agregar a TEAMS_TO_GRUPOS:")
        
        for team_id in sorted(teams_desconocidos.keys()):
            details = teams_desconocidos_detalles[team_id]
            team_name = details['name']
            
            # Sugerir grupo basado en el nombre del team
            grupo_sugerido = sugerir_grupo_por_nombre(team_name)
            
            print(f"{team_id}: \"{grupo_sugerido}\",  # {team_name}")
    
    # Obtener información adicional de una submission problemática
    if submission_ids_problematicas:
        print(f"\n🔍 ANÁLISIS DETALLADO DE SUBMISSION PROBLEMÁTICA")
        print("-" * 50)
        
        # Analizar la primera submission problemática
        for submission in all_submissions:
            if submission and submission.get('id') == submission_ids_problematicas[0]:
                analizar_submission_detallada(submission)
                break
    
    return teams_desconocidos_detalles

def sugerir_grupo_por_nombre(team_name):
    """Sugerir grupo operativo basado en el nombre del team"""
    
    team_name_upper = team_name.upper()
    
    # Patrones conocidos
    if 'TEPEYAC' in team_name_upper:
        return 'TEPEYAC'
    elif 'EXPO' in team_name_upper:
        return 'EXPO'
    elif 'OGAS' in team_name_upper:
        return 'OGAS'
    elif 'EFM' in team_name_upper:
        return 'EFM'
    elif 'TEC' in team_name_upper:
        return 'TEC'
    elif 'SALTILLO' in team_name_upper:
        return 'GRUPO SALTILLO'
    elif 'LAGUNA' in team_name_upper:
        return 'PLOG LAGUNA'
    elif 'NUEVO LEON' in team_name_upper or 'PLOG' in team_name_upper:
        return 'PLOG NUEVO LEON'
    elif 'QUERETARO' in team_name_upper:
        return 'PLOG QUERETARO'
    elif 'TAMPICO' in team_name_upper or 'OCHTER' in team_name_upper:
        return 'OCHTER TAMPICO'
    elif 'MATAMOROS' in team_name_upper:
        return 'GRUPO MATAMOROS'
    elif 'CENTRITO' in team_name_upper:
        return 'GRUPO CENTRITO'
    elif 'MORELIA' in team_name_upper or 'CANTERA' in team_name_upper:
        return 'GRUPO CANTERA ROSA (MORELIA)'
    elif 'PIEDRAS NEGRAS' in team_name_upper:
        return 'GRUPO PIEDRAS NEGRAS'
    elif 'SABINAS' in team_name_upper:
        return 'GRUPO SABINAS HIDALGO'
    elif 'RIO BRAVO' in team_name_upper:
        return 'GRUPO RIO BRAVO'
    elif 'NUEVO LAREDO' in team_name_upper or 'RUELAS' in team_name_upper:
        return 'GRUPO NUEVO LAREDO (RUELAS)'
    elif 'RAP' in team_name_upper:
        return 'RAP'
    elif 'CRR' in team_name_upper:
        return 'CRR'
    elif 'EPL' in team_name_upper:
        return 'EPL SO'
    else:
        return f'GRUPO_{team_name_upper.replace(" ", "_")}'

def analizar_submission_detallada(submission):
    """Análisis detallado de una submission problemática"""
    
    print(f"📋 Submission ID: {submission.get('id')}")
    
    smetadata = submission.get('smetadata', {})
    
    print(f"🏢 Location: {smetadata.get('location', 'None')}")
    
    teams_data = smetadata.get('teams', [])
    print(f"👥 Teams ({len(teams_data)}):")
    
    for i, team_info in enumerate(teams_data, 1):
        team_id = team_info.get('id')
        team_name = team_info.get('name', 'Sin nombre')
        is_known = team_id in TEAMS_CONOCIDOS
        
        print(f"   {i}. Team {team_id}: '{team_name}' {'✅' if is_known else '❌'}")
        
        if is_known:
            print(f"      → Mapeado a: {TEAMS_CONOCIDOS[team_id]}")
    
    # Mostrar otros datos relevantes
    created_by = smetadata.get('created_by', {})
    print(f"👤 Auditor: {created_by.get('display_name', 'Unknown')}")
    print(f"📅 Fecha: {smetadata.get('date_submitted', 'Unknown')}")
    
    # Buscar ubicación alternativa en otros campos
    print(f"\n🔍 BÚSQUEDA ALTERNATIVA DE UBICACIÓN:")
    
    if 'lat' in smetadata and 'lon' in smetadata:
        print(f"   📍 Coordenadas en smetadata: {smetadata['lat']}, {smetadata['lon']}")
    
    # Verificar si hay location_id en otros lugares
    activity = submission.get('activity', {})
    if 'location' in activity:
        print(f"   🏢 Location en activity: {activity['location']}")

if __name__ == "__main__":
    teams_faltantes = extraer_teams_desconocidos()
    
    print(f"\n🎯 PRÓXIMO PASO:")
    print("1. Agregar los teams desconocidos al mapping")
    print("2. Crear ETL corregido con mapping ampliado")
    print("3. Re-procesar las 85 submissions faltantes")