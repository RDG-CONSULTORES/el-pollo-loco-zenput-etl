#!/usr/bin/env python3
"""
SCRIPT FINAL: Extraer mapping Teams → Grupos Operativos
Autor: Claude Code
Fecha: 2025-12-18
Objetivo: Generar mapping completo para ETL sin parches
"""

import json
import csv
import requests
import time
from datetime import datetime
from typing import Dict, List, Optional

# Configuración API
API_BASE = "https://www.zenput.com/api/v3"
API_TOKEN = "cb908e0d4e0f5501c635325c611db314"
HEADERS = {'Authorization': f'Bearer {API_TOKEN}', 'Accept': 'application/json'}

def load_sucursales_master() -> Dict[int, Dict]:
    """Cargar datos master de sucursales"""
    print("📊 Cargando 86_sucursales_master.csv...")
    
    location_data = {}
    with open('data/86_sucursales_master.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            location_id = int(row['Location_Code'])
            location_data[location_id] = {
                'numero_sucursal': int(row['Numero_Sucursal']),
                'nombre_sucursal': row['Nombre_Sucursal'],
                'grupo_operativo': row['Grupo_Operativo'],
                'ciudad': row['Ciudad'],
                'estado': row['Estado']
            }
    
    print(f"✅ Cargadas {len(location_data)} sucursales")
    return location_data

def get_recent_submission(location_id: int, form_id: int = 877138) -> Optional[Dict]:
    """Obtener submission más reciente de una sucursal"""
    
    url = f"{API_BASE}/submissions"
    params = {
        'form_id': form_id,
        'location_id': location_id,
        'limit': 1,
        'ordering': '-date_submitted'
    }
    
    try:
        response = requests.get(url, params=params, headers=HEADERS)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('data') and len(data['data']) > 0:
                return data['data'][0]
        elif response.status_code == 429:
            print("⚠️ Rate limit, esperando...")
            time.sleep(2)
            return get_recent_submission(location_id, form_id)
            
    except Exception as e:
        print(f"❌ Error obteniendo submission para location {location_id}: {e}")
    
    return None

def extract_teams_from_submission(submission: Dict) -> List[int]:
    """Extraer teams de una submission"""
    
    teams = []
    
    # Buscar teams en smetadata
    if 'smetadata' in submission:
        metadata = submission['smetadata']
        
        # Teams principales
        if 'teams' in metadata:
            teams.extend([t['id'] for t in metadata['teams']])
        
        # Teams de location
        if 'location_teams' in metadata:
            teams.extend([t['id'] for t in metadata['location_teams']])
        
        # Teams de usuario
        if 'user_teams' in metadata:
            teams.extend([t['id'] for t in metadata['user_teams']])
    
    # Eliminar duplicados manteniendo orden
    seen = set()
    unique_teams = []
    for team_id in teams:
        if team_id not in seen:
            seen.add(team_id)
            unique_teams.append(team_id)
    
    return unique_teams

def extract_complete_mapping() -> List[Dict]:
    """Extraer mapping completo teams → grupos operativos"""
    
    print("🚀 Iniciando extracción de mapping teams → grupos...")
    
    # Cargar datos master
    locations_master = load_sucursales_master()
    
    # Resultados
    teams_mapping = []
    teams_seen = set()
    
    # Estadísticas
    processed = 0
    errors = 0
    total_locations = len(locations_master)
    
    print(f"📍 Procesando {total_locations} sucursales...")
    
    for location_id, info in locations_master.items():
        processed += 1
        print(f"🔄 [{processed}/{total_locations}] Procesando {info['nombre_sucursal']}...")
        
        try:
            # Obtener submission más reciente
            submission = get_recent_submission(location_id)
            
            if submission:
                # Extraer teams
                teams = extract_teams_from_submission(submission)
                
                if teams:
                    print(f"   ✅ Encontrados {len(teams)} teams: {teams}")
                    
                    # Mapear cada team al grupo operativo
                    for team_id in teams:
                        if team_id not in teams_seen:
                            teams_mapping.append({
                                'team_id': team_id,
                                'grupo_operativo': info['grupo_operativo'],
                                'location_id': location_id,
                                'numero_sucursal': info['numero_sucursal'],
                                'nombre_sucursal': info['nombre_sucursal'],
                                'ciudad': info['ciudad'],
                                'estado': info['estado'],
                                'extracted_at': datetime.now().isoformat(),
                                'form_used': 877138
                            })
                            teams_seen.add(team_id)
                else:
                    print(f"   ⚠️ No teams encontrados")
            else:
                print(f"   ❌ No submission encontrada")
                errors += 1
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
            errors += 1
            
        # Rate limiting
        time.sleep(0.5)
    
    print(f"\n📊 RESUMEN:")
    print(f"   📍 Sucursales procesadas: {processed}")
    print(f"   ✅ Teams únicos encontrados: {len(teams_mapping)}")
    print(f"   ❌ Errores: {errors}")
    print(f"   📈 Tasa de éxito: {((processed-errors)/processed*100):.1f}%")
    
    return teams_mapping

def save_mapping_results(teams_mapping: List[Dict]) -> None:
    """Guardar resultados del mapping"""
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # JSON completo
    json_file = f"data/teams_grupos_mapping_{timestamp}.json"
    with open(json_file, 'w') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'total_teams': len(teams_mapping),
            'groups_covered': len(set([m['grupo_operativo'] for m in teams_mapping])),
            'mapping': teams_mapping
        }, f, indent=2)
    print(f"💾 Guardado JSON: {json_file}")
    
    # CSV para SQL
    csv_file = f"data/teams_grupos_mapping_{timestamp}.csv"
    if teams_mapping:
        with open(csv_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=teams_mapping[0].keys())
            writer.writeheader()
            writer.writerows(teams_mapping)
        print(f"💾 Guardado CSV: {csv_file}")
    
    # SQL INSERT statements
    sql_file = f"sql/insert_teams_mapping_{timestamp}.sql"
    with open(sql_file, 'w') as f:
        f.write("-- Teams → Grupos Operativos Mapping\n")
        f.write("-- Generado automáticamente: " + datetime.now().isoformat() + "\n\n")
        
        f.write("CREATE TABLE IF NOT EXISTS teams_grupos_mapping (\n")
        f.write("    team_id INT PRIMARY KEY,\n")
        f.write("    grupo_operativo VARCHAR(50) NOT NULL,\n")
        f.write("    location_id INT NOT NULL,\n")
        f.write("    numero_sucursal INT NOT NULL,\n")
        f.write("    nombre_sucursal VARCHAR(100),\n")
        f.write("    ciudad VARCHAR(100),\n")
        f.write("    estado VARCHAR(50),\n")
        f.write("    extracted_at TIMESTAMP,\n")
        f.write("    INDEX idx_grupo_operativo (grupo_operativo),\n")
        f.write("    INDEX idx_location (location_id)\n")
        f.write(");\n\n")
        
        f.write("-- Limpiar tabla existente\n")
        f.write("DELETE FROM teams_grupos_mapping;\n\n")
        
        f.write("-- Insertar mapping\n")
        for mapping in teams_mapping:
            f.write(f"INSERT INTO teams_grupos_mapping VALUES ({mapping['team_id']}, ")
            f.write(f"'{mapping['grupo_operativo']}', {mapping['location_id']}, ")
            f.write(f"{mapping['numero_sucursal']}, '{mapping['nombre_sucursal']}', ")
            f.write(f"'{mapping['ciudad']}', '{mapping['estado']}', ")
            f.write(f"'{mapping['extracted_at']}');\n")
        
        f.write("\n-- Verificar resultados\n")
        f.write("SELECT grupo_operativo, COUNT(*) as teams_count FROM teams_grupos_mapping GROUP BY grupo_operativo ORDER BY teams_count DESC;\n")
    
    print(f"💾 Guardado SQL: {sql_file}")

def analyze_mapping_results(teams_mapping: List[Dict]) -> None:
    """Analizar resultados del mapping"""
    
    print("\n📈 ANÁLISIS DEL MAPPING:")
    
    # Grupos operativos cubiertos
    grupos = {}
    for mapping in teams_mapping:
        grupo = mapping['grupo_operativo']
        if grupo not in grupos:
            grupos[grupo] = []
        grupos[grupo].append(mapping['team_id'])
    
    print(f"\n🏢 GRUPOS OPERATIVOS CUBIERTOS ({len(grupos)}):")
    for grupo, teams in sorted(grupos.items()):
        print(f"   {grupo}: {len(teams)} teams")
    
    # Teams más comunes
    team_frequency = {}
    for mapping in teams_mapping:
        team_id = mapping['team_id']
        team_frequency[team_id] = team_frequency.get(team_id, 0) + 1
    
    print(f"\n👥 TEAMS MÁS COMUNES:")
    for team_id, freq in sorted(team_frequency.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"   Team {team_id}: presente en {freq} grupo(s)")
    
    # Validaciones
    print(f"\n✅ VALIDACIONES:")
    total_teams = len(set([m['team_id'] for m in teams_mapping]))
    total_grupos = len(grupos)
    print(f"   🎯 Teams únicos extraídos: {total_teams}")
    print(f"   🏢 Grupos operativos cubiertos: {total_grupos}")
    print(f"   📊 Promedio teams por grupo: {total_teams/total_grupos:.1f}")

def main():
    """Función principal"""
    
    print("=" * 60)
    print("🚀 EXTRACTOR DE MAPPING TEAMS → GRUPOS OPERATIVOS")
    print("   El Pollo Loco - Zenput ETL")
    print("   Fecha:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("=" * 60)
    
    try:
        # Extraer mapping
        teams_mapping = extract_complete_mapping()
        
        if teams_mapping:
            # Guardar resultados
            save_mapping_results(teams_mapping)
            
            # Analizar resultados
            analyze_mapping_results(teams_mapping)
            
            print("\n" + "=" * 60)
            print("✅ MAPPING COMPLETADO EXITOSAMENTE")
            print("🎯 Próximo paso: Ejecutar SQL generado en la base de datos")
            print("📁 Archivos generados en data/ y sql/")
            print("=" * 60)
        else:
            print("\n❌ No se pudo extraer mapping. Verificar conectividad API.")
            
    except Exception as e:
        print(f"\n❌ Error fatal: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()