#!/usr/bin/env python3
"""
📊 ANÁLISIS ESTRUCTURA COMPLETA EPL
Combina datos API Zenput + Excel Roberto para estructura organizacional definitiva
"""

import pandas as pd
import json
import requests
from datetime import datetime
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

def analyze_complete_epl_structure():
    """Analiza estructura completa combinando API + Excel de Roberto"""
    
    print("📊 ANÁLISIS ESTRUCTURA COMPLETA EL POLLO LOCO MÉXICO")
    print("=" * 65)
    
    # 1. LEER EXCEL DE ROBERTO
    print("\n📋 LEYENDO EXCEL DE ROBERTO...")
    excel_path = "/Users/robertodavila/pollo-loco-tracking-gps/grupos_operativos_final_corregido.csv"
    
    try:
        df_roberto = pd.read_csv(excel_path)
        print(f"✅ Excel cargado: {len(df_roberto)} registros")
        
        # Analizar estructura del Excel
        grupos_roberto = df_roberto['Grupo_Operativo'].value_counts()
        print(f"\n📊 GRUPOS OPERATIVOS EN EXCEL ({len(grupos_roberto)}):") 
        for grupo, count in grupos_roberto.items():
            print(f"   • {grupo}: {count} sucursales")
            
    except Exception as e:
        print(f"❌ Error leyendo Excel: {e}")
        return False
    
    # 2. CONSULTAR API TEAMS
    print("\n👥 CONSULTANDO API ZENPUT TEAMS...")
    
    api_token = "cb908e0d4e0f5501c635325c611db314"
    headers = {
        'X-API-TOKEN': api_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    try:
        response = requests.get(
            'https://www.zenput.com/api/v3/teams',
            headers=headers,
            timeout=15
        )
        
        if response.status_code == 200:
            teams_data = response.json()
            print(f"✅ API Teams consultado exitosamente")
            
            # Analizar estructura de teams
            if isinstance(teams_data, dict) and 'data' in teams_data:
                teams_list = teams_data['data']
                print(f"📊 TEAMS EN API: {len(teams_list)}")
                
                # Buscar jerarquía principal "El Pollo Loco México"
                main_team = None
                for team in teams_list:
                    if 'El Pollo Loco México' in team.get('name', ''):
                        main_team = team
                        break
                
                if main_team:
                    print(f"🏢 EQUIPO PRINCIPAL ENCONTRADO: {main_team.get('name')}")
                    
                    # Analizar hijos/grupos
                    if 'children' in main_team:
                        api_groups = main_team['children']
                        print(f"📊 GRUPOS EN API: {len(api_groups)}")
                        
                        for group in api_groups[:10]:  # Primeros 10
                            print(f"   • {group.get('name', 'N/A')}")
                else:
                    print("⚠️ No se encontró equipo principal 'El Pollo Loco México'")
                    api_groups = []
            else:
                print("⚠️ Estructura inesperada en respuesta API")
                api_groups = []
        else:
            print(f"❌ Error API: {response.status_code}")
            api_groups = []
            
    except Exception as e:
        print(f"❌ Error consultando API: {e}")
        api_groups = []
    
    # 3. COMPARAR Y MAPEAR ESTRUCTURAS
    print(f"\n🔍 ANÁLISIS COMPARATIVO API vs EXCEL")
    print("=" * 45)
    
    # Grupos únicos del Excel
    grupos_excel = set(df_roberto['Grupo_Operativo'].dropna().unique())
    print(f"📊 GRUPOS ÚNICOS EN EXCEL: {len(grupos_excel)}")
    for grupo in sorted(grupos_excel):
        print(f"   • {grupo}")
    
    # Mapear grupos API si están disponibles
    if api_groups:
        grupos_api = set([group.get('name', '') for group in api_groups])
        print(f"\n📊 GRUPOS EN API: {len(grupos_api)}")
        for grupo in sorted(grupos_api):
            if grupo:
                print(f"   • {grupo}")
        
        # Buscar coincidencias
        coincidencias = grupos_excel.intersection(grupos_api)
        solo_excel = grupos_excel - grupos_api
        solo_api = grupos_api - grupos_excel
        
        print(f"\n🎯 ANÁLISIS DE COINCIDENCIAS:")
        print(f"   ✅ Coincidencias: {len(coincidencias)}")
        for grupo in sorted(coincidencias):
            print(f"      • {grupo}")
        
        print(f"   📋 Solo en Excel: {len(solo_excel)}")
        for grupo in sorted(solo_excel):
            print(f"      • {grupo}")
        
        print(f"   🔗 Solo en API: {len(solo_api)}")
        for grupo in sorted(solo_api):
            if grupo:
                print(f"      • {grupo}")
    
    # 4. CREAR ESTRUCTURA DEFINITIVA
    print(f"\n🏗️ CREANDO ESTRUCTURA ORGANIZACIONAL DEFINITIVA")
    print("=" * 55)
    
    estructura_definitiva = {
        'timestamp': datetime.now().isoformat(),
        'fuente': 'Excel Roberto + API Zenput Teams',
        'total_sucursales': len(df_roberto),
        'total_grupos': len(grupos_excel),
        'grupos_operativos': {},
        'sucursales': [],
        'estadisticas': {}
    }
    
    # Procesar cada grupo operativo
    for grupo in sorted(grupos_excel):
        sucursales_grupo = df_roberto[df_roberto['Grupo_Operativo'] == grupo]
        
        estructura_definitiva['grupos_operativos'][grupo] = {
            'nombre': grupo,
            'total_sucursales': len(sucursales_grupo),
            'sucursales': [],
            'cobertura_geografica': {
                'estados': sorted([e for e in sucursales_grupo['Estado'].dropna().unique().tolist() if e]),
                'ciudades': sorted([c for c in sucursales_grupo['Ciudad'].dropna().unique().tolist() if c])
            },
            'coordenadas_centro': calculate_center_coordinates(sucursales_grupo),
            'director_asignado': None,  # Se completará desde API users si está disponible
            'api_team_id': None        # Se completará si hay coincidencia
        }
        
        # Procesar sucursales del grupo
        for _, sucursal in sucursales_grupo.iterrows():
            sucursal_data = {
                'numero': int(sucursal['Numero_Sucursal']) if pd.notna(sucursal['Numero_Sucursal']) else None,
                'nombre': sucursal['Nombre_Sucursal'],
                'ciudad': sucursal['Ciudad'],
                'estado': sucursal['Estado'],
                'coordenadas': {
                    'lat': float(sucursal['Latitude']) if pd.notna(sucursal['Latitude']) else None,
                    'lon': float(sucursal['Longitude']) if pd.notna(sucursal['Longitude']) else None
                },
                'location_code': sucursal.get('Location_Code'),
                'synced_at': sucursal.get('Synced_At')
            }
            
            estructura_definitiva['grupos_operativos'][grupo]['sucursales'].append(sucursal_data)
            estructura_definitiva['sucursales'].append(sucursal_data)
    
    # 5. ESTADÍSTICAS GENERALES
    print(f"\n📊 ESTADÍSTICAS ESTRUCTURA DEFINITIVA:")
    
    # Por estado
    by_state = df_roberto['Estado'].value_counts()
    print(f"📍 POR ESTADO:")
    for estado, count in by_state.items():
        print(f"   • {estado}: {count} sucursales")
    
    # Sucursales con GPS
    with_gps = df_roberto[(pd.notna(df_roberto['Latitude'])) & (pd.notna(df_roberto['Longitude']))]
    print(f"\n🗺️ COORDENADAS GPS:")
    print(f"   • Con GPS: {len(with_gps)}/{len(df_roberto)} sucursales")
    print(f"   • Sin GPS: {len(df_roberto) - len(with_gps)} sucursales")
    
    # Grupos más grandes
    print(f"\n🏢 GRUPOS MÁS GRANDES:")
    for grupo, count in grupos_roberto.head(5).items():
        print(f"   • {grupo}: {count} sucursales")
    
    estructura_definitiva['estadisticas'] = {
        'por_estado': by_state.to_dict(),
        'con_coordenadas': len(with_gps),
        'sin_coordenadas': len(df_roberto) - len(with_gps),
        'grupo_mas_grande': grupos_roberto.index[0],
        'sucursales_grupo_mas_grande': int(grupos_roberto.iloc[0])
    }
    
    # 6. GUARDAR ESTRUCTURA DEFINITIVA
    print(f"\n💾 GUARDANDO ESTRUCTURA DEFINITIVA...")
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Guardar JSON
    json_filename = f"data/estructura_definitiva_epl_{timestamp}.json"
    os.makedirs('data', exist_ok=True)
    
    with open(json_filename, 'w', encoding='utf-8') as f:
        json.dump(estructura_definitiva, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"✅ Estructura JSON guardada: {json_filename}")
    
    # Generar CSV resumen
    csv_filename = f"data/grupos_operativos_resumen_{timestamp}.csv"
    
    resumen_grupos = []
    for grupo_name, grupo_data in estructura_definitiva['grupos_operativos'].items():
        resumen_grupos.append({
            'Grupo_Operativo': grupo_name,
            'Total_Sucursales': grupo_data['total_sucursales'],
            'Estados': ', '.join(grupo_data['cobertura_geografica']['estados']),
            'Ciudades': len(grupo_data['cobertura_geografica']['ciudades']),
            'Lat_Centro': grupo_data['coordenadas_centro']['lat'],
            'Lon_Centro': grupo_data['coordenadas_centro']['lon']
        })
    
    pd.DataFrame(resumen_grupos).to_csv(csv_filename, index=False)
    print(f"✅ Resumen CSV guardado: {csv_filename}")
    
    # 7. RECOMENDACIONES FINALES
    print(f"\n🎯 ESTRUCTURA FINAL PARA RAILWAY")
    print("=" * 40)
    
    print(f"✅ ESTRUCTURA ORGANIZACIONAL COMPLETA:")
    print(f"   • {len(estructura_definitiva['grupos_operativos'])} grupos operativos")
    print(f"   • {estructura_definitiva['total_sucursales']} sucursales totales")
    print(f"   • {estructura_definitiva['estadisticas']['con_coordenadas']} con GPS")
    print(f"   • Cobertura: {len(by_state)} estados")
    
    print(f"\n📋 PRÓXIMOS PASOS:")
    print(f"   1. ✅ Estructura organizacional definida")
    print(f"   2. 🔄 Consultar API /users para directores")
    print(f"   3. 🚀 Crear base de datos Railway")
    print(f"   4. 📊 Implementar ETL completo")
    
    return estructura_definitiva, json_filename

def calculate_center_coordinates(sucursales_df):
    """Calcula coordenadas del centro geográfico de un grupo de sucursales"""
    
    # Filtrar sucursales con coordenadas válidas
    with_coords = sucursales_df[(pd.notna(sucursales_df['Latitude'])) & (pd.notna(sucursales_df['Longitude']))]
    
    if len(with_coords) == 0:
        return {'lat': None, 'lon': None}
    
    # Calcular centro geográfico
    lat_center = with_coords['Latitude'].mean()
    lon_center = with_coords['Longitude'].mean()
    
    return {
        'lat': round(float(lat_center), 6),
        'lon': round(float(lon_center), 6)
    }

if __name__ == "__main__":
    print("📊 INICIANDO ANÁLISIS ESTRUCTURA COMPLETA")
    print("Combinando Excel Roberto + API Zenput...")
    print()
    
    resultado = analyze_complete_epl_structure()
    
    if resultado:
        estructura, archivo = resultado
        print(f"\n🎉 ANÁLISIS COMPLETADO EXITOSAMENTE")
        print(f"📋 Estructura definitiva lista para Railway")
        print(f"📁 Archivo: {archivo}")
    else:
        print(f"\n❌ ERROR EN ANÁLISIS")
        print(f"💡 Verificar archivos y conexiones")