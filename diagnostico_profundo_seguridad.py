#!/usr/bin/env python3
"""
🔍 DIAGNÓSTICO PROFUNDO - Supervisiones Seguridad Faltantes
Analizar las 85 supervisiones que no se pudieron cargar
"""

import requests
import json
import psycopg2
from datetime import datetime
import traceback

# Configuración
DATABASE_URL = 'postgresql://postgres:qGgdIUuKYKMKGtSNYzARpyapBWHsloOt@turntable.proxy.rlwy.net:24097/railway'
ZENPUT_CONFIG = {
    'base_url': 'https://www.zenput.com/api/v3',
    'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}
}

# Teams mapping
TEAMS_TO_GRUPOS = {
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

def extraer_todas_submissions_seguridad():
    """Extraer TODAS las submissions de seguridad con análisis"""
    
    print("📊 Extrayendo TODAS las submissions de seguridad...")
    
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
        
        try:
            response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                submissions = data.get('data', [])
                
                if not submissions:
                    break
                
                print(f"   📄 Página {page + 1}: {len(submissions)} submissions")
                
                # Analizar cada submission en esta página
                for i, submission in enumerate(submissions):
                    print(f"      {i+1}. Type: {type(submission)}, Is None: {submission is None}")
                    if submission is None:
                        print(f"         ❌ SUBMISSION NONE DETECTADA en página {page + 1}, posición {i+1}")
                
                all_submissions.extend(submissions)
                page += 1
                
            else:
                print(f"   ❌ Error API: {response.status_code}")
                break
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
            break
    
    print(f"✅ Total extraído: {len(all_submissions)} submissions")
    
    # Contar None submissions
    none_count = sum(1 for s in all_submissions if s is None)
    valid_count = len(all_submissions) - none_count
    
    print(f"📊 Análisis inicial:")
    print(f"   ✅ Submissions válidas: {valid_count}")
    print(f"   ❌ Submissions None: {none_count}")
    
    return all_submissions

def analizar_estructura_submission(submission, index):
    """Analizar estructura detallada de una submission"""
    
    resultado = {
        'index': index,
        'es_none': submission is None,
        'tiene_id': False,
        'tiene_smetadata': False,
        'tiene_location': False,
        'tiene_teams': False,
        'teams_count': 0,
        'teams_mapeados': 0,
        'grupo_encontrado': None,
        'error_detalle': None
    }
    
    try:
        if submission is None:
            resultado['error_detalle'] = "Submission is None"
            return resultado
        
        # Verificar ID
        if 'id' in submission:
            resultado['tiene_id'] = True
        
        # Verificar smetadata
        if 'smetadata' in submission and submission['smetadata']:
            resultado['tiene_smetadata'] = True
            smetadata = submission['smetadata']
            
            # Verificar location
            if 'location' in smetadata and smetadata['location']:
                resultado['tiene_location'] = True
            
            # Verificar teams
            if 'teams' in smetadata and smetadata['teams']:
                resultado['tiene_teams'] = True
                teams = smetadata['teams']
                resultado['teams_count'] = len(teams)
                
                # Contar teams mapeados
                for team_info in teams:
                    team_id = team_info.get('id')
                    if team_id in TEAMS_TO_GRUPOS:
                        resultado['teams_mapeados'] += 1
                        if not resultado['grupo_encontrado']:
                            resultado['grupo_encontrado'] = TEAMS_TO_GRUPOS[team_id]
        
    except Exception as e:
        resultado['error_detalle'] = str(e)
    
    return resultado

def diagnostico_completo():
    """Diagnóstico completo de supervisiones de seguridad"""
    
    print("🔍 INICIANDO DIAGNÓSTICO PROFUNDO")
    print("=" * 60)
    
    # 1. Extraer todas las submissions
    submissions = extraer_todas_submissions_seguridad()
    
    if not submissions:
        print("❌ No se pudieron extraer submissions")
        return
    
    # 2. Análisis detallado de cada submission
    print(f"\n🔬 ANÁLISIS DETALLADO DE {len(submissions)} SUBMISSIONS")
    print("-" * 60)
    
    resultados = []
    submissions_problematicas = []
    
    for i, submission in enumerate(submissions):
        resultado = analizar_estructura_submission(submission, i)
        resultados.append(resultado)
        
        if resultado['es_none'] or resultado['error_detalle'] or not resultado['grupo_encontrado']:
            submissions_problematicas.append((i, submission, resultado))
    
    # 3. Estadísticas del análisis
    print(f"\n📊 ESTADÍSTICAS DEL ANÁLISIS")
    print("-" * 40)
    
    total_submissions = len(submissions)
    submissions_none = sum(1 for r in resultados if r['es_none'])
    sin_id = sum(1 for r in resultados if not r['tiene_id'])
    sin_smetadata = sum(1 for r in resultados if not r['tiene_smetadata'])
    sin_location = sum(1 for r in resultados if not r['tiene_location'])
    sin_teams = sum(1 for r in resultados if not r['tiene_teams'])
    sin_grupo = sum(1 for r in resultados if not r['grupo_encontrado'])
    submissions_validas = total_submissions - len(submissions_problematicas)
    
    print(f"   📊 Total submissions: {total_submissions}")
    print(f"   ❌ Submissions None: {submissions_none}")
    print(f"   ❌ Sin ID: {sin_id}")
    print(f"   ❌ Sin smetadata: {sin_smetadata}")
    print(f"   ❌ Sin location: {sin_location}")
    print(f"   ❌ Sin teams: {sin_teams}")
    print(f"   ❌ Sin grupo mapeado: {sin_grupo}")
    print(f"   ✅ Submissions procesables: {submissions_validas}")
    print(f"   🎯 Tasa de éxito esperada: {(submissions_validas/total_submissions)*100:.1f}%")
    
    # 4. Análisis detallado de submissions problemáticas
    if submissions_problematicas:
        print(f"\n🚨 ANÁLISIS DE {len(submissions_problematicas)} SUBMISSIONS PROBLEMÁTICAS")
        print("-" * 60)
        
        categorias_problemas = {}
        
        for i, submission, resultado in submissions_problematicas[:10]:  # Solo las primeras 10
            problema_tipo = "submission_none" if resultado['es_none'] else \
                          "sin_grupo" if not resultado['grupo_encontrado'] else \
                          "error_procesamiento"
            
            if problema_tipo not in categorias_problemas:
                categorias_problemas[problema_tipo] = []
            categorias_problemas[problema_tipo].append((i, resultado))
            
            print(f"\n   🔍 Submission #{i+1}:")
            print(f"      Es None: {resultado['es_none']}")
            print(f"      Tiene ID: {resultado['tiene_id']}")
            print(f"      Tiene smetadata: {resultado['tiene_smetadata']}")
            print(f"      Tiene location: {resultado['tiene_location']}")
            print(f"      Tiene teams: {resultado['tiene_teams']}")
            print(f"      Teams count: {resultado['teams_count']}")
            print(f"      Teams mapeados: {resultado['teams_mapeados']}")
            print(f"      Grupo encontrado: {resultado['grupo_encontrado']}")
            if resultado['error_detalle']:
                print(f"      Error: {resultado['error_detalle']}")
        
        # 5. Resumen por categorías
        print(f"\n📋 RESUMEN POR CATEGORÍAS DE PROBLEMAS")
        print("-" * 50)
        for categoria, items in categorias_problemas.items():
            print(f"   {categoria}: {len(items)} submissions")
    
    # 6. Verificar qué está cargado en la base de datos
    print(f"\n💾 VERIFICACIÓN BASE DE DATOS")
    print("-" * 40)
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM supervisiones_2026 WHERE form_type = 'SEGURIDAD';")
        cargadas_bd = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT grupo_operativo, COUNT(*) 
            FROM supervisiones_2026 
            WHERE form_type = 'SEGURIDAD' 
            GROUP BY grupo_operativo 
            ORDER BY COUNT(*) DESC;
        """)
        por_grupo = cursor.fetchall()
        
        print(f"   💾 Supervisiones en BD: {cargadas_bd}")
        print(f"   📊 Diferencia: {total_submissions - cargadas_bd} faltantes")
        
        print(f"\n   📊 Distribución por grupo (top 10):")
        for grupo, count in por_grupo[:10]:
            print(f"      {grupo}: {count}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"   ❌ Error consultando BD: {e}")
    
    # 7. Recomendaciones
    print(f"\n💡 RECOMENDACIONES")
    print("-" * 30)
    
    if submissions_none > 0:
        print(f"   🔧 Filtrar submissions None antes del procesamiento")
    
    if sin_grupo > 0:
        print(f"   🔧 Implementar mapeo alternativo para {sin_grupo} submissions sin grupo")
        print(f"      - Mapeo por location.external_key")
        print(f"      - Mapeo por location.id")
        print(f"      - Mapeo manual para casos especiales")
    
    print(f"   🚀 Crear ETL mejorado con recuperación de errores")
    print(f"   📊 Implementar logging detallado de errores")
    
    print(f"\n🎯 PRÓXIMO PASO: Crear ETL optimizado para recuperar las {total_submissions - submissions_validas} submissions faltantes")

if __name__ == "__main__":
    diagnostico_completo()