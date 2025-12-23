#!/usr/bin/env python3
"""
🔍 INVESTIGAR FECHAS REALES DE SUBMISSIONS
Ver qué fechas tienen realmente las submissions
"""

import requests
import json
from datetime import datetime
from collections import defaultdict

ZENPUT_CONFIG = {
    'base_url': 'https://www.zenput.com/api/v3',
    'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}
}

FORMULARIOS = {
    '877138': 'OPERATIVA',
    '877139': 'SEGURIDAD'
}

def investigar_fechas_submissions():
    """Investigar qué fechas tienen realmente las submissions"""
    
    print("🔍 INVESTIGANDO FECHAS REALES DE SUBMISSIONS")
    print("=" * 80)
    
    todas_las_fechas = defaultdict(int)
    ejemplos_submissions = []
    
    for form_id, tipo_form in FORMULARIOS.items():
        print(f"\n📋 Investigando {tipo_form} (Form {form_id})...")
        
        try:
            # Obtener submissions SIN filtro de fecha
            url = f"{ZENPUT_CONFIG['base_url']}/submissions"
            params = {
                'form_template_id': form_id,
                'page': 1,
                'page_size': 50  # Muestra de 50
            }
            
            response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                submissions = data.get('data', [])
                count = data.get('count', 0)
                
                print(f"   📊 Total en sistema: {count:,}")
                print(f"   📄 Muestra obtenida: {len(submissions)}")
                
                if submissions:
                    print(f"   📅 Analizando fechas de muestra...")
                    
                    for i, submission in enumerate(submissions):
                        # Extraer metadatos
                        submission_id = submission.get('id')
                        smetadata = submission.get('smetadata', {})
                        
                        # Todas las fechas disponibles
                        fecha_created = smetadata.get('date_created')
                        fecha_submitted = smetadata.get('date_submitted')
                        fecha_completed = smetadata.get('date_completed')
                        fecha_modified = smetadata.get('date_modified')
                        fecha_created_local = smetadata.get('date_created_local')
                        fecha_submitted_local = smetadata.get('date_submitted_local')
                        
                        # Usuario y location
                        created_by = smetadata.get('created_by', {})
                        usuario = created_by.get('display_name', 'Unknown')
                        
                        location = smetadata.get('location', {})
                        location_name = location.get('name', 'Unknown')
                        
                        # Coordenadas
                        lat = smetadata.get('lat')
                        lon = smetadata.get('lon')
                        
                        # Procesar fechas
                        fechas_encontradas = {}
                        
                        for campo, fecha_str in [
                            ('created', fecha_created),
                            ('submitted', fecha_submitted),
                            ('completed', fecha_completed),
                            ('modified', fecha_modified),
                            ('created_local', fecha_created_local),
                            ('submitted_local', fecha_submitted_local)
                        ]:
                            if fecha_str:
                                try:
                                    # Parsear fecha
                                    if 'T' in fecha_str:
                                        fecha_dt = datetime.fromisoformat(fecha_str.replace('Z', '+00:00'))
                                        fecha_solo = fecha_dt.date()
                                        año = fecha_dt.year
                                        mes = fecha_dt.month
                                        
                                        fechas_encontradas[campo] = {
                                            'original': fecha_str,
                                            'parsed': fecha_dt,
                                            'date_only': fecha_solo,
                                            'año': año,
                                            'mes': mes
                                        }
                                        
                                        # Contar por año-mes
                                        año_mes = f"{año}-{mes:02d}"
                                        todas_las_fechas[año_mes] += 1
                                        
                                except Exception as e:
                                    fechas_encontradas[campo] = {'error': str(e)}
                        
                        # Guardar ejemplo
                        if i < 5:  # Solo primeros 5 ejemplos
                            ejemplos_submissions.append({
                                'form_type': tipo_form,
                                'submission_id': submission_id,
                                'usuario': usuario,
                                'location_name': location_name,
                                'lat': lat,
                                'lon': lon,
                                'fechas': fechas_encontradas
                            })
                
            else:
                print(f"   ❌ Error {response.status_code}: {response.text[:100]}")
                
        except Exception as e:
            print(f"   💥 Error: {e}")
    
    # Mostrar distribución de fechas
    print(f"\n📊 DISTRIBUCIÓN DE FECHAS ENCONTRADAS:")
    print("=" * 50)
    
    if todas_las_fechas:
        fechas_ordenadas = sorted(todas_las_fechas.items())
        for año_mes, count in fechas_ordenadas:
            print(f"   {año_mes}: {count:,} submissions")
    else:
        print("   ❌ No se encontraron fechas válidas")
    
    # Mostrar ejemplos detallados
    print(f"\n🔍 EJEMPLOS DE SUBMISSIONS:")
    print("=" * 60)
    
    for i, ejemplo in enumerate(ejemplos_submissions, 1):
        print(f"\n   {i}. {ejemplo['form_type']} - ID: {ejemplo['submission_id']}")
        print(f"      Usuario: {ejemplo['usuario']}")
        print(f"      Location: {ejemplo['location_name']}")
        print(f"      Coordenadas: {ejemplo['lat']}, {ejemplo['lon']}")
        print(f"      Fechas:")
        
        for campo, info_fecha in ejemplo['fechas'].items():
            if 'error' in info_fecha:
                print(f"        {campo}: ERROR - {info_fecha['error']}")
            else:
                print(f"        {campo}: {info_fecha['date_only']} ({info_fecha['original'][:19]})")
    
    return todas_las_fechas, ejemplos_submissions

def probar_diferentes_filtros():
    """Probar diferentes filtros de fecha para entender el problema"""
    
    print(f"\n🧪 PROBANDO DIFERENTES FILTROS DE FECHA")
    print("=" * 70)
    
    filtros_a_probar = [
        {
            'nombre': 'Sin filtro',
            'params': {'form_template_id': '877138', 'page': 1, 'page_size': 5}
        },
        {
            'nombre': '2024 completo',
            'params': {
                'form_template_id': '877138',
                'created_after': '2024-01-01T00:00:00Z',
                'created_before': '2024-12-31T23:59:59Z',
                'page': 1,
                'page_size': 5
            }
        },
        {
            'nombre': '2025 completo',
            'params': {
                'form_template_id': '877138',
                'created_after': '2025-01-01T00:00:00Z',
                'created_before': '2025-12-31T23:59:59Z',
                'page': 1,
                'page_size': 5
            }
        },
        {
            'nombre': 'Últimos 30 días',
            'params': {
                'form_template_id': '877138',
                'created_after': '2024-11-18T00:00:00Z',
                'created_before': '2024-12-18T23:59:59Z',
                'page': 1,
                'page_size': 5
            }
        }
    ]
    
    for filtro in filtros_a_probar:
        print(f"\n🔬 Probando: {filtro['nombre']}")
        
        try:
            url = f"{ZENPUT_CONFIG['base_url']}/submissions"
            response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=filtro['params'], timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                count = data.get('count', 0)
                submissions = data.get('data', [])
                
                print(f"   📊 Count: {count:,}")
                print(f"   📄 Datos: {len(submissions)} submissions")
                
                if submissions:
                    primer_submission = submissions[0]
                    smetadata = primer_submission.get('smetadata', {})
                    fecha_created = smetadata.get('date_created', 'N/A')
                    fecha_submitted = smetadata.get('date_submitted', 'N/A')
                    
                    print(f"   📅 Primer submission:")
                    print(f"      Created: {fecha_created}")
                    print(f"      Submitted: {fecha_submitted}")
                
            else:
                print(f"   ❌ Error {response.status_code}")
                
        except Exception as e:
            print(f"   💥 Error: {e}")

def main():
    """Función principal"""
    
    print("🔍 INVESTIGACIÓN DE FECHAS REALES")
    print("=" * 80)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # 1. Investigar fechas reales
    todas_las_fechas, ejemplos = investigar_fechas_submissions()
    
    # 2. Probar diferentes filtros
    probar_diferentes_filtros()
    
    # 3. Guardar resultados
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"investigacion_fechas_{timestamp}.json"
    
    resultado = {
        'timestamp': timestamp,
        'distribución_fechas': dict(todas_las_fechas),
        'ejemplos_submissions': ejemplos,
        'total_fechas_encontradas': sum(todas_las_fechas.values()),
        'rango_fechas': {
            'primera': min(todas_las_fechas.keys()) if todas_las_fechas else None,
            'última': max(todas_las_fechas.keys()) if todas_las_fechas else None
        }
    }
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(resultado, f, indent=2, default=str, ensure_ascii=False)
    
    print(f"\n💾 Investigación guardada en: {filename}")
    print(f"🎉 INVESTIGACIÓN COMPLETADA")
    
    return resultado

if __name__ == "__main__":
    main()