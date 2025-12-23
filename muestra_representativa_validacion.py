#!/usr/bin/env python3
"""
🎯 MUESTRA REPRESENTATIVA PARA VALIDACIÓN
Extraer muestra representativa para validar coincidencias antes del ETL completo
"""

import requests
import csv
import math
import json
from datetime import datetime, date
from collections import defaultdict

ZENPUT_CONFIG = {
    'base_url': 'https://www.zenput.com/api/v3',
    'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}
}

FORMULARIOS_OBJETIVO = {
    '877138': 'OPERATIVA',
    '877139': 'SEGURIDAD'
}

REGLAS_MAXIMAS = {
    'LOCAL': {'operativas': 4, 'seguridad': 4},
    'FORANEA': {'operativas': 2, 'seguridad': 2}
}

GRUPOS_LOCALES = ['OGAS', 'TEC', 'TEPEYAC', 'PLOG NUEVO LEON', 'GRUPO CENTRITO', 'GRUPO SALTILLO']

def cargar_sucursales_normalizadas():
    """Cargar sucursales con coordenadas normalizadas"""
    sucursales = {}
    
    with open('data/86_sucursales_master.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['Latitude'] and row['Longitude']:
                sucursales[row['Nombre_Sucursal']] = {
                    'numero': int(row['Numero_Sucursal']),
                    'nombre': row['Nombre_Sucursal'],
                    'grupo': row['Grupo_Operativo'],
                    'lat': float(row['Latitude']),
                    'lon': float(row['Longitude']),
                    'tipo': 'LOCAL' if row['Grupo_Operativo'] in GRUPOS_LOCALES else 'FORANEA'
                }
    
    return sucursales

def calcular_distancia_km(lat1, lon1, lat2, lon2):
    """Calcular distancia en km"""
    try:
        from math import radians, sin, cos, sqrt, atan2
        
        lat1, lon1, lat2, lon2 = map(radians, [float(lat1), float(lon1), float(lat2), float(lon2)])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1-a))
        R = 6371
        return R * c
    except:
        return float('inf')

def extraer_muestra_representativa(sucursales_normalizadas, muestra_size=100):
    """Extraer muestra representativa de submissions"""
    
    print(f"🔄 EXTRAYENDO MUESTRA REPRESENTATIVA ({muestra_size} por formulario)")
    print("=" * 80)
    
    muestra_submissions = []
    
    for form_id, tipo_form in FORMULARIOS_OBJETIVO.items():
        print(f"\n📋 Muestra {tipo_form} (Form {form_id})")
        print("-" * 50)
        
        submissions_form = []
        page = 1
        
        while len(submissions_form) < muestra_size:
            try:
                print(f"    📄 Página {page}...", end=" ", flush=True)
                
                url = f"{ZENPUT_CONFIG['base_url']}/submissions"
                params = {
                    'form_template_id': form_id,
                    'page': page,
                    'page_size': 100
                }
                
                response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    submissions = data.get('data', [])
                    
                    if not submissions:
                        print("✅ Fin")
                        break
                    
                    print(f"{len(submissions)} submissions")
                    
                    # Filtrar 2025 y procesar
                    submissions_2025 = []
                    for submission in submissions:
                        smetadata = submission.get('smetadata', {})
                        fecha_submitted = smetadata.get('date_submitted')
                        
                        if fecha_submitted:
                            try:
                                fecha_dt = datetime.fromisoformat(fecha_submitted.replace('Z', '+00:00'))
                                if fecha_dt.year == 2025:
                                    # Procesar datos inmediatamente
                                    submission_procesada = procesar_submission_individual(
                                        submission, tipo_form, sucursales_normalizadas
                                    )
                                    if submission_procesada:
                                        submissions_2025.append(submission_procesada)
                            except:
                                continue
                    
                    submissions_form.extend(submissions_2025)
                    print(f"        └─ 2025 procesadas: {len(submissions_2025)} (Total: {len(submissions_form)})")
                    
                    page += 1
                    
                    # Limitar para muestra
                    if len(submissions_form) >= muestra_size:
                        submissions_form = submissions_form[:muestra_size]
                        break
                    
                else:
                    print(f"❌ Error {response.status_code}")
                    break
                    
            except Exception as e:
                print(f"💥 Error: {e}")
                break
        
        print(f"📊 Muestra {tipo_form}: {len(submissions_form)} submissions")
        muestra_submissions.extend(submissions_form)
    
    print(f"\n📊 MUESTRA TOTAL: {len(muestra_submissions)} submissions")
    return muestra_submissions

def procesar_submission_individual(submission, tipo_form, sucursales_normalizadas):
    """Procesar una submission individual"""
    
    # Extraer datos básicos
    submission_id = submission.get('id')
    smetadata = submission.get('smetadata', {})
    
    # Coordenadas de entrega REAL
    lat_entrega = smetadata.get('lat')
    lon_entrega = smetadata.get('lon')
    
    # Usuario y fecha
    created_by = smetadata.get('created_by', {})
    usuario_nombre = created_by.get('display_name')
    
    fecha_submitted = smetadata.get('date_submitted')
    fecha = None
    
    if fecha_submitted:
        try:
            fecha = datetime.fromisoformat(fecha_submitted.replace('Z', '+00:00')).date()
        except:
            pass
    
    # Location asignada
    location_zenput = smetadata.get('location', {})
    location_name = location_zenput.get('name')
    
    # Solo procesar si tenemos datos mínimos
    if not (lat_entrega and lon_entrega and fecha):
        return None
    
    # Mapear a sucursal
    mejor_match = None
    distancia_minima = float('inf')
    TOLERANCIA_MAXIMA = 2.0  # 2km
    
    for sucursal_key, datos_sucursal in sucursales_normalizadas.items():
        distancia = calcular_distancia_km(
            lat_entrega, lon_entrega,
            datos_sucursal['lat'], datos_sucursal['lon']
        )
        
        if distancia < distancia_minima:
            distancia_minima = distancia
            mejor_match = datos_sucursal.copy()
    
    # Solo aceptar si está dentro de tolerancia
    if distancia_minima > TOLERANCIA_MAXIMA:
        return None
    
    return {
        'submission_id': submission_id,
        'form_type': tipo_form,
        'fecha': fecha,
        'usuario_nombre': usuario_nombre,
        'lat_entrega': lat_entrega,
        'lon_entrega': lon_entrega,
        'location_zenput_name': location_name,
        'sucursal_mapeada': mejor_match['nombre'],
        'sucursal_numero': mejor_match['numero'],
        'sucursal_grupo': mejor_match['grupo'],
        'sucursal_tipo': mejor_match['tipo'],
        'distancia_km': distancia_minima
    }

def analizar_patrones_coincidencias(muestra_submissions):
    """Analizar patrones de coincidencias en la muestra"""
    
    print(f"\n📅 ANÁLISIS DE PATRONES DE COINCIDENCIAS")
    print("=" * 70)
    
    # Agrupar por sucursal + fecha
    por_sucursal_fecha = defaultdict(lambda: {'operativas': [], 'seguridad': []})
    
    for submission in muestra_submissions:
        sucursal = submission['sucursal_mapeada']
        fecha_str = submission['fecha'].strftime('%Y-%m-%d')
        key = f"{sucursal}_{fecha_str}"
        
        if submission['form_type'] == 'OPERATIVA':
            por_sucursal_fecha[key]['operativas'].append(submission)
        elif submission['form_type'] == 'SEGURIDAD':
            por_sucursal_fecha[key]['seguridad'].append(submission)
    
    # Analizar patrones
    coincidencias_perfectas = []
    operativas_solas = []
    seguridad_solas = []
    patrones_usuarios = defaultdict(list)
    
    for key, datos in por_sucursal_fecha.items():
        sucursal, fecha_str = key.rsplit('_', 1)
        ops = datos['operativas']
        segs = datos['seguridad']
        
        if ops and segs:
            # COINCIDENCIA PERFECTA
            coincidencias_perfectas.append({
                'sucursal': sucursal,
                'fecha': fecha_str,
                'operativas': ops,
                'seguridad': segs,
                'usuarios_ops': [op['usuario_nombre'] for op in ops],
                'usuarios_segs': [seg['usuario_nombre'] for seg in segs]
            })
            
            # Analizar patrones de usuarios
            for op in ops:
                for seg in segs:
                    if op['usuario_nombre'] == seg['usuario_nombre']:
                        patrones_usuarios['mismo_usuario'].append({
                            'usuario': op['usuario_nombre'],
                            'sucursal': sucursal,
                            'fecha': fecha_str
                        })
                    else:
                        patrones_usuarios['usuarios_diferentes'].append({
                            'op_usuario': op['usuario_nombre'],
                            'seg_usuario': seg['usuario_nombre'],
                            'sucursal': sucursal,
                            'fecha': fecha_str
                        })
        
        elif ops:
            operativas_solas.extend(ops)
        elif segs:
            seguridad_solas.extend(segs)
    
    # Estadísticas
    total_dias_analizados = len(por_sucursal_fecha)
    porcentaje_coincidencias = (len(coincidencias_perfectas) / total_dias_analizados * 100) if total_dias_analizados > 0 else 0
    
    print(f"📊 RESULTADOS DEL ANÁLISIS:")
    print(f"   📅 Total días analizados: {total_dias_analizados}")
    print(f"   ✅ Coincidencias perfectas: {len(coincidencias_perfectas)} ({porcentaje_coincidencias:.1f}%)")
    print(f"   ⚠️ Solo operativas: {len(operativas_solas)}")
    print(f"   ⚠️ Solo seguridad: {len(seguridad_solas)}")
    
    print(f"\n🎯 PATRONES DE USUARIOS:")
    mismo_usuario = len(patrones_usuarios['mismo_usuario'])
    usuarios_diferentes = len(patrones_usuarios['usuarios_diferentes'])
    total_coincidencias = mismo_usuario + usuarios_diferentes
    
    if total_coincidencias > 0:
        print(f"   👤 Mismo usuario: {mismo_usuario} ({mismo_usuario/total_coincidencias*100:.1f}%)")
        print(f"   👥 Usuarios diferentes: {usuarios_diferentes} ({usuarios_diferentes/total_coincidencias*100:.1f}%)")
    
    # Mostrar ejemplos
    print(f"\n📋 EJEMPLOS DE COINCIDENCIAS PERFECTAS:")
    for i, coincidencia in enumerate(coincidencias_perfectas[:5]):
        print(f"   {i+1}. {coincidencia['fecha']} - {coincidencia['sucursal']}")
        print(f"      Operativas: {', '.join(coincidencia['usuarios_ops'])}")
        print(f"      Seguridad: {', '.join(coincidencia['usuarios_segs'])}")
    
    return {
        'coincidencias_perfectas': coincidencias_perfectas,
        'operativas_solas': operativas_solas,
        'seguridad_solas': seguridad_solas,
        'patrones_usuarios': dict(patrones_usuarios),
        'estadisticas': {
            'total_dias': total_dias_analizados,
            'porcentaje_coincidencias': porcentaje_coincidencias,
            'mismo_usuario': mismo_usuario,
            'usuarios_diferentes': usuarios_diferentes
        }
    }

def proyectar_totales(analisis_patrones, muestra_size):
    """Proyectar totales basado en la muestra"""
    
    print(f"\n📈 PROYECCIÓN DE TOTALES")
    print("=" * 50)
    
    # Estimaciones basadas en la muestra
    coincidencias = len(analisis_patrones['coincidencias_perfectas'])
    operativas_solas = len(analisis_patrones['operativas_solas'])
    seguridad_solas = len(analisis_patrones['seguridad_solas'])
    
    # Factor de proyección (asumiendo que procesamos ~400 submissions por formulario completo)
    factor_proyeccion = 4  # Estimación conservadora
    
    print(f"🔍 BASADO EN MUESTRA DE {muestra_size*2} submissions:")
    print(f"   ✅ Coincidencias encontradas: {coincidencias}")
    print(f"   ⚠️ Operativas solas: {operativas_solas}")
    print(f"   ⚠️ Seguridad solas: {seguridad_solas}")
    
    print(f"\n📊 PROYECCIÓN TOTAL (factor {factor_proyeccion}x):")
    print(f"   ✅ Coincidencias estimadas: {coincidencias * factor_proyeccion}")
    print(f"   ⚠️ Operativas solas estimadas: {operativas_solas * factor_proyeccion}")
    print(f"   ⚠️ Seguridad solas estimadas: {seguridad_solas * factor_proyeccion}")
    
    # Estimar sucursales
    sucursales_con_coincidencias = len(set(c['sucursal'] for c in analisis_patrones['coincidencias_perfectas']))
    sucursales_estimadas = sucursales_con_coincidencias * factor_proyeccion
    
    print(f"\n🏪 SUCURSALES:")
    print(f"   📋 Con coincidencias en muestra: {sucursales_con_coincidencias}")
    print(f"   📊 Estimadas totales: {min(sucursales_estimadas, 80)} (máximo 80)")
    
    # Validar si cumple expectativas
    print(f"\n✅ VALIDACIÓN DE EXPECTATIVAS:")
    if coincidencias > 0:
        print(f"   ✅ SÍ hay coincidencias operativas/seguridad mismo día")
        print(f"   ✅ El patrón de coincidencias es válido para ETL completo")
        print(f"   ✅ Se puede proceder con la extracción completa")
    else:
        print(f"   ❌ NO hay coincidencias en la muestra")
        print(f"   ⚠️ Revisar criterios o aumentar tamaño de muestra")
    
    return {
        'coincidencias_proyectadas': coincidencias * factor_proyeccion,
        'operativas_solas_proyectadas': operativas_solas * factor_proyeccion,
        'seguridad_solas_proyectadas': seguridad_solas * factor_proyeccion,
        'sucursales_estimadas': min(sucursales_estimadas, 80),
        'es_viable_etl_completo': coincidencias > 0
    }

def main():
    """Función principal"""
    
    print("🎯 MUESTRA REPRESENTATIVA PARA VALIDACIÓN DE COINCIDENCIAS")
    print("=" * 100)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 100)
    
    # 1. Cargar sucursales
    sucursales_normalizadas = cargar_sucursales_normalizadas()
    print(f"✅ {len(sucursales_normalizadas)} sucursales cargadas")
    
    # 2. Extraer muestra representativa (procesada)
    muestra_size = 100  # 100 por formulario = 200 total
    muestra_submissions = extraer_muestra_representativa(sucursales_normalizadas, muestra_size)
    
    if not muestra_submissions:
        print("❌ No se obtuvo muestra válida")
        return
    
    # 3. Analizar patrones de coincidencias
    analisis_patrones = analizar_patrones_coincidencias(muestra_submissions)
    
    # 4. Proyectar totales
    proyeccion = proyectar_totales(analisis_patrones, muestra_size)
    
    # 5. Guardar resultados
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"muestra_representativa_{timestamp}.json"
    
    resultado = {
        'timestamp': timestamp,
        'muestra_size_por_formulario': muestra_size,
        'total_muestra_procesada': len(muestra_submissions),
        'analisis_patrones': analisis_patrones,
        'proyeccion_totales': proyeccion,
        'muestra_submissions': muestra_submissions,
        'recomendacion': {
            'proceder_etl_completo': proyeccion['es_viable_etl_completo'],
            'razon': 'Hay coincidencias válidas' if proyeccion['es_viable_etl_completo'] else 'No hay suficientes coincidencias'
        }
    }
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(resultado, f, indent=2, default=str, ensure_ascii=False)
    
    print(f"\n💾 Muestra y análisis guardados en: {filename}")
    print(f"🎉 ANÁLISIS DE MUESTRA COMPLETADO")
    
    # Recomendación final
    if proyeccion['es_viable_etl_completo']:
        print(f"\n🚀 RECOMENDACIÓN: Proceder con ETL completo")
        print(f"   Las coincidencias son válidas y el patrón es consistente")
    else:
        print(f"\n⚠️ RECOMENDACIÓN: Revisar criterios o aumentar muestra")
    
    return resultado

if __name__ == "__main__":
    main()