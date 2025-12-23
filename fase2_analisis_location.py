#!/usr/bin/env python3
"""
🔄 FASE 2: ANÁLISIS DE LOCATION_NAME
Mapear las 476 submissions a sucursales específicas usando:
- Mapeo directo: 356 submissions CON location_name
- Mapeo por coordenadas: 120 submissions SIN location_name
"""

import pandas as pd
import csv
import re
from datetime import datetime
from collections import defaultdict
import math

def cargar_sucursales_master():
    """Cargar catálogo de 86 sucursales con coordenadas normalizadas"""
    
    print("📂 CARGANDO CATÁLOGO DE SUCURSALES MASTER")
    print("=" * 50)
    
    sucursales = {}
    
    try:
        with open('SUCURSALES_MASTER_20251218_110913.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['numero'] and row['nombre'] and row['lat'] and row['lon']:
                    numero = int(row['numero'])
                    nombre = row['nombre']
                    
                    # Crear múltiples claves de búsqueda para facilitar el mapeo
                    sucursal_data = {
                        'numero': numero,
                        'nombre': nombre,
                        'grupo': row['grupo'],
                        'tipo': row['tipo'],
                        'lat': float(row['lat']),
                        'lon': float(row['lon'])
                    }
                    
                    # Claves de búsqueda múltiples
                    sucursales[nombre] = sucursal_data
                    sucursales[numero] = sucursal_data
                    sucursales[f"{numero} - {nombre}"] = sucursal_data
                    
                    # Versión normalizada sin acentos
                    nombre_norm = normalizar_texto(nombre)
                    sucursales[nombre_norm] = sucursal_data
                    sucursales[f"{numero} - {nombre_norm}"] = sucursal_data
        
        print(f"✅ {len([k for k in sucursales.keys() if isinstance(k, str)])} claves de búsqueda creadas")
        sucursales_unicas = len(set(s['numero'] for s in sucursales.values() if isinstance(s, dict)))
        print(f"📊 {sucursales_unicas} sucursales únicas disponibles")
        
        return sucursales
        
    except Exception as e:
        print(f"❌ Error cargando sucursales: {e}")
        return {}

def normalizar_texto(texto):
    """Normalizar texto removiendo acentos y caracteres especiales"""
    if not texto:
        return ""
    
    # Remover acentos
    replacements = {
        'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
        'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U',
        'ñ': 'n', 'Ñ': 'N'
    }
    
    for old, new in replacements.items():
        texto = texto.replace(old, new)
    
    return texto.lower().strip()

def extraer_numero_nombre_location(location_name):
    """Extraer número y nombre del location_name de Zenput"""
    if not location_name:
        return None, "", ""
    
    # Patrón: "número - nombre"
    match = re.match(r'^(\d+)\s*-\s*(.+)$', location_name.strip())
    if match:
        numero = int(match.group(1))
        nombre = match.group(2).strip()
        nombre_norm = normalizar_texto(nombre)
        return numero, nombre, nombre_norm
    
    # Si no tiene número, usar el nombre tal cual
    return None, location_name.strip(), normalizar_texto(location_name)

def mapear_location_directo(location_name, sucursales_master):
    """Mapear location_name directamente contra catálogo de sucursales"""
    
    if not location_name:
        return None, "SIN_LOCATION_NAME", 0.0
    
    # 1. Match exacto directo
    if location_name in sucursales_master:
        return sucursales_master[location_name], "EXACTO_DIRECTO", 1.0
    
    # 2. Extraer número y nombre para búsquedas más específicas
    numero, nombre_original, nombre_norm = extraer_numero_nombre_location(location_name)
    
    # 3. Match por número-nombre completo
    if numero:
        clave_completa = f"{numero} - {nombre_original}"
        if clave_completa in sucursales_master:
            return sucursales_master[clave_completa], "EXACTO_NUMERO_NOMBRE", 1.0
        
        # Match por número solo
        if numero in sucursales_master:
            return sucursales_master[numero], "EXACTO_NUMERO", 0.9
    
    # 4. Match por nombre original
    if nombre_original in sucursales_master:
        return sucursales_master[nombre_original], "EXACTO_NOMBRE", 0.8
    
    # 5. Match por nombre normalizado
    if nombre_norm in sucursales_master:
        return sucursales_master[nombre_norm], "EXACTO_NORMALIZADO", 0.8
    
    # 6. Búsqueda por similitud (para casos edge)
    mejor_similitud = 0.0
    mejor_match = None
    
    for clave, datos in sucursales_master.items():
        if isinstance(clave, str) and not clave.isdigit():
            similitud = calcular_similitud(location_name.lower(), clave.lower())
            if similitud > mejor_similitud and similitud >= 0.85:  # Umbral alto para evitar falsos positivos
                mejor_similitud = similitud
                mejor_match = datos
    
    if mejor_match:
        return mejor_match, f"SIMILITUD_{mejor_similitud:.2f}", mejor_similitud
    
    return None, "NO_ENCONTRADO", 0.0

def calcular_similitud(str1, str2):
    """Calcular similitud entre dos strings usando Levenshtein simple"""
    if str1 == str2:
        return 1.0
    
    len1, len2 = len(str1), len(str2)
    if len1 == 0 or len2 == 0:
        return 0.0
    
    # Algoritmo simple de similitud
    max_len = max(len1, len2)
    common_chars = sum(c1 == c2 for c1, c2 in zip(str1, str2))
    
    return common_chars / max_len

def calcular_distancia_km(lat1, lon1, lat2, lon2):
    """Calcular distancia en km usando fórmula Haversine"""
    try:
        # Convertir a radianes
        lat1, lon1, lat2, lon2 = map(math.radians, [float(lat1), float(lon1), float(lat2), float(lon2)])
        
        # Diferencias
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        
        # Fórmula Haversine
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        
        # Radio de la Tierra en km
        R = 6371
        return R * c
        
    except Exception as e:
        return float('inf')

def encontrar_sucursal_por_coordenadas(lat_entrega, lon_entrega, sucursales_master):
    """Encontrar sucursal más cercana usando coordenadas de entrega"""
    
    if not (lat_entrega and lon_entrega):
        return None, "SIN_COORDENADAS", 0.0
    
    mejor_sucursal = None
    distancia_minima = float('inf')
    
    # Buscar en sucursales únicas (evitar duplicados por claves múltiples)
    sucursales_unicas = {}
    for clave, datos in sucursales_master.items():
        if isinstance(datos, dict) and 'numero' in datos:
            sucursales_unicas[datos['numero']] = datos
    
    for sucursal_data in sucursales_unicas.values():
        try:
            distancia = calcular_distancia_km(
                lat_entrega, lon_entrega,
                sucursal_data['lat'], sucursal_data['lon']
            )
            
            if distancia < distancia_minima:
                distancia_minima = distancia
                mejor_sucursal = sucursal_data
                
        except Exception as e:
            continue
    
    # Determinar confianza basada en distancia
    if distancia_minima <= 0.5:  # Muy cerca
        confianza = 0.9
    elif distancia_minima <= 1.0:  # Cerca
        confianza = 0.8
    elif distancia_minima <= 2.0:  # Aceptable
        confianza = 0.7
    elif distancia_minima <= 5.0:  # Lejos pero posible
        confianza = 0.5
    else:  # Muy lejos
        confianza = 0.2
    
    return mejor_sucursal, f"COORDENADAS_{distancia_minima:.2f}KM", confianza

def procesar_submissions_fase2(df_submissions, sucursales_master):
    """Procesar todas las submissions para asignarlas a sucursales"""
    
    print(f"\n🎯 PROCESANDO {len(df_submissions)} SUBMISSIONS PARA ASIGNACIÓN")
    print("=" * 70)
    
    resultados = []
    
    # Contadores para estadísticas
    con_location = 0
    sin_location = 0
    asignadas_exitosas = 0
    no_asignables = 0
    
    for index, row in df_submissions.iterrows():
        if index % 50 == 0:
            print(f"   🔄 Procesadas: {index}/{len(df_submissions)}")
        
        submission_id = row['submission_id']
        form_type = row['form_type']
        fecha = row['fecha']
        usuario_nombre = row['usuario_nombre']
        location_name = row['location_name']
        lat_entrega = row['lat_entrega']
        lon_entrega = row['lon_entrega']
        tiene_location = row['tiene_location']
        
        # Datos base del resultado
        resultado = {
            'submission_id': submission_id,
            'form_type': form_type,
            'fecha': fecha,
            'usuario_nombre': usuario_nombre,
            'location_name_original': location_name,
            'lat_entrega': lat_entrega,
            'lon_entrega': lon_entrega,
            'tiene_location_original': tiene_location
        }
        
        sucursal_asignada = None
        metodo_asignacion = ""
        confianza = 0.0
        distancia_km = None
        notas = ""
        
        if tiene_location and location_name:
            # MAPEO DIRECTO POR LOCATION_NAME
            con_location += 1
            sucursal_match, metodo, conf = mapear_location_directo(location_name, sucursales_master)
            
            if sucursal_match:
                sucursal_asignada = sucursal_match
                metodo_asignacion = f"LOCATION_{metodo}"
                confianza = conf
                notas = f"Mapeada por location_name: {location_name}"
                asignadas_exitosas += 1
            else:
                # Si falló el mapeo por location, intentar por coordenadas como backup
                if lat_entrega and lon_entrega:
                    sucursal_coord, metodo_coord, conf_coord = encontrar_sucursal_por_coordenadas(
                        lat_entrega, lon_entrega, sucursales_master
                    )
                    if sucursal_coord and conf_coord >= 0.7:
                        sucursal_asignada = sucursal_coord
                        metodo_asignacion = f"BACKUP_COORD_{metodo_coord}"
                        confianza = conf_coord * 0.8  # Penalizar por ser backup
                        notas = f"Location '{location_name}' no encontrada, mapeada por coordenadas"
                        asignadas_exitosas += 1
                    else:
                        notas = f"Location '{location_name}' no encontrada y coordenadas muy lejas"
                        no_asignables += 1
                else:
                    notas = f"Location '{location_name}' no encontrada y sin coordenadas"
                    no_asignables += 1
        
        else:
            # MAPEO POR COORDENADAS (sin location_name)
            sin_location += 1
            
            if lat_entrega and lon_entrega:
                sucursal_coord, metodo_coord, conf_coord = encontrar_sucursal_por_coordenadas(
                    lat_entrega, lon_entrega, sucursales_master
                )
                
                if sucursal_coord and conf_coord >= 0.5:  # Umbral más bajo para submissions sin location
                    sucursal_asignada = sucursal_coord
                    metodo_asignacion = metodo_coord
                    confianza = conf_coord
                    distancia_km = float(metodo_coord.split('_')[1].replace('KM', '')) if '_' in metodo_coord else None
                    notas = f"Mapeada por coordenadas de entrega"
                    asignadas_exitosas += 1
                else:
                    notas = "Sin location y coordenadas muy lejas de sucursales conocidas"
                    no_asignables += 1
            else:
                notas = "Sin location_name y sin coordenadas de entrega"
                no_asignables += 1
        
        # Agregar datos de sucursal asignada
        if sucursal_asignada:
            resultado.update({
                'sucursal_numero': sucursal_asignada['numero'],
                'sucursal_nombre': sucursal_asignada['nombre'],
                'sucursal_grupo': sucursal_asignada['grupo'],
                'sucursal_tipo': sucursal_asignada['tipo'],
                'sucursal_lat': sucursal_asignada['lat'],
                'sucursal_lon': sucursal_asignada['lon'],
                'estado_asignacion': 'ASIGNADA',
                'metodo_asignacion': metodo_asignacion,
                'confianza': round(confianza, 3),
                'distancia_km': distancia_km,
                'notas': notas
            })
        else:
            resultado.update({
                'sucursal_numero': None,
                'sucursal_nombre': None,
                'sucursal_grupo': None,
                'sucursal_tipo': None,
                'sucursal_lat': None,
                'sucursal_lon': None,
                'estado_asignacion': 'NO_ASIGNADA',
                'metodo_asignacion': 'NO_ASIGNABLE',
                'confianza': 0.0,
                'distancia_km': None,
                'notas': notas
            })
        
        resultados.append(resultado)
    
    print(f"\n📊 ESTADÍSTICAS DE PROCESAMIENTO:")
    print(f"   📋 Total procesadas: {len(df_submissions)}")
    print(f"   ✅ Con location_name: {con_location}")
    print(f"   ❌ Sin location_name: {sin_location}")
    print(f"   🎯 Asignadas exitosas: {asignadas_exitosas}")
    print(f"   ⚠️ No asignables: {no_asignables}")
    print(f"   📈 Tasa de éxito: {asignadas_exitosas/len(df_submissions)*100:.1f}%")
    
    return resultados

def analizar_sucursales_activas(resultados_procesados):
    """Analizar qué sucursales están activas en 2025 con supervisiones"""
    
    print(f"\n📊 ANÁLISIS DE SUCURSALES ACTIVAS EN 2025")
    print("=" * 60)
    
    # Solo submissions asignadas
    asignadas = [r for r in resultados_procesados if r['estado_asignacion'] == 'ASIGNADA']
    
    # Agrupar por sucursal
    por_sucursal = defaultdict(lambda: {
        'operativas': [],
        'seguridad': [],
        'fechas': set(),
        'usuarios': set(),
        'info_sucursal': None
    })
    
    for resultado in asignadas:
        sucursal_nombre = resultado['sucursal_nombre']
        form_type = resultado['form_type']
        fecha = resultado['fecha'][:10]  # Solo fecha, sin hora
        usuario = resultado['usuario_nombre']
        
        # Guardar info de sucursal
        if not por_sucursal[sucursal_nombre]['info_sucursal']:
            por_sucursal[sucursal_nombre]['info_sucursal'] = {
                'numero': resultado['sucursal_numero'],
                'grupo': resultado['sucursal_grupo'],
                'tipo': resultado['sucursal_tipo']
            }
        
        # Agregar submission
        if form_type == 'OPERATIVA':
            por_sucursal[sucursal_nombre]['operativas'].append(resultado)
        elif form_type == 'SEGURIDAD':
            por_sucursal[sucursal_nombre]['seguridad'].append(resultado)
        
        por_sucursal[sucursal_nombre]['fechas'].add(fecha)
        if usuario:
            por_sucursal[sucursal_nombre]['usuarios'].add(usuario)
    
    # Generar estadísticas por sucursal
    sucursales_activas = []
    
    for sucursal_nombre, datos in por_sucursal.items():
        info = datos['info_sucursal']
        
        sucursal_stats = {
            'sucursal_nombre': sucursal_nombre,
            'sucursal_numero': info['numero'],
            'sucursal_grupo': info['grupo'],
            'sucursal_tipo': info['tipo'],
            'operativas_count': len(datos['operativas']),
            'seguridad_count': len(datos['seguridad']),
            'total_supervisiones': len(datos['operativas']) + len(datos['seguridad']),
            'fechas_activas': len(datos['fechas']),
            'usuarios_count': len(datos['usuarios']),
            'usuarios_list': ', '.join(sorted(datos['usuarios'])),
            'primera_fecha': min(datos['fechas']) if datos['fechas'] else '',
            'ultima_fecha': max(datos['fechas']) if datos['fechas'] else ''
        }
        
        sucursales_activas.append(sucursal_stats)
    
    # Ordenar por número de sucursal
    sucursales_activas.sort(key=lambda x: x['sucursal_numero'])
    
    print(f"📈 SUCURSALES ACTIVAS ENCONTRADAS: {len(sucursales_activas)}")
    
    # Estadísticas por tipo
    locales = [s for s in sucursales_activas if s['sucursal_tipo'] == 'LOCAL']
    foraneas = [s for s in sucursales_activas if s['sucursal_tipo'] == 'FORANEA']
    
    print(f"   🏢 Locales: {len(locales)}")
    print(f"   🌍 Foráneas: {len(foraneas)}")
    
    # Totales
    total_ops = sum(s['operativas_count'] for s in sucursales_activas)
    total_segs = sum(s['seguridad_count'] for s in sucursales_activas)
    
    print(f"\n📊 TOTALES ASIGNADOS:")
    print(f"   📋 Total operativas: {total_ops}")
    print(f"   📋 Total seguridad: {total_segs}")
    print(f"   📋 Total supervisiones: {total_ops + total_segs}")
    
    # Mostrar primeras 10 sucursales
    print(f"\n📋 PRIMERAS 10 SUCURSALES ACTIVAS:")
    for i, sucursal in enumerate(sucursales_activas[:10]):
        print(f"   {i+1:2d}. #{sucursal['sucursal_numero']:2d} {sucursal['sucursal_nombre']}")
        print(f"       {sucursal['operativas_count']}ops + {sucursal['seguridad_count']}seg = {sucursal['total_supervisiones']} total ({sucursal['sucursal_tipo']})")
    
    if len(sucursales_activas) > 10:
        print(f"       ... y {len(sucursales_activas) - 10} más")
    
    return sucursales_activas

def main():
    """Función principal - Fase 2"""
    
    print("🔄 FASE 2: ANÁLISIS DE LOCATION_NAME Y MAPEO A SUCURSALES")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Objetivo: Mapear 476 submissions a sucursales específicas")
    print("📋 Estrategias: Location directo + Coordenadas de entrega")
    print("=" * 80)
    
    # 1. Cargar sucursales master
    sucursales_master = cargar_sucursales_master()
    if not sucursales_master:
        print("❌ Error: No se pudo cargar el catálogo de sucursales")
        return
    
    # 2. Cargar submissions de Fase 1
    try:
        print(f"\n📂 CARGANDO SUBMISSIONS DE FASE 1")
        print("-" * 40)
        
        df_submissions = pd.read_csv('FASE1_COMPLETA_EXITO_20251218_120332.csv')
        print(f"✅ {len(df_submissions)} submissions cargadas exitosamente")
        
        # Verificar estructura
        con_location = df_submissions['tiene_location'].sum()
        sin_location = len(df_submissions) - con_location
        print(f"   📍 Con location: {con_location}")
        print(f"   🌍 Sin location: {sin_location}")
        
    except Exception as e:
        print(f"❌ Error cargando submissions: {e}")
        return
    
    # 3. Procesar submissions para asignación
    resultados_procesados = procesar_submissions_fase2(df_submissions, sucursales_master)
    
    # 4. Analizar sucursales activas
    sucursales_activas = analizar_sucursales_activas(resultados_procesados)
    
    # 5. Guardar resultados
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # DataFrame de resultados procesados
    df_resultados = pd.DataFrame(resultados_procesados)
    filename_completo = f"FASE2_SUBMISSIONS_MAPEADAS_{timestamp}.csv"
    df_resultados.to_csv(filename_completo, index=False, encoding='utf-8')
    
    # DataFrame de sucursales activas
    df_sucursales = pd.DataFrame(sucursales_activas)
    filename_sucursales = f"FASE2_SUCURSALES_ACTIVAS_{timestamp}.csv"
    df_sucursales.to_csv(filename_sucursales, index=False, encoding='utf-8')
    
    print(f"\n📁 ARCHIVOS GENERADOS:")
    print(f"   📄 Submissions mapeadas: {filename_completo}")
    print(f"   🏪 Sucursales activas: {filename_sucursales}")
    
    # Estadísticas finales
    asignadas = len([r for r in resultados_procesados if r['estado_asignacion'] == 'ASIGNADA'])
    no_asignadas = len(resultados_procesados) - asignadas
    
    print(f"\n🎉 FASE 2 COMPLETADA")
    print("=" * 50)
    print(f"📊 RESULTADOS FINALES:")
    print(f"   📋 Total submissions: {len(resultados_procesados)}")
    print(f"   ✅ Asignadas exitosamente: {asignadas}")
    print(f"   ❌ No asignadas: {no_asignadas}")
    print(f"   📈 Tasa de éxito: {asignadas/len(resultados_procesados)*100:.1f}%")
    print(f"   🏪 Sucursales activas: {len(sucursales_activas)}")
    
    print(f"\n🔜 SIGUIENTE PASO - FASE 3:")
    print(f"   🎯 Mapeo inteligente de submissions NO asignadas")
    print(f"   📏 Verificación de reglas: LOCAL (4+4), FORÁNEA (2+2)")
    print(f"   📊 Asignación final y validación completa")
    
    return resultados_procesados, sucursales_activas

if __name__ == "__main__":
    main()