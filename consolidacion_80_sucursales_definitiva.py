#!/usr/bin/env python3
"""
🎯 CONSOLIDACIÓN DEFINITIVA A 80 SUCURSALES
Consolidación inteligente usando:
1. Tolerancia de coordenadas expandida (±0.01°)
2. Mismo día (fecha, no horario)
3. Nombres similares con/sin números
"""

import psycopg2
import csv
import math
from datetime import datetime
from collections import defaultdict

# Configuración Railway
DATABASE_URL = 'postgresql://postgres:qGgdIUuKYKMKGtSNYzARpyapBWHsloOt@turntable.proxy.rlwy.net:24097/railway'

def cargar_coordenadas_sucursales_normalizadas():
    """Cargar coordenadas normalizadas del CSV"""
    sucursales_coords = {}
    
    with open('data/86_sucursales_master.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['Latitude'] and row['Longitude']:
                sucursales_coords[row['Nombre_Sucursal']] = {
                    'numero': int(row['Numero_Sucursal']),
                    'grupo': row['Grupo_Operativo'],
                    'lat': float(row['Latitude']),
                    'lon': float(row['Longitude']),
                    'location_code': int(row['Location_Code']) if row['Location_Code'] else None
                }
    
    return sucursales_coords

def calcular_distancia(lat1, lon1, lat2, lon2):
    """Calcular distancia euclidiana entre dos puntos"""
    try:
        return math.sqrt((float(lat1) - float(lat2))**2 + (float(lon1) - float(lon2))**2)
    except (TypeError, ValueError):
        return float('inf')

def son_misma_sucursal_expandida(coord1, coord2, tolerancia=0.01):
    """Determinar si dos coordenadas pertenecen a la misma sucursal con tolerancia expandida"""
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    distancia = calcular_distancia(lat1, lon1, lat2, lon2)
    return distancia <= tolerancia

def normalizar_nombre_sucursal(nombre):
    """Normalizar nombre de sucursal eliminando números y caracteres especiales"""
    if not nombre:
        return ""
    
    # Remover números al principio
    import re
    nombre_limpio = re.sub(r'^\d+\s*-\s*', '', nombre)
    nombre_limpio = re.sub(r'^\d+\s+', '', nombre_limpio)
    
    return nombre_limpio.strip()

def obtener_todas_supervisiones():
    """Obtener todas las supervisiones con sus datos de ubicación"""
    
    print("📊 OBTENIENDO TODAS LAS SUPERVISIONES")
    print("=" * 50)
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                id, submission_id, form_type, auditor_nombre,
                fecha_supervision, latitude, longitude,
                location_name, grupo_operativo, sucursal_id,
                puntos_obtenidos, puntos_maximos, calificacion_porcentaje
            FROM supervisiones_2026 
            WHERE latitude IS NOT NULL 
                AND longitude IS NOT NULL
                AND fecha_supervision IS NOT NULL
            ORDER BY fecha_supervision, latitude, longitude
        """)
        
        supervisiones = cursor.fetchall()
        
        print(f"📊 Total supervisiones con coordenadas: {len(supervisiones)}")
        
        cursor.close()
        conn.close()
        
        return supervisiones
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return []

def consolidar_sucursales_inteligente(supervisiones, tolerancia=0.01):
    """Consolidar sucursales usando proximidad de coordenadas y mismo día"""
    
    print(f"\n🧠 CONSOLIDACIÓN INTELIGENTE (tolerancia: ±{tolerancia}°)")
    print("=" * 60)
    
    sucursales_normalizadas = cargar_coordenadas_sucursales_normalizadas()
    
    # Agrupar supervisiones por coordenadas similares + mismo día
    grupos_coordenadas = defaultdict(list)
    
    for supervision in supervisiones:
        id_sup, sub_id, form_type, auditor, fecha, lat, lon, location_name, grupo, sucursal_id, pts_obtenidos, pts_maximos, calificacion = supervision
        
        # Buscar grupo existente con coordenadas similares y mismo día
        grupo_encontrado = False
        
        for key_existente, supervisiones_grupo in grupos_coordenadas.items():
            lat_grupo, lon_grupo, fecha_grupo = key_existente
            
            # Verificar si son misma sucursal (coordenadas similares) y mismo día
            if (son_misma_sucursal_expandida((lat, lon), (lat_grupo, lon_grupo), tolerancia) and 
                fecha == fecha_grupo):
                
                supervisiones_grupo.append({
                    'id': id_sup,
                    'submission_id': sub_id,
                    'form_type': form_type,
                    'auditor': auditor,
                    'fecha': fecha,
                    'lat': lat,
                    'lon': lon,
                    'location_name': location_name,
                    'grupo_operativo': grupo,
                    'sucursal_id': sucursal_id,
                    'puntos_obtenidos': pts_obtenidos,
                    'puntos_maximos': pts_maximos,
                    'calificacion_porcentaje': calificacion
                })
                grupo_encontrado = True
                break
        
        # Si no se encontró grupo existente, crear uno nuevo
        if not grupo_encontrado:
            nueva_key = (lat, lon, fecha)
            grupos_coordenadas[nueva_key].append({
                'id': id_sup,
                'submission_id': sub_id,
                'form_type': form_type,
                'auditor': auditor,
                'fecha': fecha,
                'lat': lat,
                'lon': lon,
                'location_name': location_name,
                'grupo_operativo': grupo,
                'sucursal_id': sucursal_id,
                'puntos_obtenidos': pts_obtenidos,
                'puntos_maximos': pts_maximos,
                'calificacion_porcentaje': calificacion
            })
    
    print(f"📊 Grupos de coordenadas+día únicos: {len(grupos_coordenadas)}")
    
    # Analizar grupos y consolidar por sucursal real
    sucursales_consolidadas = defaultdict(list)
    
    for key, supervisiones_grupo in grupos_coordenadas.items():
        lat_promedio, lon_promedio, fecha = key
        
        # Buscar la sucursal normalizada más cercana
        sucursal_mas_cercana = None
        distancia_minima = float('inf')
        
        for nombre_sucursal, datos_sucursal in sucursales_normalizadas.items():
            distancia = calcular_distancia(
                lat_promedio, lon_promedio,
                datos_sucursal['lat'], datos_sucursal['lon']
            )
            
            if distancia < distancia_minima:
                distancia_minima = distancia
                sucursal_mas_cercana = nombre_sucursal
        
        # Asignar supervisiones a sucursal consolidada
        if sucursal_mas_cercana:
            sucursal_key = f"{sucursal_mas_cercana}_{sucursales_normalizadas[sucursal_mas_cercana]['grupo']}"
            
            for supervision in supervisiones_grupo:
                supervision['sucursal_consolidada'] = sucursal_mas_cercana
                supervision['grupo_consolidado'] = sucursales_normalizadas[sucursal_mas_cercana]['grupo']
                supervision['distancia_a_normalizada'] = distancia_minima
                sucursales_consolidadas[sucursal_key].append(supervision)
    
    print(f"📊 Sucursales consolidadas: {len(sucursales_consolidadas)}")
    
    return sucursales_consolidadas

def analizar_patrones_supervisiones(sucursales_consolidadas):
    """Analizar patrones de supervisiones por sucursal consolidada"""
    
    print(f"\n📈 ANÁLISIS DE PATRONES")
    print("=" * 50)
    
    estadisticas = []
    perfectas = 0
    balanceadas = 0
    con_coincidencias = 0
    
    for sucursal_key, supervisiones in sucursales_consolidadas.items():
        nombre_sucursal = sucursal_key.split('_')[0]
        grupo = '_'.join(sucursal_key.split('_')[1:])
        
        # Agrupar por fecha
        por_fecha = defaultdict(lambda: {'operativas': 0, 'seguridad': 0, 'auditores': set()})
        
        for supervision in supervisiones:
            fecha = supervision['fecha']
            form_type = supervision['form_type']
            auditor = supervision['auditor']
            
            if form_type == 'OPERATIVA':
                por_fecha[fecha]['operativas'] += 1
            elif form_type == 'SEGURIDAD':
                por_fecha[fecha]['seguridad'] += 1
            
            por_fecha[fecha]['auditores'].add(auditor)
        
        # Calcular estadísticas
        fechas_con_ambos = 0
        total_operativas = sum(datos['operativas'] for datos in por_fecha.values())
        total_seguridad = sum(datos['seguridad'] for datos in por_fecha.values())
        fechas_unicas = len(por_fecha)
        
        for fecha, datos in por_fecha.items():
            if datos['operativas'] > 0 and datos['seguridad'] > 0:
                fechas_con_ambos += 1
        
        # Clasificar sucursal
        es_perfecta = total_operativas == 4 and total_seguridad == 4
        es_balanceada = total_operativas == total_seguridad
        tiene_coincidencias = fechas_con_ambos > 0
        
        if es_perfecta:
            perfectas += 1
        elif es_balanceada:
            balanceadas += 1
        if tiene_coincidencias:
            con_coincidencias += 1
        
        estadisticas.append({
            'sucursal': nombre_sucursal,
            'grupo': grupo,
            'operativas': total_operativas,
            'seguridad': total_seguridad,
            'fechas_unicas': fechas_unicas,
            'fechas_coincidentes': fechas_con_ambos,
            'es_perfecta': es_perfecta,
            'es_balanceada': es_balanceada,
            'balance': total_operativas - total_seguridad,
            'fechas_detalle': dict(por_fecha)
        })
    
    print(f"🎯 Sucursales perfectas (4 Op + 4 Seg): {perfectas}")
    print(f"⚖️ Sucursales balanceadas: {balanceadas}")
    print(f"📅 Sucursales con fechas coincidentes: {con_coincidencias}")
    
    return estadisticas

def generar_listado_completo(estadisticas):
    """Generar listado completo de sucursales con todas sus supervisiones"""
    
    print(f"\n📋 LISTADO COMPLETO DE SUCURSALES")
    print("=" * 80)
    
    # Ordenar por grupo operativo y sucursal
    estadisticas_ordenadas = sorted(estadisticas, key=lambda x: (x['grupo'], x['sucursal']))
    
    grupo_actual = None
    
    for i, stats in enumerate(estadisticas_ordenadas, 1):
        # Header de grupo si cambió
        if stats['grupo'] != grupo_actual:
            grupo_actual = stats['grupo']
            print(f"\n{'='*60}")
            print(f"🏢 {grupo_actual}")
            print(f"{'='*60}")
        
        # Información de sucursal
        status = "🎯 PERFECTA" if stats['es_perfecta'] else ("⚖️ BALANCEADA" if stats['es_balanceada'] else "⚠️ DESBALANCEADA")
        
        print(f"\n{i:2d}. {stats['sucursal']} - {status}")
        print(f"    Total: {stats['operativas']} Op + {stats['seguridad']} Seg = {stats['operativas'] + stats['seguridad']}")
        print(f"    Fechas únicas: {stats['fechas_unicas']}, Coincidentes: {stats['fechas_coincidentes']}")
        
        # Detalle por fecha
        print(f"    📅 Supervisiones por fecha:")
        for fecha, datos in sorted(stats['fechas_detalle'].items()):
            coincide = "✅" if datos['operativas'] > 0 and datos['seguridad'] > 0 else "  "
            auditores_str = ", ".join(sorted(datos['auditores']))[:50]
            print(f"       {coincide} {fecha}: {datos['operativas']} Op, {datos['seguridad']} Seg ({auditores_str})")
    
    # Resumen final
    print(f"\n{'='*80}")
    print(f"📊 RESUMEN FINAL")
    print(f"{'='*80}")
    print(f"🏪 Total sucursales con supervisiones: {len(estadisticas)}")
    print(f"🎯 Perfectas (4+4): {sum(1 for s in estadisticas if s['es_perfecta'])}")
    print(f"⚖️ Balanceadas: {sum(1 for s in estadisticas if s['es_balanceada'])}")
    print(f"📅 Con fechas coincidentes: {sum(1 for s in estadisticas if s['fechas_coincidentes'] > 0)}")
    
    total_op = sum(s['operativas'] for s in estadisticas)
    total_seg = sum(s['seguridad'] for s in estadisticas)
    print(f"📈 Total general: {total_op} Op + {total_seg} Seg = {total_op + total_seg}")
    
    if len(estadisticas) == 80:
        print(f"✅ ¡PERFECTO! Tenemos exactamente 80 sucursales con supervisiones")
    elif len(estadisticas) < 80:
        print(f"⚠️ Tenemos {len(estadisticas)} sucursales, faltan {80 - len(estadisticas)}")
    else:
        print(f"⚠️ Tenemos {len(estadisticas)} sucursales, sobran {len(estadisticas) - 80}")
    
    return estadisticas

def main():
    """Función principal"""
    
    print("🎯 CONSOLIDACIÓN DEFINITIVA A 80 SUCURSALES")
    print("=" * 70)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    # 1. Obtener todas las supervisiones
    supervisiones = obtener_todas_supervisiones()
    
    if not supervisiones:
        print("❌ No se pudieron obtener supervisiones")
        return
    
    # 2. Probar diferentes tolerancias hasta obtener 80 sucursales
    tolerancias = [0.005, 0.01, 0.015, 0.02, 0.025, 0.03]
    
    for tolerancia in tolerancias:
        print(f"\n🧪 PROBANDO TOLERANCIA: ±{tolerancia}°")
        
        # 3. Consolidar sucursales
        sucursales_consolidadas = consolidar_sucursales_inteligente(supervisiones, tolerancia)
        
        num_sucursales = len(sucursales_consolidadas)
        print(f"📊 Resultado: {num_sucursales} sucursales")
        
        if num_sucursales <= 85:  # Cercano a 80
            # 4. Analizar patrones
            estadisticas = analizar_patrones_supervisiones(sucursales_consolidadas)
            
            # 5. Generar listado completo
            print(f"\n🎯 USANDO TOLERANCIA: ±{tolerancia}° ({num_sucursales} sucursales)")
            estadisticas_finales = generar_listado_completo(estadisticas)
            
            # 6. Guardar resultado
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_file = f"listado_80_sucursales_definitivo_{timestamp}.csv"
            
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'Numero', 'Sucursal', 'Grupo_Operativo', 
                    'Operativas', 'Seguridad', 'Total', 'Balance',
                    'Fechas_Unicas', 'Fechas_Coincidentes',
                    'Es_Perfecta', 'Es_Balanceada', 'Status'
                ])
                
                for i, stats in enumerate(sorted(estadisticas_finales, key=lambda x: (x['grupo'], x['sucursal'])), 1):
                    status = "PERFECTA" if stats['es_perfecta'] else ("BALANCEADA" if stats['es_balanceada'] else "DESBALANCEADA")
                    
                    writer.writerow([
                        i, stats['sucursal'], stats['grupo'],
                        stats['operativas'], stats['seguridad'], 
                        stats['operativas'] + stats['seguridad'], stats['balance'],
                        stats['fechas_unicas'], stats['fechas_coincidentes'],
                        stats['es_perfecta'], stats['es_balanceada'], status
                    ])
            
            print(f"\n💾 Listado guardado en: {csv_file}")
            break
    
    print(f"\n🎉 CONSOLIDACIÓN COMPLETADA")

if __name__ == "__main__":
    main()