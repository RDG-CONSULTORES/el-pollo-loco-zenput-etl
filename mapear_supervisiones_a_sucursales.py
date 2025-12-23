#!/usr/bin/env python3
"""
🎯 MAPEO EXPLÍCITO DE SUPERVISIONES A SUCURSALES
Asignar sucursal específica a supervisiones basado en coordenadas
"""

import psycopg2
import csv
import math
from datetime import datetime

# Configuración Railway
DATABASE_URL = 'postgresql://postgres:qGgdIUuKYKMKGtSNYzARpyapBWHsloOt@turntable.proxy.rlwy.net:24097/railway'

# Cargar coordenadas de sucursales
SUCURSALES_COORDS = {}

def cargar_coordenadas_sucursales():
    """Cargar coordenadas de las 86 sucursales"""
    global SUCURSALES_COORDS
    
    print("📍 Cargando coordenadas de sucursales...")
    
    with open('data/86_sucursales_master.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['Latitude'] and row['Longitude']:
                numero_sucursal = int(row['Numero_Sucursal'])
                SUCURSALES_COORDS[numero_sucursal] = {
                    'nombre': row['Nombre_Sucursal'],
                    'grupo': row['Grupo_Operativo'],
                    'lat': float(row['Latitude']),
                    'lon': float(row['Longitude']),
                    'location_code': int(row['Location_Code']) if row['Location_Code'] else None
                }
    
    print(f"✅ Cargadas {len(SUCURSALES_COORDS)} sucursales con coordenadas")
    return SUCURSALES_COORDS

def calcular_distancia(lat1, lon1, lat2, lon2):
    """Calcular distancia euclidiana entre dos puntos"""
    return math.sqrt((float(lat1) - float(lat2))**2 + (float(lon1) - float(lon2))**2)

def encontrar_sucursal_mas_cercana(lat, lon, grupo_operativo=None):
    """Encontrar sucursal más cercana a las coordenadas"""
    
    if not lat or not lon:
        return None
    
    distancia_minima = float('inf')
    sucursal_mas_cercana = None
    
    for numero, info in SUCURSALES_COORDS.items():
        # Si especificamos grupo, filtrar solo ese grupo
        if grupo_operativo and info['grupo'] != grupo_operativo:
            continue
            
        distancia = calcular_distancia(lat, lon, info['lat'], info['lon'])
        
        if distancia < distancia_minima:
            distancia_minima = distancia
            sucursal_mas_cercana = {
                'numero_sucursal': numero,
                'nombre': info['nombre'],
                'grupo': info['grupo'],
                'location_code': info['location_code'],
                'distancia': distancia
            }
    
    return sucursal_mas_cercana

def analizar_supervisiones_sin_location():
    """Analizar supervisiones que no tienen location asignada"""
    
    print("🔍 ANALIZANDO SUPERVISIONES SIN LOCATION")
    print("=" * 50)
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Obtener supervisiones sin location pero con coordenadas
        cursor.execute("""
            SELECT 
                id, submission_id, grupo_operativo, latitude, longitude,
                auditor_nombre, fecha_supervision, form_type
            FROM supervisiones_2026 
            WHERE location_id IS NULL 
                AND latitude IS NOT NULL 
                AND longitude IS NOT NULL
            ORDER BY grupo_operativo, fecha_supervision;
        """)
        
        supervisiones_sin_location = cursor.fetchall()
        
        print(f"📊 Encontradas {len(supervisiones_sin_location)} supervisiones sin location")
        
        if supervisiones_sin_location:
            print(f"\n📋 ANÁLISIS DETALLADO:")
            
            mapeo_results = []
            grupos_count = {}
            
            for supervision in supervisiones_sin_location:
                id_sup, submission_id, grupo, lat, lon, auditor, fecha, form_type = supervision
                
                # Encontrar sucursal más cercana
                sucursal_cercana = encontrar_sucursal_mas_cercana(lat, lon, grupo)
                
                if sucursal_cercana:
                    mapeo_results.append({
                        'supervision_id': id_sup,
                        'submission_id': submission_id,
                        'grupo_actual': grupo,
                        'sucursal_sugerida': sucursal_cercana['numero_sucursal'],
                        'sucursal_nombre': sucursal_cercana['nombre'],
                        'location_code': sucursal_cercana['location_code'],
                        'distancia': sucursal_cercana['distancia'],
                        'auditor': auditor,
                        'fecha': fecha,
                        'form_type': form_type,
                        'lat': lat,
                        'lon': lon
                    })
                    
                    # Contar por grupo
                    grupos_count[grupo] = grupos_count.get(grupo, 0) + 1
                else:
                    print(f"   ❌ No se encontró sucursal para supervisión {submission_id}")
            
            print(f"\n📊 DISTRIBUCIÓN POR GRUPO:")
            for grupo, count in sorted(grupos_count.items()):
                print(f"   {grupo}: {count} supervisiones sin location")
            
            # Mostrar muestra de mapeos
            print(f"\n🎯 MUESTRA DE MAPEOS (primeros 10):")
            for i, mapeo in enumerate(mapeo_results[:10]):
                print(f"   {i+1}. Supervisión {mapeo['form_type']} por {mapeo['auditor']}")
                print(f"      Grupo: {mapeo['grupo_actual']}")
                print(f"      → Sucursal sugerida: {mapeo['sucursal_nombre']} (#{mapeo['sucursal_sugerida']})")
                print(f"      → Distancia: {mapeo['distancia']:.6f}")
                print()
            
            cursor.close()
            conn.close()
            
            return mapeo_results
        else:
            print("✅ Todas las supervisiones ya tienen location asignada")
            cursor.close()
            conn.close()
            return []
            
    except Exception as e:
        print(f"❌ Error analizando supervisiones: {e}")
        if 'conn' in locals():
            conn.close()
        return []

def actualizar_locations_supervisiones(mapeo_results):
    """Actualizar supervisiones con sucursales específicas"""
    
    if not mapeo_results:
        print("⚠️ No hay mapeos para actualizar")
        return False
    
    print(f"\n🔄 ACTUALIZANDO {len(mapeo_results)} SUPERVISIONES CON SUCURSALES")
    print("=" * 60)
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Obtener mapping de sucursales desde la BD
        cursor.execute("SELECT numero_sucursal, id, external_key FROM sucursales;")
        sucursales_bd = {int(row[0]): {'id': row[1], 'external_key': row[2]} for row in cursor.fetchall()}
        
        actualizadas = 0
        errores = 0
        
        print("📊 Iniciando actualización...")
        
        for mapeo in mapeo_results:
            try:
                numero_sucursal = mapeo['sucursal_sugerida']
                
                if numero_sucursal in sucursales_bd:
                    sucursal_id = sucursales_bd[numero_sucursal]['id']
                    external_key = sucursales_bd[numero_sucursal]['external_key']
                    location_code = mapeo['location_code']
                    
                    # Actualizar supervisión con sucursal específica
                    cursor.execute("""
                        UPDATE supervisiones_2026 
                        SET 
                            sucursal_id = %s,
                            external_key = %s,
                            location_id = %s,
                            location_name = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (
                        sucursal_id,
                        external_key,
                        location_code,
                        mapeo['sucursal_nombre'],
                        mapeo['supervision_id']
                    ))
                    
                    actualizadas += 1
                    
                    if actualizadas % 25 == 0:
                        print(f"   ✅ Actualizadas: {actualizadas}")
                        conn.commit()
                else:
                    print(f"   ❌ Sucursal {numero_sucursal} no encontrada en BD")
                    errores += 1
                    
            except Exception as e:
                print(f"   ❌ Error actualizando supervisión {mapeo['submission_id']}: {e}")
                errores += 1
                continue
        
        conn.commit()
        
        # Verificar resultado
        cursor.execute("""
            SELECT COUNT(*) FROM supervisiones_2026 
            WHERE location_id IS NULL AND latitude IS NOT NULL;
        """)
        pendientes = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM supervisiones_2026;")
        total = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM supervisiones_2026 WHERE location_id IS NOT NULL;")
        con_location = cursor.fetchone()[0]
        
        print(f"\n📊 RESULTADOS:")
        print(f"   ✅ Supervisiones actualizadas: {actualizadas}")
        print(f"   ❌ Errores: {errores}")
        print(f"   📊 Total supervisiones: {total}")
        print(f"   🎯 Con location: {con_location}")
        print(f"   ⚠️ Sin location: {pendientes}")
        print(f"   📈 % con location: {(con_location/total)*100:.1f}%")
        
        cursor.close()
        conn.close()
        
        print(f"\n🎉 ACTUALIZACIÓN COMPLETADA!")
        return True
        
    except Exception as e:
        print(f"❌ Error actualizando supervisiones: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        return False

def generar_reporte_mapeo():
    """Generar reporte completo del mapeo final"""
    
    print(f"\n📊 GENERANDO REPORTE FINAL DE MAPEO")
    print("=" * 50)
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Estadísticas generales
        cursor.execute("""
            SELECT 
                form_type,
                COUNT(*) as total,
                COUNT(location_id) as con_location,
                COUNT(*) - COUNT(location_id) as sin_location
            FROM supervisiones_2026 
            GROUP BY form_type;
        """)
        
        stats_por_tipo = cursor.fetchall()
        
        print("📊 ESTADÍSTICAS POR TIPO:")
        for tipo, total, con_loc, sin_loc in stats_por_tipo:
            print(f"   {tipo}: {total} total | {con_loc} con location | {sin_loc} sin location")
        
        # Distribución por grupo
        cursor.execute("""
            SELECT 
                s.grupo_operativo_nombre as grupo,
                COUNT(sv.id) as supervisiones,
                COUNT(sv.location_id) as con_location,
                COUNT(sv.id) - COUNT(sv.location_id) as sin_location
            FROM sucursales s
            LEFT JOIN supervisiones_2026 sv ON s.id = sv.sucursal_id
            GROUP BY s.grupo_operativo_nombre
            ORDER BY COUNT(sv.id) DESC;
        """)
        
        distribucion = cursor.fetchall()
        
        print(f"\n📊 DISTRIBUCIÓN POR GRUPO:")
        for grupo, total, con_loc, sin_loc in distribucion:
            if total > 0:
                print(f"   {grupo}: {total} supervisiones | {con_loc} con location | {sin_loc} sin location")
        
        # Sucursales con más supervisiones
        cursor.execute("""
            SELECT 
                s.nombre,
                s.grupo_operativo_nombre,
                COUNT(sv.id) as supervisiones,
                COUNT(CASE WHEN sv.form_type = 'OPERATIVA' THEN 1 END) as operativas,
                COUNT(CASE WHEN sv.form_type = 'SEGURIDAD' THEN 1 END) as seguridad
            FROM sucursales s
            LEFT JOIN supervisiones_2026 sv ON s.id = sv.sucursal_id
            WHERE sv.id IS NOT NULL
            GROUP BY s.nombre, s.grupo_operativo_nombre
            ORDER BY COUNT(sv.id) DESC
            LIMIT 10;
        """)
        
        top_sucursales = cursor.fetchall()
        
        print(f"\n🏪 TOP 10 SUCURSALES CON MÁS SUPERVISIONES:")
        for nombre, grupo, total, operativas, seguridad in top_sucursales:
            print(f"   {nombre} ({grupo}): {total} total | {operativas} op | {seguridad} seg")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Error generando reporte: {e}")
        if 'conn' in locals():
            conn.close()
        return False

def main():
    """Función principal"""
    
    print("🎯 MAPEO COMPLETO DE SUPERVISIONES A SUCURSALES")
    print("=" * 60)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # 1. Cargar coordenadas
    cargar_coordenadas_sucursales()
    
    # 2. Analizar supervisiones sin location
    mapeo_results = analizar_supervisiones_sin_location()
    
    if mapeo_results:
        print(f"\n¿Deseas actualizar las {len(mapeo_results)} supervisiones con sus sucursales más cercanas? (s/n)")
        respuesta = input().lower()
        
        if respuesta in ['s', 'si', 'yes', 'y']:
            # 3. Actualizar supervisiones
            success = actualizar_locations_supervisiones(mapeo_results)
            
            if success:
                # 4. Generar reporte final
                generar_reporte_mapeo()
        else:
            print("⚠️ Actualización cancelada")
            
            # Mostrar reporte sin actualizar
            print(f"\n📋 REPORTE DE MAPEO SUGERIDO:")
            
            # Guardar mapeo en CSV para revisión
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_file = f"mapeo_supervisiones_sucursales_{timestamp}.csv"
            
            with open(csv_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=mapeo_results[0].keys())
                writer.writeheader()
                writer.writerows(mapeo_results)
            
            print(f"💾 Mapeo guardado en: {csv_file}")
    else:
        # Solo generar reporte
        generar_reporte_mapeo()

if __name__ == "__main__":
    main()