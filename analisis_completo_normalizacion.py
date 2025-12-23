#!/usr/bin/env python3
"""
🔍 ANÁLISIS COMPLETO PARA NORMALIZACIÓN
1. Encontrar 3 supervisiones Seguridad faltantes
2. Corregir mapeo Centrito → Gómez Morín por coordenadas
3. Balancear 238 + 238 supervisiones
"""

import psycopg2
import csv
import math
from datetime import datetime, date
import json

# Configuración Railway
DATABASE_URL = 'postgresql://postgres:qGgdIUuKYKMKGtSNYzARpyapBWHsloOt@turntable.proxy.rlwy.net:24097/railway'

def cargar_coordenadas_sucursales():
    """Cargar coordenadas de todas las sucursales"""
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
    """Calcular distancia euclidiana"""
    return math.sqrt((float(lat1) - float(lat2))**2 + (float(lon1) - float(lon2))**2)

def analisis_supervisiones_por_dia():
    """Analizar supervisiones agrupadas por día y auditor"""
    
    print("📅 ANÁLISIS POR DÍA Y AUDITOR")
    print("=" * 50)
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Analizar por día, auditor y sucursal
        cursor.execute("""
            SELECT 
                fecha_supervision,
                auditor_nombre,
                location_name,
                grupo_operativo,
                latitude,
                longitude,
                COUNT(CASE WHEN form_type = 'OPERATIVA' THEN 1 END) as operativas,
                COUNT(CASE WHEN form_type = 'SEGURIDAD' THEN 1 END) as seguridad,
                COUNT(*) as total,
                STRING_AGG(submission_id, ', ') as submissions
            FROM supervisiones_2026 
            WHERE fecha_supervision IS NOT NULL
            GROUP BY fecha_supervision, auditor_nombre, location_name, grupo_operativo, latitude, longitude
            ORDER BY fecha_supervision DESC, auditor_nombre
        """)
        
        supervisiones_dia = cursor.fetchall()
        
        print(f"📊 Encontrados {len(supervisiones_dia)} grupos día/auditor/sucursal")
        
        # Buscar desbalanceados
        desbalanceados = []
        sin_par = []
        
        for registro in supervisiones_dia:
            fecha, auditor, location, grupo, lat, lon, ops, segs, total, submissions = registro
            
            if ops != segs:
                desbalanceados.append({
                    'fecha': fecha,
                    'auditor': auditor,
                    'location': location,
                    'grupo': grupo,
                    'lat': lat,
                    'lon': lon,
                    'operativas': ops,
                    'seguridad': segs,
                    'diferencia': abs(ops - segs),
                    'submissions': submissions
                })
            
            if ops == 0 or segs == 0:
                sin_par.append({
                    'fecha': fecha,
                    'auditor': auditor,
                    'location': location,
                    'grupo': grupo,
                    'operativas': ops,
                    'seguridad': segs,
                    'tipo_faltante': 'SEGURIDAD' if ops > 0 else 'OPERATIVA'
                })
        
        print(f"\n🔍 SUPERVISIONES DESBALANCEADAS: {len(desbalanceados)}")
        for i, item in enumerate(desbalanceados[:10]):
            print(f"   {i+1}. {item['fecha']} - {item['auditor']} - {item['location']}")
            print(f"      Op: {item['operativas']}, Seg: {item['seguridad']}, Diff: {item['diferencia']}")
        
        print(f"\n🚨 SUPERVISIONES SIN PAR: {len(sin_par)}")
        for i, item in enumerate(sin_par[:10]):
            print(f"   {i+1}. {item['fecha']} - {item['auditor']} - {item['location']}")
            print(f"      Falta: {item['tipo_faltante']}")
        
        cursor.close()
        conn.close()
        
        return desbalanceados, sin_par
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return [], []

def buscar_supervisiones_faltantes():
    """Buscar las 3 supervisiones de seguridad faltantes"""
    
    print(f"\n🔍 BUSCANDO 3 SUPERVISIONES SEGURIDAD FALTANTES")
    print("=" * 50)
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Contar supervisiones actuales
        cursor.execute("SELECT form_type, COUNT(*) FROM supervisiones_2026 GROUP BY form_type;")
        conteos = cursor.fetchall()
        
        for tipo, count in conteos:
            print(f"   {tipo}: {count}")
        
        # Buscar submissions que podrían estar duplicadas o mal procesadas
        cursor.execute("""
            SELECT submission_id, COUNT(*) 
            FROM supervisiones_2026 
            GROUP BY submission_id 
            HAVING COUNT(*) > 1
        """)
        duplicadas = cursor.fetchall()
        
        print(f"\n📋 Submissions duplicadas: {len(duplicadas)}")
        for sub_id, count in duplicadas:
            print(f"   {sub_id}: {count} veces")
        
        # Buscar operativas sin su par de seguridad
        cursor.execute("""
            WITH operativas_por_dia AS (
                SELECT 
                    fecha_supervision,
                    auditor_nombre,
                    grupo_operativo,
                    latitude,
                    longitude,
                    COUNT(*) as ops_count
                FROM supervisiones_2026 
                WHERE form_type = 'OPERATIVA'
                GROUP BY fecha_supervision, auditor_nombre, grupo_operativo, latitude, longitude
            ),
            seguridad_por_dia AS (
                SELECT 
                    fecha_supervision,
                    auditor_nombre,
                    grupo_operativo,
                    latitude,
                    longitude,
                    COUNT(*) as seg_count
                FROM supervisiones_2026 
                WHERE form_type = 'SEGURIDAD'
                GROUP BY fecha_supervision, auditor_nombre, grupo_operativo, latitude, longitude
            )
            SELECT 
                o.fecha_supervision,
                o.auditor_nombre,
                o.grupo_operativo,
                o.latitude,
                o.longitude,
                o.ops_count,
                COALESCE(s.seg_count, 0) as seg_count,
                (o.ops_count - COALESCE(s.seg_count, 0)) as diferencia
            FROM operativas_por_dia o
            LEFT JOIN seguridad_por_dia s ON (
                o.fecha_supervision = s.fecha_supervision AND
                o.auditor_nombre = s.auditor_nombre AND
                o.grupo_operativo = s.grupo_operativo AND
                ABS(o.latitude - s.latitude) < 0.001 AND
                ABS(o.longitude - s.longitude) < 0.001
            )
            WHERE o.ops_count != COALESCE(s.seg_count, 0)
            ORDER BY diferencia DESC
        """)
        
        operativas_sin_par = cursor.fetchall()
        
        print(f"\n📊 OPERATIVAS SIN PAR DE SEGURIDAD:")
        total_faltantes = 0
        for registro in operativas_sin_par:
            fecha, auditor, grupo, lat, lon, ops, segs, diff = registro
            if diff > 0:  # Más operativas que seguridad
                print(f"   {fecha} - {auditor} - {grupo}")
                print(f"      Ops: {ops}, Seg: {segs}, Faltan: {diff} seguridad")
                total_faltantes += diff
        
        print(f"\n🎯 Total supervisiones seguridad faltantes estimadas: {total_faltantes}")
        
        cursor.close()
        conn.close()
        
        return operativas_sin_par
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return []

def analizar_centrito_gomez_morin():
    """Analizar supervisiones de Centrito que deberían ser Gómez Morín"""
    
    print(f"\n🎯 ANÁLISIS CENTRITO vs GÓMEZ MORÍN")
    print("=" * 50)
    
    sucursales_coords = cargar_coordenadas_sucursales()
    
    # Coordenadas de referencia
    centrito_coords = sucursales_coords.get('Centrito Valle')
    gomez_morin_coords = sucursales_coords.get('Gomez Morin')
    
    if not centrito_coords or not gomez_morin_coords:
        print("❌ No se encontraron coordenadas de Centrito o Gómez Morín")
        return []
    
    print(f"📍 Centrito Valle: {centrito_coords['lat']}, {centrito_coords['lon']}")
    print(f"📍 Gómez Morín: {gomez_morin_coords['lat']}, {gomez_morin_coords['lon']}")
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Obtener supervisiones marcadas como Centrito
        cursor.execute("""
            SELECT 
                id, submission_id, form_type, auditor_nombre, fecha_supervision,
                latitude, longitude, location_name, grupo_operativo
            FROM supervisiones_2026 
            WHERE location_name LIKE '%Centrito%' OR grupo_operativo = 'GRUPO CENTRITO'
            ORDER BY fecha_supervision
        """)
        
        supervisiones_centrito = cursor.fetchall()
        
        print(f"\n📊 Supervisiones marcadas como Centrito: {len(supervisiones_centrito)}")
        
        correcciones_sugeridas = []
        
        for supervision in supervisiones_centrito:
            id_sup, submission_id, form_type, auditor, fecha, lat, lon, location_name, grupo = supervision
            
            if lat and lon:
                # Calcular distancia a ambas sucursales
                dist_centrito = calcular_distancia(lat, lon, centrito_coords['lat'], centrito_coords['lon'])
                dist_gomez_morin = calcular_distancia(lat, lon, gomez_morin_coords['lat'], gomez_morin_coords['lon'])
                
                # Si está más cerca de Gómez Morín
                if dist_gomez_morin < dist_centrito:
                    correcciones_sugeridas.append({
                        'supervision_id': id_sup,
                        'submission_id': submission_id,
                        'form_type': form_type,
                        'auditor': auditor,
                        'fecha': fecha,
                        'lat': lat,
                        'lon': lon,
                        'location_actual': location_name,
                        'grupo_actual': grupo,
                        'dist_centrito': dist_centrito,
                        'dist_gomez_morin': dist_gomez_morin,
                        'diferencia': dist_centrito - dist_gomez_morin
                    })
        
        print(f"\n🔄 CORRECCIONES SUGERIDAS: {len(correcciones_sugeridas)}")
        
        for i, corr in enumerate(correcciones_sugeridas[:10]):
            print(f"   {i+1}. {corr['form_type']} - {corr['auditor']} - {corr['fecha']}")
            print(f"      Actual: {corr['location_actual']}")
            print(f"      Dist Centrito: {corr['dist_centrito']:.6f}")
            print(f"      Dist Gómez Morín: {corr['dist_gomez_morin']:.6f}")
            print(f"      ✅ Más cerca de Gómez Morín por {corr['diferencia']:.6f}")
            print()
        
        cursor.close()
        conn.close()
        
        return correcciones_sugeridas
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return []

def buscar_3_supervisiones_perdidas():
    """Buscar específicamente las 3 supervisiones de seguridad que faltan"""
    
    print(f"\n🕵️ BÚSQUEDA ESPECÍFICA DE 3 SUPERVISIONES PERDIDAS")
    print("=" * 60)
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Verificar si hay submissions procesadas incorrectamente
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT submission_id) as unique_submissions,
                COUNT(*) as total_records,
                COUNT(*) - COUNT(DISTINCT submission_id) as duplicates
            FROM supervisiones_2026
        """)
        
        unique_subs, total_records, duplicates = cursor.fetchone()
        
        print(f"📊 ANÁLISIS DE SUBMISSIONS:")
        print(f"   Submissions únicas: {unique_subs}")
        print(f"   Registros totales: {total_records}")
        print(f"   Duplicados: {duplicates}")
        
        # Buscar en logs o revisar si hay submissions que no se procesaron
        cursor.execute("""
            WITH missing_seguridad AS (
                SELECT 
                    o.fecha_supervision,
                    o.auditor_nombre,
                    o.latitude,
                    o.longitude,
                    COUNT(*) as ops_sin_seg
                FROM supervisiones_2026 o
                WHERE o.form_type = 'OPERATIVA'
                AND NOT EXISTS (
                    SELECT 1 FROM supervisiones_2026 s 
                    WHERE s.form_type = 'SEGURIDAD'
                    AND s.fecha_supervision = o.fecha_supervision
                    AND s.auditor_nombre = o.auditor_nombre
                    AND ABS(s.latitude - o.latitude) < 0.001
                    AND ABS(s.longitude - o.longitude) < 0.001
                )
                GROUP BY o.fecha_supervision, o.auditor_nombre, o.latitude, o.longitude
            )
            SELECT * FROM missing_seguridad 
            ORDER BY ops_sin_seg DESC
            LIMIT 10
        """)
        
        missing_analysis = cursor.fetchall()
        
        print(f"\n🚨 OPERATIVAS SIN SEGURIDAD CORRESPONDIENTE:")
        total_missing = 0
        for record in missing_analysis:
            fecha, auditor, lat, lon, count = record
            total_missing += count
            print(f"   {fecha} - {auditor} - {count} operativas sin seguridad")
            print(f"     Coordenadas: {lat}, {lon}")
        
        print(f"\n🎯 Total estimado faltante: {total_missing}")
        
        # Verificar si las 3 faltantes podrían estar en las submissions problemáticas
        cursor.execute("""
            SELECT submission_id FROM supervisiones_2026 
            WHERE form_type = 'SEGURIDAD' 
            AND (location_id IS NULL OR location_name IS NULL)
            ORDER BY created_at DESC
            LIMIT 5
        """)
        
        problematic_subs = cursor.fetchall()
        
        print(f"\n📋 Submissions problemáticas recientes:")
        for (sub_id,) in problematic_subs:
            print(f"   {sub_id}")
        
        cursor.close()
        conn.close()
        
        return missing_analysis
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return []

def aplicar_correcciones(correcciones_centrito):
    """Aplicar las correcciones de Centrito → Gómez Morín"""
    
    if not correcciones_centrito:
        print("⚠️ No hay correcciones para aplicar")
        return False
    
    print(f"\n🔧 APLICANDO {len(correcciones_centrito)} CORRECCIONES")
    print("=" * 50)
    
    sucursales_coords = cargar_coordenadas_sucursales()
    gomez_morin_coords = sucursales_coords.get('Gomez Morin')
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Obtener datos de Gómez Morín de la BD
        cursor.execute("""
            SELECT id, external_key FROM sucursales 
            WHERE nombre = 'Gomez Morin'
        """)
        gomez_morin_bd = cursor.fetchone()
        
        if not gomez_morin_bd:
            print("❌ No se encontró Gómez Morín en BD")
            return False
        
        sucursal_id, external_key = gomez_morin_bd
        
        corregidas = 0
        
        for correccion in correcciones_centrito:
            try:
                cursor.execute("""
                    UPDATE supervisiones_2026 SET
                        sucursal_id = %s,
                        external_key = %s,
                        location_id = %s,
                        location_name = %s,
                        grupo_operativo = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (
                    sucursal_id,
                    external_key,
                    gomez_morin_coords['location_code'],
                    'Gomez Morin',
                    'PLOG NUEVO LEON',
                    correccion['supervision_id']
                ))
                
                corregidas += 1
                
            except Exception as e:
                print(f"   ❌ Error corrigiendo {correccion['submission_id']}: {e}")
        
        conn.commit()
        
        print(f"✅ Correcciones aplicadas: {corregidas}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Error aplicando correcciones: {e}")
        return False

def generar_reporte_final():
    """Generar reporte final de normalización"""
    
    print(f"\n📊 REPORTE FINAL DE NORMALIZACIÓN")
    print("=" * 50)
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Contar por tipo
        cursor.execute("SELECT form_type, COUNT(*) FROM supervisiones_2026 GROUP BY form_type;")
        conteos = cursor.fetchall()
        
        print("📊 CONTEOS FINALES:")
        for tipo, count in conteos:
            print(f"   {tipo}: {count}")
        
        # Distribución por grupo
        cursor.execute("""
            SELECT 
                grupo_operativo,
                COUNT(CASE WHEN form_type = 'OPERATIVA' THEN 1 END) as operativas,
                COUNT(CASE WHEN form_type = 'SEGURIDAD' THEN 1 END) as seguridad,
                COUNT(*) as total
            FROM supervisiones_2026 
            GROUP BY grupo_operativo 
            ORDER BY total DESC
        """)
        
        distribucion = cursor.fetchall()
        
        print(f"\n📊 DISTRIBUCIÓN POR GRUPO:")
        desbalanceados = 0
        for grupo, ops, segs, total in distribucion:
            balance = "✅" if ops == segs else "⚠️"
            if ops != segs:
                desbalanceados += 1
            print(f"   {balance} {grupo}: {ops} op, {segs} seg, {total} total")
        
        print(f"\n🎯 Grupos desbalanceados: {desbalanceados}")
        
        # Verificar Gómez Morín específicamente
        cursor.execute("""
            SELECT 
                location_name,
                COUNT(CASE WHEN form_type = 'OPERATIVA' THEN 1 END) as operativas,
                COUNT(CASE WHEN form_type = 'SEGURIDAD' THEN 1 END) as seguridad
            FROM supervisiones_2026 
            WHERE location_name IN ('Gomez Morin', 'Centrito Valle')
            GROUP BY location_name
        """)
        
        sucursales_especificas = cursor.fetchall()
        
        print(f"\n🎯 SUCURSALES ESPECÍFICAS:")
        for location, ops, segs in sucursales_especificas:
            print(f"   {location}: {ops} operativas, {segs} seguridad")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

def main():
    """Función principal de análisis y corrección"""
    
    print("🔍 ANÁLISIS COMPLETO PARA NORMALIZACIÓN 238+238")
    print("=" * 60)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # 1. Análisis por día y auditor
    desbalanceados, sin_par = analisis_supervisiones_por_dia()
    
    # 2. Buscar supervisiones faltantes
    operativas_sin_par = buscar_supervisiones_faltantes()
    
    # 3. Análisis específico de Centrito vs Gómez Morín
    correcciones_centrito = analizar_centrito_gomez_morin()
    
    # 4. Búsqueda específica de 3 supervisiones perdidas
    missing_analysis = buscar_3_supervisiones_perdidas()
    
    # 5. Aplicar correcciones si el usuario confirma
    if correcciones_centrito:
        print(f"\n¿Aplicar {len(correcciones_centrito)} correcciones Centrito → Gómez Morín? (s/n)")
        respuesta = input().lower()
        
        if respuesta in ['s', 'si', 'yes', 'y']:
            aplicar_correcciones(correcciones_centrito)
    
    # 6. Reporte final
    generar_reporte_final()

if __name__ == "__main__":
    main()