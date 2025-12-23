#!/usr/bin/env python3
"""
📍 ANÁLISIS PRECISIÓN DE COORDENADAS DE SUBMISSION
Diagnosticar por qué hay desbalances usando coordenadas exactas de entrega
"""

import psycopg2
import csv
import math
from datetime import datetime
from collections import defaultdict

# Configuración Railway
DATABASE_URL = 'postgresql://postgres:qGgdIUuKYKMKGtSNYzARpyapBWHsloOt@turntable.proxy.rlwy.net:24097/railway'

def cargar_coordenadas_sucursales():
    """Cargar coordenadas oficiales de sucursales"""
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

def analizar_supervisiones_por_coordenadas_exactas():
    """Analizar supervisiones agrupadas por coordenadas exactas de submission"""
    
    print("📍 ANÁLISIS POR COORDENADAS EXACTAS DE SUBMISSION")
    print("=" * 60)
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Agrupar por coordenadas exactas y fecha
        cursor.execute("""
            SELECT 
                latitude, longitude, fecha_supervision,
                location_name, grupo_operativo, auditor_nombre,
                COUNT(CASE WHEN form_type = 'OPERATIVA' THEN 1 END) as operativas,
                COUNT(CASE WHEN form_type = 'SEGURIDAD' THEN 1 END) as seguridad,
                COUNT(*) as total,
                STRING_AGG(DISTINCT location_name, ' | ') as ubicaciones_distintas,
                STRING_AGG(DISTINCT grupo_operativo, ' | ') as grupos_distintos,
                STRING_AGG(submission_id, ', ') as submissions
            FROM supervisiones_2026 
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            GROUP BY latitude, longitude, fecha_supervision, location_name, grupo_operativo, auditor_nombre
            ORDER BY total DESC, fecha_supervision DESC
        """)
        
        grupos_coordenadas = cursor.fetchall()
        
        print(f"📊 Grupos únicos por coordenadas exactas: {len(grupos_coordenadas)}")
        
        # Analizar desbalances
        balanceadas = 0
        desbalanceadas = 0
        problematicas = []
        
        for grupo in grupos_coordenadas:
            lat, lon, fecha, location, grupo_op, auditor, ops, segs, total, ubicaciones, grupos, submissions = grupo
            
            if ops == segs:
                balanceadas += 1
            else:
                desbalanceadas += 1
                problematicas.append({
                    'lat': lat,
                    'lon': lon,
                    'fecha': fecha,
                    'location': location,
                    'grupo': grupo_op,
                    'auditor': auditor,
                    'operativas': ops,
                    'seguridad': segs,
                    'diferencia': abs(ops - segs),
                    'ubicaciones': ubicaciones,
                    'grupos': grupos,
                    'submissions': submissions
                })
        
        print(f"✅ Coordenadas balanceadas: {balanceadas}")
        print(f"❌ Coordenadas desbalanceadas: {desbalanceadas}")
        
        print(f"\n🚨 COORDENADAS PROBLEMÁTICAS (top 10):")
        for i, item in enumerate(sorted(problematicas, key=lambda x: x['diferencia'], reverse=True)[:10]):
            print(f"   {i+1}. {item['fecha']} - {item['auditor']}")
            print(f"      Coordenadas: {item['lat']}, {item['lon']}")
            print(f"      Location actual: {item['location']} ({item['grupo']})")
            print(f"      Op: {item['operativas']}, Seg: {item['seguridad']}, Diff: {item['diferencia']}")
            print(f"      Todas ubicaciones: {item['ubicaciones']}")
            print(f"      Todos grupos: {item['grupos']}")
            print()
        
        cursor.close()
        conn.close()
        
        return problematicas
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return []

def verificar_mapeo_centrito_gomez_morin():
    """Verificar específicamente Centrito vs Gómez Morín por coordenadas"""
    
    print(f"\n🎯 VERIFICACIÓN ESPECÍFICA: CENTRITO vs GÓMEZ MORÍN")
    print("=" * 60)
    
    sucursales_coords = cargar_coordenadas_sucursales()
    
    centrito_oficial = sucursales_coords.get('Centrito Valle')
    gomez_morin_oficial = sucursales_coords.get('Gomez Morin')
    
    print(f"📍 COORDENADAS OFICIALES:")
    print(f"   Centrito Valle: {centrito_oficial['lat']}, {centrito_oficial['lon']}")
    print(f"   Gómez Morín: {gomez_morin_oficial['lat']}, {gomez_morin_oficial['lon']}")
    
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
            ORDER BY latitude, longitude, fecha_supervision
        """)
        
        supervisiones_centrito = cursor.fetchall()
        
        print(f"\n📊 Supervisiones marcadas como Centrito: {len(supervisiones_centrito)}")
        
        # Agrupar por coordenadas exactas
        coords_groups = defaultdict(list)
        for supervision in supervisiones_centrito:
            id_sup, sub_id, form_type, auditor, fecha, lat, lon, location, grupo = supervision
            key = (float(lat), float(lon))
            coords_groups[key].append({
                'id': id_sup,
                'submission_id': sub_id,
                'form_type': form_type,
                'auditor': auditor,
                'fecha': fecha,
                'location': location,
                'grupo': grupo
            })
        
        print(f"\n📍 COORDENADAS ÚNICAS EN CENTRITO: {len(coords_groups)}")
        
        for coords, supervisiones in coords_groups.items():
            lat, lon = coords
            
            # Calcular distancia a ambas sucursales
            dist_centrito = math.sqrt((lat - centrito_oficial['lat'])**2 + (lon - centrito_oficial['lon'])**2)
            dist_gomez_morin = math.sqrt((lat - gomez_morin_oficial['lat'])**2 + (lon - gomez_morin_oficial['lon'])**2)
            
            # Determinar cuál es más cercana
            mas_cercana = "Gómez Morín" if dist_gomez_morin < dist_centrito else "Centrito Valle"
            
            # Contar por tipo
            ops = sum(1 for s in supervisiones if s['form_type'] == 'OPERATIVA')
            segs = sum(1 for s in supervisiones if s['form_type'] == 'SEGURIDAD')
            
            print(f"\n   Coordenadas: {lat:.6f}, {lon:.6f}")
            print(f"   Supervisiones: {ops} Op, {segs} Seg")
            print(f"   Dist. Centrito: {dist_centrito:.6f}")
            print(f"   Dist. Gómez Morín: {dist_gomez_morin:.6f}")
            print(f"   ✅ Más cercana a: {mas_cercana}")
            
            if mas_cercana != "Centrito Valle":
                print(f"   🔧 REQUIERE CORRECCIÓN → Mover a Gómez Morín")
        
        cursor.close()
        conn.close()
        
        return coords_groups
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return {}

def analizar_supervisiones_mismo_dia_mismas_coords():
    """Analizar supervisiones del mismo día con las mismas coordenadas exactas"""
    
    print(f"\n📅 ANÁLISIS: MISMO DÍA, MISMAS COORDENADAS")
    print("=" * 60)
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Buscar casos donde mismo día y mismas coordenadas tengan diferente balance
        cursor.execute("""
            WITH coord_day_groups AS (
                SELECT 
                    latitude, longitude, fecha_supervision,
                    COUNT(CASE WHEN form_type = 'OPERATIVA' THEN 1 END) as operativas,
                    COUNT(CASE WHEN form_type = 'SEGURIDAD' THEN 1 END) as seguridad,
                    STRING_AGG(DISTINCT location_name, ' | ') as ubicaciones,
                    STRING_AGG(DISTINCT grupo_operativo, ' | ') as grupos,
                    STRING_AGG(DISTINCT auditor_nombre, ' | ') as auditores,
                    STRING_AGG(submission_id, ', ') as submissions
                FROM supervisiones_2026 
                WHERE latitude IS NOT NULL AND longitude IS NOT NULL
                GROUP BY latitude, longitude, fecha_supervision
            )
            SELECT *
            FROM coord_day_groups 
            WHERE operativas != seguridad
            ORDER BY ABS(operativas - seguridad) DESC, fecha_supervision DESC
        """)
        
        casos_problematicos = cursor.fetchall()
        
        print(f"📊 Casos problemáticos (mismo día, mismas coords, desbalance): {len(casos_problematicos)}")
        
        for i, caso in enumerate(casos_problematicos[:10]):
            lat, lon, fecha, ops, segs, ubicaciones, grupos, auditores, submissions = caso
            diff = abs(ops - segs)
            
            print(f"\n   {i+1}. {fecha} - Diferencia: {diff}")
            print(f"      Coordenadas: {lat:.6f}, {lon:.6f}")
            print(f"      Op: {ops}, Seg: {segs}")
            print(f"      Ubicaciones: {ubicaciones}")
            print(f"      Grupos: {grupos}")
            print(f"      Auditores: {auditores}")
        
        cursor.close()
        conn.close()
        
        return casos_problematicos
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return []

def generar_plan_correccion(coords_groups):
    """Generar plan de corrección basado en coordenadas precisas"""
    
    print(f"\n🔧 PLAN DE CORRECCIÓN BASADO EN COORDENADAS")
    print("=" * 60)
    
    sucursales_coords = cargar_coordenadas_sucursales()
    centrito_oficial = sucursales_coords.get('Centrito Valle')
    gomez_morin_oficial = sucursales_coords.get('Gomez Morin')
    
    correcciones = []
    
    for coords, supervisiones in coords_groups.items():
        lat, lon = coords
        
        # Calcular distancias
        dist_centrito = math.sqrt((lat - centrito_oficial['lat'])**2 + (lon - centrito_oficial['lon'])**2)
        dist_gomez_morin = math.sqrt((lat - gomez_morin_oficial['lat'])**2 + (lon - gomez_morin_oficial['lon'])**2)
        
        # Si está más cerca de Gómez Morín
        if dist_gomez_morin < dist_centrito:
            for supervision in supervisiones:
                correcciones.append({
                    'supervision_id': supervision['id'],
                    'submission_id': supervision['submission_id'],
                    'form_type': supervision['form_type'],
                    'fecha': supervision['fecha'],
                    'auditor': supervision['auditor'],
                    'lat': lat,
                    'lon': lon,
                    'ubicacion_actual': supervision['location'],
                    'grupo_actual': supervision['grupo'],
                    'ubicacion_correcta': 'Gomez Morin',
                    'grupo_correcto': 'PLOG NUEVO LEON',
                    'dist_actual': dist_centrito,
                    'dist_correcta': dist_gomez_morin,
                    'mejora_distancia': dist_centrito - dist_gomez_morin
                })
    
    print(f"📊 Correcciones sugeridas: {len(correcciones)}")
    
    if correcciones:
        print(f"\n🎯 TOP CORRECCIONES POR MEJORA DE DISTANCIA:")
        for i, corr in enumerate(sorted(correcciones, key=lambda x: x['mejora_distancia'], reverse=True)[:5]):
            print(f"   {i+1}. {corr['form_type']} - {corr['auditor']} - {corr['fecha']}")
            print(f"      {corr['ubicacion_actual']} ({corr['grupo_actual']}) → {corr['ubicacion_correcta']} ({corr['grupo_correcto']})")
            print(f"      Mejora distancia: {corr['mejora_distancia']:.6f}")
    
    return correcciones

def aplicar_correcciones_precision(correcciones):
    """Aplicar correcciones basadas en precisión de coordenadas"""
    
    if not correcciones:
        print("⚠️ No hay correcciones para aplicar")
        return False
    
    print(f"\n🔧 APLICANDO {len(correcciones)} CORRECCIONES DE PRECISIÓN")
    print("=" * 60)
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Obtener datos de Gómez Morín
        cursor.execute("""
            SELECT id, external_key, numero_sucursal FROM sucursales 
            WHERE nombre = 'Gomez Morin'
        """)
        gomez_morin_bd = cursor.fetchone()
        
        if not gomez_morin_bd:
            print("❌ No se encontró Gómez Morín en BD")
            return False
        
        sucursal_id, external_key, numero_sucursal = gomez_morin_bd
        location_code = 2247035  # Location code de Gómez Morín
        
        corregidas = 0
        
        for correccion in correcciones:
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
                    location_code,
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
        print(f"❌ Error: {e}")
        return False

def verificar_balance_post_correccion():
    """Verificar balance después de correcciones"""
    
    print(f"\n📊 VERIFICACIÓN POST-CORRECCIÓN")
    print("=" * 50)
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Balance por grupo
        cursor.execute("""
            SELECT 
                grupo_operativo,
                COUNT(CASE WHEN form_type = 'OPERATIVA' THEN 1 END) as operativas,
                COUNT(CASE WHEN form_type = 'SEGURIDAD' THEN 1 END) as seguridad,
                COUNT(CASE WHEN form_type = 'OPERATIVA' THEN 1 END) - 
                COUNT(CASE WHEN form_type = 'SEGURIDAD' THEN 1 END) as diferencia
            FROM supervisiones_2026 
            GROUP BY grupo_operativo 
            ORDER BY ABS(COUNT(CASE WHEN form_type = 'OPERATIVA' THEN 1 END) - 
                        COUNT(CASE WHEN form_type = 'SEGURIDAD' THEN 1 END)) DESC
        """)
        
        balance_grupos = cursor.fetchall()
        
        print(f"📊 BALANCE POR GRUPO POST-CORRECCIÓN:")
        balanceados = 0
        desbalanceados = 0
        
        for grupo, ops, segs, diff in balance_grupos:
            status = "✅" if diff == 0 else "⚠️"
            if diff == 0:
                balanceados += 1
            else:
                desbalanceados += 1
            print(f"   {status} {grupo}: {ops} op, {segs} seg ({diff:+})")
        
        print(f"\n🎯 RESUMEN:")
        print(f"   ✅ Grupos balanceados: {balanceados}")
        print(f"   ⚠️ Grupos desbalanceados: {desbalanceados}")
        
        # Verificar Centrito y PLOG NUEVO LEON específicamente
        cursor.execute("""
            SELECT 
                location_name,
                COUNT(CASE WHEN form_type = 'OPERATIVA' THEN 1 END) as operativas,
                COUNT(CASE WHEN form_type = 'SEGURIDAD' THEN 1 END) as seguridad
            FROM supervisiones_2026 
            WHERE location_name IN ('Centrito Valle', 'Gomez Morin')
            GROUP BY location_name
        """)
        
        sucursales_clave = cursor.fetchall()
        
        print(f"\n🏪 SUCURSALES CLAVE:")
        for location, ops, segs in sucursales_clave:
            balance = "✅" if ops == segs else "⚠️"
            print(f"   {balance} {location}: {ops} operativas, {segs} seguridad")
        
        cursor.close()
        conn.close()
        
        return balanceados, desbalanceados
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 0, -1

def main():
    """Función principal de análisis de precisión"""
    
    print("📍 ANÁLISIS DE PRECISIÓN DE COORDENADAS DE SUBMISSION")
    print("=" * 70)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    # 1. Análisis por coordenadas exactas
    problematicas = analizar_supervisiones_por_coordenadas_exactas()
    
    # 2. Verificación específica Centrito vs Gómez Morín
    coords_groups = verificar_mapeo_centrito_gomez_morin()
    
    # 3. Análisis mismo día, mismas coordenadas
    casos_problematicos = analizar_supervisiones_mismo_dia_mismas_coords()
    
    # 4. Generar plan de corrección
    correcciones = generar_plan_correccion(coords_groups)
    
    # 5. Aplicar correcciones
    if correcciones:
        print(f"\n¿Aplicar {len(correcciones)} correcciones de precisión? (s/n)")
        respuesta = input().lower()
        
        if respuesta in ['s', 'si', 'yes', 'y']:
            exito = aplicar_correcciones_precision(correcciones)
            
            if exito:
                # 6. Verificar resultado
                balanceados, desbalanceados = verificar_balance_post_correccion()
                
                if desbalanceados == 0:
                    print(f"\n🎉 ¡PERFECTO! Todos los grupos están balanceados")
                else:
                    print(f"\n⚠️ Aún quedan {desbalanceados} grupos desbalanceados")

if __name__ == "__main__":
    main()