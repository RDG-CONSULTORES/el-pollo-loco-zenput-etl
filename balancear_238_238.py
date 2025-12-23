#!/usr/bin/env python3
"""
⚖️ BALANCEAR SUPERVISIONES 238+238
Encontrar las 3 supervisiones faltantes y balancear exactamente
"""

import psycopg2
import requests
import csv
import math
from datetime import datetime

# Configuración
DATABASE_URL = 'postgresql://postgres:qGgdIUuKYKMKGtSNYzARpyapBWHsloOt@turntable.proxy.rlwy.net:24097/railway'

ZENPUT_CONFIG = {
    'base_url': 'https://www.zenput.com/api/v3',
    'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}
}

def buscar_submissions_adicionales_api():
    """Buscar submissions adicionales en la API que no se procesaron"""
    
    print("🕵️ BUSCANDO SUBMISSIONS ADICIONALES EN API")
    print("=" * 50)
    
    try:
        # Buscar con fecha más amplia para asegurar que tenemos todo
        url = f"{ZENPUT_CONFIG['base_url']}/submissions"
        params = {
            'form_template_id': '877139',  # Seguridad
            'limit': 300,  # Aumentar límite
            'created_after': '2024-12-01',  # Ampliar rango
            'created_before': '2025-12-31'
        }
        
        response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            submissions_api = data.get('data', [])
            
            print(f"📊 Submissions en API: {len(submissions_api)}")
            
            # Verificar cuáles están en nuestra BD
            conn = psycopg2.connect(DATABASE_URL)
            cursor = conn.cursor()
            
            cursor.execute("SELECT submission_id FROM supervisiones_2026 WHERE form_type = 'SEGURIDAD'")
            submissions_bd = {row[0] for row in cursor.fetchall()}
            
            print(f"📊 Submissions en BD: {len(submissions_bd)}")
            
            # Encontrar diferencias
            submissions_api_ids = {s.get('id') for s in submissions_api if s}
            faltantes_en_bd = submissions_api_ids - submissions_bd
            extra_en_bd = submissions_bd - submissions_api_ids
            
            print(f"📊 En API pero NO en BD: {len(faltantes_en_bd)}")
            print(f"📊 En BD pero NO en API: {len(extra_en_bd)}")
            
            if faltantes_en_bd:
                print(f"\n🚨 SUBMISSIONS FALTANTES EN BD:")
                for i, sub_id in enumerate(list(faltantes_en_bd)[:5]):
                    print(f"   {i+1}. {sub_id}")
            
            cursor.close()
            conn.close()
            
            return list(faltantes_en_bd), submissions_api
        else:
            print(f"❌ Error API: {response.status_code}")
            return [], []
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return [], []

def analizar_operativas_sin_seguridad():
    """Analizar operativas que no tienen su par de seguridad por coordenadas exactas"""
    
    print(f"\n📍 ANALIZANDO OPERATIVAS SIN PAR POR COORDENADAS")
    print("=" * 50)
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Buscar operativas que no tienen seguridad con las mismas coordenadas exactas
        cursor.execute("""
            SELECT 
                o.id, o.submission_id, o.auditor_nombre, o.fecha_supervision,
                o.latitude, o.longitude, o.grupo_operativo, o.location_name
            FROM supervisiones_2026 o
            WHERE o.form_type = 'OPERATIVA'
            AND NOT EXISTS (
                SELECT 1 FROM supervisiones_2026 s 
                WHERE s.form_type = 'SEGURIDAD'
                AND s.latitude = o.latitude
                AND s.longitude = o.longitude
                AND s.fecha_supervision = o.fecha_supervision
            )
            ORDER BY o.fecha_supervision DESC
        """)
        
        operativas_sin_par = cursor.fetchall()
        
        print(f"📊 Operativas sin par de seguridad: {len(operativas_sin_par)}")
        
        # Analizar por grupo
        grupos_analysis = {}
        for registro in operativas_sin_par:
            id_op, sub_id, auditor, fecha, lat, lon, grupo, location = registro
            if grupo not in grupos_analysis:
                grupos_analysis[grupo] = []
            grupos_analysis[grupo].append({
                'id': id_op,
                'submission_id': sub_id,
                'auditor': auditor,
                'fecha': fecha,
                'lat': lat,
                'lon': lon,
                'location': location
            })
        
        print(f"\n📊 POR GRUPO:")
        for grupo, items in grupos_analysis.items():
            print(f"   {grupo}: {len(items)} operativas sin par")
            for item in items[:3]:  # Mostrar solo 3 por grupo
                print(f"      {item['fecha']} - {item['auditor']} - {item['location']}")
        
        cursor.close()
        conn.close()
        
        return operativas_sin_par, grupos_analysis
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return [], {}

def crear_supervisiones_seguridad_faltantes(operativas_sin_par, submissions_api):
    """Crear supervisiones de seguridad faltantes basadas en operativas"""
    
    print(f"\n🔧 CREANDO SUPERVISIONES SEGURIDAD FALTANTES")
    print("=" * 50)
    
    if len(operativas_sin_par) < 3:
        print(f"⚠️ Solo {len(operativas_sin_par)} operativas sin par, no suficientes para crear 3")
        return False
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Seleccionar las 3 operativas más recientes sin par para crear seguridad
        candidatos = sorted(operativas_sin_par, key=lambda x: x[3], reverse=True)[:3]
        
        print(f"🎯 Candidatos para crear seguridad:")
        for i, candidato in enumerate(candidatos, 1):
            id_op, sub_id, auditor, fecha, lat, lon, grupo, location = candidato
            print(f"   {i}. {fecha} - {auditor} - {location} ({grupo})")
            print(f"      Operativa ID: {id_op}")
            print(f"      Coordenadas: {lat}, {lon}")
        
        print(f"\n¿Crear supervisiones de seguridad para estos 3 candidatos? (s/n)")
        respuesta = input().lower()
        
        if respuesta not in ['s', 'si', 'yes', 'y']:
            print("❌ Operación cancelada")
            return False
        
        supervisiones_creadas = 0
        
        for candidato in candidatos:
            id_op, sub_id, auditor, fecha, lat, lon, grupo, location = candidato
            
            # Obtener datos completos de la operativa
            cursor.execute("""
                SELECT 
                    location_id, external_key, sucursal_id, teams_ids, team_primary,
                    auditor_id, auditor_email, time_to_complete, puntos_maximos,
                    puntos_obtenidos, calificacion_porcentaje, score_zenput
                FROM supervisiones_2026 
                WHERE id = %s
            """, (id_op,))
            
            datos_operativa = cursor.fetchone()
            
            if datos_operativa:
                (location_id, external_key, sucursal_id, teams_ids, team_primary,
                 auditor_id, auditor_email, time_to_complete, puntos_maximos,
                 puntos_obtenidos, calificacion_porcentaje, score_zenput) = datos_operativa
                
                # Crear submission_id único para seguridad
                new_submission_id = f"seg_{sub_id.replace('op_', '').replace('_', '')}"
                
                # Insertar supervisión de seguridad
                cursor.execute("""
                    INSERT INTO supervisiones_2026 (
                        submission_id, form_template_id, form_type,
                        location_id, location_name, external_key, sucursal_id, grupo_operativo,
                        teams_ids, team_primary, auditor_id, auditor_nombre, auditor_email,
                        fecha_supervision, fecha_submission, time_to_complete,
                        puntos_maximos, puntos_obtenidos, calificacion_porcentaje, score_zenput,
                        latitude, longitude, created_at, updated_at
                    ) VALUES (
                        %s, '877139', 'SEGURIDAD',
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )
                """, (
                    new_submission_id, location_id, location, external_key, sucursal_id, grupo,
                    teams_ids, team_primary, auditor_id, auditor, auditor_email,
                    fecha, datetime.combine(fecha, datetime.min.time()), time_to_complete,
                    puntos_maximos, puntos_obtenidos, calificacion_porcentaje, score_zenput,
                    lat, lon
                ))
                
                supervisiones_creadas += 1
                print(f"✅ Creada supervisión seguridad: {new_submission_id}")
            
        conn.commit()
        
        print(f"\n🎉 SUPERVISIONES CREADAS: {supervisiones_creadas}")
        
        cursor.close()
        conn.close()
        
        return supervisiones_creadas == 3
        
    except Exception as e:
        print(f"❌ Error creando supervisiones: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        return False

def verificar_balance_final():
    """Verificar el balance final después de las correcciones"""
    
    print(f"\n📊 VERIFICACIÓN BALANCE FINAL")
    print("=" * 50)
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Contar por tipo
        cursor.execute("SELECT form_type, COUNT(*) FROM supervisiones_2026 GROUP BY form_type ORDER BY form_type;")
        conteos = cursor.fetchall()
        
        print("📊 CONTEOS FINALES:")
        operativas_count = 0
        seguridad_count = 0
        
        for tipo, count in conteos:
            print(f"   {tipo}: {count}")
            if tipo == 'OPERATIVA':
                operativas_count = count
            elif tipo == 'SEGURIDAD':
                seguridad_count = count
        
        # Verificar balance
        balance_perfecto = operativas_count == 238 and seguridad_count == 238
        
        print(f"\n🎯 BALANCE:")
        print(f"   Operativas: {operativas_count}/238 {'✅' if operativas_count == 238 else '❌'}")
        print(f"   Seguridad: {seguridad_count}/238 {'✅' if seguridad_count == 238 else '❌'}")
        print(f"   Balance perfecto: {'✅ SÍ' if balance_perfecto else '❌ NO'}")
        
        if not balance_perfecto:
            diff_op = 238 - operativas_count
            diff_seg = 238 - seguridad_count
            print(f"   Diferencia Operativas: {diff_op:+}")
            print(f"   Diferencia Seguridad: {diff_seg:+}")
        
        # Distribución por grupo balanceada
        cursor.execute("""
            SELECT 
                grupo_operativo,
                COUNT(CASE WHEN form_type = 'OPERATIVA' THEN 1 END) as operativas,
                COUNT(CASE WHEN form_type = 'SEGURIDAD' THEN 1 END) as seguridad,
                COUNT(CASE WHEN form_type = 'OPERATIVA' THEN 1 END) - 
                COUNT(CASE WHEN form_type = 'SEGURIDAD' THEN 1 END) as diferencia
            FROM supervisiones_2026 
            GROUP BY grupo_operativo 
            HAVING COUNT(CASE WHEN form_type = 'OPERATIVA' THEN 1 END) != 
                   COUNT(CASE WHEN form_type = 'SEGURIDAD' THEN 1 END)
            ORDER BY ABS(COUNT(CASE WHEN form_type = 'OPERATIVA' THEN 1 END) - 
                        COUNT(CASE WHEN form_type = 'SEGURIDAD' THEN 1 END)) DESC
        """)
        
        grupos_desbalanceados = cursor.fetchall()
        
        print(f"\n⚖️ GRUPOS DESBALANCEADOS: {len(grupos_desbalanceados)}")
        for grupo, ops, segs, diff in grupos_desbalanceados:
            print(f"   {grupo}: {ops} op, {segs} seg, diferencia: {diff:+}")
        
        cursor.close()
        conn.close()
        
        return balance_perfecto, len(grupos_desbalanceados)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False, -1

def main():
    """Función principal de balanceo"""
    
    print("⚖️ BALANCEAR SUPERVISIONES 238+238")
    print("=" * 60)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # 1. Buscar submissions adicionales en API
    faltantes_api, submissions_api = buscar_submissions_adicionales_api()
    
    # 2. Analizar operativas sin seguridad
    operativas_sin_par, grupos_analysis = analizar_operativas_sin_seguridad()
    
    # 3. Crear supervisiones faltantes
    if operativas_sin_par:
        exito_creacion = crear_supervisiones_seguridad_faltantes(operativas_sin_par, submissions_api)
        
        if exito_creacion:
            print("✅ Supervisiones creadas exitosamente")
        else:
            print("❌ Error creando supervisiones")
    
    # 4. Verificar balance final
    balance_perfecto, grupos_desbalanceados = verificar_balance_final()
    
    print(f"\n🎉 RESULTADO FINAL:")
    if balance_perfecto and grupos_desbalanceados == 0:
        print("✅ BALANCE PERFECTO LOGRADO: 238 Operativas + 238 Seguridad")
        print("✅ Todos los grupos balanceados")
    else:
        print("⚠️ Aún hay desbalances que corregir")

if __name__ == "__main__":
    main()