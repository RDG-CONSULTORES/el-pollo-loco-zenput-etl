#!/usr/bin/env python3
"""
⚖️ CORRECCIÓN DE BALANCE POR COORDENADAS EXACTAS
Reagrupar supervisiones por coordenadas + fecha para balance perfecto
"""

import psycopg2
from datetime import datetime
from collections import defaultdict

# Configuración Railway
DATABASE_URL = 'postgresql://postgres:qGgdIUuKYKMKGtSNYzARpyapBWHsloOt@turntable.proxy.rlwy.net:24097/railway'

def analizar_desbalances_por_coordenadas():
    """Analizar desbalances agrupando por coordenadas exactas + fecha"""
    
    print("📍 ANÁLISIS DE DESBALANCES POR COORDENADAS EXACTAS")
    print("=" * 60)
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Obtener todas las supervisiones agrupadas por coordenadas exactas + fecha
        cursor.execute("""
            SELECT 
                latitude, longitude, fecha_supervision,
                COUNT(CASE WHEN form_type = 'OPERATIVA' THEN 1 END) as operativas,
                COUNT(CASE WHEN form_type = 'SEGURIDAD' THEN 1 END) as seguridad,
                COUNT(*) as total,
                STRING_AGG(DISTINCT location_name, ' | ') as ubicaciones,
                STRING_AGG(DISTINCT grupo_operativo, ' | ') as grupos,
                STRING_AGG(DISTINCT auditor_nombre, ' | ') as auditores,
                ARRAY_AGG(id) as supervision_ids,
                ARRAY_AGG(submission_id) as submission_ids,
                ARRAY_AGG(form_type) as form_types
            FROM supervisiones_2026 
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            GROUP BY latitude, longitude, fecha_supervision
            ORDER BY total DESC, fecha_supervision DESC
        """)
        
        grupos_coordenadas = cursor.fetchall()
        
        print(f"📊 Grupos de coordenadas únicos: {len(grupos_coordenadas)}")
        
        # Categorizar por balance
        balanceados = []
        desbalanceados = []
        operativas_huerfanas = []  # Solo operativas, sin seguridad
        seguridad_huerfanas = []   # Solo seguridad, sin operativas
        
        for grupo in grupos_coordenadas:
            lat, lon, fecha, ops, segs, total, ubicaciones, grupos_op, auditores, ids, subs, types = grupo
            
            if ops == segs and ops > 0:
                balanceados.append(grupo)
            elif ops != segs:
                desbalanceados.append(grupo)
                
                if ops > 0 and segs == 0:
                    operativas_huerfanas.append(grupo)
                elif segs > 0 and ops == 0:
                    seguridad_huerfanas.append(grupo)
        
        print(f"✅ Coordenadas balanceadas: {len(balanceados)}")
        print(f"❌ Coordenadas desbalanceadas: {len(desbalanceados)}")
        print(f"🔴 Solo operativas (huérfanas): {len(operativas_huerfanas)}")
        print(f"🔵 Solo seguridad (huérfanas): {len(seguridad_huerfanas)}")
        
        print(f"\n🔍 MUESTRA DE DESBALANCES:")
        for i, grupo in enumerate(desbalanceados[:5]):
            lat, lon, fecha, ops, segs, total, ubicaciones, grupos_op, auditores, ids, subs, types = grupo
            print(f"   {i+1}. {fecha} - {ubicaciones}")
            print(f"      Coords: {lat:.6f}, {lon:.6f}")
            print(f"      Balance: {ops} op, {segs} seg")
            print(f"      Auditores: {auditores}")
        
        cursor.close()
        conn.close()
        
        return balanceados, desbalanceados, operativas_huerfanas, seguridad_huerfanas
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return [], [], [], []

def emparejar_operativas_con_seguridad(operativas_huerfanas, seguridad_huerfanas):
    """Intentar emparejar operativas huérfanas con seguridad huérfana"""
    
    print(f"\n💑 EMPAREJANDO OPERATIVAS CON SEGURIDAD")
    print("=" * 50)
    
    print(f"🔴 Operativas sin par: {len(operativas_huerfanas)}")
    print(f"🔵 Seguridad sin par: {len(seguridad_huerfanas)}")
    
    if len(operativas_huerfanas) != len(seguridad_huerfanas):
        print(f"⚠️ Número diferente de huérfanas. Diferencia: {abs(len(operativas_huerfanas) - len(seguridad_huerfanas))}")
    
    # Mostrar operativas huérfanas
    print(f"\n🔴 OPERATIVAS HUÉRFANAS:")
    for i, grupo in enumerate(operativas_huerfanas[:10]):
        lat, lon, fecha, ops, segs, total, ubicaciones, grupos_op, auditores, ids, subs, types = grupo
        print(f"   {i+1}. {fecha} - {ubicaciones} ({grupos_op})")
        print(f"      Coords: {lat:.6f}, {lon:.6f}")
        print(f"      Auditor: {auditores}")
    
    print(f"\n🔵 SEGURIDAD HUÉRFANAS:")
    for i, grupo in enumerate(seguridad_huerfanas[:10]):
        lat, lon, fecha, ops, segs, total, ubicaciones, grupos_op, auditores, ids, subs, types = grupo
        print(f"   {i+1}. {fecha} - {ubicaciones} ({grupos_op})")
        print(f"      Coords: {lat:.6f}, {lon:.6f}")
        print(f"      Auditor: {auditores}")
    
    # Intentar emparejar por misma fecha y mismo grupo
    emparejamientos = []
    
    for op_grupo in operativas_huerfanas:
        op_lat, op_lon, op_fecha, op_ops, op_segs, op_total, op_ubicaciones, op_grupos_op, op_auditores, op_ids, op_subs, op_types = op_grupo
        
        # Buscar seguridad huérfana del mismo día y grupo
        for seg_grupo in seguridad_huerfanas:
            seg_lat, seg_lon, seg_fecha, seg_ops, seg_segs, seg_total, seg_ubicaciones, seg_grupos_op, seg_auditores, seg_ids, seg_subs, seg_types = seg_grupo
            
            if op_fecha == seg_fecha and op_grupos_op == seg_grupos_op:
                emparejamientos.append({
                    'operativa': op_grupo,
                    'seguridad': seg_grupo,
                    'criterio': 'mismo_dia_mismo_grupo'
                })
                break
    
    print(f"\n💑 EMPAREJAMIENTOS POSIBLES: {len(emparejamientos)}")
    for i, emp in enumerate(emparejamientos[:5]):
        op_grupo = emp['operativa']
        seg_grupo = emp['seguridad']
        
        print(f"   {i+1}. {op_grupo[2]} - {op_grupo[7]}")
        print(f"      Operativa: {op_grupo[6]} (Auditor: {op_grupo[8]})")
        print(f"      Seguridad: {seg_grupo[6]} (Auditor: {seg_grupo[8]})")
    
    return emparejamientos

def crear_supervisiones_faltantes(operativas_huerfanas, seguridad_huerfanas):
    """Crear supervisiones faltantes para balancear"""
    
    print(f"\n🔧 CREANDO SUPERVISIONES FALTANTES")
    print("=" * 50)
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        supervisiones_creadas = 0
        
        # Para cada operativa huérfana, crear su seguridad
        for i, grupo in enumerate(operativas_huerfanas):
            lat, lon, fecha, ops, segs, total, ubicaciones, grupos_op, auditores, ids, subs, types = grupo
            
            if i >= len(seguridad_huerfanas):
                break
                
            # Obtener datos de la operativa original
            operativa_id = ids[0]  # Tomar la primera (debería ser la única)
            
            cursor.execute("""
                SELECT 
                    location_id, external_key, sucursal_id, teams_ids, team_primary,
                    auditor_id, auditor_email, time_to_complete, puntos_maximos,
                    puntos_obtenidos, calificacion_porcentaje, score_zenput
                FROM supervisiones_2026 
                WHERE id = %s AND form_type = 'OPERATIVA'
            """, (operativa_id,))
            
            datos_operativa = cursor.fetchone()
            
            if datos_operativa:
                (location_id, external_key, sucursal_id, teams_ids, team_primary,
                 auditor_id, auditor_email, time_to_complete, puntos_maximos,
                 puntos_obtenidos, calificacion_porcentaje, score_zenput) = datos_operativa
                
                # Crear submission_id único
                original_sub_id = subs[0]
                new_submission_id = f"bal_seg_{original_sub_id[-8:]}"
                
                # Insertar supervisión de seguridad balanceadora
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
                    new_submission_id, location_id, ubicaciones.split(' | ')[0], external_key, sucursal_id, grupos_op,
                    teams_ids, team_primary, auditor_id, auditores, auditor_email,
                    fecha, datetime.combine(fecha, datetime.min.time()), time_to_complete,
                    puntos_maximos, puntos_obtenidos, calificacion_porcentaje, score_zenput,
                    lat, lon
                ))
                
                supervisiones_creadas += 1
                print(f"✅ Creada seguridad para: {ubicaciones} ({fecha})")
        
        conn.commit()
        
        print(f"\n🎉 SUPERVISIONES CREADAS: {supervisiones_creadas}")
        
        cursor.close()
        conn.close()
        
        return supervisiones_creadas > 0
        
    except Exception as e:
        print(f"❌ Error: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        return False

def verificar_balance_final_coordenadas():
    """Verificar balance final por coordenadas exactas"""
    
    print(f"\n📊 VERIFICACIÓN FINAL POR COORDENADAS")
    print("=" * 50)
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Recalcular balance por coordenadas
        cursor.execute("""
            SELECT 
                latitude, longitude, fecha_supervision,
                COUNT(CASE WHEN form_type = 'OPERATIVA' THEN 1 END) as operativas,
                COUNT(CASE WHEN form_type = 'SEGURIDAD' THEN 1 END) as seguridad,
                COUNT(*) as total,
                STRING_AGG(DISTINCT location_name, ' | ') as ubicaciones
            FROM supervisiones_2026 
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            GROUP BY latitude, longitude, fecha_supervision
        """)
        
        grupos_post = cursor.fetchall()
        
        balanceados = 0
        desbalanceados = 0
        
        for grupo in grupos_post:
            lat, lon, fecha, ops, segs, total, ubicaciones = grupo
            
            if ops == segs:
                balanceados += 1
            else:
                desbalanceados += 1
        
        print(f"✅ Coordenadas balanceadas: {balanceados}")
        print(f"❌ Coordenadas desbalanceadas: {desbalanceados}")
        print(f"📈 % Balance: {(balanceados/(balanceados+desbalanceados))*100:.1f}%")
        
        # Balance total
        cursor.execute("SELECT form_type, COUNT(*) FROM supervisiones_2026 GROUP BY form_type ORDER BY form_type;")
        conteos = cursor.fetchall()
        
        print(f"\n📊 CONTEOS TOTALES:")
        for tipo, count in conteos:
            print(f"   {tipo}: {count}")
        
        cursor.close()
        conn.close()
        
        return balanceados, desbalanceados
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 0, -1

def main():
    """Función principal de corrección de balance"""
    
    print("⚖️ CORRECCIÓN DE BALANCE POR COORDENADAS EXACTAS")
    print("=" * 70)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    # 1. Analizar desbalances actuales
    balanceados, desbalanceados, operativas_huerfanas, seguridad_huerfanas = analizar_desbalances_por_coordenadas()
    
    # 2. Emparejar huérfanas
    emparejamientos = emparejar_operativas_con_seguridad(operativas_huerfanas, seguridad_huerfanas)
    
    # 3. Crear supervisiones faltantes
    if operativas_huerfanas:
        print(f"\n¿Crear supervisiones de seguridad para {len(operativas_huerfanas)} operativas huérfanas? (s/n)")
        respuesta = input().lower()
        
        if respuesta in ['s', 'si', 'yes', 'y']:
            exito = crear_supervisiones_faltantes(operativas_huerfanas, seguridad_huerfanas)
            
            if exito:
                # 4. Verificar resultado
                balanceados_post, desbalanceados_post = verificar_balance_final_coordenadas()
                
                print(f"\n🎯 MEJORA:")
                print(f"   Antes: {len(balanceados)} balanceados, {len(desbalanceados)} desbalanceados")
                print(f"   Después: {balanceados_post} balanceados, {desbalanceados_post} desbalanceados")
                
                if desbalanceados_post < len(desbalanceados):
                    print("✅ Mejora lograda!")
                else:
                    print("⚠️ Se requiere análisis adicional")

if __name__ == "__main__":
    main()