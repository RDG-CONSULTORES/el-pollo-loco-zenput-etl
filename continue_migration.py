#!/usr/bin/env python3
"""
🚀 CONTINUAR MIGRACIÓN RAILWAY
Completar la migración de datos restantes
Roberto: Finalizar migración para dashboard completo
"""

import pandas as pd
import psycopg2
import json
import uuid
import time

def continue_migration():
    """Continuar migración de datos faltantes"""
    
    print("🚀 CONTINUANDO MIGRACIÓN RAILWAY")
    print("=" * 50)
    
    database_url = "postgresql://postgres:tWeSxUREoYODoFroTAurHwcisymBotbz@yamanote.proxy.rlwy.net:29534/railway"
    
    try:
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        # PASO 1: Estado actual
        cursor.execute("SELECT COUNT(*) FROM supervisiones WHERE tipo_supervision = 'operativas'")
        operativas_actuales = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM supervisiones WHERE tipo_supervision = 'seguridad'")
        seguridad_actuales = cursor.fetchone()[0]
        
        print(f"📊 ESTADO ACTUAL:")
        print(f"   🔧 Operativas: {operativas_actuales}/238")
        print(f"   🛡️ Seguridad: {seguridad_actuales}/238")
        print(f"   📊 Total: {operativas_actuales + seguridad_actuales}/476")
        
        # PASO 2: Obtener IDs existentes
        cursor.execute("SELECT submission_id FROM supervisiones")
        existentes = {row[0] for row in cursor.fetchall()}
        print(f"   📝 IDs existentes: {len(existentes)}")
        
        # PASO 3: Cargar exceles
        df_operativas = pd.read_excel("OPERATIVAS_POSTGRESQL_20251223_113008.xlsx", 
                                    sheet_name='Operativas_PostgreSQL')
        df_seguridad = pd.read_excel("SEGURIDAD_POSTGRESQL_20251223_113008.xlsx", 
                                   sheet_name='Seguridad_PostgreSQL')
        
        # PASO 4: Migrar operativas faltantes
        operativas_faltantes = []
        for _, row in df_operativas.iterrows():
            if str(row['ID_SUPERVISION']) not in existentes:
                operativas_faltantes.append(row)
        
        print(f"\n🔧 MIGRANDO {len(operativas_faltantes)} OPERATIVAS FALTANTES...")
        
        migrados_op = 0
        for i, row in enumerate(operativas_faltantes):
            try:
                # Verificar sucursal
                cursor.execute("SELECT id FROM sucursales WHERE nombre = %s", (row['SUCURSAL'],))
                sucursal_result = cursor.fetchone()
                if not sucursal_result:
                    continue
                
                sucursal_id = sucursal_result[0]
                
                # Preparar áreas
                areas_dict = {}
                ignore_cols = ['ID_SUPERVISION', 'SUCURSAL', 'FECHA', 'CALIFICACION_GENERAL', 'USUARIO']
                
                for col in row.index:
                    if col in ignore_cols or str(col).startswith(('sucursal_', 'grupo_', 'tipo_')):
                        continue
                    if pd.notna(row[col]):
                        try:
                            val = float(row[col])
                            if 0 <= val <= 100:
                                areas_dict[str(col)] = val
                        except:
                            continue
                
                # Insertar supervisión
                supervision_id = str(uuid.uuid4())
                
                cursor.execute("""
                    INSERT INTO supervisiones 
                    (id, submission_id, sucursal_id, tipo_supervision, fecha_supervision, 
                     calificacion_general, areas_evaluadas, usuario)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    supervision_id,
                    str(row['ID_SUPERVISION']),
                    sucursal_id,
                    'operativas',
                    pd.to_datetime(row['FECHA']),
                    float(row['CALIFICACION_GENERAL']),
                    json.dumps(areas_dict),
                    str(row.get('USUARIO', 'Sistema'))
                ))
                
                # Insertar áreas
                for area, calif in areas_dict.items():
                    cursor.execute("""
                        INSERT INTO areas_calificaciones 
                        (supervision_id, area_nombre, calificacion)
                        VALUES (%s, %s, %s)
                    """, (supervision_id, area, calif))
                
                migrados_op += 1
                
                # Commit cada 10
                if migrados_op % 10 == 0:
                    conn.commit()
                    print(f"   📊 Operativas: {migrados_op}/{len(operativas_faltantes)}")
                
            except Exception as e:
                continue
        
        conn.commit()
        print(f"   ✅ {migrados_op} operativas migradas")
        
        # PASO 5: Migrar seguridad faltantes
        seguridad_faltantes = []
        for _, row in df_seguridad.iterrows():
            if str(row['ID_SUPERVISION']) not in existentes:
                seguridad_faltantes.append(row)
        
        print(f"\n🛡️ MIGRANDO {len(seguridad_faltantes)} SEGURIDAD FALTANTES...")
        
        migrados_seg = 0
        for i, row in enumerate(seguridad_faltantes):
            try:
                # Verificar sucursal
                cursor.execute("SELECT id FROM sucursales WHERE nombre = %s", (row['SUCURSAL'],))
                sucursal_result = cursor.fetchone()
                if not sucursal_result:
                    continue
                
                sucursal_id = sucursal_result[0]
                
                # Preparar áreas
                areas_dict = {}
                ignore_cols = ['ID_SUPERVISION', 'SUCURSAL', 'FECHA', 'CALIFICACION_GENERAL', 'USUARIO']
                
                for col in row.index:
                    if col in ignore_cols or str(col).startswith(('sucursal_', 'grupo_', 'tipo_')):
                        continue
                    if pd.notna(row[col]):
                        try:
                            val = float(row[col])
                            if 0 <= val <= 100:
                                areas_dict[str(col)] = val
                        except:
                            continue
                
                # Insertar supervisión
                supervision_id = str(uuid.uuid4())
                
                cursor.execute("""
                    INSERT INTO supervisiones 
                    (id, submission_id, sucursal_id, tipo_supervision, fecha_supervision, 
                     calificacion_general, areas_evaluadas, usuario)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    supervision_id,
                    str(row['ID_SUPERVISION']),
                    sucursal_id,
                    'seguridad',
                    pd.to_datetime(row['FECHA']),
                    float(row['CALIFICACION_GENERAL']),
                    json.dumps(areas_dict),
                    str(row.get('USUARIO', 'Sistema'))
                ))
                
                # Insertar áreas
                for area, calif in areas_dict.items():
                    cursor.execute("""
                        INSERT INTO areas_calificaciones 
                        (supervision_id, area_nombre, calificacion)
                        VALUES (%s, %s, %s)
                    """, (supervision_id, area, calif))
                
                migrados_seg += 1
                
                # Commit cada 10
                if migrados_seg % 10 == 0:
                    conn.commit()
                    print(f"   📊 Seguridad: {migrados_seg}/{len(seguridad_faltantes)}")
                    
                # Pausa cada 20 para evitar timeout
                if migrados_seg % 20 == 0:
                    time.sleep(2)
                
            except Exception as e:
                continue
        
        conn.commit()
        print(f"   ✅ {migrados_seg} seguridad migradas")
        
        # PASO 6: Verificación final
        print(f"\n📊 VERIFICACIÓN FINAL...")
        
        cursor.execute("SELECT COUNT(*) FROM supervisiones WHERE tipo_supervision = 'operativas'")
        total_op = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM supervisiones WHERE tipo_supervision = 'seguridad'")
        total_seg = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM areas_calificaciones")
        total_areas = cursor.fetchone()[0]
        
        print(f"   🔧 Operativas finales: {total_op}/238")
        print(f"   🛡️ Seguridad finales: {total_seg}/238")
        print(f"   📊 Total supervisiones: {total_op + total_seg}/476")
        print(f"   📋 Total áreas: {total_areas}")
        
        progreso = ((total_op + total_seg) / 476) * 100
        
        if progreso >= 95:
            print(f"\n🎉 MIGRACIÓN CASI COMPLETA ({progreso:.1f}%)")
        elif progreso >= 80:
            print(f"\n✅ MIGRACIÓN AVANZADA ({progreso:.1f}%)")
        else:
            print(f"\n🔄 MIGRACIÓN EN PROGRESO ({progreso:.1f}%)")
        
        cursor.close()
        conn.close()
        
        print(f"\n🌐 DASHBOARD ACTUALIZADO:")
        print(f"   🔗 https://el-pollo-loco-zenput-etl-production.up.railway.app")
        print(f"   🎨 Nuevo diseño iOS: index-ios-exact.html")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False

if __name__ == "__main__":
    continue_migration()