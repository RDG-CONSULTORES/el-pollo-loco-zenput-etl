#!/usr/bin/env python3
"""
🛡️ MIGRAR SEGURIDAD EN LOTES
Migrar supervisiones de seguridad en lotes pequeños
"""

import pandas as pd
import psycopg2
import json
import uuid
import time

def migrate_seguridad_batch():
    """Migrar seguridad en lotes de 50"""
    
    print("🛡️ MIGRANDO SEGURIDAD EN LOTES")
    print("=" * 40)
    
    database_url = "postgresql://postgres:tWeSxUREoYODoFroTAurHwcisymBotbz@yamanote.proxy.rlwy.net:29534/railway"
    
    try:
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        # Cargar seguridad
        df_seguridad = pd.read_excel("SEGURIDAD_POSTGRESQL_20251223_113008.xlsx", 
                                   sheet_name='Seguridad_PostgreSQL')
        
        print(f"📊 Total seguridad en Excel: {len(df_seguridad)}")
        
        # Obtener IDs ya existentes
        cursor.execute("SELECT submission_id FROM supervisiones WHERE tipo_supervision = 'seguridad'")
        existentes = {row[0] for row in cursor.fetchall()}
        print(f"📊 Ya existentes: {len(existentes)}")
        
        # Procesar en lotes de 30
        batch_size = 30
        migrados = 0
        errores = 0
        
        for start_idx in range(0, len(df_seguridad), batch_size):
            end_idx = min(start_idx + batch_size, len(df_seguridad))
            batch = df_seguridad.iloc[start_idx:end_idx]
            
            print(f"\n📦 Procesando lote {start_idx//batch_size + 1}: {start_idx}-{end_idx}")
            
            batch_migrados = 0
            for i, row in batch.iterrows():
                try:
                    # Verificar si existe
                    if str(row['ID_SUPERVISION']) in existentes:
                        continue
                    
                    # Buscar sucursal
                    cursor.execute("SELECT id FROM sucursales WHERE nombre = %s", (row['SUCURSAL'],))
                    sucursal_result = cursor.fetchone()
                    if not sucursal_result:
                        errores += 1
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
                    
                    batch_migrados += 1
                    migrados += 1
                    
                except Exception as e:
                    errores += 1
                    continue
            
            # Commit lote
            conn.commit()
            print(f"   ✅ Migrados en lote: {batch_migrados}")
            print(f"   📊 Total migrados: {migrados}")
            
            # Pausa breve
            time.sleep(1)
        
        # Verificar final
        cursor.execute("SELECT COUNT(*) FROM supervisiones WHERE tipo_supervision = 'seguridad'")
        total_final = cursor.fetchone()[0]
        
        print(f"\n🎉 MIGRACIÓN SEGURIDAD COMPLETADA")
        print(f"   ✅ Migrados: {migrados}")
        print(f"   ❌ Errores: {errores}")
        print(f"   📊 Total en DB: {total_final}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False

if __name__ == "__main__":
    migrate_seguridad_batch()