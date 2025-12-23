#!/usr/bin/env python3
"""
🚀 MIGRAR SOLO SUPERVISIONES - RAILWAY
Migrar únicamente las supervisiones operativas y seguridad
Roberto: Fix específico para supervisiones
"""

import pandas as pd
import psycopg2
import json
import uuid
from datetime import datetime

def migrate_supervisiones_only():
    """Migrar solo supervisiones a Railway"""
    
    print("🚀 MIGRAR SUPERVISIONES ÚNICAMENTE - RAILWAY")
    print("=" * 60)
    
    # Railway Database URL
    database_url = "postgresql://postgres:tWeSxUREoYODoFroTAurHwcisymBotbz@yamanote.proxy.rlwy.net:29534/railway"
    
    try:
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        # PASO 1: Migrar Operativas
        print("\n🔧 MIGRANDO SUPERVISIONES OPERATIVAS...")
        
        df_operativas = pd.read_excel("OPERATIVAS_POSTGRESQL_20251223_113008.xlsx", 
                                    sheet_name='Operativas_PostgreSQL')
        
        print(f"📊 Total operativas en Excel: {len(df_operativas)}")
        
        operativas_count = 0
        for i, row in df_operativas.iterrows():
            try:
                # Buscar sucursal_id
                cursor.execute("SELECT id FROM sucursales WHERE nombre = %s", (row['SUCURSAL'],))
                sucursal_result = cursor.fetchone()
                if not sucursal_result:
                    print(f"   ⚠️ Sucursal no encontrada: {row['SUCURSAL']}")
                    continue
                    
                sucursal_id = sucursal_result[0]
                
                # Verificar si ya existe
                cursor.execute("SELECT id FROM supervisiones WHERE submission_id = %s", (row['ID_SUPERVISION'],))
                if cursor.fetchone():
                    continue  # Ya existe
                
                # Extraer áreas
                areas_dict = {}
                ignore_prefixes = ['ID_', 'SUCURSAL', 'FECHA', 'CALIFICACION_GENERAL', 'USUARIO', 
                                 'sucursal_', 'latitud', 'longitud', 'grupo_', 'tipo_', 'estado', 'pais', 'region', 'zona_']
                
                for col in row.index:
                    if any(str(col).startswith(prefix) for prefix in ignore_prefixes):
                        continue
                    if pd.notna(row[col]):
                        try:
                            # Convertir a número si es posible
                            val = str(row[col]).strip()
                            if val.replace('.', '').replace(',', '').isdigit():
                                areas_dict[col] = float(val)
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
                    json.dumps(areas_dict, default=str),
                    str(row.get('USUARIO', 'Sistema'))
                ))
                
                # Insertar áreas
                for area_nombre, calificacion in areas_dict.items():
                    cursor.execute("""
                        INSERT INTO areas_calificaciones 
                        (supervision_id, area_nombre, calificacion)
                        VALUES (%s, %s, %s)
                    """, (supervision_id, area_nombre, float(calificacion)))
                
                operativas_count += 1
                
                if operativas_count % 50 == 0:
                    print(f"   📊 Progreso: {operativas_count} operativas migradas")
                    conn.commit()
                
            except Exception as e:
                print(f"   ❌ Error operativa {i}: {str(e)}")
        
        conn.commit()
        print(f"   ✅ {operativas_count} supervisiones operativas migradas")
        
        # PASO 2: Migrar Seguridad
        print("\n🛡️ MIGRANDO SUPERVISIONES SEGURIDAD...")
        
        df_seguridad = pd.read_excel("SEGURIDAD_POSTGRESQL_20251223_113008.xlsx", 
                                   sheet_name='Seguridad_PostgreSQL')
        
        print(f"📊 Total seguridad en Excel: {len(df_seguridad)}")
        
        seguridad_count = 0
        for i, row in df_seguridad.iterrows():
            try:
                # Buscar sucursal_id
                cursor.execute("SELECT id FROM sucursales WHERE nombre = %s", (row['SUCURSAL'],))
                sucursal_result = cursor.fetchone()
                if not sucursal_result:
                    print(f"   ⚠️ Sucursal no encontrada: {row['SUCURSAL']}")
                    continue
                    
                sucursal_id = sucursal_result[0]
                
                # Verificar si ya existe
                cursor.execute("SELECT id FROM supervisiones WHERE submission_id = %s", (row['ID_SUPERVISION'],))
                if cursor.fetchone():
                    continue  # Ya existe
                
                # Extraer áreas
                areas_dict = {}
                ignore_prefixes = ['ID_', 'SUCURSAL', 'FECHA', 'CALIFICACION_GENERAL', 'USUARIO', 
                                 'sucursal_', 'latitud', 'longitud', 'grupo_', 'tipo_', 'estado', 'pais', 'region', 'zona_']
                
                for col in row.index:
                    if any(str(col).startswith(prefix) for prefix in ignore_prefixes):
                        continue
                    if pd.notna(row[col]):
                        try:
                            # Convertir a número si es posible
                            val = str(row[col]).strip()
                            if val.replace('.', '').replace(',', '').isdigit():
                                areas_dict[col] = float(val)
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
                    json.dumps(areas_dict, default=str),
                    str(row.get('USUARIO', 'Sistema'))
                ))
                
                # Insertar áreas
                for area_nombre, calificacion in areas_dict.items():
                    cursor.execute("""
                        INSERT INTO areas_calificaciones 
                        (supervision_id, area_nombre, calificacion)
                        VALUES (%s, %s, %s)
                    """, (supervision_id, area_nombre, float(calificacion)))
                
                seguridad_count += 1
                
                if seguridad_count % 50 == 0:
                    print(f"   📊 Progreso: {seguridad_count} seguridad migradas")
                    conn.commit()
                
            except Exception as e:
                print(f"   ❌ Error seguridad {i}: {str(e)}")
        
        conn.commit()
        print(f"   ✅ {seguridad_count} supervisiones seguridad migradas")
        
        # PASO 3: Verificar migración
        print("\n✅ VERIFICANDO MIGRACIÓN FINAL...")
        
        queries = [
            ("Operativas", "SELECT COUNT(*) FROM supervisiones WHERE tipo_supervision = 'operativas'"),
            ("Seguridad", "SELECT COUNT(*) FROM supervisiones WHERE tipo_supervision = 'seguridad'"),
            ("Áreas Total", "SELECT COUNT(*) FROM areas_calificaciones"),
            ("Promedio Operativas", "SELECT ROUND(AVG(calificacion_general), 1) FROM supervisiones WHERE tipo_supervision = 'operativas'"),
            ("Promedio Seguridad", "SELECT ROUND(AVG(calificacion_general), 1) FROM supervisiones WHERE tipo_supervision = 'seguridad'")
        ]
        
        for name, query in queries:
            cursor.execute(query)
            result = cursor.fetchone()
            print(f"   📊 {name}: {result[0] if result else 'N/A'}")
        
        cursor.close()
        conn.close()
        
        print(f"\n🎉 MIGRACIÓN SUPERVISIONES COMPLETADA")
        print("=" * 50)
        print(f"✅ {operativas_count} supervisiones operativas")
        print(f"✅ {seguridad_count} supervisiones seguridad")
        print(f"✅ Áreas evaluadas migradas")
        print("🌐 Dashboard ahora debería funcionar correctamente")
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR CRÍTICO: {str(e)}")
        return False

if __name__ == "__main__":
    migrate_supervisiones_only()