#!/usr/bin/env python3
"""
🔍 VERIFICAR DATOS ESPECÍFICOS FALTANTES
Identificar exactamente qué supervisiones faltan
"""

import pandas as pd
import psycopg2

def check_missing_data():
    """Verificar datos específicos faltantes"""
    
    print("🔍 VERIFICANDO DATOS ESPECÍFICOS FALTANTES")
    print("=" * 50)
    
    database_url = "postgresql://postgres:tWeSxUREoYODoFroTAurHwcisymBotbz@yamanote.proxy.rlwy.net:29534/railway"
    
    try:
        # Cargar exceles
        df_operativas = pd.read_excel("OPERATIVAS_POSTGRESQL_20251223_113008.xlsx", 
                                    sheet_name='Operativas_PostgreSQL')
        df_seguridad = pd.read_excel("SEGURIDAD_POSTGRESQL_20251223_113008.xlsx", 
                                   sheet_name='Seguridad_PostgreSQL')
        
        print(f"📊 Excel operativas: {len(df_operativas)}")
        print(f"📊 Excel seguridad: {len(df_seguridad)}")
        
        # Conectar a Railway
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        # Obtener IDs existentes
        cursor.execute("SELECT submission_id FROM supervisiones WHERE tipo_supervision = 'operativas'")
        operativas_existentes = {row[0] for row in cursor.fetchall()}
        
        cursor.execute("SELECT submission_id FROM supervisiones WHERE tipo_supervision = 'seguridad'")
        seguridad_existentes = {row[0] for row in cursor.fetchall()}
        
        print(f"📊 Railway operativas: {len(operativas_existentes)}")
        print(f"📊 Railway seguridad: {len(seguridad_existentes)}")
        
        # Calcular faltantes
        operativas_excel_ids = set(df_operativas['ID_SUPERVISION'].astype(str))
        seguridad_excel_ids = set(df_seguridad['ID_SUPERVISION'].astype(str))
        
        operativas_faltantes = operativas_excel_ids - operativas_existentes
        seguridad_faltantes = seguridad_excel_ids - seguridad_existentes
        
        print(f"\n❌ DATOS FALTANTES:")
        print(f"   🔧 Operativas faltantes: {len(operativas_faltantes)}")
        print(f"   🛡️ Seguridad faltantes: {len(seguridad_faltantes)}")
        
        # Verificar sucursales disponibles
        cursor.execute("SELECT COUNT(DISTINCT nombre) FROM sucursales")
        total_sucursales = cursor.fetchone()[0]
        
        cursor.execute("SELECT nombre FROM sucursales ORDER BY nombre")
        sucursales_railway = {row[0] for row in cursor.fetchall()}
        
        # Verificar coincidencia sucursales
        sucursales_operativas = set(df_operativas['SUCURSAL'].unique())
        sucursales_seguridad = set(df_seguridad['SUCURSAL'].unique())
        
        operativas_sucursales_ok = sucursales_operativas.intersection(sucursales_railway)
        seguridad_sucursales_ok = sucursales_seguridad.intersection(sucursales_railway)
        
        print(f"\n🏢 SUCURSALES:")
        print(f"   📊 Railway total: {total_sucursales}")
        print(f"   🔧 Operativas coinciden: {len(operativas_sucursales_ok)}/{len(sucursales_operativas)}")
        print(f"   🛡️ Seguridad coinciden: {len(seguridad_sucursales_ok)}/{len(sucursales_seguridad)}")
        
        # Mostrar ejemplos de faltantes
        if operativas_faltantes:
            print(f"\n🔧 EJEMPLOS OPERATIVAS FALTANTES:")
            for i, id_supervision in enumerate(list(operativas_faltantes)[:5]):
                row = df_operativas[df_operativas['ID_SUPERVISION'].astype(str) == id_supervision].iloc[0]
                print(f"   📋 {id_supervision}: {row['SUCURSAL']} - {row['CALIFICACION_GENERAL']}")
        
        if seguridad_faltantes:
            print(f"\n🛡️ EJEMPLOS SEGURIDAD FALTANTES:")
            for i, id_supervision in enumerate(list(seguridad_faltantes)[:5]):
                row = df_seguridad[df_seguridad['ID_SUPERVISION'].astype(str) == id_supervision].iloc[0]
                print(f"   📋 {id_supervision}: {row['SUCURSAL']} - {row['CALIFICACION_GENERAL']}")
        
        cursor.close()
        conn.close()
        
        return {
            'operativas_faltantes': len(operativas_faltantes),
            'seguridad_faltantes': len(seguridad_faltantes),
            'total_faltantes': len(operativas_faltantes) + len(seguridad_faltantes)
        }
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return None

if __name__ == "__main__":
    result = check_missing_data()
    if result:
        print(f"\n📊 RESUMEN: {result['total_faltantes']} supervisiones faltantes")