#!/usr/bin/env python3
"""
🔧 FIX SCHEMA PARA RECUPERACIÓN
Permitir NULL en location_id para submissions corporativas
"""

import psycopg2

# Configuración Railway
DATABASE_URL = 'postgresql://postgres:qGgdIUuKYKMKGtSNYzARpyapBWHsloOt@turntable.proxy.rlwy.net:24097/railway'

def fix_schema_table():
    """Permitir NULL en location_id para submissions corporativas"""
    
    print("🔧 ARREGLANDO SCHEMA PARA RECUPERACIÓN")
    print("=" * 50)
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Modificar columna location_id para permitir NULL
        print("📝 Modificando columna location_id para permitir NULL...")
        
        cursor.execute("""
            ALTER TABLE supervisiones_2026 
            ALTER COLUMN location_id DROP NOT NULL;
        """)
        
        print("✅ location_id ahora permite NULL")
        
        # Verificar schema actualizado
        cursor.execute("""
            SELECT column_name, is_nullable, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'supervisiones_2026' 
            AND column_name = 'location_id';
        """)
        schema_info = cursor.fetchone()
        
        print(f"📊 Schema actualizado:")
        print(f"   Columna: {schema_info[0]}")
        print(f"   Permite NULL: {schema_info[1]}")
        print(f"   Tipo: {schema_info[2]}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("\n🎉 SCHEMA ARREGLADO EXITOSAMENTE!")
        print("✅ Listo para re-ejecutar ETL de recuperación")
        
        return True
        
    except Exception as e:
        print(f"❌ Error arreglando schema: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        return False

if __name__ == "__main__":
    fix_schema_table()