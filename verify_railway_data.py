#!/usr/bin/env python3
"""
🔍 VERIFICAR DATOS RAILWAY - DIAGNÓSTICO
Verificar si hay datos en Railway PostgreSQL y diagnosticar problemas
"""

import psycopg2

def verify_railway_data():
    """Verificar datos en Railway"""
    
    print("🔍 VERIFICANDO DATOS RAILWAY POSTGRESQL")
    print("=" * 60)
    
    # Railway Database URL
    database_url = "postgresql://postgres:tWeSxUREoYODoFroTAurHwcisymBotbz@yamanote.proxy.rlwy.net:29534/railway"
    
    try:
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        # Verificar tablas
        cursor.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        tablas = cursor.fetchall()
        
        print("📊 TABLAS EN BASE DE DATOS:")
        for tabla in tablas:
            print(f"   ✅ {tabla[0]}")
        
        if not tablas:
            print("❌ NO HAY TABLAS - Ejecutar setup_railway_database.py")
            return False
        
        # Verificar datos
        queries = [
            ("Sucursales", "SELECT COUNT(*) FROM sucursales"),
            ("Supervisiones Total", "SELECT COUNT(*) FROM supervisiones"),
            ("Supervisiones Operativas", "SELECT COUNT(*) FROM supervisiones WHERE tipo_supervision = 'operativas'"),
            ("Supervisiones Seguridad", "SELECT COUNT(*) FROM supervisiones WHERE tipo_supervision = 'seguridad'"),
            ("Áreas Evaluadas", "SELECT COUNT(*) FROM areas_calificaciones"),
            ("Coordenadas", "SELECT COUNT(*) FROM sucursales WHERE latitud IS NOT NULL"),
        ]
        
        print(f"\n📈 DATOS EN BASE:")
        for name, query in queries:
            try:
                cursor.execute(query)
                result = cursor.fetchone()
                count = result[0] if result else 0
                print(f"   📊 {name}: {count}")
                
                if name == "Sucursales" and count == 0:
                    print("❌ NO HAY SUCURSALES - Datos no migrados")
                    return False
                    
            except Exception as e:
                print(f"   ❌ Error en {name}: {str(e)}")
        
        # Test query específica
        cursor.execute("""
            SELECT s.grupo_operativo, COUNT(*) as total
            FROM sucursales s
            GROUP BY s.grupo_operativo
            ORDER BY total DESC
            LIMIT 5
        """)
        
        grupos = cursor.fetchall()
        print(f"\n🔧 TOP GRUPOS OPERATIVOS:")
        for grupo, total in grupos:
            print(f"   📋 {grupo}: {total} sucursales")
        
        cursor.close()
        conn.close()
        
        print(f"\n✅ VERIFICACIÓN COMPLETADA")
        return True
        
    except Exception as e:
        print(f"❌ ERROR CONEXIÓN: {str(e)}")
        return False

if __name__ == "__main__":
    verify_railway_data()