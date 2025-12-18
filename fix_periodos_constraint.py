#!/usr/bin/env python3
"""
🔧 FIX PERIODOS CONSTRAINT
Arregla constraint de periodos y carga datos
"""

import psycopg2

# CONFIGURACIÓN RAILWAY - Roberto's PostgreSQL Credentials
RAILWAY_CONFIG = {
    'host': 'turntable.proxy.rlwy.net',
    'port': '24097', 
    'database': 'railway',
    'user': 'postgres',
    'password': 'qGgdIUuKYKMKGtSNYzARpyapBWHsloOt'
}

def conectar_railway():
    try:
        conn = psycopg2.connect(**RAILWAY_CONFIG)
        return conn
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def fix_and_load_periodos():
    conn = conectar_railway()
    if not conn:
        return
    
    cursor = conn.cursor()
    
    try:
        # 1. Agregar constraint único en nombre
        print("🔧 Agregando constraint único...")
        cursor.execute("ALTER TABLE periodos_supervision ADD CONSTRAINT periodos_supervision_nombre_key UNIQUE (nombre);")
        
        # 2. Cargar períodos
        print("📅 Cargando períodos 2025...")
        
        periodos = [
            ('T1', 'Trimestral 1 - Locales NL + Saltillo', '2025-03-12', '2025-04-16', 'TRIMESTRAL', 'LOCAL'),
            ('T2', 'Trimestral 2 - Locales NL + Saltillo', '2025-06-11', '2025-08-18', 'TRIMESTRAL', 'LOCAL'),
            ('T3', 'Trimestral 3 - Locales NL + Saltillo', '2025-08-19', '2025-10-29', 'TRIMESTRAL', 'LOCAL'),
            ('T4', 'Trimestral 4 - Locales NL + Saltillo', '2025-10-30', '2025-12-31', 'TRIMESTRAL', 'LOCAL'),
            ('S1', 'Semestral 1 - Foráneas fuera NL', '2025-04-10', '2025-06-09', 'SEMESTRAL', 'FORANEA'),
            ('S2', 'Semestral 2 - Foráneas fuera NL', '2025-07-30', '2025-11-07', 'SEMESTRAL', 'FORANEA')
        ]
        
        for periodo in periodos:
            cursor.execute("""
                INSERT INTO periodos_supervision (
                    nombre, descripcion, fecha_inicio, fecha_fin, tipo, aplicable_a
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (nombre) DO NOTHING;
            """, periodo)
            print(f"   ✅ {periodo[0]}")
        
        conn.commit()
        
        # Verificar
        cursor.execute("SELECT COUNT(*) FROM periodos_supervision;")
        total = cursor.fetchone()[0]
        print(f"\n✅ {total} períodos cargados correctamente")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        if "already exists" in str(e):
            print("   ⚠️ Constraint ya existe, continuando...")
            
            # Solo cargar períodos
            for periodo in periodos:
                cursor.execute("""
                    INSERT INTO periodos_supervision (
                        nombre, descripcion, fecha_inicio, fecha_fin, tipo, aplicable_a
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (nombre) DO NOTHING;
                """, periodo)
            
            conn.commit()
            print("✅ Períodos cargados")
        else:
            print(f"❌ Error: {e}")
            conn.rollback()

if __name__ == "__main__":
    fix_and_load_periodos()