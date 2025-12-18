#!/usr/bin/env python3
"""
📋 RAILWAY LOAD VALIDATED DATA
Carga datos ya validados en Railway PostgreSQL (sin llamadas API)
"""

import psycopg2
from datetime import datetime

# CONFIGURACIÓN RAILWAY - Roberto's PostgreSQL Credentials
RAILWAY_CONFIG = {
    'host': 'turntable.proxy.rlwy.net',
    'port': '24097', 
    'database': 'railway',
    'user': 'postgres',
    'password': 'qGgdIUuKYKMKGtSNYzARpyapBWHsloOt'
}

def conectar_railway():
    """Conectar a Railway PostgreSQL"""
    try:
        conn = psycopg2.connect(**RAILWAY_CONFIG)
        print("✅ Conexión exitosa a Railway PostgreSQL")
        return conn
    except Exception as e:
        print(f"❌ Error conectando a Railway: {e}")
        return None

def cargar_grupos_validados(conn):
    """Cargar 20 grupos operativos validados"""
    
    print("👥 Cargando grupos operativos validados...")
    
    # Grupos validados con clasificación corregida
    grupos_validados = [
        # GRUPOS LOCALES (7)
        ('PLOG APODACA', 'LOCAL'),
        ('PLOG GARCIA', 'LOCAL'),
        ('PLOG GUADALUPE', 'LOCAL'),
        ('PLOG MONTERREY', 'LOCAL'),
        ('PLOG NUEVO LEON', 'LOCAL'),  # Corregido - todas locales
        ('PLOG SAN NICOLAS', 'LOCAL'),
        ('PLOG SANTA CATARINA', 'LOCAL'),
        
        # GRUPOS FORÁNEOS (10)
        ('PLOG COAHUILA', 'FORANEA'),
        ('PLOG TAMAULIPAS', 'FORANEA'),
        ('PLOG SINALOA', 'FORANEA'),
        ('PLOG SONORA', 'FORANEA'),
        ('PLOG VERACRUZ', 'FORANEA'),
        ('PLOG CHIHUAHUA', 'FORANEA'),
        ('PLOG MICHOACAN', 'FORANEA'),
        ('PLOG GUANAJUATO', 'FORANEA'),
        ('PLOG JALISCO', 'FORANEA'),
        ('PLOG BAJA CALIFORNIA', 'FORANEA'),
        
        # GRUPOS MIXTOS (3)
        ('PLOG SALTILLO', 'MIXTO'),
        ('PLOG GENERAL ESCOBEDO', 'MIXTO'),
        ('PLOG LA SALLE', 'MIXTO')
    ]
    
    cursor = conn.cursor()
    
    for nombre, tipo in grupos_validados:
        cursor.execute("""
            INSERT INTO grupos_operativos (nombre, tipo_supervision) 
            VALUES (%s, %s) 
            ON CONFLICT (nombre) DO UPDATE SET tipo_supervision = EXCLUDED.tipo_supervision
            RETURNING id;
        """, (nombre, tipo))
        
        grupo_id = cursor.fetchone()[0]
        print(f"   ✅ {nombre}: {tipo} (ID: {grupo_id})")
    
    conn.commit()
    cursor.close()
    
    return len(grupos_validados)

def cargar_sucursales_validadas(conn):
    """Cargar sucursales con ejemplos validados"""
    
    print("🏪 Cargando sucursales validadas...")
    
    cursor = conn.cursor()
    
    # Obtener grupos para foreign keys
    cursor.execute("SELECT id, nombre FROM grupos_operativos;")
    grupos_map = {row[1]: row[0] for row in cursor.fetchall()}
    
    # Sucursales ejemplo validadas (representativas de cada tipo)
    sucursales_validadas = [
        # PLOG NUEVO LEON - Todas locales (ejemplo validado)
        (86, 'Miguel de la Madrid', 'Guadalupe, Nuevo León', 25.69411591, -100.1743994, 'PLOG NUEVO LEON', 'LOCAL', '86'),
        (35, 'Expo', 'Guadalupe, Nuevo León', 25.6866, -100.2569, 'PLOG NUEVO LEON', 'LOCAL', '35'),
        (36, 'Centrito', 'Monterrey, Nuevo León', 25.6751, -100.3185, 'PLOG NUEVO LEON', 'LOCAL', '36'),
        
        # PLOG SALTILLO - Mixto ejemplo
        (53, 'Lienzo Charro', 'Saltillo, Coahuila', 25.4521, -100.9737, 'PLOG SALTILLO', 'LOCAL', '53'),
        (54, 'Saltillo Centro', 'Saltillo, Coahuila', 25.4232, -100.9934, 'PLOG SALTILLO', 'LOCAL', '54'),
        
        # PLOG TAMAULIPAS - Foráneo ejemplo
        (10, 'Matamoros', 'Matamoros, Tamaulipas', 25.8693, -97.5047, 'PLOG TAMAULIPAS', 'FORANEA', '10'),
        (11, 'Reynosa Centro', 'Reynosa, Tamaulipas', 26.0753, -98.2777, 'PLOG TAMAULIPAS', 'FORANEA', '11'),
        
        # PLOG SINALOA - Foráneo ejemplo
        (15, 'Guasave', 'Guasave, Sinaloa', 25.5677, -108.4688, 'PLOG SINALOA', 'FORANEA', '15'),
        (16, 'Culiacán Centro', 'Culiacán, Sinaloa', 24.8067, -107.3940, 'PLOG SINALOA', 'FORANEA', '16')
    ]
    
    cargadas = 0
    
    for sucursal in sucursales_validadas:
        suc_id, nombre, ubicacion, lat, lon, grupo_nombre, tipo, external_key = sucursal
        
        if grupo_nombre in grupos_map:
            grupo_id = grupos_map[grupo_nombre]
            
            cursor.execute("""
                INSERT INTO sucursales (
                    id, nombre, direccion, latitud, longitud, 
                    grupo_operativo_id, grupo_operativo_nombre, 
                    tipo_ubicacion, external_key
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) ON CONFLICT (id) DO UPDATE SET 
                    tipo_ubicacion = EXCLUDED.tipo_ubicacion,
                    grupo_operativo_id = EXCLUDED.grupo_operativo_id;
            """, (suc_id, nombre, ubicacion, lat, lon, grupo_id, grupo_nombre, tipo, external_key))
            
            cargadas += 1
            print(f"   ✅ {nombre} ({tipo})")
    
    conn.commit()
    cursor.close()
    
    return cargadas

def cargar_periodos_2025(conn):
    """Cargar períodos 2025 confirmados"""
    
    print("📅 Cargando períodos 2025 confirmados...")
    
    cursor = conn.cursor()
    
    # Períodos 2025 confirmados por Roberto
    periodos_2025 = [
        ('T1', 'Trimestral 1 - Locales NL + Saltillo', '2025-03-12', '2025-04-16', 'TRIMESTRAL', 'LOCAL'),
        ('T2', 'Trimestral 2 - Locales NL + Saltillo', '2025-06-11', '2025-08-18', 'TRIMESTRAL', 'LOCAL'),
        ('T3', 'Trimestral 3 - Locales NL + Saltillo', '2025-08-19', '2025-10-29', 'TRIMESTRAL', 'LOCAL'),
        ('T4', 'Trimestral 4 - Locales NL + Saltillo', '2025-10-30', '2025-12-31', 'TRIMESTRAL', 'LOCAL'),
        ('S1', 'Semestral 1 - Foráneas fuera NL', '2025-04-10', '2025-06-09', 'SEMESTRAL', 'FORANEA'),
        ('S2', 'Semestral 2 - Foráneas fuera NL', '2025-07-30', '2025-11-07', 'SEMESTRAL', 'FORANEA')
    ]
    
    cargados = 0
    
    for periodo in periodos_2025:
        cursor.execute("""
            INSERT INTO periodos_supervision (
                nombre, descripcion, fecha_inicio, fecha_fin, tipo, aplicable_a
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (nombre) DO UPDATE SET 
                descripcion = EXCLUDED.descripcion,
                fecha_inicio = EXCLUDED.fecha_inicio,
                fecha_fin = EXCLUDED.fecha_fin;
        """, periodo)
        
        cargados += 1
        print(f"   ✅ {periodo[0]}: {periodo[1]}")
    
    conn.commit()
    cursor.close()
    
    return cargados

def verificar_datos(conn):
    """Verificar datos cargados"""
    
    print("\n🔍 Verificando datos cargados...")
    
    cursor = conn.cursor()
    
    # Contar registros
    cursor.execute("SELECT COUNT(*) FROM grupos_operativos;")
    total_grupos = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM sucursales;")
    total_sucursales = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM periodos_supervision;")
    total_periodos = cursor.fetchone()[0]
    
    # Verificar clasificación grupos
    cursor.execute("SELECT tipo_supervision, COUNT(*) FROM grupos_operativos GROUP BY tipo_supervision;")
    clasificacion_grupos = cursor.fetchall()
    
    # Verificar clasificación sucursales
    cursor.execute("SELECT tipo_ubicacion, COUNT(*) FROM sucursales GROUP BY tipo_ubicacion;")
    clasificacion_sucursales = cursor.fetchall()
    
    cursor.close()
    
    print(f"   📊 Grupos operativos: {total_grupos}")
    for tipo, count in clasificacion_grupos:
        print(f"      {tipo}: {count} grupos")
    
    print(f"   🏪 Sucursales: {total_sucursales}")
    for tipo, count in clasificacion_sucursales:
        print(f"      {tipo}: {count} sucursales")
    
    print(f"   📅 Períodos 2025: {total_periodos}")
    
    return total_grupos, total_sucursales, total_periodos

def main():
    """Cargar datos validados en Railway"""
    
    print("📋 CARGANDO DATOS VALIDADOS EN RAILWAY")
    print("=" * 50)
    
    # Conectar a Railway
    conn = conectar_railway()
    if not conn:
        return
    
    try:
        # 1. Cargar grupos operativos
        grupos_cargados = cargar_grupos_validados(conn)
        
        # 2. Cargar sucursales ejemplo
        sucursales_cargadas = cargar_sucursales_validadas(conn)
        
        # 3. Cargar períodos 2025
        periodos_cargados = cargar_periodos_2025(conn)
        
        # 4. Verificar datos
        verificar_datos(conn)
        
        print(f"\n🎉 DATOS VALIDADOS CARGADOS EXITOSAMENTE!")
        print("=" * 50)
        print(f"✅ {grupos_cargados} grupos operativos")
        print(f"✅ {sucursales_cargadas} sucursales ejemplo")
        print(f"✅ {periodos_cargados} períodos 2025")
        print("✅ Base de datos lista para ETL completo")
        
        print(f"\n🌐 VERIFICAR EN WEB APP:")
        print("Railway app → /database para ver conexión")
        print("Railway app → /stats para ver estadísticas")
        
        print(f"\n🚀 PRÓXIMO PASO:")
        print("Ejecutar ETL completo para extraer supervisiones 2025")
        
    except Exception as e:
        print(f"❌ Error general: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main()