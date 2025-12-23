#!/usr/bin/env python3
"""
🚀 MIGRACIÓN COMPLETA A RAILWAY - EL POLLO LOCO
Migrar todos los datos desde Excel a PostgreSQL Railway
Roberto: Script completo para migrar 476 supervisiones + áreas
"""

import pandas as pd
import psycopg2
from datetime import datetime
import json
import uuid
import os
from decimal import Decimal
import numpy as np

class RailwayMigrator:
    def __init__(self, railway_db_url):
        """Inicializar conexión Railway PostgreSQL"""
        self.conn = psycopg2.connect(railway_db_url)
        self.cursor = self.conn.cursor()
        self.stats = {
            'sucursales_migradas': 0,
            'operativas_migradas': 0,
            'seguridad_migradas': 0,
            'areas_migradas': 0,
            'errores': []
        }
    
    def setup_database(self):
        """Ejecutar schema completo"""
        print("🗄️ CONFIGURANDO DATABASE RAILWAY")
        print("=" * 50)
        
        # Leer y ejecutar schema
        with open('railway_schema_optimizado.sql', 'r', encoding='utf-8') as f:
            schema_sql = f.read()
        
        # Ejecutar por bloques (PostgreSQL no soporta múltiples statements)
        statements = schema_sql.split(';')
        
        for i, statement in enumerate(statements):
            statement = statement.strip()
            if statement and not statement.startswith('--') and not statement.startswith('/*'):
                try:
                    self.cursor.execute(statement)
                    self.conn.commit()
                except Exception as e:
                    if 'already exists' not in str(e).lower():
                        print(f"   ⚠️ Statement {i}: {str(e)[:100]}")
        
        print("✅ Schema Railway configurado")
    
    def migrate_sucursales(self):
        """Migrar catálogo sucursales"""
        print("\n📍 MIGRANDO SUCURSALES")
        print("=" * 40)
        
        # Cargar archivo validado
        df = pd.read_csv("SUCURSALES_CORRECCIONES_ROBERTO_20251218_171807.csv")
        
        for _, row in df.iterrows():
            try:
                self.cursor.execute("""
                    INSERT INTO sucursales (numero, nombre, grupo_operativo, tipo_sucursal, latitud, longitud, estado)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (numero) DO UPDATE SET
                        nombre = EXCLUDED.nombre,
                        grupo_operativo = EXCLUDED.grupo_operativo,
                        tipo_sucursal = EXCLUDED.tipo_sucursal,
                        latitud = EXCLUDED.latitud,
                        longitud = EXCLUDED.longitud,
                        estado = EXCLUDED.estado,
                        updated_at = NOW()
                """, (
                    int(row['numero']), 
                    row['nombre'], 
                    row['grupo'], 
                    row['tipo'], 
                    float(row['lat']), 
                    float(row['lon']),
                    'Nuevo León'  # Default estado
                ))
                
                self.stats['sucursales_migradas'] += 1
                
            except Exception as e:
                error_msg = f"Error sucursal {row['numero']}: {str(e)}"
                self.stats['errores'].append(error_msg)
                print(f"   ❌ {error_msg}")
        
        self.conn.commit()
        print(f"✅ Migradas {self.stats['sucursales_migradas']} sucursales")
    
    def get_sucursal_id(self, sucursal_nombre):
        """Buscar ID de sucursal por nombre"""
        self.cursor.execute(
            "SELECT id FROM sucursales WHERE nombre = %s", 
            (sucursal_nombre,)
        )
        result = self.cursor.fetchone()
        return result[0] if result else None
    
    def extract_areas_from_row(self, row, tipo_supervision):
        """Extraer áreas evaluadas de una fila Excel"""
        areas_dict = {}
        
        # Columnas a ignorar (metadatos)
        ignore_prefixes = [
            'ID_', 'SUCURSAL', 'FECHA', 'CALIFICACION_GENERAL', 'USUARIO',
            'sucursal_', 'latitud', 'longitud', 'grupo_', 'tipo_', 
            'estado', 'pais', 'region', 'zona_', 'Unnamed:'
        ]
        
        for col in row.index:
            # Saltar columnas de metadatos
            if any(col.startswith(prefix) for prefix in ignore_prefixes):
                continue
            
            # Solo áreas con valores válidos
            if pd.notna(row[col]) and str(row[col]).replace('.', '').replace(',', '').isdigit():
                try:
                    areas_dict[col] = float(row[col])
                except:
                    continue
        
        return areas_dict
    
    def migrate_operativas(self):
        """Migrar supervisiones operativas con áreas"""
        print("\n🔧 MIGRANDO SUPERVISIONES OPERATIVAS")
        print("=" * 50)
        
        # Cargar Excel operativas
        df = pd.read_excel("OPERATIVAS_POSTGRESQL_20251223_113008.xlsx", 
                          sheet_name='Operativas_PostgreSQL')
        
        print(f"📊 Total operativas: {len(df)}")
        
        for i, row in df.iterrows():
            try:
                # 1. Buscar sucursal_id
                sucursal_id = self.get_sucursal_id(row['SUCURSAL'])
                if not sucursal_id:
                    print(f"   ⚠️ Sucursal no encontrada: {row['SUCURSAL']}")
                    continue
                
                # 2. Extraer áreas
                areas_dict = self.extract_areas_from_row(row, 'operativas')
                
                # 3. Insertar supervisión
                supervision_id = str(uuid.uuid4())
                
                self.cursor.execute("""
                    INSERT INTO supervisiones 
                    (id, submission_id, sucursal_id, tipo_supervision, fecha_supervision, 
                     calificacion_general, areas_evaluadas, usuario)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (submission_id) DO UPDATE SET
                        calificacion_general = EXCLUDED.calificacion_general,
                        areas_evaluadas = EXCLUDED.areas_evaluadas,
                        updated_at = NOW()
                """, (
                    supervision_id,
                    row['ID_SUPERVISION'],
                    sucursal_id,
                    'operativas',
                    pd.to_datetime(row['FECHA']),
                    float(row['CALIFICACION_GENERAL']),
                    json.dumps(areas_dict, default=str),
                    row.get('USUARIO', 'Sistema')
                ))
                
                # 4. Insertar áreas individuales
                for area_nombre, calificacion in areas_dict.items():
                    self.cursor.execute("""
                        INSERT INTO areas_calificaciones 
                        (supervision_id, area_nombre, calificacion)
                        VALUES (%s, %s, %s)
                    """, (supervision_id, area_nombre, float(calificacion)))
                    
                    self.stats['areas_migradas'] += 1
                
                self.stats['operativas_migradas'] += 1
                
                # Progreso cada 50
                if (i + 1) % 50 == 0:
                    print(f"   📊 Progreso: {i + 1}/{len(df)} operativas")
                
            except Exception as e:
                error_msg = f"Error operativa {i}: {str(e)}"
                self.stats['errores'].append(error_msg)
                print(f"   ❌ {error_msg}")
        
        self.conn.commit()
        print(f"✅ Migradas {self.stats['operativas_migradas']} supervisiones operativas")
    
    def migrate_seguridad(self):
        """Migrar supervisiones seguridad"""
        print("\n🛡️ MIGRANDO SUPERVISIONES SEGURIDAD")
        print("=" * 50)
        
        # Cargar Excel seguridad
        df = pd.read_excel("SEGURIDAD_POSTGRESQL_20251223_113008.xlsx", 
                          sheet_name='Seguridad_PostgreSQL')
        
        print(f"📊 Total seguridad: {len(df)}")
        
        for i, row in df.iterrows():
            try:
                # 1. Buscar sucursal_id
                sucursal_id = self.get_sucursal_id(row['SUCURSAL'])
                if not sucursal_id:
                    print(f"   ⚠️ Sucursal no encontrada: {row['SUCURSAL']}")
                    continue
                
                # 2. Extraer áreas
                areas_dict = self.extract_areas_from_row(row, 'seguridad')
                
                # 3. Insertar supervisión
                supervision_id = str(uuid.uuid4())
                
                self.cursor.execute("""
                    INSERT INTO supervisiones 
                    (id, submission_id, sucursal_id, tipo_supervision, fecha_supervision, 
                     calificacion_general, areas_evaluadas, usuario)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (submission_id) DO UPDATE SET
                        calificacion_general = EXCLUDED.calificacion_general,
                        areas_evaluadas = EXCLUDED.areas_evaluadas,
                        updated_at = NOW()
                """, (
                    supervision_id,
                    row['ID_SUPERVISION'],
                    sucursal_id,
                    'seguridad',
                    pd.to_datetime(row['FECHA']),
                    float(row['CALIFICACION_GENERAL']),
                    json.dumps(areas_dict, default=str),
                    row.get('USUARIO', 'Sistema')
                ))
                
                # 4. Insertar áreas individuales
                for area_nombre, calificacion in areas_dict.items():
                    self.cursor.execute("""
                        INSERT INTO areas_calificaciones 
                        (supervision_id, area_nombre, calificacion)
                        VALUES (%s, %s, %s)
                    """, (supervision_id, area_nombre, float(calificacion)))
                    
                    self.stats['areas_migradas'] += 1
                
                self.stats['seguridad_migradas'] += 1
                
                # Progreso cada 50
                if (i + 1) % 50 == 0:
                    print(f"   📊 Progreso: {i + 1}/{len(df)} seguridad")
                
            except Exception as e:
                error_msg = f"Error seguridad {i}: {str(e)}"
                self.stats['errores'].append(error_msg)
                print(f"   ❌ {error_msg}")
        
        self.conn.commit()
        print(f"✅ Migradas {self.stats['seguridad_migradas']} supervisiones seguridad")
    
    def create_performance_indexes(self):
        """Crear índices de performance después de la carga"""
        print("\n⚡ CREANDO ÍNDICES PERFORMANCE")
        print("=" * 40)
        
        # Los índices ya están en el schema, pero asegurar que están creados
        performance_indexes = [
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_supervisiones_submission ON supervisiones(submission_id)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_areas_area_nombre ON areas_calificaciones(area_nombre, calificacion)",
        ]
        
        for index_sql in performance_indexes:
            try:
                self.cursor.execute(index_sql)
                self.conn.commit()
            except Exception as e:
                print(f"   ⚠️ Índice: {str(e)}")
        
        print("✅ Índices performance verificados")
    
    def refresh_materialized_views(self):
        """Refresh vistas materializadas"""
        print("\n📊 REFRESHING VISTAS MATERIALIZADAS")
        print("=" * 45)
        
        try:
            self.cursor.execute("SELECT refresh_dashboard_views()")
            self.conn.commit()
            print("✅ Vistas materializadas actualizadas")
        except Exception as e:
            print(f"⚠️ Error refresh views: {str(e)}")
            
            # Refresh manual si falla la función
            views = ['dashboard_operativas', 'dashboard_seguridad', 'kpis_operativas', 'kpis_seguridad']
            for view in views:
                try:
                    self.cursor.execute(f"REFRESH MATERIALIZED VIEW {view}")
                    self.conn.commit()
                    print(f"   ✅ {view}")
                except Exception as e2:
                    print(f"   ❌ {view}: {str(e2)}")
    
    def validate_migration(self):
        """Validar migración completa"""
        print("\n✅ VALIDANDO MIGRACIÓN")
        print("=" * 35)
        
        # Contar registros
        queries = [
            ("Sucursales", "SELECT COUNT(*) FROM sucursales"),
            ("Operativas", "SELECT COUNT(*) FROM supervisiones WHERE tipo_supervision = 'operativas'"),
            ("Seguridad", "SELECT COUNT(*) FROM supervisiones WHERE tipo_supervision = 'seguridad'"),
            ("Áreas", "SELECT COUNT(*) FROM areas_calificaciones"),
            ("KPI Operativas", "SELECT promedio_general FROM kpis_operativas"),
            ("KPI Seguridad", "SELECT promedio_general FROM kpis_seguridad")
        ]
        
        for name, query in queries:
            try:
                self.cursor.execute(query)
                result = self.cursor.fetchone()
                print(f"   📊 {name}: {result[0] if result else 'N/A'}")
            except Exception as e:
                print(f"   ❌ {name}: Error - {str(e)}")
        
        # Verificar coordenadas
        self.cursor.execute("""
            SELECT COUNT(*) FROM sucursales 
            WHERE latitud IS NOT NULL AND longitud IS NOT NULL
        """)
        coords_count = self.cursor.fetchone()[0]
        print(f"   🗺️ Sucursales con coordenadas: {coords_count}")
        
        # Verificar períodos CAS
        self.cursor.execute("""
            SELECT periodo_cas, COUNT(*) 
            FROM supervisiones 
            WHERE periodo_cas IS NOT NULL
            GROUP BY periodo_cas 
            ORDER BY COUNT(*) DESC
            LIMIT 5
        """)
        periodos = self.cursor.fetchall()
        print(f"   📅 Períodos CAS principales:")
        for periodo, count in periodos:
            print(f"      {periodo}: {count} supervisiones")
    
    def close(self):
        """Cerrar conexión"""
        self.cursor.close()
        self.conn.close()

def main():
    """Función principal migración"""
    
    print("🚀 MIGRACIÓN COMPLETA RAILWAY - EL POLLO LOCO")
    print("=" * 80)
    print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Roberto: Migración completa 476 supervisiones + áreas")
    print("=" * 80)
    
    # URL Railway PostgreSQL (placeholder)
    railway_url = os.getenv('DATABASE_URL') or input("🔗 URL Railway PostgreSQL: ")
    
    # Inicializar migrador
    try:
        migrator = RailwayMigrator(railway_url)
        
        # PASO 1: Setup database
        migrator.setup_database()
        
        # PASO 2: Migrar sucursales
        migrator.migrate_sucursales()
        
        # PASO 3: Migrar operativas
        migrator.migrate_operativas()
        
        # PASO 4: Migrar seguridad
        migrator.migrate_seguridad()
        
        # PASO 5: Índices performance
        migrator.create_performance_indexes()
        
        # PASO 6: Refresh vistas
        migrator.refresh_materialized_views()
        
        # PASO 7: Validar migración
        migrator.validate_migration()
        
        # ESTADÍSTICAS FINALES
        stats = migrator.stats
        print(f"\n🎯 MIGRACIÓN COMPLETADA")
        print("=" * 50)
        print(f"✅ Sucursales: {stats['sucursales_migradas']}")
        print(f"✅ Operativas: {stats['operativas_migradas']}")
        print(f"✅ Seguridad: {stats['seguridad_migradas']}")
        print(f"✅ Áreas: {stats['areas_migradas']}")
        print(f"⚠️ Errores: {len(stats['errores'])}")
        
        if stats['errores']:
            print(f"\n📋 ERRORES REPORTADOS:")
            for error in stats['errores'][:5]:  # Mostrar solo primeros 5
                print(f"   ❌ {error}")
            if len(stats['errores']) > 5:
                print(f"   ... y {len(stats['errores']) - 5} errores más")
        
        print(f"\n🚀 RAILWAY POSTGRESQL LISTO")
        print("🎯 Continuar con clonación dashboard frontend")
        
        migrator.close()
        
    except Exception as e:
        print(f"❌ ERROR CRÍTICO: {str(e)}")
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)