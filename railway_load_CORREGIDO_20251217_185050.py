#!/usr/bin/env python3
"""
📊 CARGA DATOS RAILWAY - PERÍODOS 2025 REALES
Carga estructura con períodos correctos y manejo grupos mixtos
"""

import psycopg2
import json
import pandas as pd
from datetime import datetime, date

def load_initial_data_corregido():
    """Carga datos con períodos 2025 reales"""
    
    print("📊 CARGANDO DATOS CON PERÍODOS 2025 REALES")
    print("=" * 50)
    
    railway_config = {
        'host': 'TU_HOST_RAILWAY.railway.app',
        'port': 5432,
        'database': 'railway', 
        'user': 'postgres',
        'password': 'TU_PASSWORD_RAILWAY'
    }
    
    try:
        conn = psycopg2.connect(**railway_config)
        cur = conn.cursor()
        
        print("✅ Conectado a Railway PostgreSQL")
        
        # 1. CARGAR GRUPOS OPERATIVOS CON CLASIFICACIÓN
        load_grupos_operativos_clasificados(cur)
        
        # 2. CARGAR SUCURSALES CON PATRÓN SUPERVISIÓN
        load_sucursales_con_patron(cur)
        
        # 3. CARGAR PERÍODOS 2025 REALES
        load_periodos_2025_reales(cur)
        
        # 4. CARGAR ÁREAS SUPERVISIÓN
        load_supervision_areas(cur)
        
        # 5. CARGAR USUARIOS ZENPUT
        load_usuarios_zenput(cur)
        
        conn.commit()
        print("\n🎉 DATOS CARGADOS CON PERÍODOS CORRECTOS")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'conn' in locals():
            conn.close()

def load_grupos_operativos_clasificados(cur):
    """Carga grupos con clasificación LOCAL/FORANEO/MIXTO"""
    
    print("\n👥 CARGANDO GRUPOS CON CLASIFICACIÓN...")
    
    # Datos corregidos con clasificación
    grupos_data = [
        # LOCALES (7)
        ('TEPEYAC', 7, 7, 0, 'LOCAL', ['Nuevo León'], 'arangel@epl.mx'),
        ('OGAS', 10, 10, 0, 'LOCAL', ['Nuevo León'], 'afarfan@epl.mx'),
        ('PLOG NUEVO LEON', 8, 8, 0, 'LOCAL', ['Nuevo León'], 'a.aguirre@plog.com.mx'),
        ('EFM', 3, 3, 0, 'LOCAL', ['Nuevo León'], None),
        ('EPL SO', 2, 2, 0, 'LOCAL', ['Nuevo León'], None),
        ('GRUPO CENTRITO', 1, 1, 0, 'LOCAL', ['Nuevo León'], None),
        ('GRUPO SABINAS HIDALGO', 1, 1, 0, 'LOCAL', ['Nuevo León'], None),
        
        # FORÁNEOS (10)
        ('CRR', 3, 0, 3, 'FORANEO', ['Tamaulipas'], None),
        ('RAP', 3, 0, 3, 'FORANEO', ['Tamaulipas'], None),
        ('GRUPO RIO BRAVO', 1, 0, 1, 'FORANEO', ['Tamaulipas'], None),
        ('GRUPO NUEVO LAREDO (RUELAS)', 2, 0, 2, 'FORANEO', ['Tamaulipas'], None),
        ('OCHTER TAMPICO', 4, 0, 4, 'FORANEO', ['Tamaulipas'], None),
        ('GRUPO MATAMOROS', 5, 0, 5, 'FORANEO', ['Tamaulipas'], None),
        ('GRUPO CANTERA ROSA (MORELIA)', 3, 0, 3, 'FORANEO', ['Michoacán'], None),
        ('PLOG QUERETARO', 4, 0, 4, 'FORANEO', ['Querétaro'], 'a.aguirre@plog.com.mx'),
        ('PLOG LAGUNA', 6, 0, 6, 'FORANEO', ['Coahuila', 'Durango'], 'a.aguirre@plog.com.mx'),
        ('GRUPO PIEDRAS NEGRAS', 1, 0, 1, 'FORANEO', ['Coahuila'], None),
        
        # MIXTOS (3)
        ('EXPO', 12, 9, 3, 'MIXTO', ['Nuevo León', 'Tamaulipas'], None),
        ('GRUPO SALTILLO', 6, 5, 1, 'MIXTO', ['Coahuila'], None),
        ('TEC', 4, 3, 1, 'MIXTO', ['Nuevo León', 'Sinaloa'], None)
    ]
    
    for nombre, total, locales, foraneas, tipo, estados, director in grupos_data:
        cur.execute("""
        INSERT INTO grupos_operativos 
        (nombre, total_sucursales, sucursales_locales, sucursales_foraneas, tipo_grupo, estados_cobertura, director_email)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (nombre, total, locales, foraneas, tipo, estados, director))
    
    print(f"   ✅ {len(grupos_data)} grupos cargados con clasificación")

def load_sucursales_con_patron(cur):
    """Carga sucursales con patrón supervisión automático"""
    
    print("\n🏪 CARGANDO SUCURSALES CON PATRÓN SUPERVISIÓN...")
    
    # Leer Excel Roberto
    excel_path = "/Users/robertodavila/pollo-loco-tracking-gps/grupos_operativos_final_corregido.csv"
    df = pd.read_csv(excel_path)
    
    for _, row in df.iterrows():
        # Buscar grupo_operativo_id
        cur.execute("SELECT id FROM grupos_operativos WHERE nombre = %s", (row['Grupo_Operativo'],))
        grupo_result = cur.fetchone()
        
        if grupo_result:
            grupo_id = grupo_result[0]
            
            # Preparar datos
            numero = int(row['Numero_Sucursal']) if pd.notna(row['Numero_Sucursal']) else None
            estado = row['Estado'] if pd.notna(row['Estado']) else None
            ciudad = row['Ciudad'] if pd.notna(row['Ciudad']) else None
            
            # Coordenadas
            coordenadas = None
            if pd.notna(row['Latitude']) and pd.notna(row['Longitude']):
                coordenadas = f"POINT({row['Longitude']} {row['Latitude']})"
            
            # Insertar (trigger calculará clasificacion y patron_supervision)
            cur.execute("""
            INSERT INTO sucursales_master 
            (numero, nombre_oficial, nombre_zenput, grupo_operativo_id, ciudad, estado, coordenadas, location_code)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                numero,
                row['Nombre_Sucursal'],
                f"{numero} - {row['Nombre_Sucursal']}",
                grupo_id,
                ciudad,
                estado,
                coordenadas,
                row.get('Location_Code') if pd.notna(row.get('Location_Code')) else None
            ))
    
    print(f"   ✅ {len(df)} sucursales cargadas con patrón automático")

def load_periodos_2025_reales(cur):
    """Carga períodos 2025 con fechas reales ajustadas"""
    
    print("\n📅 CARGANDO PERÍODOS 2025 REALES...")
    
    periodos_2025_reales = [
        # TRIMESTRALES LOCALES (fechas ajustadas por entrega marzo)
        (2025, 'TRIMESTRE', 1, 'T1', '2025-03-12', '2025-04-16', 'T1 2025 (12 Mar - 16 Abr)', 'LOCALES'),
        (2025, 'TRIMESTRE', 2, 'T2', '2025-06-11', '2025-08-18', 'T2 2025 (11 Jun - 18 Ago)', 'LOCALES'),
        (2025, 'TRIMESTRE', 3, 'T3', '2025-08-19', '2025-10-29', 'T3 2025 (19 Ago - 29 Oct)', 'LOCALES'),
        (2025, 'TRIMESTRE', 4, 'T4', '2025-10-30', '2025-12-31', 'T4 2025 (30 Oct - 31 Dic)', 'LOCALES'),
        
        # SEMESTRALES FORÁNEAS (fechas ajustadas)
        (2025, 'SEMESTRE', 1, 'S1', '2025-04-10', '2025-06-09', 'S1 2025 (10 Abr - 9 Jun)', 'FORANEAS'),
        (2025, 'SEMESTRE', 2, 'S2', '2025-07-30', '2025-11-07', 'S2 2025 (30 Jul - 7 Nov)', 'FORANEAS'),
        
        # PERÍODOS 2026 NORMALIZADOS (preparación futura)
        (2026, 'TRIMESTRE', 1, 'T1_2026', '2026-01-01', '2026-03-31', 'T1 2026 Normalizado', 'TODAS'),
        (2026, 'TRIMESTRE', 2, 'T2_2026', '2026-04-01', '2026-06-30', 'T2 2026 Normalizado', 'TODAS'),
        (2026, 'TRIMESTRE', 3, 'T3_2026', '2026-07-01', '2026-09-30', 'T3 2026 Normalizado', 'TODAS'),
        (2026, 'TRIMESTRE', 4, 'T4_2026', '2026-10-01', '2026-12-31', 'T4 2026 Normalizado', 'TODAS')
    ]
    
    for year, tipo, numero, codigo, inicio, fin, desc, aplica in periodos_2025_reales:
        cur.execute("""
        INSERT INTO periodos_supervision (year, tipo, numero, codigo, fecha_inicio, fecha_fin, descripcion, aplica_a)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (year, tipo, numero, codigo, inicio, fin, desc, aplica))
    
    print(f"   ✅ {len(periodos_2025_reales)} períodos cargados (2025 reales + 2026 preparación)")

def load_supervision_areas(cur):
    """Carga 43 áreas de supervisión"""
    
    print("\n📋 CARGANDO 43 ÁREAS SUPERVISIÓN...")
    
    # Áreas Form 877139 - Control Operativo Seguridad (12 áreas)
    areas_seguridad = [
        'GENERAL', 'UNIFORME', 'APARIENCIA PERSONAL', 'COFIA Y MALLA',
        'LAVADO DE MANOS', 'RECEPCION DE ALIMENTOS', 'ALMACENAMIENTO',
        'PROCESO DE ALIMENTOS', 'EQUIPO DE LIMPIEZA', 'PREVENCION CONTAMINACION',
        'TEMPERATURA DE ALIMENTOS', 'SANITIZADO'
    ]
    
    for i, area in enumerate(areas_seguridad, 1):
        cur.execute("""
        INSERT INTO supervision_areas (form_id, area_nombre, area_orden)
        VALUES (%s, %s, %s)
        """, (877139, area, i))
    
    # Áreas Form 877138 - Supervisión Operativa (31 áreas)
    areas_operativa = [
        'GENERAL', 'UNIFORME', 'APARIENCIA PERSONAL', 'INICIO DE OPERACION',
        'PROCESO DE COCINADO', 'CALIDAD DEL PRODUCTO', 'BEBIDAS',
        'PROCESO FRITANGA', 'PROCESO PARRILLA', 'EMPACADO Y ENTREGA',
        'ATENCION AL CLIENTE', 'CAJA DINERO', 'PROCESO LIMPIEZA',
        'LIMPIEZA COCINA', 'LIMPIEZA SALON', 'LIMPIEZA BAÑOS',
        'MANTENIMIENTO PREVENTIVO', 'TEMPERATURA EQUIPOS', 'INVENTARIO',
        'BODEGA SECOS', 'CUARTO FRIO', 'ALMACENAMIENTO VARIOS',
        'LIMPIEZA ALMACEN', 'PROTECCION CIVIL', 'SEGURIDAD',
        'DOCUMENTOS VARIOS', 'CAPACITACION', 'CIERRE OPERACION',
        'SEGURIDAD ADMINISTRATIVA', 'SISTEMA PUNTO VENTA', 'AUTOLOCO'
    ]
    
    for i, area in enumerate(areas_operativa, 1):
        cur.execute("""
        INSERT INTO supervision_areas (form_id, area_nombre, area_orden)
        VALUES (%s, %s, %s)
        """, (877138, area, i))
    
    print(f"   ✅ {len(areas_seguridad)} áreas seguridad + {len(areas_operativa)} áreas operativa")

def load_usuarios_zenput(cur):
    """Carga usuarios desde datos API"""
    
    print("\n👤 CARGANDO USUARIOS ZENPUT...")
    
    try:
        with open('data/users_data_20251217_182215.json', 'r') as f:
            users_data = json.load(f)
        
        for user in users_data['usuarios']:
            cur.execute("""
            INSERT INTO usuarios_zenput 
            (id, username, nombre_completo, email, telefono, grupos, teams, 
             sucursales_asignadas, default_team_id, default_team_name, raw_user_data)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                user['id'],
                user.get('username'),
                user.get('display_name'),
                user.get('email'),
                user.get('sms_number'),
                json.dumps(user.get('groups', [])),
                json.dumps(user.get('teams', [])),
                json.dumps(user.get('owned_locations', [])),
                user.get('default_team', {}).get('id'),
                user.get('default_team', {}).get('name'),
                json.dumps(user)
            ))
        
        print(f"   ✅ {len(users_data['usuarios'])} usuarios cargados")
        
    except FileNotFoundError:
        print("   ⚠️ Archivo usuarios no encontrado")

if __name__ == "__main__":
    print("📊 CARGA DATOS RAILWAY - PERÍODOS 2025 REALES")
    print("Roberto: configurar credenciales Railway antes de ejecutar")
    print()
    
    load_initial_data_corregido()
