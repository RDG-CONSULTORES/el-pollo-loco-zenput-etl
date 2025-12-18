#!/usr/bin/env python3
"""
📊 CARGA DATOS INICIALES RAILWAY
Carga estructura organizacional completa en Railway PostgreSQL
"""

import psycopg2
import json
import pandas as pd
from datetime import datetime, date

def load_initial_data():
    """Carga todos los datos iniciales en Railway"""
    
    print("📊 CARGANDO DATOS INICIALES EN RAILWAY")
    print("=" * 45)
    
    # Configuración Railway - Roberto debe completar
    railway_config = {
        'host': 'TU_HOST_RAILWAY.railway.app',
        'port': 5432,
        'database': 'railway',
        'user': 'postgres',
        'password': 'TU_PASSWORD_RAILWAY'
    }
    
    try:
        # Conectar a Railway
        conn = psycopg2.connect(**railway_config)
        cur = conn.cursor()
        
        print("✅ Conectado a Railway PostgreSQL")
        
        # 1. CARGAR GRUPOS OPERATIVOS
        load_grupos_operativos(cur)
        
        # 2. CARGAR SUCURSALES
        load_sucursales_master(cur)
        
        # 3. CARGAR PERIODOS 2025
        load_periodos_supervision(cur)
        
        # 4. CARGAR ÁREAS DE SUPERVISIÓN
        load_supervision_areas(cur)
        
        # 5. CARGAR USUARIOS ZENPUT
        load_usuarios_zenput(cur)
        
        # Confirmar transacciones
        conn.commit()
        print("\n🎉 DATOS INICIALES CARGADOS EXITOSAMENTE")
        
    except Exception as e:
        print(f"❌ Error cargando datos: {e}")
        if 'conn' in locals():
            conn.rollback()
    
    finally:
        if 'conn' in locals():
            conn.close()

def load_grupos_operativos(cur):
    """Carga 20 grupos operativos"""
    
    print("\n👥 CARGANDO GRUPOS OPERATIVOS...")
    
    # Datos desde Excel Roberto
    grupos_data = [
        ('TEPEYAC', 7, ['Nuevo León'], 'arangel@epl.mx'),
        ('EXPO', 12, ['Nuevo León', 'Tamaulipas'], None),
        ('OGAS', 10, ['Nuevo León'], 'afarfan@epl.mx'),
        ('PLOG NUEVO LEON', 8, ['Nuevo León'], 'a.aguirre@plog.com.mx'),
        ('PLOG LAGUNA', 6, ['Coahuila', 'Durango'], 'a.aguirre@plog.com.mx'),
        ('PLOG QUERETARO', 4, ['Querétaro'], 'a.aguirre@plog.com.mx'),
        ('GRUPO SALTILLO', 6, ['Coahuila'], None),
        ('GRUPO MATAMOROS', 5, ['Tamaulipas'], None),
        ('OCHTER TAMPICO', 4, ['Tamaulipas'], None),
        ('GRUPO CANTERA ROSA (MORELIA)', 3, ['Michoacán'], None),
        ('TEC', 4, ['Nuevo León', 'Sinaloa'], None),
        ('EFM', 3, ['Nuevo León'], None),
        ('CRR', 3, ['Tamaulipas'], None),
        ('RAP', 3, ['Tamaulipas'], None),
        ('EPL SO', 2, ['Nuevo León'], None),
        ('GRUPO NUEVO LAREDO (RUELAS)', 2, ['Tamaulipas'], None),
        ('GRUPO PIEDRAS NEGRAS', 1, ['Coahuila'], None),
        ('GRUPO CENTRITO', 1, ['Nuevo León'], None),
        ('GRUPO SABINAS HIDALGO', 1, ['Nuevo León'], None),
        ('GRUPO RIO BRAVO', 1, ['Tamaulipas'], None)
    ]
    
    for nombre, total, estados, director_email in grupos_data:
        cur.execute("""
        INSERT INTO grupos_operativos (nombre, total_sucursales, estados_cobertura, director_email)
        VALUES (%s, %s, %s, %s)
        """, (nombre, total, estados, director_email))
    
    print(f"   ✅ {len(grupos_data)} grupos operativos cargados")

def load_sucursales_master(cur):
    """Carga 86 sucursales desde Excel Roberto"""
    
    print("\n🏪 CARGANDO SUCURSALES MASTER...")
    
    # Leer Excel Roberto
    excel_path = "/Users/robertodavila/pollo-loco-tracking-gps/grupos_operativos_final_corregido.csv"
    df = pd.read_csv(excel_path)
    
    for _, row in df.iterrows():
        # Buscar grupo_operativo_id
        cur.execute("SELECT id FROM grupos_operativos WHERE nombre = %s", (row['Grupo_Operativo'],))
        grupo_id = cur.fetchone()
        
        if grupo_id:
            grupo_id = grupo_id[0]
            
            # Insertar sucursal
            coordenadas = None
            if pd.notna(row['Latitude']) and pd.notna(row['Longitude']):
                coordenadas = f"POINT({row['Longitude']} {row['Latitude']})"
            
            cur.execute("""
            INSERT INTO sucursales_master 
            (numero, nombre_oficial, nombre_zenput, grupo_operativo_id, ciudad, estado, coordenadas, location_code)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                int(row['Numero_Sucursal']),
                row['Nombre_Sucursal'],
                f"{int(row['Numero_Sucursal'])} - {row['Nombre_Sucursal']}",
                grupo_id,
                row['Ciudad'],
                row['Estado'],
                coordenadas,
                row.get('Location_Code')
            ))
    
    print(f"   ✅ {len(df)} sucursales cargadas")

def load_periodos_supervision(cur):
    """Carga períodos de supervisión 2025"""
    
    print("\n📅 CARGANDO PERÍODOS SUPERVISIÓN 2025...")
    
    periodos_2025 = [
        (2025, 'TRIMESTRE', 1, 'T1', '2025-01-01', '2025-03-31', 'Primer Trimestre 2025'),
        (2025, 'TRIMESTRE', 2, 'T2', '2025-04-01', '2025-06-30', 'Segundo Trimestre 2025'),
        (2025, 'TRIMESTRE', 3, 'T3', '2025-07-01', '2025-09-30', 'Tercer Trimestre 2025'),
        (2025, 'TRIMESTRE', 4, 'T4', '2025-10-01', '2025-12-31', 'Cuarto Trimestre 2025'),
        (2025, 'SEMESTRE', 1, 'S1', '2025-01-01', '2025-06-30', 'Primer Semestre 2025'),
        (2025, 'SEMESTRE', 2, 'S2', '2025-07-01', '2025-12-31', 'Segundo Semestre 2025')
    ]
    
    for year, tipo, numero, codigo, inicio, fin, desc in periodos_2025:
        cur.execute("""
        INSERT INTO periodos_supervision (year, tipo, numero, codigo, fecha_inicio, fecha_fin, descripcion)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (year, tipo, numero, codigo, inicio, fin, desc))
    
    print(f"   ✅ {len(periodos_2025)} períodos cargados")

def load_supervision_areas(cur):
    """Carga 43 áreas de supervisión"""
    
    print("\n📋 CARGANDO ÁREAS DE SUPERVISIÓN...")
    
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
    
    # Áreas Form 877138 - Supervisión Operativa (31 áreas principales)
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
    
    print(f"   ✅ {len(areas_seguridad)} áreas seguridad + {len(areas_operativa)} áreas operativa cargadas")

def load_usuarios_zenput(cur):
    """Carga usuarios desde API datos"""
    
    print("\n👤 CARGANDO USUARIOS ZENPUT...")
    
    # Leer datos de usuarios extraídos
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
        print("   ⚠️ Archivo usuarios no encontrado, cargar manualmente")

if __name__ == "__main__":
    print("📊 INICIANDO CARGA DATOS INICIALES RAILWAY")
    print("Roberto debe configurar credenciales Railway antes de ejecutar")
    print()
    
    load_initial_data()
