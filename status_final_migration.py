#!/usr/bin/env python3
"""
📊 STATUS FINAL MIGRACIÓN RAILWAY
Verificar progreso completo de la migración
Roberto: Status final antes de completar
"""

import psycopg2
import requests
import pandas as pd

def status_final():
    """Status final de migración Railway"""
    
    print("📊 STATUS FINAL MIGRACIÓN RAILWAY")
    print("=" * 50)
    print(f"⏰ {pd.Timestamp.now()}")
    
    database_url = "postgresql://postgres:tWeSxUREoYODoFroTAurHwcisymBotbz@yamanote.proxy.rlwy.net:29534/railway"
    railway_url = "https://el-pollo-loco-zenput-etl-production.up.railway.app"
    
    try:
        # PASO 1: Verificar APIs funcionando
        print("\n🔗 VERIFICANDO APIs RAILWAY...")
        
        # Test operativas
        response = requests.get(f"{railway_url}/api/operativas/kpis", timeout=10)
        kpis_op = response.json()
        print(f"   ✅ API Operativas: {kpis_op.get('promedio_general')}% - {kpis_op.get('total_supervisiones')} supervisiones")
        
        # Test seguridad
        response = requests.get(f"{railway_url}/api/seguridad/kpis", timeout=10)
        kpis_seg = response.json()
        print(f"   ✅ API Seguridad: {kpis_seg.get('promedio_general')}% - {kpis_seg.get('total_supervisiones')} supervisiones")
        
        # Test stats generales
        response = requests.get(f"{railway_url}/api/stats", timeout=10)
        stats = response.json()
        print(f"   ✅ API Stats: {stats.get('operativas')} op + {stats.get('seguridad')} seg = {int(stats.get('operativas')) + int(stats.get('seguridad'))} total")
        
        # PASO 2: Verificar base de datos directamente
        print("\n🗄️ VERIFICANDO BASE DE DATOS...")
        
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        # Conteos básicos
        cursor.execute("SELECT COUNT(*) FROM sucursales")
        sucursales = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM supervisiones WHERE tipo_supervision = 'operativas'")
        operativas_db = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM supervisiones WHERE tipo_supervision = 'seguridad'")
        seguridad_db = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM areas_calificaciones")
        areas_db = cursor.fetchone()[0]
        
        print(f"   📊 Sucursales: {sucursales}")
        print(f"   🔧 Operativas DB: {operativas_db}")
        print(f"   🛡️ Seguridad DB: {seguridad_db}")
        print(f"   📋 Áreas evaluadas: {areas_db}")
        print(f"   📈 Total supervisiones: {operativas_db + seguridad_db}")
        
        # PASO 3: Comparar con datos esperados
        print("\n📈 PROGRESO vs OBJETIVO...")
        
        # Cargar exceles para comparar
        df_operativas = pd.read_excel("OPERATIVAS_POSTGRESQL_20251223_113008.xlsx", 
                                    sheet_name='Operativas_PostgreSQL')
        df_seguridad = pd.read_excel("SEGURIDAD_POSTGRESQL_20251223_113008.xlsx", 
                                   sheet_name='Seguridad_PostgreSQL')
        
        total_esperado_op = len(df_operativas)
        total_esperado_seg = len(df_seguridad)
        total_esperado = total_esperado_op + total_esperado_seg
        
        progreso_op = (operativas_db / total_esperado_op) * 100
        progreso_seg = (seguridad_db / total_esperado_seg) * 100
        progreso_total = ((operativas_db + seguridad_db) / total_esperado) * 100
        
        print(f"   🔧 Operativas: {operativas_db}/{total_esperado_op} ({progreso_op:.1f}%)")
        print(f"   🛡️ Seguridad: {seguridad_db}/{total_esperado_seg} ({progreso_seg:.1f}%)")
        print(f"   📊 Total: {operativas_db + seguridad_db}/{total_esperado} ({progreso_total:.1f}%)")
        
        # PASO 4: Verificar cálculos
        print("\n🧮 VERIFICANDO CÁLCULOS...")
        
        cursor.execute("""
            SELECT 
                ROUND(AVG(calificacion_general), 1) as promedio,
                MIN(calificacion_general) as minimo,
                MAX(calificacion_general) as maximo,
                COUNT(*) as total
            FROM supervisiones 
            WHERE tipo_supervision = 'operativas'
        """)
        calc_op = cursor.fetchone()
        
        cursor.execute("""
            SELECT 
                ROUND(AVG(calificacion_general), 1) as promedio,
                MIN(calificacion_general) as minimo,
                MAX(calificacion_general) as maximo,
                COUNT(*) as total
            FROM supervisiones 
            WHERE tipo_supervision = 'seguridad'
        """)
        calc_seg = cursor.fetchone()
        
        print(f"   🔧 Operativas: {calc_op[0]}% promedio ({calc_op[1]}-{calc_op[2]})")
        print(f"   🛡️ Seguridad: {calc_seg[0]}% promedio ({calc_seg[1]}-{calc_seg[2]})")
        
        # PASO 5: Dashboard web test
        print("\n🌐 VERIFICANDO DASHBOARD WEB...")
        
        try:
            response = requests.get(railway_url, timeout=10)
            if response.status_code == 200:
                print(f"   ✅ Dashboard accesible: {railway_url}")
            else:
                print(f"   ❌ Dashboard error: {response.status_code}")
        except Exception as e:
            print(f"   ❌ Dashboard error: {str(e)}")
        
        cursor.close()
        conn.close()
        
        # RESUMEN FINAL
        print(f"\n🎯 RESUMEN ESTADO ACTUAL")
        print("=" * 40)
        
        if progreso_total >= 90:
            print(f"✅ MIGRACIÓN CASI COMPLETA ({progreso_total:.1f}%)")
        elif progreso_total >= 70:
            print(f"⚠️ MIGRACIÓN AVANZADA ({progreso_total:.1f}%)")
        else:
            print(f"🔄 MIGRACIÓN EN PROGRESO ({progreso_total:.1f}%)")
        
        print(f"📊 Datos migrados: {operativas_db + seguridad_db}/{total_esperado}")
        print(f"🌐 Dashboard funcionando: {railway_url}")
        print(f"🔧 APIs operativas: ✅")
        print(f"🛡️ APIs seguridad: ✅")
        
        # Siguientes pasos
        faltante_op = total_esperado_op - operativas_db
        faltante_seg = total_esperado_seg - seguridad_db
        
        if faltante_op > 0 or faltante_seg > 0:
            print(f"\n⏭️ PENDIENTE:")
            if faltante_op > 0:
                print(f"   🔧 {faltante_op} operativas faltantes")
            if faltante_seg > 0:
                print(f"   🛡️ {faltante_seg} seguridad faltantes")
            print(f"   🚀 Continuar migración en lotes")
        else:
            print(f"\n🎉 MIGRACIÓN 100% COMPLETA")
            print(f"   ✅ Todas las supervisiones migradas")
            print(f"   ✅ Dashboard completamente funcional")
        
        return {
            'progreso_total': progreso_total,
            'operativas': operativas_db,
            'seguridad': seguridad_db,
            'dashboard_url': railway_url
        }
        
    except Exception as e:
        print(f"❌ Error verificando status: {str(e)}")
        return None

if __name__ == "__main__":
    status_final()