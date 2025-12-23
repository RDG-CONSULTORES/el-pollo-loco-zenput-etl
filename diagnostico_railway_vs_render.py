#!/usr/bin/env python3
"""
🔍 DIAGNÓSTICO COMPLETO RAILWAY vs RENDER
Comparar datos y cálculos entre dashboard original y Railway
Roberto: Análisis completo antes de continuar
"""

import requests
import pandas as pd
import psycopg2
from datetime import datetime
import json

def diagnostico_completo():
    """Diagnóstico completo Railway vs Render"""
    
    print("🔍 DIAGNÓSTICO COMPLETO - RAILWAY vs RENDER")
    print("=" * 80)
    print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Roberto: Análisis completo antes de continuar")
    print("=" * 80)
    
    # URLs
    render_url = "https://pollo-loco-supervision.onrender.com"
    railway_url = "https://el-pollo-loco-zenput-etl-production.up.railway.app"
    railway_db = "postgresql://postgres:tWeSxUREoYODoFroTAurHwcisymBotbz@yamanote.proxy.rlwy.net:29534/railway"
    
    # PASO 1: Comparar KPIs Dashboard Original vs Railway
    print("📊 COMPARANDO KPIS - RENDER vs RAILWAY")
    print("=" * 50)
    
    # KPIs Render Original
    try:
        response = requests.get(f"{render_url}/api/kpis", timeout=10)
        kpis_render = response.json()
        print("✅ RENDER - KPIs originales:")
        print(f"   📊 Performance Red: {kpis_render.get('averageScore', 'N/A')}%")
        print(f"   🏢 Sucursales: {kpis_render.get('totalLocations', 'N/A')}")
        print(f"   🔧 Grupos: {kpis_render.get('activeGroups', 'N/A')}")
        print(f"   📋 Supervisiones: {kpis_render.get('totalSupervisions', 'N/A')}")
    except Exception as e:
        print(f"❌ Error KPIs Render: {str(e)}")
        kpis_render = {}
    
    # KPIs Railway Operativas
    try:
        response = requests.get(f"{railway_url}/api/operativas/kpis", timeout=10)
        kpis_railway_op = response.json()
        print("\n🔧 RAILWAY - KPIs operativas:")
        print(f"   📊 Performance: {kpis_railway_op.get('promedio_general', 'N/A')}%")
        print(f"   🏢 Sucursales: {kpis_railway_op.get('total_sucursales', 'N/A')}")
        print(f"   🔧 Grupos: {kpis_railway_op.get('total_grupos', 'N/A')}")
        print(f"   📋 Supervisiones: {kpis_railway_op.get('total_supervisiones', 'N/A')}")
    except Exception as e:
        print(f"❌ Error KPIs Railway: {str(e)}")
        kpis_railway_op = {}
    
    # PASO 2: Analizar datos en bases de datos
    print("\n🗄️ ANÁLISIS BASE DE DATOS RAILWAY")
    print("=" * 40)
    
    try:
        conn = psycopg2.connect(railway_db)
        cursor = conn.cursor()
        
        # Análisis detallado
        queries = [
            ("Total Supervisiones", "SELECT COUNT(*) FROM supervisiones"),
            ("Supervisiones por Tipo", """
                SELECT tipo_supervision, COUNT(*) 
                FROM supervisiones 
                GROUP BY tipo_supervision
            """),
            ("Sucursales con Supervisiones", """
                SELECT COUNT(DISTINCT s.id) 
                FROM sucursales s 
                JOIN supervisiones sup ON s.id = sup.sucursal_id
            """),
            ("Rango Fechas Supervisiones", """
                SELECT 
                    MIN(fecha_supervision) as primera,
                    MAX(fecha_supervision) as ultima
                FROM supervisiones
            """),
            ("Promedio por Tipo", """
                SELECT 
                    tipo_supervision, 
                    ROUND(AVG(calificacion_general), 2) as promedio,
                    MIN(calificacion_general) as minimo,
                    MAX(calificacion_general) as maximo
                FROM supervisiones 
                GROUP BY tipo_supervision
            """),
            ("Top Grupos con más Supervisiones", """
                SELECT 
                    s.grupo_operativo,
                    COUNT(sup.id) as total_supervisiones,
                    ROUND(AVG(sup.calificacion_general), 1) as promedio
                FROM sucursales s
                JOIN supervisiones sup ON s.id = sup.sucursal_id
                GROUP BY s.grupo_operativo
                ORDER BY total_supervisiones DESC
                LIMIT 10
            """)
        ]
        
        for name, query in queries:
            print(f"\n📋 {name}:")
            cursor.execute(query)
            results = cursor.fetchall()
            
            if len(results) == 1 and len(results[0]) == 1:
                print(f"   📊 {results[0][0]}")
            else:
                for row in results:
                    print(f"   📊 {row}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error análisis DB: {str(e)}")
    
    # PASO 3: Comparar estructura dashboard original
    print(f"\n📱 ANÁLISIS DASHBOARD ORIGINAL RENDER")
    print("=" * 50)
    
    try:
        # Dashboard data original
        response = requests.get(f"{render_url}/api/dashboard-data", timeout=10)
        dashboard_render = response.json()
        
        print("✅ ESTRUCTURA DASHBOARD ORIGINAL:")
        print(f"   📊 Grupos: {len(dashboard_render.get('grupos', []))}")
        print(f"   🎯 Áreas: {len(dashboard_render.get('areas', []))}")
        print(f"   📈 Tendencias: {len(dashboard_render.get('tendencias', []))}")
        
        # Mostrar algunos grupos para comparar
        if dashboard_render.get('grupos'):
            print(f"\n🔧 TOP 5 GRUPOS RENDER:")
            for grupo in dashboard_render['grupos'][:5]:
                print(f"   📋 {grupo.get('grupo', 'N/A')}: {grupo.get('promedio', 'N/A')}%")
        
        if dashboard_render.get('areas'):
            print(f"\n🎯 TOP 5 ÁREAS RENDER:")
            for area in dashboard_render['areas'][:5]:
                print(f"   📋 {area.get('area', 'N/A')}: {area.get('promedio', 'N/A')}%")
                
    except Exception as e:
        print(f"❌ Error dashboard Render: {str(e)}")
    
    # PASO 4: Comparar con Railway dashboard
    print(f"\n🚀 ANÁLISIS DASHBOARD RAILWAY")
    print("=" * 40)
    
    try:
        response = requests.get(f"{railway_url}/api/operativas/dashboard", timeout=10)
        dashboard_railway = response.json()
        
        print(f"✅ DASHBOARD RAILWAY OPERATIVAS:")
        print(f"   📊 Registros: {len(dashboard_railway)}")
        
        # Agrupar por grupo operativo
        grupos_railway = {}
        for item in dashboard_railway:
            grupo = item.get('grupo_operativo')
            if grupo:
                if grupo not in grupos_railway:
                    grupos_railway[grupo] = []
                grupos_railway[grupo].append(item.get('promedio_calificacion', 0))
        
        # Calcular promedios
        grupos_promedio = {}
        for grupo, califs in grupos_railway.items():
            califs_validas = [c for c in califs if c is not None]
            if califs_validas:
                grupos_promedio[grupo] = sum(califs_validas) / len(califs_validas)
        
        print(f"\n🔧 GRUPOS RAILWAY (calculados):")
        for grupo, promedio in sorted(grupos_promedio.items(), key=lambda x: x[1] or 0, reverse=True)[:10]:
            print(f"   📋 {grupo}: {promedio:.1f}%")
            
    except Exception as e:
        print(f"❌ Error dashboard Railway: {str(e)}")
    
    # PASO 5: Análisis datos faltantes
    print(f"\n⚠️ ANÁLISIS DATOS FALTANTES")
    print("=" * 40)
    
    # Verificar exceles originales
    try:
        df_operativas = pd.read_excel("OPERATIVAS_POSTGRESQL_20251223_113008.xlsx", 
                                    sheet_name='Operativas_PostgreSQL')
        df_seguridad = pd.read_excel("SEGURIDAD_POSTGRESQL_20251223_113008.xlsx", 
                                   sheet_name='Seguridad_PostgreSQL')
        
        print(f"📊 DATOS DISPONIBLES EN EXCELES:")
        print(f"   🔧 Operativas en Excel: {len(df_operativas)}")
        print(f"   🛡️ Seguridad en Excel: {len(df_seguridad)}")
        print(f"   📋 Total en Excel: {len(df_operativas) + len(df_seguridad)}")
        
        print(f"\n📊 DATOS EN RAILWAY:")
        print(f"   🔧 Operativas en Railway: {kpis_railway_op.get('total_supervisiones', 0)}")
        print(f"   🛡️ Seguridad en Railway: 0")
        print(f"   📋 Total en Railway: {kpis_railway_op.get('total_supervisiones', 0)}")
        
        # Calcular faltantes
        faltante_op = len(df_operativas) - kpis_railway_op.get('total_supervisiones', 0)
        faltante_seg = len(df_seguridad)
        
        print(f"\n❌ DATOS FALTANTES:")
        print(f"   🔧 Operativas faltantes: {faltante_op}")
        print(f"   🛡️ Seguridad faltantes: {faltante_seg}")
        print(f"   📋 Total faltante: {faltante_op + faltante_seg}")
        
    except Exception as e:
        print(f"❌ Error análisis exceles: {str(e)}")
    
    # PASO 6: Recomendaciones
    print(f"\n🎯 RECOMENDACIONES PARA CORREGIR")
    print("=" * 50)
    
    print("✅ PROBLEMAS IDENTIFICADOS:")
    print("   1. 📊 Datos parciales: Solo 50 de 238+ operativas")
    print("   2. 🛡️ Seguridad faltante: 0 de 238+ seguridad")
    print("   3. 🧮 Cálculos diferentes: Posible diferencia en agrupación")
    print("   4. 📱 UI diferente: No réplica exacta iOS design")
    
    print(f"\n🔧 PLAN DE CORRECCIÓN:")
    print("   1. 📊 Migración completa: Todas las 476+ supervisiones")
    print("   2. 🧮 Verificar cálculos: Usar misma lógica que Render")
    print("   3. 📱 Clonar UI exacta: Copiar diseño iOS original")
    print("   4. 🔄 Toggle funcional: Ambos tipos con datos completos")
    
    print(f"\n⏭️ SIGUIENTES PASOS:")
    print("   1. ✅ Completar migración datos faltantes")
    print("   2. ✅ Verificar cálculos vs dashboard original")
    print("   3. ✅ Clonar diseño exacto iOS")
    print("   4. ✅ Testing completo ambos tipos")

def main():
    diagnostico_completo()

if __name__ == "__main__":
    main()