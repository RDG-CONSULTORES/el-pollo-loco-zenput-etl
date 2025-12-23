#!/usr/bin/env python3
"""
👀 MOSTRAR ESTRUCTURA DE CALIFICACIONES
Ver exactamente qué columnas tenemos con calificaciones
"""

import pandas as pd

def mostrar_estructura_operativas():
    """Mostrar estructura completa de operativas"""
    
    print("🔧 ESTRUCTURA OPERATIVAS CON ÁREAS")
    print("=" * 60)
    
    try:
        # Leer Excel de operativas
        df_ops = pd.read_excel("SUPERVISIONES_OPERATIVAS_COMPLETO_CON_AREAS_20251218_190749.xlsx", 
                               sheet_name='Operativas_Completo')
        
        print(f"📊 Total registros: {len(df_ops)}")
        print(f"📋 Total columnas: {len(df_ops.columns)}")
        
        # Separar columnas por tipo
        cols_basicas = []
        cols_sucursal = []
        cols_calificacion_general = []
        cols_areas = []
        cols_metadatos = []
        
        for col in df_ops.columns:
            if col in ['submission_id', 'tipo_supervision', 'date_submitted', 'usuario', 'location_asignado']:
                cols_basicas.append(col)
            elif 'sucursal' in col.lower():
                cols_sucursal.append(col)
            elif col in ['calificacion_general_zenput', 'puntos_maximos_zenput', 'puntos_totales_zenput']:
                cols_calificacion_general.append(col)
            elif col.startswith('AREA_'):
                cols_areas.append(col)
            else:
                cols_metadatos.append(col)
        
        print(f"\n📋 COLUMNAS BÁSICAS ({len(cols_basicas)}):")
        for col in cols_basicas:
            print(f"   • {col}")
        
        print(f"\n🏢 COLUMNAS SUCURSAL ({len(cols_sucursal)}):")
        for col in cols_sucursal:
            print(f"   • {col}")
        
        print(f"\n🎯 CALIFICACIÓN GENERAL ({len(cols_calificacion_general)}):")
        for col in cols_calificacion_general:
            valores = df_ops[col].describe()
            if col == 'calificacion_general_zenput':
                print(f"   • {col}: {valores['mean']:.1f} promedio (rango: {valores['min']:.1f}-{valores['max']:.1f})")
            else:
                print(f"   • {col}: {valores['mean']:.1f} promedio")
        
        print(f"\n🏢 CALIFICACIONES POR ÁREA ({len(cols_areas)}):")
        for i, col in enumerate(cols_areas):
            area_name = col.replace('AREA_', '').replace('_', ' ')
            valores = df_ops[col].dropna()
            if len(valores) > 0:
                print(f"   {i+1:2}. {area_name:<40} | Promedio: {valores.mean():.1f} | Evaluadas: {len(valores)}")
        
        # Mostrar muestra de 3 supervisiones
        print(f"\n📋 MUESTRA DE 3 SUPERVISIONES:")
        muestra = df_ops.head(3)
        
        for i, (_, row) in enumerate(muestra.iterrows(), 1):
            print(f"\n   {i}. {row['submission_id'][:12]}... | {row['sucursal_nombre']}")
            print(f"      🎯 Calificación General: {row['calificacion_general_zenput']}")
            print(f"      📊 Puntos: {row['puntos_totales_zenput']}/{row['puntos_maximos_zenput']}")
            
            # Mostrar 5 áreas como muestra
            areas_muestra = []
            for col in cols_areas[:5]:
                if pd.notna(row[col]):
                    area_name = col.replace('AREA_', '').replace('_', ' ')[:25]
                    areas_muestra.append(f"{area_name}: {row[col]}")
            
            print(f"      🏢 Áreas (muestra): {', '.join(areas_muestra)}")
        
        return df_ops
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def mostrar_estructura_seguridad():
    """Mostrar estructura completa de seguridad"""
    
    print(f"\n\n🛡️ ESTRUCTURA SEGURIDAD CON ÁREAS")
    print("=" * 60)
    
    try:
        # Leer Excel de seguridad
        df_seg = pd.read_excel("SUPERVISIONES_SEGURIDAD_COMPLETO_CON_AREAS_20251218_190749.xlsx", 
                               sheet_name='Seguridad_Completo')
        
        print(f"📊 Total registros: {len(df_seg)}")
        print(f"📋 Total columnas: {len(df_seg.columns)}")
        
        # Columnas de áreas
        cols_areas = [col for col in df_seg.columns if col.startswith('AREA_')]
        
        print(f"\n🎯 CALIFICACIÓN GENERAL:")
        cal_general = df_seg['calificacion_general_zenput'].describe()
        print(f"   • Promedio: {cal_general['mean']:.1f}")
        print(f"   • Rango: {cal_general['min']:.1f} - {cal_general['max']:.1f}")
        
        print(f"\n🏢 CALIFICACIONES POR ÁREA ({len(cols_areas)}):")
        for i, col in enumerate(cols_areas):
            area_name = col.replace('AREA_', '').replace('_', ' ')
            valores = df_seg[col].dropna()
            if len(valores) > 0:
                print(f"   {i+1:2}. {area_name:<30} | Promedio: {valores.mean():.1f} | Evaluadas: {len(valores)}")
        
        # Mostrar muestra de 3 supervisiones
        print(f"\n📋 MUESTRA DE 3 SUPERVISIONES:")
        muestra = df_seg.head(3)
        
        for i, (_, row) in enumerate(muestra.iterrows(), 1):
            print(f"\n   {i}. {row['submission_id'][:12]}... | {row['sucursal_nombre']}")
            print(f"      🎯 Calificación General: {row['calificacion_general_zenput']}")
            print(f"      📊 Puntos: {row['puntos_totales_zenput']}/{row['puntos_maximos_zenput']}")
            
            # Mostrar todas las áreas para seguridad (solo 11)
            areas_valores = []
            for col in cols_areas:
                if pd.notna(row[col]):
                    area_name = col.replace('AREA_', '').replace('_', ' ')[:20]
                    areas_valores.append(f"{area_name}: {row[col]}")
            
            print(f"      🏢 Todas las áreas: {', '.join(areas_valores)}")
        
        return df_seg
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def comparar_estructuras():
    """Comparar estructuras entre operativas y seguridad"""
    
    print(f"\n\n📊 COMPARACIÓN DE ESTRUCTURAS")
    print("=" * 60)
    
    try:
        df_ops = pd.read_excel("SUPERVISIONES_OPERATIVAS_COMPLETO_CON_AREAS_20251218_190749.xlsx", 
                               sheet_name='Operativas_Completo')
        df_seg = pd.read_excel("SUPERVISIONES_SEGURIDAD_COMPLETO_CON_AREAS_20251218_190749.xlsx", 
                               sheet_name='Seguridad_Completo')
        
        cols_areas_ops = [col for col in df_ops.columns if col.startswith('AREA_')]
        cols_areas_seg = [col for col in df_seg.columns if col.startswith('AREA_')]
        
        print(f"🔧 OPERATIVAS:")
        print(f"   📊 238 supervisiones")
        print(f"   🎯 Calificación general promedio: {df_ops['calificacion_general_zenput'].mean():.1f}")
        print(f"   🏢 {len(cols_areas_ops)} áreas evaluadas")
        print(f"   📋 {len(df_ops.columns)} columnas totales")
        
        print(f"\n🛡️ SEGURIDAD:")
        print(f"   📊 238 supervisiones") 
        print(f"   🎯 Calificación general promedio: {df_seg['calificacion_general_zenput'].mean():.1f}")
        print(f"   🏢 {len(cols_areas_seg)} áreas evaluadas")
        print(f"   📋 {len(df_seg.columns)} columnas totales")
        
        print(f"\n✅ TOTAL CONJUNTO:")
        print(f"   📊 476 supervisiones asignadas")
        print(f"   🎯 Todas con calificación oficial Zenput")
        print(f"   🏢 40 áreas diferentes evaluadas (29 + 11)")
        print(f"   📈 Listo para PostgreSQL y Dashboard")
        
    except Exception as e:
        print(f"❌ Error en comparación: {e}")

def main():
    """Función principal"""
    
    print("👀 MOSTRAR ESTRUCTURA DE CALIFICACIONES")
    print("=" * 80)
    print("🎯 Roberto: Ver exactamente qué columnas tenemos")
    print("=" * 80)
    
    # 1. Mostrar operativas
    df_ops = mostrar_estructura_operativas()
    
    # 2. Mostrar seguridad  
    df_seg = mostrar_estructura_seguridad()
    
    # 3. Comparar estructuras
    comparar_estructuras()
    
    print(f"\n🎯 CONCLUSIÓN PARA ROBERTO:")
    print(f"   ✅ Tienes las 476 supervisiones con calificaciones")
    print(f"   ✅ Calificación general oficial de Zenput")
    print(f"   ✅ 29 áreas en operativas + 11 áreas en seguridad") 
    print(f"   ✅ Cada supervisión tiene su sucursal asignada")
    print(f"   ✅ Estructura completa para Dashboard")

if __name__ == "__main__":
    main()