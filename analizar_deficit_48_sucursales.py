#!/usr/bin/env python3
"""
📊 ANALIZAR DÉFICIT DE 48 SUCURSALES
Identificar estrategias para completar 4+4=8 en sucursales con déficit
"""

import pandas as pd
from datetime import datetime

def cargar_analisis_completo():
    """Cargar análisis con todas las sucursales como LOCALES"""
    
    df_analisis = pd.read_csv("ANALISIS_TODAS_LOCALES_20251218_163400.csv")
    df_dataset = pd.read_csv("DATASET_FINAL_COMPLETO.csv")
    
    return df_analisis, df_dataset

def categorizar_deficits(df_analisis):
    """Categorizar los déficits por severidad y patrón"""
    
    print("📊 CATEGORIZACIÓN DE DÉFICITS")
    print("=" * 70)
    
    # Filtrar solo déficits
    deficits = df_analisis[df_analisis['diferencia'] < 0].copy()
    deficits['deficit_abs'] = abs(deficits['diferencia'])
    
    # Categorizar por severidad
    categorias = {
        'CRÍTICO (-8)': deficits[deficits['deficit_abs'] == 8],
        'SEVERO (-6)': deficits[deficits['deficit_abs'] == 6],
        'ALTO (-4)': deficits[deficits['deficit_abs'] == 4],
        'MODERADO (-3)': deficits[deficits['deficit_abs'] == 3],
        'BAJO (-2)': deficits[deficits['deficit_abs'] == 2],
        'MÍNIMO (-1)': deficits[deficits['deficit_abs'] == 1]
    }
    
    print(f"{'Categoría':<15} {'Cantidad':<8} {'Sucursales'}")
    print("-" * 70)
    
    total_supervisiones_faltantes = 0
    
    for categoria, grupo in categorias.items():
        if len(grupo) > 0:
            deficit_valor = int(categoria.split('(')[1].split(')')[0])
            supervisiones_faltantes = len(grupo) * abs(deficit_valor)
            total_supervisiones_faltantes += supervisiones_faltantes
            
            print(f"{categoria:<15} {len(grupo):<8} {supervisiones_faltantes} supervisiones faltantes")
            
            # Mostrar primeras 3 sucursales como ejemplo
            for i, (_, row) in enumerate(grupo.head(3).iterrows()):
                ops_seg = f"{row['ops_actuales']}+{row['seg_actuales']}"
                print(f"   • {row['location_key']} ({ops_seg})")
            
            if len(grupo) > 3:
                print(f"   ... y {len(grupo)-3} más")
            print()
    
    print(f"🚨 TOTAL SUPERVISIONES FALTANTES: {total_supervisiones_faltantes}")
    
    return deficits, categorias, total_supervisiones_faltantes

def analizar_disponibilidad_redistribucion(df_dataset, df_analisis):
    """Analizar qué supervisiones están disponibles para redistribuir"""
    
    print("\n🔄 DISPONIBILIDAD PARA REDISTRIBUCIÓN")
    print("=" * 70)
    
    # Sucursales con exceso
    excesos = df_analisis[df_analisis['diferencia'] > 0]
    
    print(f"📈 SUCURSALES CON EXCESO:")
    if len(excesos) > 0:
        for _, row in excesos.iterrows():
            print(f"   • {row['location_key']}: +{row['diferencia']} supervisiones")
    else:
        print("   ⚠️ No hay sucursales con exceso para redistribuir")
    
    # Calcular disponibilidad total
    exceso_total = excesos['diferencia'].sum() if len(excesos) > 0 else 0
    
    # Buscar sucursales sin submissions (pueden recibir redistribución)
    sucursales_sin_submissions = df_analisis[df_analisis['total_actual'] == 0]
    
    print(f"\n📍 SUCURSALES SIN SUBMISSIONS:")
    if len(sucursales_sin_submissions) > 0:
        print(f"   📊 {len(sucursales_sin_submissions)} sucursales sin ninguna supervisión:")
        for _, row in sucursales_sin_submissions.iterrows():
            print(f"   • {row['location_key']} (necesita 8 supervisiones)")
    else:
        print("   ✅ Todas las sucursales tienen al menos 1 supervisión")
    
    return exceso_total, sucursales_sin_submissions

def identificar_estrategias(deficits, exceso_total, total_faltantes):
    """Identificar estrategias para resolver los déficits"""
    
    print(f"\n💡 ESTRATEGIAS DE RESOLUCIÓN")
    print("=" * 70)
    
    print(f"📊 BALANCE GENERAL:")
    print(f"   🚨 Supervisiones faltantes: {total_faltantes}")
    print(f"   📈 Supervisiones disponibles: {exceso_total}")
    print(f"   ⚖️ Balance neto: {exceso_total - total_faltantes}")
    
    if exceso_total < total_faltantes:
        faltante_neto = total_faltantes - exceso_total
        print(f"\n⚠️ DÉFICIT NETO: -{faltante_neto} supervisiones")
        print(f"💡 NECESARIAS ESTRATEGIAS ADICIONALES:")
        print(f"   1. 🔄 Redistribuir las {exceso_total} disponibles")
        print(f"   2. 🆕 Generar {faltante_neto} supervisiones adicionales")
        print(f"   3. 📋 Revisar asignaciones incorrectas en submissions existentes")
        print(f"   4. 🔍 Buscar supervisiones no contabilizadas en el dataset")
    else:
        exceso_neto = exceso_total - total_faltantes
        print(f"\n✅ EXCESO NETO: +{exceso_neto} supervisiones")
        print(f"💡 ESTRATEGIA ÓPTIMA:")
        print(f"   1. 🔄 Redistribuir supervisiones existentes")
        print(f"   2. 📊 Quedarían {exceso_neto} supervisiones adicionales")

def proponer_redistribucion_prioritaria(deficits):
    """Proponer redistribución prioritaria basada en severidad"""
    
    print(f"\n🎯 REDISTRIBUCIÓN PRIORITARIA")
    print("=" * 70)
    
    # Priorizar por severidad (más fáciles de resolver primero)
    deficits_ordenados = deficits.sort_values(['deficit_abs', 'location_key'])
    
    print(f"📋 ORDEN DE PRIORIDAD (del más fácil al más difícil):")
    print(f"{'Prioridad':<10} {'Sucursal':<35} {'Actual':<10} {'Falta'}")
    print("-" * 75)
    
    for i, (_, row) in enumerate(deficits_ordenados.iterrows(), 1):
        actual_str = f"{row['ops_actuales']}+{row['seg_actuales']}={row['total_actual']}"
        falta = abs(row['diferencia'])
        prioridad = "ALTA" if falta <= 2 else "MEDIA" if falta <= 4 else "BAJA"
        
        print(f"{prioridad:<10} {row['location_key']:<35} {actual_str:<10} -{falta}")
        
        if i <= 10:  # Mostrar primeras 10
            continue
        elif i == 11:
            print(f"   ... y {len(deficits_ordenados)-10} más")
            break

def buscar_submissions_redistribuibles(df_dataset, deficits):
    """Buscar submissions específicas que pueden redistribuirse"""
    
    print(f"\n🔍 BUSCAR SUBMISSIONS REDISTRIBUIBLES")
    print("=" * 70)
    
    # Sucursales con exceso de 1 supervisión (Centrito Valle)
    centrito_valle = df_dataset[df_dataset['location_asignado'] == '71 - Centrito Valle']
    
    print(f"📍 CENTRITO VALLE (exceso +2):")
    print(f"   📊 Total submissions: {len(centrito_valle)}")
    
    if len(centrito_valle) > 0:
        ops_cv = centrito_valle[centrito_valle['tipo'] == 'operativas']
        seg_cv = centrito_valle[centrito_valle['tipo'] == 'seguridad']
        
        print(f"   🔧 Operativas: {len(ops_cv)} (esperado: 4)")
        print(f"   🛡️ Seguridad: {len(seg_cv)} (esperado: 4)")
        
        if len(ops_cv) > 4:
            print(f"   💡 Puede redistribuir {len(ops_cv)-4} operativas")
        if len(seg_cv) > 4:
            print(f"   💡 Puede redistribuir {len(seg_cv)-4} seguridad")
    
    # Buscar sucursales que necesitan solo 1-2 supervisiones (más fáciles)
    faciles = deficits[deficits['deficit_abs'].isin([1, 2])]
    
    print(f"\n🎯 SUCURSALES FÁCILES DE RESOLVER:")
    if len(faciles) > 0:
        for _, row in faciles.iterrows():
            falta = abs(row['diferencia'])
            actual = f"{row['ops_actuales']}+{row['seg_actuales']}"
            print(f"   • {row['location_key']}: {actual}, falta {falta}")
    else:
        print("   ⚠️ No hay sucursales con déficit menor a 3")

def main():
    """Función principal"""
    
    print("📊 ANALIZAR DÉFICIT DE 48 SUCURSALES")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Objetivo: Estrategias para completar 4+4=8 en sucursales con déficit")
    print("=" * 80)
    
    # 1. Cargar datos
    df_analisis, df_dataset = cargar_analisis_completo()
    
    # 2. Categorizar déficits
    deficits, categorias, total_faltantes = categorizar_deficits(df_analisis)
    
    # 3. Analizar disponibilidad
    exceso_total, sucursales_sin_submissions = analizar_disponibilidad_redistribucion(df_dataset, df_analisis)
    
    # 4. Identificar estrategias
    identificar_estrategias(deficits, exceso_total, total_faltantes)
    
    # 5. Proponer redistribución prioritaria
    proponer_redistribucion_prioritaria(deficits)
    
    # 6. Buscar submissions redistribuibles
    buscar_submissions_redistribuibles(df_dataset, deficits)
    
    # 7. Guardar análisis detallado
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archivo_deficit = f"ANALISIS_DEFICIT_DETALLADO_{timestamp}.csv"
    deficits.to_csv(archivo_deficit, index=False, encoding='utf-8')
    
    print(f"\n📁 ANÁLISIS GUARDADO:")
    print(f"   ✅ Archivo: {archivo_deficit}")
    print(f"   📊 {len(deficits)} sucursales con déficit analizadas")
    
    print(f"\n🎯 RESUMEN EJECUTIVO:")
    print(f"   🚨 48 sucursales necesitan {total_faltantes} supervisiones")
    print(f"   📈 Disponibles para redistribución: {exceso_total}")
    print(f"   ⚖️ Balance neto: {exceso_total - total_faltantes}")
    
    if exceso_total < total_faltantes:
        print(f"   💡 Necesarias {total_faltantes - exceso_total} supervisiones adicionales")
    else:
        print(f"   ✅ Redistribución suficiente para resolver todos los déficits")
    
    print(f"\n✅ ANÁLISIS DE DÉFICIT COMPLETADO")
    
    return deficits, categorias, total_faltantes, exceso_total

if __name__ == "__main__":
    main()