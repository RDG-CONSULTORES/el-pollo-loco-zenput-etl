#!/usr/bin/env python3
"""
📊 DÉFICIT REAL - SOLO SUCURSALES ACTIVAS
Excluir las 6 sucursales nuevas sin submissions como Roberto indicó
"""

import pandas as pd
from datetime import datetime

def analizar_deficit_sucursales_activas():
    """Analizar déficit excluyendo sucursales nuevas sin submissions"""
    
    print("📊 DÉFICIT REAL - SOLO SUCURSALES ACTIVAS")
    print("=" * 70)
    print("🚫 Excluyendo 6 sucursales nuevas sin submissions")
    print("=" * 70)
    
    # Cargar análisis
    df_analisis = pd.read_csv("ANALISIS_TODAS_LOCALES_20251218_163400.csv")
    
    # Excluir las 6 sucursales nuevas sin submissions
    sucursales_nuevas = [
        '35 - Apodaca',
        '82 - Aeropuerto Nuevo Laredo', 
        '83 - Cerradas de Anahuac',
        '84 - Aeropuerto del Norte',
        '85 - Diego Diaz',
        '86 - Miguel de la Madrid'
    ]
    
    print(f"🚫 SUCURSALES NUEVAS EXCLUIDAS:")
    for sucursal in sucursales_nuevas:
        print(f"   • {sucursal}")
    
    # Filtrar solo sucursales activas
    df_activas = df_analisis[~df_analisis['location_key'].isin(sucursales_nuevas)].copy()
    
    print(f"\n📊 ANÁLISIS SOLO SUCURSALES ACTIVAS:")
    print(f"   📍 Total sucursales activas: {len(df_activas)}")
    print(f"   📍 Sucursales excluidas: {len(sucursales_nuevas)}")
    
    # Categorizar resultados
    perfectos = df_activas[df_activas['diferencia'] == 0]
    excesos = df_activas[df_activas['diferencia'] > 0] 
    deficits = df_activas[df_activas['diferencia'] < 0]
    
    print(f"\n🎯 RESULTADOS SUCURSALES ACTIVAS:")
    print(f"   ✅ PERFECTOS: {len(perfectos)}/{len(df_activas)} ({len(perfectos)/len(df_activas)*100:.1f}%)")
    print(f"   ⚠️ EXCESOS: {len(excesos)} sucursales")
    print(f"   ❌ DÉFICITS: {len(deficits)} sucursales")
    
    if len(deficits) > 0:
        # Calcular déficit total real
        deficit_total = abs(deficits['diferencia'].sum())
        print(f"   🚨 Total supervisiones faltantes: {deficit_total}")
        
        # Categorizar déficits
        print(f"\n📋 DÉFICITS POR SEVERIDAD:")
        for deficit_val in sorted(deficits['diferencia'].unique()):
            grupo = deficits[deficits['diferencia'] == deficit_val]
            faltantes = len(grupo) * abs(deficit_val)
            print(f"   {deficit_val:>3}: {len(grupo):>2} sucursales = {faltantes:>3} supervisiones faltantes")
    
    # Disponibilidad para redistribución
    if len(excesos) > 0:
        exceso_total = excesos['diferencia'].sum()
        print(f"\n📈 DISPONIBLE PARA REDISTRIBUCIÓN:")
        for _, row in excesos.iterrows():
            print(f"   • {row['location_key']}: +{row['diferencia']}")
        print(f"   📊 Total disponible: {exceso_total}")
        
        if len(deficits) > 0:
            balance = exceso_total - deficit_total
            print(f"\n⚖️ BALANCE NETO: {balance:+d}")
            if balance >= 0:
                print(f"   ✅ Redistribución suficiente para resolver déficits")
            else:
                print(f"   ⚠️ Faltan {abs(balance)} supervisiones adicionales")
    
    return df_activas, perfectos, excesos, deficits

def mostrar_deficit_detallado(deficits):
    """Mostrar déficit detallado por sucursal"""
    
    if len(deficits) == 0:
        print("\n✅ No hay déficits en sucursales activas")
        return
    
    print(f"\n📋 DÉFICIT DETALLADO POR SUCURSAL:")
    print(f"{'Sucursal':<35} {'Actual':<10} {'Esperado':<10} {'Falta'}")
    print("-" * 75)
    
    for _, row in deficits.sort_values('diferencia').iterrows():
        actual = f"{row['ops_actuales']}+{row['seg_actuales']}={row['total_actual']}"
        esperado = f"{row['ops_esperadas']}+{row['seg_esperadas']}={row['total_esperado']}"
        falta = abs(row['diferencia'])
        print(f"{row['location_key']:<35} {actual:<10} {esperado:<10} -{falta}")

def main():
    """Función principal"""
    
    print("📊 DÉFICIT REAL - SOLO SUCURSALES ACTIVAS")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Roberto: Olvidar 6 sucursales nuevas sin submissions")
    print("📊 Dataset: 238 operativas + 238 seguridad = 476 total")
    print("=" * 80)
    
    # Analizar déficit real
    df_activas, perfectos, excesos, deficits = analizar_deficit_sucursales_activas()
    
    # Mostrar détails del déficit
    mostrar_deficit_detallado(deficits)
    
    # Guardar análisis corregido
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archivo_activas = f"SUCURSALES_ACTIVAS_DEFICIT_REAL_{timestamp}.csv"
    deficits.to_csv(archivo_activas, index=False, encoding='utf-8')
    
    print(f"\n📁 ANÁLISIS GUARDADO:")
    print(f"   ✅ Archivo: {archivo_activas}")
    print(f"   📊 {len(deficits)} sucursales activas con déficit")
    
    print(f"\n🎯 RESUMEN EJECUTIVO:")
    print(f"   📍 {len(df_activas)} sucursales activas analizadas")
    print(f"   ✅ {len(perfectos)} perfectas ({len(perfectos)/len(df_activas)*100:.1f}%)")
    print(f"   ❌ {len(deficits)} con déficit")
    print(f"   ⚠️ {len(excesos)} con exceso")
    
    if len(deficits) > 0:
        deficit_total = abs(deficits['diferencia'].sum())
        print(f"   🚨 Total supervisiones faltantes: {deficit_total}")
        
        if len(excesos) > 0:
            exceso_total = excesos['diferencia'].sum()
            balance = exceso_total - deficit_total
            print(f"   📈 Disponibles: {exceso_total}")
            print(f"   ⚖️ Balance: {balance:+d}")
    
    print(f"\n✅ ANÁLISIS DÉFICIT REAL COMPLETADO")
    
    return df_activas, deficits

if __name__ == "__main__":
    main()