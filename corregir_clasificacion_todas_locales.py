#!/usr/bin/env python3
"""
🏢 CORREGIR CLASIFICACIÓN - TODAS SON LOCALES
Roberto confirmó: todas las sucursales son LOCALES (4+4=8)
"""

import pandas as pd
from datetime import datetime

def corregir_catalogo_sucursales():
    """Corregir catálogo - todas las sucursales son LOCALES"""
    
    print("🏢 CORRECCIÓN: TODAS LAS SUCURSALES SON LOCALES")
    print("=" * 70)
    
    # Cargar catálogo actual
    df_sucursales = pd.read_csv('SUCURSALES_MASTER_20251218_110913.csv')
    
    print(f"📊 ESTADO ACTUAL:")
    tipo_counts = df_sucursales['tipo'].value_counts()
    for tipo, count in tipo_counts.items():
        print(f"   {tipo}: {count} sucursales")
    
    # Cambiar TODAS a LOCAL
    df_sucursales['tipo'] = 'LOCAL'
    
    print(f"\n✅ CORRECCIÓN APLICADA:")
    print(f"   LOCAL: {len(df_sucursales)} sucursales")
    print(f"   FORÁNEA: 0 sucursales")
    
    return df_sucursales

def crear_catalogo_reglas_corregido():
    """Crear catálogo con reglas corregidas - todo LOCAL 4+4"""
    
    df_sucursales = pd.read_csv('SUCURSALES_MASTER_20251218_110913.csv')
    
    # Reglas especiales confirmadas por Roberto (mantienen 3+3)
    reglas_especiales = {
        '1 - Pino Suarez': {'ops': 3, 'seg': 3, 'total': 6, 'tipo': 'ESPECIAL_3_3'},
        '5 - Felix U. Gomez': {'ops': 3, 'seg': 3, 'total': 6, 'tipo': 'ESPECIAL_3_3'},
        '2 - Madero': {'ops': 3, 'seg': 3, 'total': 6, 'tipo': 'ESPECIAL_3_3'},
        '3 - Matamoros': {'ops': 3, 'seg': 3, 'total': 6, 'tipo': 'ESPECIAL_3_3'}
    }
    
    catalogo = {}
    
    for _, row in df_sucursales.iterrows():
        if pd.notna(row['numero']) and pd.notna(row['nombre']):
            numero = int(row['numero'])
            nombre = row['nombre']
            location_key = f"{numero} - {nombre}"
            
            # Aplicar reglas específicas
            if location_key in reglas_especiales:
                regla = reglas_especiales[location_key]
                ops_esperadas = regla['ops']
                seg_esperadas = regla['seg'] 
                total_esperado = regla['total']
                tipo_regla = regla['tipo']
            else:
                # TODAS las demás son LOCAL 4+4
                ops_esperadas = 4
                seg_esperadas = 4
                total_esperado = 8
                tipo_regla = 'LOCAL_4_4'
            
            catalogo[location_key] = {
                'tipo_original': 'LOCAL',  # Ahora todas son LOCAL
                'grupo': row.get('grupo', ''),
                'ops_esperadas': ops_esperadas,
                'seg_esperadas': seg_esperadas,
                'total_esperado': total_esperado,
                'tipo_regla': tipo_regla
            }
    
    return catalogo

def analizar_con_clasificacion_corregida():
    """Analizar dataset con clasificación corregida"""
    
    print("\n🔍 ANÁLISIS CON CLASIFICACIÓN CORREGIDA")
    print("=" * 70)
    
    # Cargar dataset final
    df = pd.read_csv("DATASET_FINAL_COMPLETO.csv")
    catalogo = crear_catalogo_reglas_corregido()
    
    # Contar supervisiones actuales
    ops_por_sucursal = df[df['tipo'] == 'operativas']['location_asignado'].value_counts()
    seg_por_sucursal = df[df['tipo'] == 'seguridad']['location_asignado'].value_counts()
    
    # Analizar cada sucursal con reglas corregidas
    resultados = []
    
    for location_key, reglas in catalogo.items():
        ops_actuales = ops_por_sucursal.get(location_key, 0)
        seg_actuales = seg_por_sucursal.get(location_key, 0)
        total_actual = ops_actuales + seg_actuales
        total_esperado = reglas['total_esperado']
        diferencia = total_actual - total_esperado
        
        if diferencia == 0:
            estado = "✅ PERFECTO"
        elif diferencia > 0:
            estado = f"⚠️ EXCESO (+{diferencia})"
        else:
            estado = f"❌ DEFICIT ({diferencia})"
        
        resultados.append({
            'location_key': location_key,
            'tipo_regla': reglas['tipo_regla'],
            'ops_actuales': ops_actuales,
            'seg_actuales': seg_actuales,
            'total_actual': total_actual,
            'ops_esperadas': reglas['ops_esperadas'],
            'seg_esperadas': reglas['seg_esperadas'],
            'total_esperado': total_esperado,
            'diferencia': diferencia,
            'estado': estado
        })
    
    # Mostrar resumen
    perfectos = sum(1 for r in resultados if r['diferencia'] == 0)
    excesos = sum(1 for r in resultados if r['diferencia'] > 0)
    deficits = sum(1 for r in resultados if r['diferencia'] < 0)
    
    print(f"📊 RESUMEN CON REGLAS CORREGIDAS:")
    print(f"   ✅ PERFECTOS: {perfectos}/{len(resultados)} sucursales")
    print(f"   ⚠️ EXCESOS: {excesos} sucursales")
    print(f"   ❌ DÉFICITS: {deficits} sucursales")
    
    # Mostrar solo los que NO están perfectos
    problemas = [r for r in resultados if r['diferencia'] != 0]
    
    if problemas:
        print(f"\n🚨 SUCURSALES QUE AÚN NECESITAN AJUSTE:")
        print(f"{'Sucursal':<35} {'Actual':<10} {'Esperado':<10} {'Estado'}")
        print("-" * 80)
        
        for r in problemas:
            actual_str = f"{r['ops_actuales']}+{r['seg_actuales']}={r['total_actual']}"
            esperado_str = f"{r['ops_esperadas']}+{r['seg_esperadas']}={r['total_esperado']}"
            print(f"{r['location_key']:<35} {actual_str:<10} {esperado_str:<10} {r['estado']}")
    
    return resultados

def main():
    """Función principal"""
    
    print("🏢 CORREGIR CLASIFICACIÓN - TODAS SON LOCALES")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Roberto confirmó: todas las sucursales son LOCALES")
    print("=" * 80)
    
    # 1. Corregir catálogo
    df_corregido = corregir_catalogo_sucursales()
    
    # 2. Guardar catálogo corregido
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archivo_corregido = f"SUCURSALES_TODAS_LOCALES_{timestamp}.csv"
    df_corregido.to_csv(archivo_corregido, index=False, encoding='utf-8')
    print(f"\n📁 CATÁLOGO CORREGIDO GUARDADO: {archivo_corregido}")
    
    # 3. Analizar con clasificación corregida
    resultados = analizar_con_clasificacion_corregida()
    
    # 4. Guardar análisis corregido
    df_resultados = pd.DataFrame(resultados)
    archivo_analisis = f"ANALISIS_TODAS_LOCALES_{timestamp}.csv"
    df_resultados.to_csv(archivo_analisis, index=False, encoding='utf-8')
    print(f"📁 ANÁLISIS CORREGIDO GUARDADO: {archivo_analisis}")
    
    print(f"\n✅ CORRECCIÓN COMPLETADA - TODAS LAS SUCURSALES SON LOCALES")
    
    return df_corregido, resultados

if __name__ == "__main__":
    main()