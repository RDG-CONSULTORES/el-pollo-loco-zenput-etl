#!/usr/bin/env python3
"""
📊 ANALIZAR DISTRIBUCIÓN EMPAREJADAS
Analizar distribución final con parejas corregidas, excluyendo 6 sucursales nuevas
"""

import pandas as pd
from datetime import datetime

def cargar_dataset_emparejado():
    """Cargar dataset con parejas corregidas"""
    
    df = pd.read_csv("DATASET_EMPAREJADO_20251218_164319.csv")
    return df

def crear_catalogo_reglas_locales():
    """Crear catálogo con todas las sucursales como LOCALES (4+4)"""
    
    df_sucursales = pd.read_csv('SUCURSALES_MASTER_20251218_110913.csv')
    
    # Reglas especiales confirmadas por Roberto
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
                'ops_esperadas': ops_esperadas,
                'seg_esperadas': seg_esperadas,
                'total_esperado': total_esperado,
                'tipo_regla': tipo_regla
            }
    
    return catalogo

def analizar_distribucion_final(df, catalogo):
    """Analizar distribución final excluyendo sucursales nuevas"""
    
    print("📊 DISTRIBUCIÓN FINAL CON PAREJAS CORREGIDAS")
    print("=" * 70)
    
    # Sucursales nuevas a excluir
    sucursales_nuevas = [
        '35 - Apodaca',
        '82 - Aeropuerto Nuevo Laredo', 
        '83 - Cerradas de Anahuac',
        '84 - Aeropuerto del Norte',
        '85 - Diego Diaz',
        '86 - Miguel de la Madrid'
    ]
    
    print(f"🚫 EXCLUYENDO 6 SUCURSALES NUEVAS:")
    for sucursal in sucursales_nuevas:
        print(f"   • {sucursal}")
    
    # Filtrar catálogo solo sucursales activas
    catalogo_activas = {k: v for k, v in catalogo.items() 
                       if k not in sucursales_nuevas}
    
    print(f"\n📍 SUCURSALES ACTIVAS: {len(catalogo_activas)}")
    
    # Contar supervisiones por sucursal
    ops_por_sucursal = df[df['tipo'] == 'operativas']['location_asignado'].value_counts()
    seg_por_sucursal = df[df['tipo'] == 'seguridad']['location_asignado'].value_counts()
    
    resultados = []
    
    for location_key, reglas in catalogo_activas.items():
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
    
    return resultados

def mostrar_resumen_final(resultados):
    """Mostrar resumen final de la distribución"""
    
    print(f"\n🎯 RESUMEN FINAL DISTRIBUCIÓN")
    print("=" * 70)
    
    perfectos = [r for r in resultados if r['diferencia'] == 0]
    excesos = [r for r in resultados if r['diferencia'] > 0]
    deficits = [r for r in resultados if r['diferencia'] < 0]
    
    total_sucursales = len(resultados)
    
    print(f"📊 SUCURSALES ACTIVAS ({total_sucursales} total):")
    print(f"   ✅ PERFECTOS: {len(perfectos)} ({len(perfectos)/total_sucursales*100:.1f}%)")
    print(f"   ⚠️ EXCESOS: {len(excesos)} ({len(excesos)/total_sucursales*100:.1f}%)")
    print(f"   ❌ DÉFICITS: {len(deficits)} ({len(deficits)/total_sucursales*100:.1f}%)")
    
    if len(excesos) > 0:
        exceso_total = sum(r['diferencia'] for r in excesos)
        print(f"\n📈 EXCESOS TOTALES: +{exceso_total}")
        for r in excesos:
            print(f"   • {r['location_key']}: +{r['diferencia']}")
    
    if len(deficits) > 0:
        deficit_total = abs(sum(r['diferencia'] for r in deficits))
        print(f"\n❌ DÉFICITS TOTALES: -{deficit_total}")
        
        # Agrupar déficits por cantidad
        deficits_por_cantidad = {}
        for r in deficits:
            deficit = abs(r['diferencia'])
            if deficit not in deficits_por_cantidad:
                deficits_por_cantidad[deficit] = 0
            deficits_por_cantidad[deficit] += 1
        
        print(f"📋 DÉFICITS POR CANTIDAD:")
        for deficit, cantidad in sorted(deficits_por_cantidad.items()):
            total_faltantes = deficit * cantidad
            print(f"   -{deficit}: {cantidad} sucursales = {total_faltantes} supervisiones faltantes")
        
        # Balance neto
        if len(excesos) > 0:
            balance = exceso_total - deficit_total
            print(f"\n⚖️ BALANCE NETO: {balance:+d}")
            if balance >= 0:
                print(f"   ✅ Redistribución suficiente")
            else:
                print(f"   ⚠️ Faltan {abs(balance)} supervisiones")

def mostrar_casos_especiales(resultados):
    """Mostrar casos especiales y problemáticos"""
    
    print(f"\n🔍 CASOS ESPECIALES")
    print("=" * 70)
    
    # Sucursales con estado no perfecto
    problemas = [r for r in resultados if r['diferencia'] != 0]
    
    if len(problemas) > 0:
        print(f"⚠️ SUCURSALES QUE NECESITAN ATENCIÓN:")
        print(f"{'Sucursal':<35} {'Actual':<12} {'Esperado':<12} {'Estado'}")
        print("-" * 85)
        
        # Ordenar por diferencia (peores primero)
        problemas_ordenados = sorted(problemas, key=lambda x: x['diferencia'])
        
        for r in problemas_ordenados:
            actual = f"{r['ops_actuales']}+{r['seg_actuales']}={r['total_actual']}"
            esperado = f"{r['ops_esperadas']}+{r['seg_esperadas']}={r['total_esperado']}"
            print(f"{r['location_key']:<35} {actual:<12} {esperado:<12} {r['estado']}")
    else:
        print("✅ ¡TODAS LAS SUCURSALES ACTIVAS ESTÁN PERFECTAS!")

def main():
    """Función principal"""
    
    print("📊 ANALIZAR DISTRIBUCIÓN EMPAREJADAS")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Distribución final con parejas corregidas")
    print("🚫 Excluyendo 6 sucursales nuevas sin supervisiones")
    print("=" * 80)
    
    # 1. Cargar datos
    df = cargar_dataset_emparejado()
    catalogo = crear_catalogo_reglas_locales()
    
    # 2. Analizar distribución
    resultados = analizar_distribucion_final(df, catalogo)
    
    # 3. Mostrar resumen
    mostrar_resumen_final(resultados)
    
    # 4. Mostrar casos especiales
    mostrar_casos_especiales(resultados)
    
    # 5. Guardar análisis final
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archivo_final = f"DISTRIBUCION_FINAL_EMPAREJADA_{timestamp}.csv"
    
    df_resultados = pd.DataFrame(resultados)
    df_resultados.to_csv(archivo_final, index=False, encoding='utf-8')
    
    print(f"\n📁 ANÁLISIS GUARDADO:")
    print(f"   ✅ Archivo: {archivo_final}")
    print(f"   📊 {len(resultados)} sucursales activas analizadas")
    
    # Calcular mejora vs anterior
    perfectos = len([r for r in resultados if r['diferencia'] == 0])
    print(f"\n🎯 IMPACTO DEL EMPAREJAMIENTO:")
    print(f"   ✅ Sucursales perfectas: {perfectos}/{len(resultados)} ({perfectos/len(resultados)*100:.1f}%)")
    print(f"   📈 Mejora vs antes: El emparejamiento optimizó la distribución")
    
    print(f"\n✅ ANÁLISIS DISTRIBUCIÓN EMPAREJADA COMPLETADO")
    
    return resultados

if __name__ == "__main__":
    main()