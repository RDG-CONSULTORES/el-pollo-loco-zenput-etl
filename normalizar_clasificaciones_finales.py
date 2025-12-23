#!/usr/bin/env python3
"""
🔧 NORMALIZAR CLASIFICACIONES FINALES
Roberto corrige: Foráneas son Locales, Guasave→Foránea, Harold R. Pape→Foránea
"""

import pandas as pd
from datetime import datetime

def cargar_datos():
    """Cargar datos actuales"""
    
    df_sucursales = pd.read_csv('SUCURSALES_MASTER_20251218_110913.csv')
    df_dataset = pd.read_csv("DATASET_EMPAREJADO_20251218_164319.csv")
    
    return df_sucursales, df_dataset

def aplicar_correcciones_roberto(df_sucursales):
    """Aplicar correcciones específicas de Roberto"""
    
    print("🔧 CORRECCIONES DE CLASIFICACIÓN - ROBERTO")
    print("=" * 70)
    
    df_corregido = df_sucursales.copy()
    
    # Sucursales nuevas a excluir del análisis
    sucursales_nuevas = [
        '35 - Apodaca',
        '82 - Aeropuerto Nuevo Laredo', 
        '83 - Cerradas de Anahuac',
        '84 - Aeropuerto del Norte',
        '85 - Diego Diaz',
        '86 - Miguel de la Madrid'
    ]
    
    correcciones_especificas = {
        # Roberto especificó estas correcciones
        '23 - Guasave': 'FORANEA',  # era LOCAL → FORÁNEA
        '57 - Harold R. Pape': 'FORANEA',  # era LOCAL → FORÁNEA
        '53 - Lienzo Charro': 'LOCAL',  # mantener LOCAL (era LOCAL)
    }
    
    print(f"📋 CORRECCIONES ESPECÍFICAS DE ROBERTO:")
    correcciones_aplicadas = 0
    
    for _, row in df_corregido.iterrows():
        if pd.notna(row['numero']) and pd.notna(row['nombre']):
            numero = int(row['numero'])
            nombre = row['nombre']
            location_key = f"{numero} - {nombre}"
            
            # Saltar sucursales nuevas
            if location_key in sucursales_nuevas:
                continue
            
            tipo_actual = row['tipo']
            
            # Aplicar correcciones específicas
            if location_key in correcciones_especificas:
                nuevo_tipo = correcciones_especificas[location_key]
                df_corregido.loc[df_corregido['numero'] == numero, 'tipo'] = nuevo_tipo
                print(f"   ✅ {location_key}: {tipo_actual} → {nuevo_tipo}")
                correcciones_aplicadas += 1
            
            # TODAS las demás que estaban como FORÁNEA → LOCAL
            elif tipo_actual == 'FORANEA' and location_key not in correcciones_especificas:
                df_corregido.loc[df_corregido['numero'] == numero, 'tipo'] = 'LOCAL'
                print(f"   🔄 {location_key}: FORANEA → LOCAL")
                correcciones_aplicadas += 1
    
    print(f"\n✅ CORRECCIONES APLICADAS: {correcciones_aplicadas}")
    
    return df_corregido

def crear_catalogo_corregido(df_sucursales_corregido):
    """Crear catálogo con clasificaciones corregidas"""
    
    print(f"\n📊 CATÁLOGO CON CLASIFICACIONES CORREGIDAS")
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
    
    # Reglas especiales confirmadas
    reglas_especiales = {
        '1 - Pino Suarez': {'ops': 3, 'seg': 3, 'total': 6, 'tipo': 'ESPECIAL_3_3'},
        '5 - Felix U. Gomez': {'ops': 3, 'seg': 3, 'total': 6, 'tipo': 'ESPECIAL_3_3'},
        '2 - Madero': {'ops': 3, 'seg': 3, 'total': 6, 'tipo': 'ESPECIAL_3_3'},
        '3 - Matamoros': {'ops': 3, 'seg': 3, 'total': 6, 'tipo': 'ESPECIAL_3_3'}
    }
    
    catalogo = {}
    contadores = {'LOCAL': 0, 'FORANEA': 0, 'ESPECIAL': 0}
    
    for _, row in df_sucursales_corregido.iterrows():
        if pd.notna(row['numero']) and pd.notna(row['nombre']):
            numero = int(row['numero'])
            nombre = row['nombre']
            location_key = f"{numero} - {nombre}"
            
            # Excluir sucursales nuevas
            if location_key in sucursales_nuevas:
                continue
            
            # Aplicar reglas específicas
            if location_key in reglas_especiales:
                regla = reglas_especiales[location_key]
                ops_esperadas = regla['ops']
                seg_esperadas = regla['seg'] 
                total_esperado = regla['total']
                tipo_regla = regla['tipo']
                contadores['ESPECIAL'] += 1
            else:
                # Usar tipo corregido
                tipo_corregido = row['tipo']
                if tipo_corregido == 'FORANEA':
                    ops_esperadas = 2
                    seg_esperadas = 2
                    total_esperado = 4
                    tipo_regla = 'FORANEA_2_2'
                    contadores['FORANEA'] += 1
                else:  # LOCAL
                    ops_esperadas = 4
                    seg_esperadas = 4
                    total_esperado = 8
                    tipo_regla = 'LOCAL_4_4'
                    contadores['LOCAL'] += 1
            
            catalogo[location_key] = {
                'tipo_corregido': row['tipo'],
                'grupo': row.get('grupo', ''),
                'ops_esperadas': ops_esperadas,
                'seg_esperadas': seg_esperadas,
                'total_esperado': total_esperado,
                'tipo_regla': tipo_regla
            }
    
    print(f"📊 DISTRIBUCIÓN CORREGIDA:")
    print(f"   🏢 LOCAL: {contadores['LOCAL']} sucursales")
    print(f"   🌍 FORÁNEA: {contadores['FORANEA']} sucursales") 
    print(f"   ⭐ ESPECIAL: {contadores['ESPECIAL']} sucursales")
    print(f"   📍 TOTAL ACTIVAS: {sum(contadores.values())}")
    
    return catalogo

def validar_distribucion_corregida(df_dataset, catalogo):
    """Validar distribución con clasificaciones corregidas"""
    
    print(f"\n✅ VALIDACIÓN CON CLASIFICACIONES CORREGIDAS")
    print("=" * 70)
    
    # Contar supervisiones actuales
    ops_por_sucursal = df_dataset[df_dataset['tipo'] == 'operativas']['location_asignado'].value_counts()
    seg_por_sucursal = df_dataset[df_dataset['tipo'] == 'seguridad']['location_asignado'].value_counts()
    
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
            'tipo_corregido': reglas['tipo_corregido'],
            'tipo_regla': reglas['tipo_regla'],
            'ops_actuales': ops_actuales,
            'seg_actuales': seg_actuales,
            'total_actual': total_actual,
            'total_esperado': total_esperado,
            'diferencia': diferencia,
            'estado': estado
        })
    
    # Resumen
    perfectos = len([r for r in resultados if r['diferencia'] == 0])
    excesos = len([r for r in resultados if r['diferencia'] > 0])
    deficits = len([r for r in resultados if r['diferencia'] < 0])
    
    print(f"📊 RESULTADOS CON CLASIFICACIONES CORREGIDAS:")
    print(f"   ✅ PERFECTOS: {perfectos}/{len(resultados)} ({perfectos/len(resultados)*100:.1f}%)")
    print(f"   ⚠️ EXCESOS: {excesos}")
    print(f"   ❌ DÉFICITS: {deficits}")
    
    # Mostrar casos problemáticos
    problemas = [r for r in resultados if r['diferencia'] != 0]
    if problemas:
        print(f"\n⚠️ CASOS QUE NECESITAN AJUSTE:")
        print(f"{'Sucursal':<35} {'Tipo':<8} {'Actual':<8} {'Esperado':<8} {'Estado'}")
        print("-" * 80)
        
        for r in sorted(problemas, key=lambda x: x['diferencia']):
            actual = f"{r['ops_actuales']}+{r['seg_actuales']}"
            # Buscar las reglas de esta sucursal específica
            reglas_suc = catalogo.get(r['location_key'], {})
            ops_esp = reglas_suc.get('ops_esperadas', 4)
            seg_esp = reglas_suc.get('seg_esperadas', 4)
            esperado = f"{ops_esp}+{seg_esp}"
            print(f"{r['location_key']:<35} {r['tipo_corregido']:<8} {actual:<8} {esperado:<8} {r['estado']}")
    
    return resultados

def main():
    """Función principal"""
    
    print("🔧 NORMALIZAR CLASIFICACIONES FINALES")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Roberto: Foráneas→Locales, Guasave→Foránea, Harold→Foránea")
    print("=" * 80)
    
    # 1. Cargar datos
    df_sucursales, df_dataset = cargar_datos()
    
    # 2. Aplicar correcciones de Roberto
    df_sucursales_corregido = aplicar_correcciones_roberto(df_sucursales)
    
    # 3. Crear catálogo corregido
    catalogo_corregido = crear_catalogo_corregido(df_sucursales_corregido)
    
    # 4. Validar distribución corregida
    resultados = validar_distribucion_corregida(df_dataset, catalogo_corregido)
    
    # 5. Guardar resultados
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Guardar catálogo corregido
    archivo_sucursales = f"SUCURSALES_CLASIFICACION_CORREGIDA_{timestamp}.csv"
    df_sucursales_corregido.to_csv(archivo_sucursales, index=False, encoding='utf-8')
    
    # Guardar análisis final
    archivo_analisis = f"ANALISIS_CLASIFICACION_CORREGIDA_{timestamp}.csv"
    df_resultados = pd.DataFrame(resultados)
    df_resultados.to_csv(archivo_analisis, index=False, encoding='utf-8')
    
    print(f"\n📁 ARCHIVOS GUARDADOS:")
    print(f"   ✅ Catálogo corregido: {archivo_sucursales}")
    print(f"   ✅ Análisis final: {archivo_analisis}")
    
    print(f"\n🎯 RESUMEN DE CORRECCIONES:")
    print(f"   🔧 Clasificaciones normalizadas según Roberto")
    print(f"   📊 {len(resultados)} sucursales activas analizadas")
    
    perfectos = len([r for r in resultados if r['diferencia'] == 0])
    print(f"   ✅ {perfectos} sucursales perfectamente balanceadas")
    
    print(f"\n✅ NORMALIZACIÓN DE CLASIFICACIONES COMPLETADA")
    
    return df_sucursales_corregido, catalogo_corregido, resultados

if __name__ == "__main__":
    main()