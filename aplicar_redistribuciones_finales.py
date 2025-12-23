#!/usr/bin/env python3
"""
🔄 APLICAR REDISTRIBUCIONES FINALES
Implementar todas las redistribuciones confirmadas y revisar estado final
"""

import pandas as pd
from datetime import datetime

def cargar_dataset():
    """Cargar dataset normalizado"""
    df = pd.read_csv("DATASET_NORMALIZADO_20251218_155659.csv")
    return df

def cargar_catalogo_sucursales():
    """Cargar catálogo con reglas de negocio"""
    
    df_sucursales = pd.read_csv('SUCURSALES_MASTER_20251218_110913.csv')
    
    # Reglas especiales confirmadas por Roberto
    reglas_especiales = {
        '1 - Pino Suarez': {'ops': 3, 'seg': 3, 'total': 6, 'tipo': 'ESPECIAL_3_3'},
        '5 - Felix U. Gomez': {'ops': 3, 'seg': 3, 'total': 6, 'tipo': 'ESPECIAL_3_3'},
        '2 - Madero': {'ops': 3, 'seg': 3, 'total': 6, 'tipo': 'ESPECIAL_3_3'},
        '3 - Matamoros': {'ops': 3, 'seg': 3, 'total': 6, 'tipo': 'ESPECIAL_3_3'}
    }
    
    sucursales_con_reglas = []
    
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
                # Reglas por tipo
                tipo = row.get('tipo', 'LOCAL')
                if tipo == 'FORANEA':
                    ops_esperadas = 2
                    seg_esperadas = 2
                    total_esperado = 4
                    tipo_regla = 'FORANEA_2_2'
                else:  # LOCAL
                    ops_esperadas = 4
                    seg_esperadas = 4
                    total_esperado = 8
                    tipo_regla = 'LOCAL_4_4'
            
            sucursales_con_reglas.append({
                'location_key': location_key,
                'ops_esperadas': ops_esperadas,
                'seg_esperadas': seg_esperadas,
                'total_esperado': total_esperado,
                'tipo_regla': tipo_regla
            })
    
    return pd.DataFrame(sucursales_con_reglas)

def aplicar_redistribuciones(df):
    """Aplicar todas las redistribuciones confirmadas"""
    
    print("🔄 APLICAR REDISTRIBUCIONES CONFIRMADAS")
    print("=" * 60)
    
    df_redistribuido = df.copy()
    
    redistribuciones = [
        {
            'submission_id': '685065c72c100d8',
            'de': '29 - Pablo Livas',
            'a': '18 - Linda Vista',
            'fecha': '2025-06-16',
            'tipo': 'seguridad',
            'motivo': 'Completar par faltante Linda Vista'
        },
        {
            'submission_id': '6824f8062b3388ec0',
            'de': '65 - Pedro Cardenas', 
            'a': '69 - Puerto Rico',
            'fecha': '2025-05-14',
            'tipo': 'seguridad',
            'motivo': 'Pedro Cardenas duplicada, Puerto Rico necesita par'
        },
        {
            'submission_id': '685b2812604e495',
            'de': '71 - Centrito Valle',
            'a': '40 - Plaza 1500',
            'fecha': '2025-06-24',
            'tipo': 'seguridad', 
            'motivo': 'Centrito sobrante, Plaza 1500 necesita par'
        },
        {
            'submission_id': '68a8b8ece1c1b03',
            'de': '71 - Centrito Valle',
            'a': '38 - Gomez Morin',
            'fecha': '2025-08-22',
            'tipo': 'seguridad',
            'motivo': 'Centrito sobrante, Gomez Morin necesita par'
        }
    ]
    
    print(f"📋 REDISTRIBUCIONES A APLICAR:")
    for i, redist in enumerate(redistribuciones, 1):
        print(f"\n{i}. {redist['submission_id'][:15]} - {redist['fecha']}")
        print(f"   📤 DE: {redist['de']}")
        print(f"   📥 A:  {redist['a']}")
        print(f"   💡 Motivo: {redist['motivo']}")
        
        # Aplicar cambio
        mask = df_redistribuido['submission_id'] == redist['submission_id']
        if mask.any():
            df_redistribuido.loc[mask, 'location_asignado'] = redist['a']
            print(f"   ✅ Aplicado")
        else:
            print(f"   ❌ No encontrado")
    
    print(f"\n✅ {len(redistribuciones)} REDISTRIBUCIONES APLICADAS")
    
    return df_redistribuido

def analizar_cumplimiento_final(df_final, df_reglas):
    """Analizar cumplimiento después de redistribuciones"""
    
    print(f"\n📊 ANÁLISIS CUMPLIMIENTO FINAL")
    print("=" * 60)
    
    # Contar por sucursal
    ops_por_sucursal = df_final[df_final['tipo'] == 'operativas']['location_asignado'].value_counts()
    seg_por_sucursal = df_final[df_final['tipo'] == 'seguridad']['location_asignado'].value_counts()
    
    analisis_final = []
    
    for _, sucursal in df_reglas.iterrows():
        location_key = sucursal['location_key']
        ops_actuales = ops_por_sucursal.get(location_key, 0)
        seg_actuales = seg_por_sucursal.get(location_key, 0)
        total_actual = ops_actuales + seg_actuales
        
        ops_esperadas = sucursal['ops_esperadas']
        seg_esperadas = sucursal['seg_esperadas']
        total_esperado = sucursal['total_esperado']
        tipo_regla = sucursal['tipo_regla']
        
        diferencia = total_actual - total_esperado
        
        if diferencia == 0:
            estado = "✅ PERFECTO"
        elif diferencia > 0:
            estado = f"⚠️ EXCESO (+{diferencia})"
        else:
            estado = f"❌ DEFICIT (-{abs(diferencia)})"
        
        analisis_final.append({
            'location_key': location_key,
            'tipo_regla': tipo_regla,
            'ops_actuales': ops_actuales,
            'seg_actuales': seg_actuales,
            'total_actual': total_actual,
            'ops_esperadas': ops_esperadas,
            'seg_esperadas': seg_esperadas,
            'total_esperado': total_esperado,
            'diferencia': diferencia,
            'estado': estado
        })
    
    df_analisis = pd.DataFrame(analisis_final)
    
    # Resumen
    perfectas = len(df_analisis[df_analisis['diferencia'] == 0])
    con_exceso = len(df_analisis[df_analisis['diferencia'] > 0])
    con_deficit = len(df_analisis[df_analisis['diferencia'] < 0])
    sin_supervisiones = len(df_analisis[df_analisis['total_actual'] == 0])
    
    print(f"📈 RESUMEN CUMPLIMIENTO FINAL:")
    print(f"   ✅ Perfectas: {perfectas}")
    print(f"   ⚠️ Con exceso: {con_exceso}")
    print(f"   ❌ Con déficit: {con_deficit}")
    print(f"   ⭕ Sin supervisiones: {sin_supervisiones}")
    
    # Mostrar casos problemáticos
    if con_exceso > 0:
        print(f"\n⚠️ SUCURSALES CON EXCESO:")
        excesos = df_analisis[df_analisis['diferencia'] > 0].sort_values('diferencia', ascending=False)
        for _, row in excesos.head(10).iterrows():
            print(f"   {row['location_key']}: {row['ops_actuales']}+{row['seg_actuales']}={row['total_actual']} ({row['estado']})")
    
    if con_deficit > 0:
        print(f"\n❌ SUCURSALES CON DÉFICIT:")
        deficits = df_analisis[df_analisis['diferencia'] < 0].sort_values('diferencia')
        for _, row in deficits.head(10).iterrows():
            print(f"   {row['location_key']}: {row['ops_actuales']}+{row['seg_actuales']}={row['total_actual']} ({row['estado']})")
    
    return df_analisis

def verificar_redistribuciones_especificas(df_final):
    """Verificar que las redistribuciones específicas funcionaron"""
    
    print(f"\n✅ VERIFICAR REDISTRIBUCIONES ESPECÍFICAS")
    print("=" * 60)
    
    sucursales_verificar = [
        '18 - Linda Vista',
        '29 - Pablo Livas',
        '65 - Pedro Cardenas',
        '69 - Puerto Rico',
        '71 - Centrito Valle',
        '40 - Plaza 1500',
        '38 - Gomez Morin'
    ]
    
    for sucursal in sucursales_verificar:
        suc_data = df_final[df_final['location_asignado'] == sucursal]
        ops = len(suc_data[suc_data['tipo'] == 'operativas'])
        seg = len(suc_data[suc_data['tipo'] == 'seguridad'])
        total = ops + seg
        
        print(f"📍 {sucursal}:")
        print(f"   🏗️ Operativas: {ops}")
        print(f"   🛡️ Seguridad: {seg}")
        print(f"   📊 Total: {total}")

def identificar_problemas_restantes(df_analisis):
    """Identificar qué problemas quedan por resolver"""
    
    print(f"\n🔍 PROBLEMAS RESTANTES POR RESOLVER")
    print("=" * 60)
    
    # Problemas significativos
    problemas_grandes = df_analisis[abs(df_analisis['diferencia']) >= 2].copy()
    problemas_medianos = df_analisis[(abs(df_analisis['diferencia']) == 1) & (df_analisis['diferencia'] != 0)].copy()
    
    if len(problemas_grandes) > 0:
        print(f"🚨 PROBLEMAS GRANDES (diferencia ≥2):")
        for _, row in problemas_grandes.iterrows():
            print(f"   {row['location_key']}: {row['estado']}")
    
    if len(problemas_medianos) > 0:
        print(f"\n⚠️ PROBLEMAS MEDIANOS (diferencia =1):")
        for _, row in problemas_medianos.iterrows():
            print(f"   {row['location_key']}: {row['estado']}")
    
    # Sucursales sin supervisiones
    sin_supervisiones = df_analisis[df_analisis['total_actual'] == 0]
    if len(sin_supervisiones) > 0:
        print(f"\n⭕ SUCURSALES SIN SUPERVISIONES 2025:")
        for _, row in sin_supervisiones.iterrows():
            print(f"   {row['location_key']} ({row['tipo_regla']})")
    
    return problemas_grandes, problemas_medianos

def main():
    """Función principal"""
    
    print("🔄 APLICAR REDISTRIBUCIONES FINALES")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Objetivo: Aplicar redistribuciones confirmadas y revisar estado final")
    print("=" * 80)
    
    # 1. Cargar datos
    df = cargar_dataset()
    df_reglas = cargar_catalogo_sucursales()
    
    # 2. Aplicar redistribuciones
    df_final = aplicar_redistribuciones(df)
    
    # 3. Verificar redistribuciones específicas
    verificar_redistribuciones_especificas(df_final)
    
    # 4. Analizar cumplimiento final
    df_analisis = analizar_cumplimiento_final(df_final, df_reglas)
    
    # 5. Identificar problemas restantes
    problemas_grandes, problemas_medianos = identificar_problemas_restantes(df_analisis)
    
    # 6. Guardar dataset final
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archivo_final = f"DATASET_FINAL_REDISTRIBUIDO_{timestamp}.csv"
    archivo_analisis = f"ANALISIS_FINAL_{timestamp}.csv"
    
    df_final.to_csv(archivo_final, index=False, encoding='utf-8')
    df_analisis.to_csv(archivo_analisis, index=False, encoding='utf-8')
    
    print(f"\n📁 ARCHIVOS GENERADOS:")
    print(f"   ✅ Dataset final: {archivo_final}")
    print(f"   ✅ Análisis final: {archivo_analisis}")
    
    # 7. Resumen final
    total_problemas = len(problemas_grandes) + len(problemas_medianos)
    
    print(f"\n🎯 RESUMEN FINAL:")
    print(f"   📊 Total sucursales: {len(df_analisis)}")
    print(f"   ✅ Redistribuciones aplicadas: 4")
    print(f"   🚨 Problemas restantes: {total_problemas}")
    
    if total_problemas > 0:
        print(f"\n💡 PRÓXIMOS PASOS:")
        print(f"   1. Revisar problemas grandes (diferencia ≥2)")
        print(f"   2. Evaluar redistribuciones adicionales")
        print(f"   3. Confirmar sucursales sin supervisiones 2025")
    else:
        print(f"\n🎉 ¡PERFECTO! Todas las sucursales están balanceadas")
    
    print(f"\n✅ REDISTRIBUCIONES FINALES COMPLETADAS")
    
    return df_final, df_analisis

if __name__ == "__main__":
    main()