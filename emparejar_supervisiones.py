#!/usr/bin/env python3
"""
👥 EMPAREJAR SUPERVISIONES
Roberto: Las operativas tienen Location, buscar sus parejas de seguridad por fecha/hora
"""

import pandas as pd
from datetime import datetime, timedelta

def cargar_dataset_completo():
    """Cargar dataset completo"""
    
    df = pd.read_csv("DATASET_FINAL_COMPLETO.csv")
    
    # Convertir fecha
    df['date_submitted'] = pd.to_datetime(df['date_submitted'])
    df['fecha_str'] = df['date_submitted'].dt.strftime('%Y-%m-%d')
    df['hora'] = df['date_submitted'].dt.strftime('%H:%M:%S')
    
    return df

def analizar_emparejamiento_actual(df):
    """Analizar emparejamiento actual por fecha/sucursal"""
    
    print("👥 ANÁLISIS DE EMPAREJAMIENTO ACTUAL")
    print("=" * 70)
    
    # Agrupar por sucursal y fecha
    grupos = df.groupby(['location_asignado', 'fecha_str']).agg({
        'submission_id': 'count',
        'tipo': lambda x: f"{sum(x=='operativas')}ops+{sum(x=='seguridad')}seg"
    }).rename(columns={'submission_id': 'total'})
    
    # Categorizar emparejamientos
    perfectos = grupos[grupos['total'] == 2]
    desequilibrados = grupos[grupos['total'] != 2]
    
    print(f"📊 EMPAREJAMIENTO POR SUCURSAL-FECHA:")
    print(f"   ✅ Parejas perfectas: {len(perfectos)} (1ops+1seg)")
    print(f"   ⚠️ Desequilibrados: {len(desequilibrados)}")
    
    if len(desequilibrados) > 0:
        print(f"\n🔍 DESEQUILIBRIOS DETECTADOS:")
        print(f"{'Sucursal':<35} {'Fecha':<12} {'Total':<6} {'Tipo'}")
        print("-" * 70)
        
        for (sucursal, fecha), row in desequilibrados.head(10).iterrows():
            print(f"{sucursal:<35} {fecha:<12} {row['total']:<6} {row['tipo']}")
        
        if len(desequilibrados) > 10:
            print(f"   ... y {len(desequilibrados)-10} más")
    
    return perfectos, desequilibrados

def encontrar_parejas_por_fecha_hora(df):
    """Encontrar parejas de operativa-seguridad por fecha y hora aproximada"""
    
    print(f"\n🔍 BUSCAR PAREJAS POR FECHA/HORA")
    print("=" * 70)
    
    operativas = df[df['tipo'] == 'operativas'].copy()
    seguridad = df[df['tipo'] == 'seguridad'].copy()
    
    print(f"📊 DISPONIBLES:")
    print(f"   🔧 Operativas: {len(operativas)}")
    print(f"   🛡️ Seguridad: {len(seguridad)}")
    
    parejas_encontradas = []
    seguridad_sin_pareja = seguridad.copy()
    
    # Para cada operativa, buscar su pareja de seguridad
    for _, ops in operativas.iterrows():
        fecha_ops = ops['fecha_str']
        hora_ops = ops['date_submitted']
        
        # Buscar seguridad en la misma fecha
        seguridad_misma_fecha = seguridad_sin_pareja[
            seguridad_sin_pareja['fecha_str'] == fecha_ops
        ].copy()
        
        if len(seguridad_misma_fecha) > 0:
            # Calcular diferencia de tiempo
            seguridad_misma_fecha['diff_tiempo'] = abs(
                seguridad_misma_fecha['date_submitted'] - hora_ops
            ).dt.total_seconds() / 3600  # diferencia en horas
            
            # Encontrar la más cercana en tiempo
            pareja_mas_cercana = seguridad_misma_fecha.loc[
                seguridad_misma_fecha['diff_tiempo'].idxmin()
            ]
            
            parejas_encontradas.append({
                'ops_id': ops['submission_id'],
                'seg_id': pareja_mas_cercana['submission_id'],
                'fecha': fecha_ops,
                'ops_hora': ops['hora'],
                'seg_hora': pareja_mas_cercana['hora'],
                'diff_horas': pareja_mas_cercana['diff_tiempo'],
                'ops_location': ops['location_asignado'],
                'seg_location': pareja_mas_cercana.get('location_asignado', 'SIN_ASIGNAR'),
                'coincide_location': ops['location_asignado'] == pareja_mas_cercana.get('location_asignado', '')
            })
            
            # Remover de disponibles
            seguridad_sin_pareja = seguridad_sin_pareja.drop(pareja_mas_cercana.name)
    
    print(f"\n✅ PAREJAS ENCONTRADAS: {len(parejas_encontradas)}")
    print(f"🔍 Seguridad sin pareja: {len(seguridad_sin_pareja)}")
    
    return parejas_encontradas, seguridad_sin_pareja

def analizar_calidad_emparejamiento(parejas):
    """Analizar calidad del emparejamiento encontrado"""
    
    print(f"\n📊 CALIDAD DEL EMPAREJAMIENTO")
    print("=" * 70)
    
    df_parejas = pd.DataFrame(parejas)
    
    # Análisis de diferencias de tiempo
    diff_stats = df_parejas['diff_horas'].describe()
    print(f"⏰ DIFERENCIAS DE TIEMPO (horas):")
    print(f"   📊 Promedio: {diff_stats['mean']:.2f}h")
    print(f"   📊 Mediana: {diff_stats['50%']:.2f}h") 
    print(f"   📊 Máximo: {diff_stats['max']:.2f}h")
    
    # Análisis de coincidencia de location
    coincidencias = df_parejas['coincide_location'].sum()
    total = len(df_parejas)
    
    print(f"\n🎯 COINCIDENCIA DE LOCATION:")
    print(f"   ✅ Coinciden: {coincidencias}/{total} ({coincidencias/total*100:.1f}%)")
    print(f"   ❌ No coinciden: {total-coincidencias}/{total} ({(total-coincidencias)/total*100:.1f}%)")
    
    # Mostrar casos con diferencias de tiempo grandes
    problematicas = df_parejas[df_parejas['diff_horas'] > 3]
    if len(problematicas) > 0:
        print(f"\n⚠️ PAREJAS CON >3H DIFERENCIA:")
        print(f"{'Fecha':<12} {'Ops':<8} {'Seg':<8} {'Diff':<6} {'Location Match'}")
        print("-" * 60)
        
        for _, row in problematicas.head(5).iterrows():
            match_str = "✅" if row['coincide_location'] else "❌"
            print(f"{row['fecha']:<12} {row['ops_hora']:<8} {row['seg_hora']:<8} {row['diff_horas']:.1f}h   {match_str}")
    
    return df_parejas

def aplicar_emparejamiento_corregido(df, parejas, seguridad_sin_pareja):
    """Aplicar emparejamiento corregido al dataset"""
    
    print(f"\n🔄 APLICAR EMPAREJAMIENTO CORREGIDO")
    print("=" * 70)
    
    df_corregido = df.copy()
    reasignaciones = 0
    
    # Para cada pareja, asignar la seguridad al mismo location que la operativa
    for pareja in parejas:
        if not pareja['coincide_location'] and pareja['seg_location'] != 'SIN_ASIGNAR':
            # Reasignar seguridad al location de la operativa
            mask = df_corregido['submission_id'] == pareja['seg_id']
            if mask.sum() > 0:
                df_corregido.loc[mask, 'location_asignado'] = pareja['ops_location']
                reasignaciones += 1
    
    print(f"✅ Reasignaciones aplicadas: {reasignaciones}")
    
    # Asignar seguridad sin pareja usando coordenadas o campo Sucursal
    seguridad_reasignada = 0
    for _, seg in seguridad_sin_pareja.iterrows():
        if pd.isna(seg.get('location_asignado')) or seg.get('location_asignado') == 'SIN_ASIGNAR':
            # Usar campo Sucursal si está disponible
            if pd.notna(seg.get('sucursal_campo')):
                mask = df_corregido['submission_id'] == seg['submission_id']
                if mask.sum() > 0:
                    df_corregido.loc[mask, 'location_asignado'] = seg['sucursal_campo']
                    seguridad_reasignada += 1
    
    print(f"✅ Seguridad sin pareja reasignada: {seguridad_reasignada}")
    
    return df_corregido, reasignaciones

def main():
    """Función principal"""
    
    print("👥 EMPAREJAR SUPERVISIONES")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Roberto: Emparejar operativas con sus seguridad por fecha/hora")
    print("📊 238 operativas + 238 seguridad = 238 parejas esperadas")
    print("=" * 80)
    
    # 1. Cargar dataset
    df = cargar_dataset_completo()
    
    # 2. Analizar emparejamiento actual
    perfectos, desequilibrados = analizar_emparejamiento_actual(df)
    
    # 3. Encontrar parejas por fecha/hora
    parejas, seguridad_sin_pareja = encontrar_parejas_por_fecha_hora(df)
    
    # 4. Analizar calidad
    df_parejas = analizar_calidad_emparejamiento(parejas)
    
    # 5. Aplicar emparejamiento corregido
    df_corregido, reasignaciones = aplicar_emparejamiento_corregido(df, parejas, seguridad_sin_pareja)
    
    # 6. Guardar resultados
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Guardar dataset corregido
    archivo_corregido = f"DATASET_EMPAREJADO_{timestamp}.csv"
    df_corregido.to_csv(archivo_corregido, index=False, encoding='utf-8')
    
    # Guardar análisis de parejas
    archivo_parejas = f"ANALISIS_PAREJAS_{timestamp}.csv"
    df_parejas.to_csv(archivo_parejas, index=False, encoding='utf-8')
    
    print(f"\n📁 ARCHIVOS GUARDADOS:")
    print(f"   ✅ Dataset corregido: {archivo_corregido}")
    print(f"   ✅ Análisis parejas: {archivo_parejas}")
    
    print(f"\n🎯 RESUMEN FINAL:")
    print(f"   👥 Parejas encontradas: {len(parejas)}/238")
    print(f"   🔄 Reasignaciones: {reasignaciones}")
    print(f"   🛡️ Seguridad sin pareja: {len(seguridad_sin_pareja)}")
    print(f"   ✅ Tasa éxito: {len(parejas)/238*100:.1f}%")
    
    print(f"\n✅ EMPAREJAMIENTO COMPLETADO")
    
    return df_corregido, parejas, seguridad_sin_pareja

if __name__ == "__main__":
    main()