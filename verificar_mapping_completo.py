#!/usr/bin/env python3
"""
✅ VERIFICAR MAPPING COMPLETO
Confirmar que todas las 476 supervisiones tienen sucursal asignada para Dashboard
"""

import pandas as pd
from datetime import datetime

def verificar_mapping_completo():
    """Verificar que todas las supervisiones tienen sucursal asignada"""
    
    print("✅ VERIFICAR MAPPING COMPLETO PARA DASHBOARD")
    print("=" * 80)
    
    # Cargar dataset final
    df = pd.read_csv("DATASET_EMPAREJADO_20251218_164319.csv")
    
    print(f"📊 DATASET CARGADO:")
    print(f"   📋 Total registros: {len(df)}")
    print(f"   🔧 Operativas: {len(df[df['tipo'] == 'operativas'])}")
    print(f"   🛡️ Seguridad: {len(df[df['tipo'] == 'seguridad'])}")
    
    # Verificar asignaciones
    print(f"\n🔍 VERIFICAR ASIGNACIONES:")
    
    # Sin asignar
    sin_asignar = df[df['location_asignado'].isna()]
    print(f"   ❌ Sin location_asignado: {len(sin_asignar)}")
    
    # Asignadas
    asignadas = df[df['location_asignado'].notna()]
    print(f"   ✅ Con location_asignado: {len(asignadas)}")
    
    # Verificar que todas están asignadas
    if len(sin_asignar) == 0:
        print(f"   🎯 PERFECTO: 100% de supervisiones asignadas")
    else:
        print(f"   ⚠️ ATENCIÓN: {len(sin_asignar)} supervisiones sin asignar")
        
        if len(sin_asignar) <= 10:
            print(f"   📋 Supervisiones sin asignar:")
            for _, row in sin_asignar.iterrows():
                print(f"      • {row['submission_id']} | {row['tipo']} | {row.get('sucursal_campo', 'N/A')}")
    
    return asignadas, sin_asignar

def analizar_distribusion_por_sucursal(asignadas):
    """Analizar distribución final por sucursal"""
    
    print(f"\n📊 DISTRIBUCIÓN FINAL POR SUCURSAL")
    print("=" * 80)
    
    # Contar por sucursal y tipo
    distribucion = asignadas.groupby(['location_asignado', 'tipo']).size().unstack(fill_value=0)
    distribucion['total'] = distribucion.sum(axis=1)
    distribucion = distribucion.sort_index()
    
    print(f"{'Sucursal':<40} {'Ops':<4} {'Seg':<4} {'Total'}")
    print("-" * 65)
    
    sucursales_perfectas = 0
    
    for location_key in distribucion.index:
        ops = distribucion.loc[location_key, 'operativas'] if 'operativas' in distribucion.columns else 0
        seg = distribucion.loc[location_key, 'seguridad'] if 'seguridad' in distribucion.columns else 0
        total = distribucion.loc[location_key, 'total']
        
        # Determinar si está balanceada (ops == seg)
        if ops == seg and ops > 0:
            estado = "✅"
            sucursales_perfectas += 1
        else:
            estado = "⚠️"
        
        print(f"{location_key:<40} {ops:<4} {seg:<4} {total:<5} {estado}")
    
    print("-" * 65)
    print(f"{'TOTAL':<40} {distribucion['operativas'].sum() if 'operativas' in distribucion.columns else 0:<4} {distribucion['seguridad'].sum() if 'seguridad' in distribucion.columns else 0:<4} {distribucion['total'].sum()}")
    
    print(f"\n📊 RESUMEN DE BALANCEO:")
    print(f"   ✅ Sucursales balanceadas (ops=seg): {sucursales_perfectas}/{len(distribucion)}")
    print(f"   📊 Porcentaje balanceado: {sucursales_perfectas/len(distribucion)*100:.1f}%")
    
    return distribucion

def crear_resumen_dashboard(distribucion):
    """Crear resumen para Dashboard"""
    
    print(f"\n🎯 RESUMEN PARA DASHBOARD")
    print("=" * 80)
    
    total_sucursales = len(distribucion)
    total_supervisiones = distribucion['total'].sum()
    total_ops = distribucion['operativas'].sum() if 'operativas' in distribucion.columns else 0
    total_seg = distribucion['seguridad'].sum() if 'seguridad' in distribucion.columns else 0
    
    # Categorizar por cantidad de supervisiones
    por_cantidad = distribucion['total'].value_counts().sort_index()
    
    print(f"📊 ESTADÍSTICAS GENERALES:")
    print(f"   🏢 Total sucursales: {total_sucursales}")
    print(f"   📋 Total supervisiones: {total_supervisiones}")
    print(f"   🔧 Total operativas: {total_ops}")
    print(f"   🛡️ Total seguridad: {total_seg}")
    print(f"   ⚖️ Balance ops/seg: {'✅ PERFECTO' if total_ops == total_seg else '❌ DESBALANCEADO'}")
    
    print(f"\n📊 DISTRIBUCIÓN POR CANTIDAD DE SUPERVISIONES:")
    for cantidad, count in por_cantidad.items():
        print(f"   {cantidad} supervisiones: {count} sucursales")
    
    # Sucursales con más/menos supervisiones
    max_supervisiones = distribucion['total'].max()
    min_supervisiones = distribucion['total'].min()
    
    sucursal_max = distribucion[distribucion['total'] == max_supervisiones].index[0]
    sucursal_min = distribucion[distribucion['total'] == min_supervisiones].index[0]
    
    print(f"\n🔝 EXTREMOS:")
    print(f"   📈 Máximo: {sucursal_max} ({max_supervisiones} supervisiones)")
    print(f"   📉 Mínimo: {sucursal_min} ({min_supervisiones} supervisiones)")
    
    return {
        'total_sucursales': total_sucursales,
        'total_supervisiones': total_supervisiones,
        'total_operativas': total_ops,
        'total_seguridad': total_seg,
        'balanceado': total_ops == total_seg,
        'distribucion_cantidad': dict(por_cantidad),
        'sucursal_max': sucursal_max,
        'max_supervisiones': max_supervisiones,
        'sucursal_min': sucursal_min,
        'min_supervisiones': min_supervisiones
    }

def verificar_integridad_fechas(asignadas):
    """Verificar integridad de fechas para Dashboard"""
    
    print(f"\n📅 VERIFICAR INTEGRIDAD DE FECHAS")
    print("=" * 80)
    
    # Convertir fechas
    asignadas_copy = asignadas.copy()
    asignadas_copy['date_submitted'] = pd.to_datetime(asignadas_copy['date_submitted'])
    
    # Rango de fechas
    fecha_min = asignadas_copy['date_submitted'].min()
    fecha_max = asignadas_copy['date_submitted'].max()
    
    print(f"📅 RANGO DE FECHAS:")
    print(f"   📆 Desde: {fecha_min.strftime('%Y-%m-%d')}")
    print(f"   📆 Hasta: {fecha_max.strftime('%Y-%m-%d')}")
    
    # Supervisiones por mes
    asignadas_copy['mes'] = asignadas_copy['date_submitted'].dt.to_period('M')
    por_mes = asignadas_copy.groupby('mes').size().sort_index()
    
    print(f"\n📊 SUPERVISIONES POR MES:")
    for mes, cantidad in por_mes.items():
        print(f"   {mes}: {cantidad} supervisiones")
    
    # Verificar fechas nulas
    fechas_nulas = asignadas_copy['date_submitted'].isna().sum()
    print(f"\n🔍 CALIDAD DE DATOS:")
    print(f"   ❌ Fechas nulas: {fechas_nulas}")
    print(f"   ✅ Fechas válidas: {len(asignadas_copy) - fechas_nulas}")
    
    return {
        'fecha_min': fecha_min,
        'fecha_max': fecha_max,
        'por_mes': dict(por_mes),
        'fechas_nulas': fechas_nulas
    }

def main():
    """Función principal"""
    
    print("✅ VERIFICAR MAPPING COMPLETO PARA DASHBOARD")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Roberto: Confirmar que cada supervisión tiene sucursal asignada")
    print("=" * 80)
    
    # 1. Verificar mapping completo
    asignadas, sin_asignar = verificar_mapping_completo()
    
    # 2. Analizar distribución por sucursal
    distribucion = analizar_distribusion_por_sucursal(asignadas)
    
    # 3. Crear resumen para Dashboard
    resumen_dashboard = crear_resumen_dashboard(distribucion)
    
    # 4. Verificar integridad de fechas
    integridad_fechas = verificar_integridad_fechas(asignadas)
    
    # 5. Guardar resúmenes
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Guardar distribución por sucursal
    archivo_distribucion = f"DISTRIBUCION_SUCURSALES_DASHBOARD_{timestamp}.csv"
    distribucion.to_csv(archivo_distribucion, encoding='utf-8')
    
    # Guardar resumen completo
    resumen_completo = {
        'mapping': {
            'total_supervisiones': len(asignadas) + len(sin_asignar),
            'asignadas': len(asignadas),
            'sin_asignar': len(sin_asignar),
            'porcentaje_asignado': len(asignadas) / (len(asignadas) + len(sin_asignar)) * 100
        },
        'dashboard': resumen_dashboard,
        'fechas': integridad_fechas
    }
    
    archivo_resumen = f"RESUMEN_DASHBOARD_{timestamp}.json"
    import json
    with open(archivo_resumen, 'w', encoding='utf-8') as f:
        json.dump(resumen_completo, f, indent=2, default=str, ensure_ascii=False)
    
    print(f"\n📁 ARCHIVOS GENERADOS:")
    print(f"   ✅ Distribución: {archivo_distribucion}")
    print(f"   ✅ Resumen: {archivo_resumen}")
    
    # Conclusión final
    print(f"\n🎯 CONCLUSIÓN PARA DASHBOARD:")
    if len(sin_asignar) == 0:
        print(f"   ✅ LISTO: 100% supervisiones mapeadas ({len(asignadas)}/476)")
        print(f"   📊 {len(distribucion)} sucursales con supervisiones asignadas")
        print(f"   🎯 Dashboard puede ser construido exitosamente")
        print(f"   📅 Rango de fechas: {integridad_fechas['fecha_min'].strftime('%Y-%m-%d')} a {integridad_fechas['fecha_max'].strftime('%Y-%m-%d')}")
    else:
        print(f"   ⚠️ ATENCIÓN: {len(sin_asignar)} supervisiones sin asignar")
        print(f"   📊 Mapping incompleto - revisar antes de Dashboard")
    
    print(f"\n✅ VERIFICACIÓN COMPLETA")
    
    return asignadas, distribucion, resumen_completo

if __name__ == "__main__":
    main()