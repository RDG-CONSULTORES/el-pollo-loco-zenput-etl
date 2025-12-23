#!/usr/bin/env python3
"""
📋 MOSTRAR TODAS LAS 80 SUCURSALES
Para que Roberto valide clasificación LOCAL/FORÁNEA completa
"""

import pandas as pd
from datetime import datetime

def cargar_datos_finales():
    """Cargar dataset emparejado"""
    
    df = pd.read_csv("DATASET_EMPAREJADO_20251218_164319.csv")
    df['date_submitted'] = pd.to_datetime(df['date_submitted'])
    df['fecha_str'] = df['date_submitted'].dt.strftime('%Y-%m-%d')
    
    return df

def obtener_sucursales_activas():
    """Obtener lista de 80 sucursales activas"""
    
    df_sucursales = pd.read_csv('SUCURSALES_MASTER_20251218_110913.csv')
    
    # Sucursales nuevas a excluir
    sucursales_nuevas = [
        '35 - Apodaca',
        '82 - Aeropuerto Nuevo Laredo', 
        '83 - Cerradas de Anahuac',
        '84 - Aeropuerto del Norte',
        '85 - Diego Diaz',
        '86 - Miguel de la Madrid'
    ]
    
    sucursales_activas = []
    
    for _, row in df_sucursales.iterrows():
        if pd.notna(row['numero']) and pd.notna(row['nombre']):
            numero = int(row['numero'])
            nombre = row['nombre']
            location_key = f"{numero} - {nombre}"
            
            if location_key not in sucursales_nuevas:
                tipo_original = row.get('tipo', 'LOCAL')
                grupo = row.get('grupo', '')
                
                sucursales_activas.append({
                    'location_key': location_key,
                    'numero': numero,
                    'nombre': nombre,
                    'tipo_original': tipo_original,
                    'grupo': grupo
                })
    
    # Ordenar por número
    sucursales_activas.sort(key=lambda x: x['numero'])
    
    return sucursales_activas

def analizar_supervisiones_por_sucursal(df, location_key):
    """Analizar supervisiones de una sucursal específica"""
    
    # Filtrar supervisiones de esta sucursal
    sucursal_data = df[df['location_asignado'] == location_key].copy()
    
    if len(sucursal_data) == 0:
        return {
            'ops_count': 0,
            'seg_count': 0,
            'total_count': 0,
            'fechas_ops': [],
            'fechas_seg': [],
            'coincidencias': [],
            'estado': 'SIN_SUPERVISIONES'
        }
    
    # Separar operativas y seguridad
    ops = sucursal_data[sucursal_data['tipo'] == 'operativas']
    seg = sucursal_data[sucursal_data['tipo'] == 'seguridad']
    
    # Fechas únicas
    fechas_ops = sorted(ops['fecha_str'].unique()) if len(ops) > 0 else []
    fechas_seg = sorted(seg['fecha_str'].unique()) if len(seg) > 0 else []
    
    # Encontrar coincidencias de fechas
    coincidencias = list(set(fechas_ops) & set(fechas_seg))
    
    return {
        'ops_count': len(ops),
        'seg_count': len(seg),
        'total_count': len(sucursal_data),
        'fechas_ops': fechas_ops,
        'fechas_seg': fechas_seg,
        'coincidencias': sorted(coincidencias),
        'estado': 'ACTIVA'
    }

def mostrar_todas_las_sucursales(sucursales_activas, df):
    """Mostrar análisis de todas las sucursales"""
    
    print("📋 TODAS LAS 80 SUCURSALES ACTIVAS")
    print("=" * 120)
    print(f"{'#':<3} {'Sucursal':<35} {'Tipo':<8} {'Ops':<3} {'Seg':<3} {'Total':<5} {'Coincidentes':<12} {'Estado'}")
    print("-" * 120)
    
    # Contadores
    total_ops = 0
    total_seg = 0
    sucursales_perfectas = 0
    sucursales_con_datos = 0
    
    for i, sucursal in enumerate(sucursales_activas, 1):
        location_key = sucursal['location_key']
        
        # Analizar supervisiones
        analisis = analizar_supervisiones_por_sucursal(df, location_key)
        
        total_ops += analisis['ops_count']
        total_seg += analisis['seg_count']
        
        if analisis['estado'] == 'SIN_SUPERVISIONES':
            estado_str = "❌ SIN DATOS"
        else:
            sucursales_con_datos += 1
            if (analisis['ops_count'] == analisis['seg_count'] and 
                len(analisis['coincidencias']) == analisis['ops_count'] and 
                len(analisis['coincidencias']) > 0):
                estado_str = "✅ PERFECTO"
                sucursales_perfectas += 1
            elif len(analisis['coincidencias']) > 0:
                estado_str = "⚠️ PARCIAL"
            else:
                estado_str = "❌ DESEMPAREJADO"
        
        # Mostrar fechas coincidentes de forma compacta
        coincidentes_str = f"{len(analisis['coincidencias'])} fechas" if len(analisis['coincidencias']) > 0 else "Ninguna"
        
        print(f"{i:<3} {location_key:<35} {sucursal['tipo_original']:<8} {analisis['ops_count']:<3} {analisis['seg_count']:<3} {analisis['total_count']:<5} {coincidentes_str:<12} {estado_str}")
    
    # Mostrar resumen
    print("-" * 120)
    print(f"📊 RESUMEN GENERAL:")
    print(f"   🏢 Total sucursales activas: {len(sucursales_activas)}")
    print(f"   ✅ Con supervisiones: {sucursales_con_datos}")
    print(f"   ❌ Sin supervisiones: {len(sucursales_activas) - sucursales_con_datos}")
    print(f"   🎯 Perfectamente emparejadas: {sucursales_perfectas}")
    print(f"   📊 Total operativas: {total_ops}")
    print(f"   📊 Total seguridad: {total_seg}")
    print(f"   💯 Porcentaje perfecto: {sucursales_perfectas/sucursales_con_datos*100:.1f}%")

def mostrar_detalle_por_grupos(sucursales_activas, df):
    """Mostrar detalle por grupos de clasificación"""
    
    print(f"\n\n📋 DETALLE POR TIPO DE CLASIFICACIÓN")
    print("=" * 120)
    
    # Agrupar por tipo original
    por_tipo = {}
    for sucursal in sucursales_activas:
        tipo = sucursal['tipo_original']
        if tipo not in por_tipo:
            por_tipo[tipo] = []
        por_tipo[tipo].append(sucursal)
    
    for tipo, lista in por_tipo.items():
        print(f"\n🏢 TIPO {tipo} ({len(lista)} sucursales):")
        
        # Contadores para este tipo
        ops_total = 0
        seg_total = 0
        perfectas = 0
        
        for sucursal in lista[:10]:  # Mostrar primeras 10
            location_key = sucursal['location_key']
            analisis = analizar_supervisiones_por_sucursal(df, location_key)
            
            ops_total += analisis['ops_count']
            seg_total += analisis['seg_count']
            
            estado = "✅" if (analisis['ops_count'] == analisis['seg_count'] and 
                           len(analisis['coincidencias']) == analisis['ops_count'] and 
                           len(analisis['coincidencias']) > 0) else "❌"
            
            if estado == "✅":
                perfectas += 1
            
            print(f"   • {location_key}: {analisis['ops_count']}+{analisis['seg_count']}={analisis['total_count']} ({len(analisis['coincidencias'])} coincidentes) {estado}")
        
        if len(lista) > 10:
            print(f"   ... y {len(lista)-10} más")
        
        print(f"   📊 Subtotal {tipo}: {ops_total} ops + {seg_total} seg, {perfectas}/{min(len(lista), 10)} perfectas")

def main():
    """Función principal"""
    
    print("📋 MOSTRAR TODAS LAS 80 SUCURSALES")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Roberto valida: clasificación y coincidencia de fechas")
    print("=" * 80)
    
    # 1. Cargar datos
    df = cargar_datos_finales()
    sucursales_activas = obtener_sucursales_activas()
    
    # 2. Mostrar todas las sucursales
    mostrar_todas_las_sucursales(sucursales_activas, df)
    
    # 3. Mostrar detalle por grupos
    mostrar_detalle_por_grupos(sucursales_activas, df)
    
    # 4. Guardar para Roberto
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Crear DataFrame para Roberto
    resultados = []
    for sucursal in sucursales_activas:
        analisis = analizar_supervisiones_por_sucursal(df, sucursal['location_key'])
        
        resultados.append({
            'numero': sucursal['numero'],
            'sucursal': sucursal['location_key'],
            'tipo_actual': sucursal['tipo_original'],
            'grupo': sucursal['grupo'],
            'ops_count': analisis['ops_count'],
            'seg_count': analisis['seg_count'],
            'total_supervisiones': analisis['total_count'],
            'fechas_coincidentes': len(analisis['coincidencias']),
            'perfectamente_emparejado': 'SÍ' if (analisis['ops_count'] == analisis['seg_count'] and 
                                               len(analisis['coincidencias']) == analisis['ops_count'] and 
                                               len(analisis['coincidencias']) > 0) else 'NO',
            'necesita_validacion': 'SÍ' if analisis['total_count'] != 8 else 'NO'
        })
    
    df_resultados = pd.DataFrame(resultados)
    archivo_validacion = f"VALIDACION_80_SUCURSALES_{timestamp}.csv"
    df_resultados.to_csv(archivo_validacion, index=False, encoding='utf-8')
    
    print(f"\n📁 ARCHIVO PARA ROBERTO:")
    print(f"   ✅ {archivo_validacion}")
    print(f"   📊 80 sucursales con detalles completos")
    
    print(f"\n✅ VALIDACIÓN COMPLETA LISTA PARA ROBERTO")
    
    return sucursales_activas, resultados

if __name__ == "__main__":
    main()