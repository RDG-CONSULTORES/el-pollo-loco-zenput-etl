#!/usr/bin/env python3
"""
📋 VALIDACIÓN 80 SUCURSALES DE 5 EN 5
Para que Roberto valide clasificación LOCAL/FORÁNEA y coincidencia de fechas
"""

import pandas as pd
from datetime import datetime

def cargar_datos_finales():
    """Cargar dataset emparejado y catálogo"""
    
    df = pd.read_csv("DATASET_EMPAREJADO_20251218_164319.csv")
    df['date_submitted'] = pd.to_datetime(df['date_submitted'])
    df['fecha_str'] = df['date_submitted'].dt.strftime('%Y-%m-%d')
    
    return df

def obtener_sucursales_activas():
    """Obtener lista de 80 sucursales activas excluyendo las 6 nuevas"""
    
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

def mostrar_grupo_5_sucursales(grupo_num, sucursales_grupo, df):
    """Mostrar análisis de un grupo de 5 sucursales"""
    
    print(f"\n📋 GRUPO {grupo_num} - SUCURSALES {(grupo_num-1)*5+1} A {grupo_num*5}")
    print("=" * 100)
    
    for i, sucursal in enumerate(sucursales_grupo, 1):
        location_key = sucursal['location_key']
        numero_grupo = (grupo_num-1)*5 + i
        
        # Analizar supervisiones
        analisis = analizar_supervisiones_por_sucursal(df, location_key)
        
        print(f"\n{numero_grupo}. {location_key}")
        print(f"   🏢 Tipo actual: {sucursal['tipo_original']}")
        print(f"   📍 Grupo: {sucursal['grupo']}")
        print(f"   📊 Supervisiones: {analisis['ops_count']} ops + {analisis['seg_count']} seg = {analisis['total_count']} total")
        
        if analisis['estado'] == 'SIN_SUPERVISIONES':
            print(f"   ❌ Sin supervisiones asignadas")
        else:
            print(f"   📅 Fechas operativas: {len(analisis['fechas_ops'])} → {analisis['fechas_ops']}")
            print(f"   📅 Fechas seguridad: {len(analisis['fechas_seg'])} → {analisis['fechas_seg']}")
            print(f"   ✅ Fechas coincidentes: {len(analisis['coincidencias'])} → {analisis['coincidencias']}")
            
            # Estado de completitud
            if analisis['ops_count'] == analisis['seg_count'] and len(analisis['coincidencias']) == analisis['ops_count']:
                estado_parejas = "✅ PAREJAS COMPLETAS"
            elif len(analisis['coincidencias']) > 0:
                estado_parejas = "⚠️ PARCIALMENTE EMPAREJADAS"
            else:
                estado_parejas = "❌ SIN EMPAREJAMIENTO"
            
            print(f"   🎯 Estado parejas: {estado_parejas}")

def main():
    """Función principal"""
    
    print("📋 VALIDACIÓN 80 SUCURSALES DE 5 EN 5")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Roberto valida: LOCAL/FORÁNEA y coincidencia de fechas")
    print("📊 238 parejas de supervisiones distribuidas en 80 sucursales activas")
    print("=" * 80)
    
    # 1. Cargar datos
    df = cargar_datos_finales()
    sucursales_activas = obtener_sucursales_activas()
    
    print(f"📍 TOTAL SUCURSALES ACTIVAS: {len(sucursales_activas)}")
    print(f"📊 TOTAL SUPERVISIONES: {len(df)} ({len(df[df['tipo']=='operativas'])} ops + {len(df[df['tipo']=='seguridad'])} seg)")
    
    # 2. Dividir en grupos de 5
    grupos = []
    for i in range(0, len(sucursales_activas), 5):
        grupo = sucursales_activas[i:i+5]
        grupos.append(grupo)
    
    # 3. Mostrar cada grupo
    for grupo_num, sucursales_grupo in enumerate(grupos, 1):
        mostrar_grupo_5_sucursales(grupo_num, sucursales_grupo, df)
        
        if grupo_num < len(grupos):
            print(f"\n" + "─" * 100)
            input("Presiona ENTER para continuar con el siguiente grupo...")
    
    # 4. Resumen final
    print(f"\n\n🎯 RESUMEN FINAL")
    print("=" * 80)
    
    # Contar totales
    total_ops = len(df[df['tipo'] == 'operativas'])
    total_seg = len(df[df['tipo'] == 'seguridad'])
    sucursales_con_supervisiones = len([s for s in sucursales_activas 
                                      if len(df[df['location_asignado'] == s['location_key']]) > 0])
    
    print(f"📊 SUPERVISIONES TOTALES:")
    print(f"   🔧 Operativas: {total_ops}")
    print(f"   🛡️ Seguridad: {total_seg}")
    print(f"   👥 Parejas: {min(total_ops, total_seg)}")
    
    print(f"\n📍 DISTRIBUCIÓN:")
    print(f"   🏢 Sucursales activas: {len(sucursales_activas)}")
    print(f"   ✅ Con supervisiones: {sucursales_con_supervisiones}")
    print(f"   ❌ Sin supervisiones: {len(sucursales_activas) - sucursales_con_supervisiones}")
    
    print(f"\n✅ VALIDACIÓN DE 5 EN 5 COMPLETADA")
    print(f"🎯 Roberto puede ahora validar cada sucursal si es LOCAL/FORÁNEA")
    
    return sucursales_activas, grupos

if __name__ == "__main__":
    main()