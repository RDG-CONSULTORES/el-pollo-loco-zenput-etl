#!/usr/bin/env python3
"""
🔍 VALIDACIÓN DISTRIBUCIÓN COMPLETA
Mostrar estado actual y validar 4+4/2+2 con fechas coincidentes de 5 en 5 para Roberto
"""

import pandas as pd
from datetime import datetime

def cargar_datos_actualizados():
    """Cargar todos los datos actualizados"""
    
    print("📊 CARGANDO DATOS ACTUALIZADOS")
    print("=" * 50)
    
    # Operativas (Excel directo)
    df_ops = pd.read_excel("SUPERVISION_OPERATIVA_CAS_11_REV_250125_Submissions-2025-12-18__1228CST-1766141816.xlsx")
    
    # Seguridad Excel directo (con Location ya asignado)
    df_seg_excel = pd.read_excel("CONTROL_OPERATIVO_DE_SEGURIDAD_CAS_11_REV_25012025_Submissions-2025-12-18__1232CST-1766104009.xlsx")
    seg_con_location = df_seg_excel[df_seg_excel['Location'].notna()].copy()
    
    # Seguridad asignaciones (sin Location original)
    df_asignaciones = pd.read_csv("ASIGNACIONES_FINALES_CORREGIDAS_20251218_140924.csv")
    
    print(f"✅ Operativas: {len(df_ops)}")
    print(f"✅ Seguridad con Location (Excel): {len(seg_con_location)}")
    print(f"✅ Seguridad asignadas (sin Location): {len(df_asignaciones)}")
    
    return df_ops, seg_con_location, df_asignaciones

def combinar_seguridad_completa(seg_con_location, df_asignaciones):
    """Combinar todas las supervisiones de seguridad"""
    
    print(f"\n🔄 COMBINANDO SEGURIDAD COMPLETA")
    print("=" * 40)
    
    # Seguridad del Excel (ya con Location)
    seg_excel_data = []
    for idx, row in seg_con_location.iterrows():
        seg_excel_data.append({
            'sucursal': row['Location'],
            'fecha': pd.to_datetime(row['Date Submitted']).date(),
            'usuario': row['Submitted By'],
            'origen': 'EXCEL_DIRECTO',
            'index_original': idx
        })
    
    # Seguridad asignada (sin Location original)
    seg_asignada_data = []
    for _, row in df_asignaciones.iterrows():
        seg_asignada_data.append({
            'sucursal': row['sucursal_asignada'],
            'fecha': pd.to_datetime(row['fecha']).date(),
            'usuario': row['usuario'],
            'origen': 'ASIGNACION_GOOGLE_MAPS',
            'index_original': row['index_original']
        })
    
    # Combinar
    todas_seguridad = seg_excel_data + seg_asignada_data
    df_seg_completa = pd.DataFrame(todas_seguridad)
    
    print(f"📊 Total seguridad combinada: {len(df_seg_completa)}")
    print(f"   📋 Excel directo: {len(seg_excel_data)}")
    print(f"   📋 Asignaciones Google Maps: {len(seg_asignada_data)}")
    
    return df_seg_completa

def analizar_distribucion_por_sucursal(df_ops, df_seg_completa):
    """Analizar distribución actual por sucursal"""
    
    print(f"\n📊 DISTRIBUCIÓN ACTUAL POR SUCURSAL")
    print("=" * 60)
    
    # Contar operativas por sucursal
    ops_count = df_ops[df_ops['Location'].notna()]['Location'].value_counts()
    
    # Contar seguridad por sucursal
    seg_count = df_seg_completa['sucursal'].value_counts()
    
    # Cargar tipos de sucursales
    df_sucursales = pd.read_csv('SUCURSALES_MASTER_20251218_110913.csv')
    tipo_map = {}
    for _, row in df_sucursales.iterrows():
        if pd.notna(row['numero']) and pd.notna(row['nombre']):
            numero = int(row['numero'])
            nombre = row['nombre']
            location_key = f"{numero} - {nombre}"
            tipo_map[location_key] = row.get('tipo', 'DESCONOCIDO')
    
    # Combinar todas las sucursales
    todas_sucursales = set(ops_count.index.tolist() + seg_count.index.tolist())
    
    distribucion = []
    for sucursal in sorted(todas_sucursales):
        ops = ops_count.get(sucursal, 0)
        seg = seg_count.get(sucursal, 0)
        total = ops + seg
        tipo = tipo_map.get(sucursal, 'DESCONOCIDO')
        
        # Determinar estado según reglas
        if tipo == 'LOCAL':
            esperado = 8  # 4+4
            estado = "✅" if total == esperado else "⚠️" if total > 0 else "❌"
        elif tipo == 'FORANEA':
            esperado = 4  # 2+2
            estado = "✅" if total == esperado else "⚠️" if total > 0 else "❌"
        else:
            esperado = "?"
            estado = "❓"
        
        distribucion.append({
            'sucursal': sucursal,
            'operativas': ops,
            'seguridad': seg,
            'total': total,
            'tipo': tipo,
            'esperado': esperado,
            'estado': estado
        })
    
    return distribucion

def mostrar_distribucion_grupos_de_5(distribucion):
    """Mostrar distribución en grupos de 5 para validación de Roberto"""
    
    print(f"\n📋 VALIDACIÓN DISTRIBUCIÓN (Grupos de 5)")
    print("=" * 80)
    print(f"{'#':<3} {'Sucursal':<35} {'Ops':<4} {'Seg':<4} {'Tot':<4} {'Tipo':<8} {'Esp':<4} {'Estado'}")
    print("-" * 80)
    
    grupos_problemas = {'perfectas': [], 'problemas': [], 'vacias': []}
    
    for i, item in enumerate(distribucion, 1):
        sucursal_short = item['sucursal'][:34]
        print(f"{i:<3} {sucursal_short:<35} {item['operativas']:<4} {item['seguridad']:<4} {item['total']:<4} {item['tipo']:<8} {item['esperado']:<4} {item['estado']}")
        
        # Clasificar para análisis
        if item['estado'] == '✅':
            grupos_problemas['perfectas'].append(item)
        elif item['estado'] == '⚠️':
            grupos_problemas['problemas'].append(item)
        else:
            grupos_problemas['vacias'].append(item)
        
        # Cada 5 líneas, pausa
        if i % 5 == 0:
            print("-" * 80)
            print(f"📊 Grupo {(i//5)}: Completado - ¿Continuar? (Enter para seguir)")
            # En implementación real aquí habría input(), por ahora continúo
    
    return grupos_problemas

def analizar_problemas_especificos(grupos_problemas):
    """Analizar problemas específicos que necesitan normalización"""
    
    print(f"\n🔍 ANÁLISIS DE PROBLEMAS ESPECÍFICOS")
    print("=" * 60)
    
    perfectas = grupos_problemas['perfectas']
    problemas = grupos_problemas['problemas']
    vacias = grupos_problemas['vacias']
    
    print(f"📊 RESUMEN GENERAL:")
    print(f"   ✅ Perfectas (4+4 o 2+2): {len(perfectas)}")
    print(f"   ⚠️ Con problemas: {len(problemas)}")
    print(f"   ❌ Vacías: {len(vacias)}")
    
    if problemas:
        print(f"\n⚠️ SUCURSALES CON PROBLEMAS QUE NECESITAN NORMALIZACIÓN:")
        print(f"{'Sucursal':<35} {'Actual':<12} {'Esperado':<10} {'Problema'}")
        print("-" * 70)
        
        for item in problemas:
            problema_desc = ""
            actual_str = f"{item['operativas']}+{item['seguridad']}={item['total']}"
            esperado_str = f"4+4=8" if item['tipo'] == 'LOCAL' else f"2+2=4"
            
            if item['total'] > item['esperado']:
                problema_desc = f"EXCESO (+{item['total'] - item['esperado']})"
            elif item['total'] < item['esperado']:
                problema_desc = f"DEFICIT (-{item['esperado'] - item['total']})"
            else:
                problema_desc = "DISTRIBUCIÓN INCORRECTA"
            
            print(f"{item['sucursal'][:34]:<35} {actual_str:<12} {esperado_str:<10} {problema_desc}")
    
    return problemas

def analizar_fechas_coincidentes(df_ops, df_seg_completa, sucursal_especifica=None):
    """Analizar fechas coincidentes entre operativas y seguridad"""
    
    print(f"\n📅 ANÁLISIS FECHAS COINCIDENTES")
    print("=" * 50)
    
    if sucursal_especifica:
        print(f"🎯 Enfoque: {sucursal_especifica}")
        ops_filtradas = df_ops[df_ops['Location'] == sucursal_especifica]
        seg_filtradas = df_seg_completa[df_seg_completa['sucursal'] == sucursal_especifica]
    else:
        print(f"🎯 Enfoque: Todas las sucursales")
        ops_filtradas = df_ops[df_ops['Location'].notna()]
        seg_filtradas = df_seg_completa
    
    # Buscar coincidencias
    coincidencias = []
    
    for _, op in ops_filtradas.iterrows():
        fecha_op = pd.to_datetime(op['Date Submitted']).date()
        sucursal_op = op['Location']
        usuario_op = op['Submitted By']
        
        # Buscar seguridad el mismo día en la misma sucursal
        seg_mismo_dia = df_seg_completa[
            (df_seg_completa['fecha'] == fecha_op) & 
            (df_seg_completa['sucursal'] == sucursal_op)
        ]
        
        if len(seg_mismo_dia) > 0:
            for _, seg in seg_mismo_dia.iterrows():
                coincidencias.append({
                    'fecha': fecha_op,
                    'sucursal': sucursal_op,
                    'usuario_op': usuario_op,
                    'usuario_seg': seg['usuario'],
                    'mismo_usuario': usuario_op == seg['usuario']
                })
    
    print(f"📊 Coincidencias encontradas: {len(coincidencias)}")
    
    if coincidencias:
        # Mostrar algunas coincidencias ejemplo
        print(f"\n📋 EJEMPLOS DE COINCIDENCIAS (primeras 10):")
        print(f"{'Fecha':<12} {'Sucursal':<25} {'Usuario Op':<15} {'Usuario Seg':<15} {'Mismo'}")
        print("-" * 85)
        
        for i, coin in enumerate(coincidencias[:10], 1):
            mismo_str = "✅" if coin['mismo_usuario'] else "❌"
            sucursal_short = coin['sucursal'][:24]
            print(f"{coin['fecha']:<12} {sucursal_short:<25} {coin['usuario_op']:<15} {coin['usuario_seg']:<15} {mismo_str}")
    
    return coincidencias

def main():
    """Función principal"""
    
    print("🔍 VALIDACIÓN DISTRIBUCIÓN COMPLETA")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Validar estado actual 4+4/2+2 con fechas coincidentes")
    print("💡 Metodología: Fechas → Coordenadas → Campo Sucursal (apoyo)")
    print("=" * 80)
    
    # 1. Cargar datos
    df_ops, seg_con_location, df_asignaciones = cargar_datos_actualizados()
    
    # 2. Combinar seguridad completa
    df_seg_completa = combinar_seguridad_completa(seg_con_location, df_asignaciones)
    
    # 3. Analizar distribución
    distribucion = analizar_distribucion_por_sucursal(df_ops, df_seg_completa)
    
    # 4. Mostrar en grupos de 5
    grupos_problemas = mostrar_distribucion_grupos_de_5(distribucion)
    
    # 5. Analizar problemas específicos
    problemas = analizar_problemas_especificos(grupos_problemas)
    
    # 6. Analizar fechas coincidentes
    coincidencias = analizar_fechas_coincidentes(df_ops, df_seg_completa)
    
    print(f"\n🎯 PRÓXIMOS PASOS VALIDADOS:")
    print(f"   1. Roberto valida distribuciones de 5 en 5")
    print(f"   2. Identificamos sucursales con problemas específicos")
    print(f"   3. Normalizamos usando fechas coincidentes PRIMERO")
    print(f"   4. Solo después coordenadas y campo Sucursal de apoyo")
    
    return distribucion, problemas, coincidencias

if __name__ == "__main__":
    main()