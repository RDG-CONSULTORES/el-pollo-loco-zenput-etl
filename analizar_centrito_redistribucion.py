#!/usr/bin/env python3
"""
🔧 ANÁLISIS CENTRITO VALLE PARA REDISTRIBUCIÓN
Identificar cuáles supervisiones mover a Gómez Morín
"""

import pandas as pd
from datetime import datetime

def analizar_centrito_detallado():
    """Analizar en detalle las supervisiones de Centrito Valle"""
    
    print("🔧 ANÁLISIS DETALLADO CENTRITO VALLE")
    print("=" * 60)
    
    # Cargar datos
    df_ops = pd.read_excel("SUPERVISION_OPERATIVA_CAS_11_REV_250125_Submissions-2025-12-18__1228CST-1766141816.xlsx")
    df_seg = pd.read_excel("CONTROL_OPERATIVO_DE_SEGURIDAD_CAS_11_REV_25012025_Submissions-2025-12-18__1232CST-1766104009.xlsx")
    df_asignaciones = pd.read_csv("ASIGNACIONES_FINALES_CORREGIDAS_20251218_140924.csv")
    
    print("✅ Datos cargados")
    
    # OPERATIVAS de Centrito Valle
    ops_centrito = df_ops[df_ops['Location'] == '71 - Centrito Valle'].copy()
    ops_centrito['tipo'] = 'OPERATIVA'
    
    print(f"\n🏗️ OPERATIVAS CENTRITO VALLE ({len(ops_centrito)}):")
    print(f"{'#':<3} {'Fecha':<12} {'Hora':<8} {'Usuario':<15} {'Submission ID'}")
    print("-" * 60)
    
    operativas_lista = []
    for i, (idx, row) in enumerate(ops_centrito.iterrows(), 1):
        fecha_dt = pd.to_datetime(row['Date Submitted'])
        fecha_str = fecha_dt.strftime('%Y-%m-%d')
        hora_str = fecha_dt.strftime('%H:%M')
        usuario = row['Submitted By']
        sub_id = row.get('Submission ID', f'ops_{idx}')
        
        print(f"{i:<3} {fecha_str:<12} {hora_str:<8} {usuario:<15} {sub_id}")
        
        operativas_lista.append({
            'numero': i,
            'index_excel': idx,
            'fecha': row['Date Submitted'],
            'usuario': usuario,
            'submission_id': sub_id,
            'tipo': 'OPERATIVA',
            'location_original': '71 - Centrito Valle'
        })
    
    # SEGURIDAD de Centrito Valle (del Excel original)
    seg_centrito_excel = df_seg[df_seg['Location'] == '71 - Centrito Valle'].copy()
    
    print(f"\n🛡️ SEGURIDAD CENTRITO VALLE - EXCEL DIRECTO ({len(seg_centrito_excel)}):")
    print(f"{'#':<3} {'Fecha':<12} {'Hora':<8} {'Usuario':<15} {'Submission ID'}")
    print("-" * 60)
    
    seguridad_excel_lista = []
    for i, (idx, row) in enumerate(seg_centrito_excel.iterrows(), 1):
        fecha_dt = pd.to_datetime(row['Date Submitted'])
        fecha_str = fecha_dt.strftime('%Y-%m-%d')
        hora_str = fecha_dt.strftime('%H:%M')
        usuario = row['Submitted By']
        sub_id = row.get('Submission ID', f'seg_{idx}')
        
        print(f"{i:<3} {fecha_str:<12} {hora_str:<8} {usuario:<15} {sub_id}")
        
        seguridad_excel_lista.append({
            'numero': i,
            'index_excel': idx,
            'fecha': row['Date Submitted'],
            'usuario': usuario,
            'submission_id': sub_id,
            'tipo': 'SEGURIDAD_EXCEL',
            'location_original': '71 - Centrito Valle'
        })
    
    # SEGURIDAD asignada a Centrito Valle (de asignaciones)
    seg_asignada_centrito = df_asignaciones[df_asignaciones['sucursal_asignada'] == '71 - Centrito Valle']
    
    print(f"\n🛡️ SEGURIDAD CENTRITO VALLE - ASIGNADA ({len(seg_asignada_centrito)}):")
    print(f"{'#':<3} {'Fecha':<12} {'Hora':<8} {'Usuario':<15} {'Index Excel':<12} {'Método'}")
    print("-" * 75)
    
    seguridad_asignada_lista = []
    for i, (_, row) in enumerate(seg_asignada_centrito.iterrows(), 1):
        fecha_dt = pd.to_datetime(row['fecha'])
        fecha_str = fecha_dt.strftime('%Y-%m-%d')
        hora_str = fecha_dt.strftime('%H:%M')
        usuario = row['usuario']
        index_excel = row['index_original']
        metodo = row['metodo']
        
        print(f"{i:<3} {fecha_str:<12} {hora_str:<8} {usuario:<15} {index_excel:<12} {metodo}")
        
        seguridad_asignada_lista.append({
            'numero': i,
            'index_excel': index_excel,
            'fecha': row['fecha'],
            'usuario': usuario,
            'submission_id': f'seg_{index_excel}',
            'tipo': 'SEGURIDAD_ASIGNADA',
            'metodo': metodo,
            'location_original': 'SIN_LOCATION'
        })
    
    return operativas_lista, seguridad_excel_lista, seguridad_asignada_lista

def sugerir_redistribucion(operativas_lista, seguridad_excel_lista, seguridad_asignada_lista):
    """Sugerir cuáles supervisiones redistribuir"""
    
    print(f"\n💡 SUGERENCIAS DE REDISTRIBUCIÓN")
    print("=" * 60)
    
    print(f"🎯 OBJETIVO: Mover 1 operativa + 1 seguridad de Centrito Valle → Gómez Morín")
    print(f"📊 Estado actual: Centrito 5+5=10, Gómez Morín 3+4=7")
    print(f"📊 Estado objetivo: Centrito 4+4=8, Gómez Morín 4+4=8")
    
    # Criterios de selección:
    # 1. Fecha más reciente (más fácil justificar el cambio)
    # 2. Mismo día operativa + seguridad (si es posible)
    # 3. Usuario que también trabaja en Gómez Morín
    
    # Ordenar por fecha (más recientes primero)
    operativas_ordenadas = sorted(operativas_lista, key=lambda x: x['fecha'], reverse=True)
    seguridad_total = seguridad_excel_lista + seguridad_asignada_lista
    seguridad_ordenada = sorted(seguridad_total, key=lambda x: x['fecha'], reverse=True)
    
    print(f"\n🔍 ANÁLISIS PARA SELECCIÓN:")
    
    # Buscar pares del mismo día
    print(f"\n📅 PARES MISMO DÍA (operativa + seguridad):")
    pares_mismo_dia = []
    
    for op in operativas_ordenadas:
        fecha_op = pd.to_datetime(op['fecha']).date()
        
        for seg in seguridad_ordenada:
            fecha_seg = pd.to_datetime(seg['fecha']).date()
            
            if fecha_op == fecha_seg:
                pares_mismo_dia.append({
                    'fecha': fecha_op,
                    'operativa': op,
                    'seguridad': seg
                })
    
    if pares_mismo_dia:
        print(f"   ✅ Encontrados {len(pares_mismo_dia)} pares del mismo día")
        
        # Mostrar el par más reciente
        par_mas_reciente = max(pares_mismo_dia, key=lambda x: x['fecha'])
        
        print(f"\n🎯 RECOMENDACIÓN PRINCIPAL - PAR DEL MISMO DÍA:")
        print(f"   📅 Fecha: {par_mas_reciente['fecha']}")
        print(f"   🏗️ Operativa: {par_mas_reciente['operativa']['usuario']} (Index: {par_mas_reciente['operativa']['index_excel']})")
        print(f"   🛡️ Seguridad: {par_mas_reciente['seguridad']['usuario']} (Index: {par_mas_reciente['seguridad']['index_excel']}, Tipo: {par_mas_reciente['seguridad']['tipo']})")
        
        return par_mas_reciente
    
    else:
        print(f"   ⚠️ No hay pares del mismo día")
        
        # Seleccionar por separado - más recientes
        operativa_sugerida = operativas_ordenadas[0]
        seguridad_sugerida = seguridad_ordenada[0]
        
        print(f"\n🎯 RECOMENDACIÓN ALTERNATIVA - MÁS RECIENTES:")
        print(f"   🏗️ Operativa más reciente: {operativa_sugerida['fecha']} - {operativa_sugerida['usuario']} (Index: {operativa_sugerida['index_excel']})")
        print(f"   🛡️ Seguridad más reciente: {seguridad_sugerida['fecha']} - {seguridad_sugerida['usuario']} (Index: {seguridad_sugerida['index_excel']}, Tipo: {seguridad_sugerida['tipo']})")
        
        return {
            'fecha': 'DIFERENTES',
            'operativa': operativa_sugerida,
            'seguridad': seguridad_sugerida
        }

def mostrar_opciones_completas(operativas_lista, seguridad_excel_lista, seguridad_asignada_lista):
    """Mostrar todas las opciones para que Roberto elija"""
    
    print(f"\n📋 TODAS LAS OPCIONES DISPONIBLES PARA ROBERTO")
    print("=" * 80)
    
    print(f"\n🏗️ OPERATIVAS DISPONIBLES (elegir 1):")
    print(f"{'Opt':<4} {'Fecha':<12} {'Usuario':<15} {'Index':<8} {'Submission ID'}")
    print("-" * 60)
    
    for i, op in enumerate(operativas_lista, 1):
        fecha_str = pd.to_datetime(op['fecha']).strftime('%Y-%m-%d')
        print(f"O{i:<3} {fecha_str:<12} {op['usuario']:<15} {op['index_excel']:<8} {op['submission_id']}")
    
    print(f"\n🛡️ SEGURIDAD DISPONIBLES (elegir 1):")
    print(f"{'Opt':<4} {'Fecha':<12} {'Usuario':<15} {'Index':<8} {'Tipo':<15} {'Origen'}")
    print("-" * 80)
    
    todas_seguridad = seguridad_excel_lista + seguridad_asignada_lista
    for i, seg in enumerate(todas_seguridad, 1):
        fecha_str = pd.to_datetime(seg['fecha']).strftime('%Y-%m-%d')
        tipo = seg['tipo']
        origen = seg['location_original']
        print(f"S{i:<3} {fecha_str:<12} {seg['usuario']:<15} {seg['index_excel']:<8} {tipo:<15} {origen}")
    
    print(f"\n💡 COMANDO PARA ROBERTO:")
    print(f"   Dime: 'O<número> S<número>' para elegir qué operativa y seguridad mover")
    print(f"   Ejemplo: 'O1 S1' = mover operativa #1 y seguridad #1 a Gómez Morín")

def main():
    """Función principal"""
    
    print("🔧 ANÁLISIS CENTRITO VALLE PARA REDISTRIBUCIÓN")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Objetivo: Identificar supervisiones específicas para mover a Gómez Morín")
    print("=" * 80)
    
    # 1. Analizar Centrito en detalle
    operativas_lista, seguridad_excel_lista, seguridad_asignada_lista = analizar_centrito_detallado()
    
    # 2. Sugerir redistribución óptima
    sugerencia = sugerir_redistribucion(operativas_lista, seguridad_excel_lista, seguridad_asignada_lista)
    
    # 3. Mostrar todas las opciones
    mostrar_opciones_completas(operativas_lista, seguridad_excel_lista, seguridad_asignada_lista)
    
    # 4. Guardar para referencia
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    import json
    with open(f"CENTRITO_REDISTRIBUCION_OPCIONES_{timestamp}.json", 'w', encoding='utf-8') as f:
        json.dump({
            'timestamp': timestamp,
            'operativas_disponibles': operativas_lista,
            'seguridad_excel_disponibles': seguridad_excel_lista,
            'seguridad_asignada_disponibles': seguridad_asignada_lista,
            'sugerencia_automatica': sugerencia
        }, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"\n📁 OPCIONES GUARDADAS: CENTRITO_REDISTRIBUCION_OPCIONES_{timestamp}.json")
    
    return operativas_lista, seguridad_excel_lista, seguridad_asignada_lista, sugerencia

if __name__ == "__main__":
    main()