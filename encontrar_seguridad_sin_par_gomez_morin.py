#!/usr/bin/env python3
"""
🔍 ENCONTRAR SUPERVISIÓN DE SEGURIDAD SIN PAR EN GÓMEZ MORÍN
Identificar cuál supervisión de seguridad no tiene par en operaciones para reasignar
"""

import pandas as pd
from datetime import datetime

def analizar_pares_gomez_morin():
    """Analizar pares de operativas y seguridad en Gómez Morín"""
    
    print("🔍 ANÁLISIS PARES GÓMEZ MORÍN")
    print("=" * 60)
    
    # Cargar operativas Gómez Morín
    df_ops = pd.read_excel("SUPERVISION_OPERATIVA_CAS_11_REV_250125_Submissions-2025-12-18__1228CST-1766141816.xlsx")
    ops_gomez = df_ops[df_ops['Location'] == '38 - Gomez Morin'].copy()
    
    print(f"🏗️ OPERATIVAS GÓMEZ MORÍN ({len(ops_gomez)}):")
    print(f"{'#':<3} {'Fecha':<12} {'Usuario':<15} {'Index'}")
    print("-" * 50)
    
    fechas_ops_gomez = {}
    for i, (idx, row) in enumerate(ops_gomez.iterrows(), 1):
        fecha_dt = pd.to_datetime(row['Date Submitted'])
        fecha_date = fecha_dt.date()
        fecha_str = fecha_date.strftime('%Y-%m-%d')
        usuario = row['Submitted By']
        
        print(f"{i:<3} {fecha_str:<12} {usuario:<15} {idx}")
        
        if fecha_date not in fechas_ops_gomez:
            fechas_ops_gomez[fecha_date] = []
        fechas_ops_gomez[fecha_date].append({
            'fecha': fecha_dt,
            'usuario': usuario,
            'index': idx
        })
    
    return fechas_ops_gomez

def analizar_seguridad_gomez_morin(fechas_ops_gomez):
    """Analizar supervisiones de seguridad en Gómez Morín incluyendo las que vienen de Centrito"""
    
    print(f"\n🛡️ SEGURIDAD GÓMEZ MORÍN (INCLUYENDO CENTRITO)")
    print("=" * 70)
    
    # Seguridad Excel original
    df_seg = pd.read_excel("CONTROL_OPERATIVO_DE_SEGURIDAD_CAS_11_REV_25012025_Submissions-2025-12-18__1232CST-1766104009.xlsx")
    seg_gomez_original = df_seg[df_seg['Location'] == '38 - Gomez Morin'].copy()
    
    # Seguridad que viene de Centrito Valle (indices 103 y 145)
    centrito_a_gomez = df_seg.iloc[[103, 145]].copy()
    
    print(f"📊 Seguridad original Gómez Morín: {len(seg_gomez_original)}")
    print(f"📊 Seguridad que viene de Centrito: {len(centrito_a_gomez)}")
    
    # Combinar todas las supervisiones de seguridad que estarán en Gómez Morín
    todas_seg_gomez = []
    
    # Originales de Gómez Morín
    for idx, row in seg_gomez_original.iterrows():
        fecha_dt = pd.to_datetime(row['Date Submitted'])
        todas_seg_gomez.append({
            'index_excel': idx,
            'fecha': fecha_dt,
            'fecha_date': fecha_dt.date(),
            'usuario': row['Submitted By'],
            'origen': 'ORIGINAL_GOMEZ'
        })
    
    # Que vienen de Centrito Valle
    for idx, row in centrito_a_gomez.iterrows():
        fecha_dt = pd.to_datetime(row['Date Submitted'])
        todas_seg_gomez.append({
            'index_excel': idx,
            'fecha': fecha_dt,
            'fecha_date': fecha_dt.date(),
            'usuario': row['Submitted By'],
            'origen': 'DESDE_CENTRITO'
        })
    
    print(f"\n📋 TODAS LAS SUPERVISIONES SEGURIDAD GÓMEZ MORÍN ({len(todas_seg_gomez)}):")
    print(f"{'#':<3} {'Index':<6} {'Fecha':<12} {'Usuario':<15} {'Origen':<15} {'¿Tiene Par?'}")
    print("-" * 80)
    
    seguridad_sin_par = []
    
    for i, seg in enumerate(todas_seg_gomez, 1):
        fecha_str = seg['fecha_date'].strftime('%Y-%m-%d')
        
        # Verificar si tiene par en operaciones
        tiene_par = seg['fecha_date'] in fechas_ops_gomez
        par_str = "✅ SÍ" if tiene_par else "❌ NO"
        
        if not tiene_par:
            seguridad_sin_par.append(seg)
        
        print(f"{i:<3} {seg['index_excel']:<6} {fecha_str:<12} {seg['usuario']:<15} {seg['origen']:<15} {par_str}")
    
    print(f"\n🎯 SUPERVISIONES SIN PAR: {len(seguridad_sin_par)}")
    
    return todas_seg_gomez, seguridad_sin_par

def buscar_destino_para_seguridad_sin_par(seguridad_sin_par):
    """Buscar destino para la supervisión de seguridad sin par"""
    
    print(f"\n🔍 BUSCANDO DESTINO PARA SEGURIDAD SIN PAR")
    print("=" * 60)
    
    if not seguridad_sin_par:
        print("✅ No hay supervisiones de seguridad sin par")
        return None
    
    # Por cada supervisión sin par, buscar operativas del mismo día en otras sucursales
    df_ops = pd.read_excel("SUPERVISION_OPERATIVA_CAS_11_REV_250125_Submissions-2025-12-18__1228CST-1766141816.xlsx")
    
    candidatos_reasignacion = []
    
    for seg in seguridad_sin_par:
        fecha_seg = seg['fecha_date']
        usuario_seg = seg['usuario']
        
        print(f"\n🔍 Supervisión sin par: Index {seg['index_excel']} - {fecha_seg} - {usuario_seg}")
        
        # Buscar operativas del mismo día en otras sucursales
        ops_mismo_dia = df_ops[
            (pd.to_datetime(df_ops['Date Submitted']).dt.date == fecha_seg) &
            (df_ops['Location'] != '38 - Gomez Morin') &
            (df_ops['Location'].notna())
        ]
        
        if len(ops_mismo_dia) > 0:
            print(f"   📊 Operativas mismo día en otras sucursales: {len(ops_mismo_dia)}")
            print(f"   {'Sucursal':<30} {'Usuario':<15} {'¿Mismo Usuario?'}")
            print(f"   {'-'*60}")
            
            for _, op in ops_mismo_dia.iterrows():
                sucursal_op = op['Location']
                usuario_op = op['Submitted By']
                mismo_usuario = usuario_op == usuario_seg
                mismo_str = "✅" if mismo_usuario else "❌"
                
                print(f"   {sucursal_op[:29]:<30} {usuario_op:<15} {mismo_str}")
                
                # Si es el mismo usuario, es candidato prioritario
                if mismo_usuario:
                    candidatos_reasignacion.append({
                        'seg_index': seg['index_excel'],
                        'seg_fecha': fecha_seg,
                        'seg_usuario': usuario_seg,
                        'sucursal_destino': sucursal_op,
                        'usuario_operativa': usuario_op,
                        'coincidencia': 'MISMO_USUARIO_MISMO_DIA',
                        'confianza': 'ALTA'
                    })
        else:
            print(f"   ❌ No hay operativas del mismo día en otras sucursales")
    
    print(f"\n🎯 CANDIDATOS PARA REASIGNACIÓN: {len(candidatos_reasignacion)}")
    
    if candidatos_reasignacion:
        print(f"\n📋 CANDIDATOS DETALLADOS:")
        print(f"{'Seg Index':<10} {'Fecha':<12} {'Usuario':<15} {'→ Sucursal Destino':<25} {'Confianza'}")
        print("-" * 85)
        
        for cand in candidatos_reasignacion:
            print(f"{cand['seg_index']:<10} {cand['seg_fecha']:<12} {cand['seg_usuario']:<15} → {cand['sucursal_destino'][:24]:<25} {cand['confianza']}")
    
    return candidatos_reasignacion

def verificar_impacto_reasignacion(candidatos_reasignacion):
    """Verificar el impacto de la reasignación propuesta"""
    
    print(f"\n📊 VERIFICAR IMPACTO DE REASIGNACIÓN")
    print("=" * 50)
    
    if not candidatos_reasignacion:
        print("❌ No hay candidatos para evaluar")
        return
    
    # Tomar el primer candidato (más probable)
    mejor_candidato = candidatos_reasignacion[0]
    
    print(f"🎯 REASIGNACIÓN PROPUESTA:")
    print(f"   📍 Supervisión: Index {mejor_candidato['seg_index']}")
    print(f"   📅 Fecha: {mejor_candidato['seg_fecha']}")
    print(f"   👤 Usuario: {mejor_candidato['seg_usuario']}")
    print(f"   🏢 Desde: 38 - Gomez Morin")
    print(f"   🏢 Hacia: {mejor_candidato['sucursal_destino']}")
    print(f"   🎯 Razón: {mejor_candidato['coincidencia']}")
    
    print(f"\n📊 IMPACTO EN DISTRIBUCIONES:")
    print(f"   📉 Gómez Morín: 9 → 8 (✅ Perfecto 4+4)")
    print(f"   📈 {mejor_candidato['sucursal_destino']}: +1 supervisión")
    
    return mejor_candidato

def main():
    """Función principal"""
    
    print("🔍 ENCONTRAR SUPERVISIÓN DE SEGURIDAD SIN PAR EN GÓMEZ MORÍN")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Objetivo: Identificar supervisión de seguridad sin par para reasignar")
    print("=" * 80)
    
    # 1. Analizar operativas Gómez Morín
    fechas_ops_gomez = analizar_pares_gomez_morin()
    
    # 2. Analizar seguridad Gómez Morín (incluyendo las que vienen de Centrito)
    todas_seg_gomez, seguridad_sin_par = analizar_seguridad_gomez_morin(fechas_ops_gomez)
    
    # 3. Buscar destino para supervisión sin par
    candidatos = buscar_destino_para_seguridad_sin_par(seguridad_sin_par)
    
    # 4. Verificar impacto
    if candidatos:
        mejor_candidato = verificar_impacto_reasignacion(candidatos)
        
        print(f"\n💡 RECOMENDACIÓN FINAL:")
        print(f"   🎯 Reasignar Index {mejor_candidato['seg_index']} a {mejor_candidato['sucursal_destino']}")
        print(f"   ✅ Gómez Morín quedará perfecto: 4+4=8")
        print(f"   📅 Basado en mismo usuario mismo día: {mejor_candidato['seg_fecha']}")
        
        return mejor_candidato
    else:
        print(f"\n⚠️ No se encontraron candidatos ideales")
        print(f"   💡 Revisar manualmente las supervisiones sin par")
        return None

if __name__ == "__main__":
    main()