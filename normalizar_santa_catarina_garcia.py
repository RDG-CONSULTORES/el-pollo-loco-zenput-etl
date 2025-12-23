#!/usr/bin/env python3
"""
🔧 NORMALIZAR SANTA CATARINA Y GARCIA
Ambas necesitan +1 supervisión para llegar a 4+4
Buscar supervisiones con fechas coincidentes para transferir
"""

import pandas as pd
from datetime import datetime

def analizar_estado_actual_santa_garcia():
    """Analizar estado actual de Santa Catarina y Garcia"""
    
    print("🔍 ESTADO ACTUAL SANTA CATARINA Y GARCIA")
    print("=" * 60)
    
    # Operativas
    df_ops = pd.read_excel("SUPERVISION_OPERATIVA_CAS_11_REV_250125_Submissions-2025-12-18__1228CST-1766141816.xlsx")
    ops_santa = df_ops[df_ops['Location'] == '4 - Santa Catarina']
    ops_garcia = df_ops[df_ops['Location'] == '6 - Garcia']
    
    # Seguridad Excel directo
    df_seg = pd.read_excel("CONTROL_OPERATIVO_DE_SEGURIDAD_CAS_11_REV_25012025_Submissions-2025-12-18__1232CST-1766104009.xlsx")
    seg_santa_excel = df_seg[df_seg['Location'] == '4 - Santa Catarina']
    seg_garcia_excel = df_seg[df_seg['Location'] == '6 - Garcia']
    
    # Seguridad de asignaciones Google Maps
    df_asignaciones = pd.read_csv("ASIGNACIONES_NORMALIZADAS_TEMP.csv")
    seg_santa_asig = df_asignaciones[df_asignaciones['sucursal_asignada'] == '4 - Santa Catarina']
    seg_garcia_asig = df_asignaciones[df_asignaciones['sucursal_asignada'] == '6 - Garcia']
    
    print(f"📊 SANTA CATARINA:")
    print(f"   🏗️ Operativas: {len(ops_santa)}")
    print(f"   🛡️ Seguridad Excel: {len(seg_santa_excel)}")
    print(f"   🛡️ Seguridad asignada: {len(seg_santa_asig)}")
    print(f"   📊 Total: {len(ops_santa)} + {len(seg_santa_excel) + len(seg_santa_asig)} = {len(ops_santa) + len(seg_santa_excel) + len(seg_santa_asig)}")
    
    print(f"\n📊 GARCIA:")
    print(f"   🏗️ Operativas: {len(ops_garcia)}")
    print(f"   🛡️ Seguridad Excel: {len(seg_garcia_excel)}")
    print(f"   🛡️ Seguridad asignada: {len(seg_garcia_asig)}")
    print(f"   📊 Total: {len(ops_garcia)} + {len(seg_garcia_excel) + len(seg_garcia_asig)} = {len(ops_garcia) + len(seg_garcia_excel) + len(seg_garcia_asig)}")
    
    return {
        'santa_catarina': {
            'operativas': ops_santa,
            'seguridad_excel': seg_santa_excel,
            'seguridad_asignada': seg_santa_asig
        },
        'garcia': {
            'operativas': ops_garcia,
            'seguridad_excel': seg_garcia_excel,
            'seguridad_asignada': seg_garcia_asig
        }
    }

def analizar_fechas_coincidentes_santa_garcia(datos):
    """Analizar fechas coincidentes para Santa Catarina y Garcia"""
    
    print(f"\n📅 ANÁLISIS FECHAS COINCIDENTES")
    print("=" * 50)
    
    for sucursal, data in datos.items():
        print(f"\n🏢 {sucursal.upper().replace('_', ' ')}:")
        
        # Recopilar todas las fechas
        fechas_ops = []
        fechas_seg = []
        
        # Operativas
        for _, row in data['operativas'].iterrows():
            fecha = pd.to_datetime(row['Date Submitted']).date()
            usuario = row['Submitted By']
            fechas_ops.append({'fecha': fecha, 'usuario': usuario, 'tipo': 'operativa'})
        
        # Seguridad Excel
        for _, row in data['seguridad_excel'].iterrows():
            fecha = pd.to_datetime(row['Date Submitted']).date()
            usuario = row['Submitted By']
            fechas_seg.append({'fecha': fecha, 'usuario': usuario, 'tipo': 'seguridad_excel'})
        
        # Seguridad asignada
        for _, row in data['seguridad_asignada'].iterrows():
            fecha = pd.to_datetime(row['fecha']).date()
            usuario = row['usuario']
            fechas_seg.append({'fecha': fecha, 'usuario': usuario, 'tipo': 'seguridad_asignada'})
        
        # Análisis de coincidencias
        fechas_ops_set = {item['fecha'] for item in fechas_ops}
        fechas_seg_set = {item['fecha'] for item in fechas_seg}
        coincidencias = fechas_ops_set & fechas_seg_set
        
        print(f"   📅 Fechas operativas: {sorted(fechas_ops_set)}")
        print(f"   📅 Fechas seguridad: {sorted(fechas_seg_set)}")
        print(f"   ✅ Coincidencias: {sorted(coincidencias)} ({len(coincidencias)} días)")
        print(f"   📊 Balance: {len(fechas_ops)}+{len(fechas_seg)}={len(fechas_ops)+len(fechas_seg)} | Objetivo: 4+4=8")
        
        if len(fechas_ops) + len(fechas_seg) == 7:
            print(f"   🎯 Necesita: +1 supervisión")

def buscar_supervisiones_transferibles():
    """Buscar supervisiones que se pueden transferir a Santa Catarina o Garcia"""
    
    print(f"\n🔍 BUSCAR SUPERVISIONES TRANSFERIBLES")
    print("=" * 60)
    
    # Buscar sucursales con EXCESO que puedan dar supervisiones
    df_ops = pd.read_excel("SUPERVISION_OPERATIVA_CAS_11_REV_250125_Submissions-2025-12-18__1228CST-1766141816.xlsx")
    df_seg = pd.read_excel("CONTROL_OPERATIVO_DE_SEGURIDAD_CAS_11_REV_25012025_Submissions-2025-12-18__1232CST-1766104009.xlsx")
    df_asignaciones = pd.read_csv("ASIGNACIONES_NORMALIZADAS_TEMP.csv")
    
    # Contar por sucursal
    ops_count = df_ops[df_ops['Location'].notna()]['Location'].value_counts()
    
    # Seguridad Excel
    seg_excel_count = df_seg[df_seg['Location'].notna()]['Location'].value_counts()
    
    # Seguridad asignada
    seg_asig_count = df_asignaciones['sucursal_asignada'].value_counts()
    
    # Combinar conteos
    todas_sucursales = set(ops_count.index.tolist() + seg_excel_count.index.tolist() + seg_asig_count.index.tolist())
    
    sucursales_con_exceso = []
    
    print(f"🔍 SUCURSALES CON POSIBLE EXCESO:")
    print(f"{'Sucursal':<35} {'Ops':<4} {'Seg':<4} {'Tot':<4} {'Estado'}")
    print("-" * 60)
    
    for sucursal in sorted(todas_sucursales):
        ops = ops_count.get(sucursal, 0)
        seg_excel = seg_excel_count.get(sucursal, 0)
        seg_asig = seg_asig_count.get(sucursal, 0)
        seg_total = seg_excel + seg_asig
        total = ops + seg_total
        
        # Buscar sucursales con total > 8
        if total > 8:
            exceso = total - 8
            sucursales_con_exceso.append({
                'sucursal': sucursal,
                'operativas': ops,
                'seguridad': seg_total,
                'total': total,
                'exceso': exceso
            })
            print(f"{sucursal[:34]:<35} {ops:<4} {seg_total:<4} {total:<4} ⚠️ +{exceso}")
        elif total == 7:
            print(f"{sucursal[:34]:<35} {ops:<4} {seg_total:<4} {total:<4} ⚠️ -1")
    
    return sucursales_con_exceso

def buscar_candidatos_transferencia_por_fechas():
    """Buscar candidatos específicos para transferir basado en fechas coincidentes"""
    
    print(f"\n📅 BUSCAR CANDIDATOS POR FECHAS COINCIDENTES")
    print("=" * 70)
    
    # Fechas donde Santa Catarina y Garcia tienen operativas
    df_ops = pd.read_excel("SUPERVISION_OPERATIVA_CAS_11_REV_250125_Submissions-2025-12-18__1228CST-1766141816.xlsx")
    
    ops_santa = df_ops[df_ops['Location'] == '4 - Santa Catarina']
    ops_garcia = df_ops[df_ops['Location'] == '6 - Garcia']
    
    fechas_santa = {pd.to_datetime(row['Date Submitted']).date() for _, row in ops_santa.iterrows()}
    fechas_garcia = {pd.to_datetime(row['Date Submitted']).date() for _, row in ops_garcia.iterrows()}
    
    print(f"📅 Santa Catarina tiene operativas en: {sorted(fechas_santa)}")
    print(f"📅 Garcia tiene operativas en: {sorted(fechas_garcia)}")
    
    # Buscar supervisiones de seguridad en esas fechas específicas en otras sucursales
    df_seg = pd.read_excel("CONTROL_OPERATIVO_DE_SEGURIDAD_CAS_11_REV_25012025_Submissions-2025-12-18__1232CST-1766104009.xlsx")
    df_asignaciones = pd.read_csv("ASIGNACIONES_NORMALIZADAS_TEMP.csv")
    
    candidatos = []
    
    print(f"\n🔍 CANDIDATOS PARA SANTA CATARINA:")
    for fecha in fechas_santa:
        # Buscar seguridad en Excel esa fecha
        seg_fecha = df_seg[pd.to_datetime(df_seg['Date Submitted']).dt.date == fecha]
        seg_fecha = seg_fecha[seg_fecha['Location'].notna()]
        seg_fecha = seg_fecha[~seg_fecha['Location'].isin(['4 - Santa Catarina', '6 - Garcia'])]  # Excluir las que ya están
        
        if len(seg_fecha) > 0:
            print(f"   📅 {fecha}:")
            for _, row in seg_fecha.iterrows():
                print(f"      📍 Index {row.name}: {row['Location']} - {row['Submitted By']}")
                candidatos.append({
                    'index': row.name,
                    'fecha': fecha,
                    'sucursal_actual': row['Location'],
                    'usuario': row['Submitted By'],
                    'destino_sugerido': '4 - Santa Catarina',
                    'razon': f'Operativa Santa Catarina mismo día'
                })
    
    print(f"\n🔍 CANDIDATOS PARA GARCIA:")
    for fecha in fechas_garcia:
        # Buscar seguridad en Excel esa fecha
        seg_fecha = df_seg[pd.to_datetime(df_seg['Date Submitted']).dt.date == fecha]
        seg_fecha = seg_fecha[seg_fecha['Location'].notna()]
        seg_fecha = seg_fecha[~seg_fecha['Location'].isin(['4 - Santa Catarina', '6 - Garcia'])]
        
        if len(seg_fecha) > 0:
            print(f"   📅 {fecha}:")
            for _, row in seg_fecha.iterrows():
                print(f"      📍 Index {row.name}: {row['Location']} - {row['Submitted By']}")
                candidatos.append({
                    'index': row.name,
                    'fecha': fecha,
                    'sucursal_actual': row['Location'],
                    'usuario': row['Submitted By'],
                    'destino_sugerido': '6 - Garcia',
                    'razon': f'Operativa Garcia mismo día'
                })
    
    return candidatos

def proponer_transferencias(candidatos):
    """Proponer transferencias específicas para Roberto"""
    
    print(f"\n💡 PROPUESTAS DE TRANSFERENCIA")
    print("=" * 60)
    
    if not candidatos:
        print("❌ No se encontraron candidatos ideales por fechas coincidentes")
        print("💡 Recomendación: Revisar manualmente supervisiones de sucursales con exceso")
        return []
    
    print(f"🎯 CANDIDATOS ENCONTRADOS: {len(candidatos)}")
    print(f"\n{'Index':<6} {'Fecha':<12} {'Desde':<25} {'→ Hacia':<25} {'Usuario':<15} {'Razón'}")
    print("-" * 100)
    
    mejores_candidatos = []
    
    for cand in candidatos[:5]:  # Top 5 candidatos
        index = cand['index']
        fecha = cand['fecha']
        desde = cand['sucursal_actual'][:24]
        hacia = cand['destino_sugerido'][:24]
        usuario = cand['usuario'][:14]
        razon = cand['razon'][:20]
        
        print(f"{index:<6} {fecha:<12} {desde:<25} → {hacia:<25} {usuario:<15} {razon}")
        mejores_candidatos.append(cand)
    
    return mejores_candidatos

def main():
    """Función principal"""
    
    print("🔧 NORMALIZAR SANTA CATARINA Y GARCIA")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Ambas necesitan +1 supervisión para llegar a 4+4")
    print("📅 Metodología: Buscar por fechas coincidentes")
    print("=" * 80)
    
    # 1. Analizar estado actual
    datos = analizar_estado_actual_santa_garcia()
    
    # 2. Analizar fechas coincidentes
    analizar_fechas_coincidentes_santa_garcia(datos)
    
    # 3. Buscar supervisiones transferibles
    sucursales_exceso = buscar_supervisiones_transferibles()
    
    # 4. Buscar candidatos por fechas
    candidatos = buscar_candidatos_transferencia_por_fechas()
    
    # 5. Proponer transferencias
    mejores_candidatos = proponer_transferencias(candidatos)
    
    print(f"\n🎯 RESUMEN:")
    print(f"   📊 Santa Catarina: 3+4=7, necesita +1")
    print(f"   📊 Garcia: 3+4=7, necesita +1") 
    print(f"   🔍 Candidatos encontrados: {len(candidatos)}")
    print(f"   💡 Esperando confirmación Roberto para transferencias específicas")
    
    return datos, candidatos, mejores_candidatos

if __name__ == "__main__":
    main()