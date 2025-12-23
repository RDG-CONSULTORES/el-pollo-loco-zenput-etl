#!/usr/bin/env python3
"""
🔍 BUSCAR OPERATIVAS 2025-05-14
Encontrar sucursales con operativa del 14-mayo para redistribuir seguridad sobrante de Pedro Cardenas
"""

import pandas as pd
from datetime import datetime

def cargar_dataset():
    """Cargar dataset normalizado"""
    df = pd.read_csv("DATASET_NORMALIZADO_20251218_155659.csv")
    return df

def buscar_operativas_fecha_especifica(df, fecha_target):
    """Buscar todas las operativas de una fecha específica"""
    
    print(f"🔍 BUSCAR OPERATIVAS DEL {fecha_target}")
    print("=" * 60)
    
    # Filtrar operativas del día específico
    operativas_fecha = df[(df['tipo'] == 'operativas') & (df['fecha_str'] == fecha_target)].copy()
    
    print(f"📊 OPERATIVAS ENCONTRADAS DEL {fecha_target}: {len(operativas_fecha)}")
    
    if len(operativas_fecha) == 0:
        print(f"❌ No se encontraron operativas del {fecha_target}")
        return None
    
    print(f"\n📋 LISTADO DE OPERATIVAS:")
    print(f"{'#':<3} {'Sucursal':<30} {'Submission ID':<18} {'Hora'}")
    print("-" * 80)
    
    operativas_por_sucursal = {}
    
    for i, (_, row) in enumerate(operativas_fecha.iterrows(), 1):
        sucursal = row['location_asignado']
        submission_id = row['submission_id']
        
        # Extraer hora si está disponible
        try:
            fecha_dt = pd.to_datetime(row['date_submitted'])
            hora = fecha_dt.strftime('%H:%M') if fecha_dt else 'N/A'
        except:
            hora = 'N/A'
        
        print(f"{i:<3} {sucursal:<30} {submission_id[:17]:<18} {hora}")
        
        # Agregar a diccionario
        if sucursal not in operativas_por_sucursal:
            operativas_por_sucursal[sucursal] = []
        operativas_por_sucursal[sucursal].append({
            'submission_id': submission_id,
            'hora': hora
        })
    
    return operativas_por_sucursal

def verificar_seguridad_misma_fecha(df, fecha_target, operativas_por_sucursal):
    """Verificar qué sucursales ya tienen seguridad del mismo día"""
    
    print(f"\n🔍 VERIFICAR SEGURIDAD DEL {fecha_target}")
    print("=" * 60)
    
    # Filtrar seguridad del día específico
    seguridad_fecha = df[(df['tipo'] == 'seguridad') & (df['fecha_str'] == fecha_target)].copy()
    
    # Agrupar por sucursal
    seguridad_por_sucursal = {}
    for _, row in seguridad_fecha.iterrows():
        sucursal = row['location_asignado']
        if sucursal not in seguridad_por_sucursal:
            seguridad_por_sucursal[sucursal] = []
        seguridad_por_sucursal[sucursal].append(row['submission_id'])
    
    print(f"📊 ANÁLISIS DE PARES DEL {fecha_target}:")
    print(f"{'Sucursal':<35} {'Operativas':<4} {'Seguridad':<4} {'Estado':<20} {'Candidato'}")
    print("-" * 90)
    
    candidatos = []
    
    for sucursal, ops_list in operativas_por_sucursal.items():
        ops_count = len(ops_list)
        seg_count = len(seguridad_por_sucursal.get(sucursal, []))
        
        if ops_count == seg_count:
            estado = "✅ BALANCEADO"
            candidato = "❌ No"
        elif ops_count > seg_count:
            deficit = ops_count - seg_count
            estado = f"❓ FALTA {deficit} seg"
            candidato = "✅ Sí" 
            candidatos.append({
                'sucursal': sucursal,
                'operativas': ops_count,
                'seguridad': seg_count,
                'falta': deficit
            })
        else:
            exceso = seg_count - ops_count
            estado = f"⚠️ EXCESO {exceso} seg"
            candidato = "❌ No"
        
        print(f"{sucursal:<35} {ops_count:<4} {seg_count:<4} {estado:<20} {candidato}")
    
    return candidatos

def mostrar_detalles_candidatos(df, candidatos, fecha_target):
    """Mostrar detalles de sucursales candidatas"""
    
    if not candidatos:
        print(f"\n❌ No hay candidatos que necesiten seguridad del {fecha_target}")
        return
    
    print(f"\n🎯 SUCURSALES CANDIDATAS PARA RECIBIR SEGURIDAD:")
    print("=" * 60)
    
    for i, candidato in enumerate(candidatos, 1):
        sucursal = candidato['sucursal']
        falta = candidato['falta']
        
        print(f"\n{i}. {sucursal} (falta {falta} seguridad)")
        
        # Mostrar operativas de esa sucursal y fecha
        ops_sucursal = df[(df['location_asignado'] == sucursal) & 
                         (df['fecha_str'] == fecha_target) & 
                         (df['tipo'] == 'operativas')]
        
        print(f"   📋 Operativas del {fecha_target}:")
        for _, ops_row in ops_sucursal.iterrows():
            try:
                fecha_dt = pd.to_datetime(ops_row['date_submitted'])
                hora = fecha_dt.strftime('%H:%M') if fecha_dt else 'N/A'
            except:
                hora = 'N/A'
            print(f"      🏗️ {hora} - {ops_row['submission_id'][:17]}")
        
        # Mostrar conteo general de la sucursal
        suc_total = df[df['location_asignado'] == sucursal]
        ops_total = len(suc_total[suc_total['tipo'] == 'operativas'])
        seg_total = len(suc_total[suc_total['tipo'] == 'seguridad'])
        
        print(f"   📊 Conteo total sucursal: {ops_total} ops + {seg_total} seg = {ops_total + seg_total}")
        
        # Determinar si es LOCAL o FORÁNEA (simplificado)
        if ops_total + seg_total <= 4:
            tipo_esperado = "FORÁNEA 2+2"
            esperado = 4
        else:
            tipo_esperado = "LOCAL 4+4"  
            esperado = 8
        
        diferencia = (ops_total + seg_total) - esperado
        estado_general = "✅ PERFECTO" if diferencia == 0 else f"❌ DEFICIT (-{abs(diferencia)})" if diferencia < 0 else f"⚠️ EXCESO (+{diferencia})"
        
        print(f"   🎯 Clasificación: {tipo_esperado}, Estado: {estado_general}")

def mostrar_pedro_cardenas_detalles(df):
    """Mostrar detalles de las supervisiones duplicadas de Pedro Cardenas"""
    
    print(f"\n📍 PEDRO CARDENAS - DETALLES DE SUPERVISIONES 2025-05-14")
    print("=" * 70)
    
    pedro_seg = df[(df['location_asignado'] == '65 - Pedro Cardenas') & 
                   (df['fecha_str'] == '2025-05-14') & 
                   (df['tipo'] == 'seguridad')]
    
    print(f"🛡️ SEGURIDAD 2025-05-14 (2 supervisiones):")
    for i, (_, row) in enumerate(pedro_seg.iterrows(), 1):
        try:
            fecha_dt = pd.to_datetime(row['date_submitted'])
            hora = fecha_dt.strftime('%H:%M:%S') if fecha_dt else 'N/A'
        except:
            hora = 'N/A'
        
        lat = row['lat_entrega']
        lon = row['lon_entrega']
        coords = f"{lat:.6f}, {lon:.6f}" if pd.notna(lat) and pd.notna(lon) else "Sin coordenadas"
        
        print(f"   {i}. {row['submission_id'][:17]} - {hora}")
        print(f"      📍 Coordenadas: {coords}")
        print()

def main():
    """Función principal"""
    
    print("🔍 BUSCAR OPERATIVAS 2025-05-14")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Objetivo: Encontrar candidatos para seguridad sobrante de Pedro Cardenas")
    print("=" * 80)
    
    # 1. Cargar datos
    df = cargar_dataset()
    fecha_target = '2025-05-14'
    
    # 2. Buscar operativas del 14-mayo
    operativas_por_sucursal = buscar_operativas_fecha_especifica(df, fecha_target)
    
    if operativas_por_sucursal is None:
        return
    
    # 3. Verificar qué sucursales necesitan seguridad del mismo día
    candidatos = verificar_seguridad_misma_fecha(df, fecha_target, operativas_por_sucursal)
    
    # 4. Mostrar detalles de candidatos
    mostrar_detalles_candidatos(df, candidatos, fecha_target)
    
    # 5. Mostrar detalles de Pedro Cardenas
    mostrar_pedro_cardenas_detalles(df)
    
    # 6. Resumen
    print(f"\n🎯 RESUMEN:")
    print(f"   📅 Operativas del {fecha_target}: {len(operativas_por_sucursal)} sucursales")
    print(f"   🎯 Candidatos para recibir: {len(candidatos)} sucursales")
    print(f"   🚨 Pedro Cardenas: 1 seguridad sobrante del {fecha_target}")
    
    if candidatos:
        print(f"\n💡 RECOMENDACIÓN:")
        mejor_candidato = candidatos[0]  # Tomar el primer candidato
        print(f"   Mover 1 seguridad de Pedro Cardenas → {mejor_candidato['sucursal']}")
        print(f"   Esto completará el par del {fecha_target}")
    
    print(f"\n✅ BÚSQUEDA COMPLETADA")

if __name__ == "__main__":
    main()