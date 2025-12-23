#!/usr/bin/env python3
"""
🔍 ANÁLISIS CAMPO 'Sucursal' EN EXCEL DE SEGURIDAD
Revisar supervisiones de seguridad con campo Sucursal manual para detectar errores
"""

import pandas as pd
from datetime import datetime

def analizar_campo_sucursal_centrito_gomez():
    """Analizar campo Sucursal en supervisiones de Centrito Valle y Gómez Morín"""
    
    print("🔍 ANÁLISIS CAMPO 'Sucursal' - CENTRITO VALLE Y GÓMEZ MORÍN")
    print("=" * 80)
    
    # Cargar Excel de seguridad
    df_seg = pd.read_excel("CONTROL_OPERATIVO_DE_SEGURIDAD_CAS_11_REV_25012025_Submissions-2025-12-18__1232CST-1766104009.xlsx")
    
    print(f"✅ Cargado Excel seguridad: {len(df_seg)} submissions")
    
    # Verificar si existe campo Sucursal
    if 'Sucursal' not in df_seg.columns:
        print("❌ Campo 'Sucursal' no encontrado")
        return None
    
    print(f"✅ Campo 'Sucursal' encontrado")
    
    # Filtrar supervisiones de Centrito Valle con campo Sucursal
    print(f"\n🔍 SUPERVISIONES CENTRITO VALLE CON CAMPO 'Sucursal':")
    print("=" * 70)
    
    centrito_con_location = df_seg[df_seg['Location'] == '71 - Centrito Valle'].copy()
    
    if len(centrito_con_location) > 0:
        print(f"📊 Supervisiones Centrito Valle con Location: {len(centrito_con_location)}")
        
        print(f"\n{'#':<3} {'Fecha':<12} {'Hora':<8} {'Usuario':<15} {'Campo Sucursal':<20} {'Index'}")
        print("-" * 80)
        
        for i, (idx, row) in enumerate(centrito_con_location.iterrows(), 1):
            fecha_dt = pd.to_datetime(row['Date Submitted'])
            fecha_str = fecha_dt.strftime('%Y-%m-%d')
            hora_str = fecha_dt.strftime('%H:%M')
            usuario = row['Submitted By']
            sucursal_campo = str(row.get('Sucursal', 'N/A'))
            
            print(f"{i:<3} {fecha_str:<12} {hora_str:<8} {usuario:<15} {sucursal_campo:<20} {idx}")
    
    # Buscar supervisiones SIN location pero CON campo Sucursal que mencione Gómez Morín o Centrito
    print(f"\n🔍 SUPERVISIONES SIN LOCATION CON CAMPO 'Sucursal' RELEVANTE:")
    print("=" * 80)
    
    sin_location = df_seg[df_seg['Location'].isna()].copy()
    
    # Buscar menciones de Gómez Morín, Centrito, etc.
    keywords = ['gomez', 'morin', 'centrito', 'valle', 'centro']
    
    relevantes = []
    
    for idx, row in sin_location.iterrows():
        sucursal_campo = str(row.get('Sucursal', '')).lower()
        
        for keyword in keywords:
            if keyword in sucursal_campo:
                fecha_dt = pd.to_datetime(row['Date Submitted'])
                
                relevantes.append({
                    'index': idx,
                    'fecha': fecha_dt,
                    'fecha_str': fecha_dt.strftime('%Y-%m-%d %H:%M'),
                    'usuario': row['Submitted By'],
                    'sucursal_campo': row.get('Sucursal', 'N/A'),
                    'keyword_encontrado': keyword
                })
                break
    
    if relevantes:
        print(f"📊 Supervisiones SIN location pero con Sucursal relevante: {len(relevantes)}")
        print(f"\n{'#':<3} {'Fecha':<17} {'Usuario':<15} {'Campo Sucursal':<25} {'Keyword':<10} {'Index'}")
        print("-" * 90)
        
        for i, item in enumerate(relevantes, 1):
            print(f"{i:<3} {item['fecha_str']:<17} {item['usuario']:<15} {item['sucursal_campo']:<25} {item['keyword_encontrado']:<10} {item['index']}")
    else:
        print("❌ No se encontraron supervisiones sin location con campo Sucursal relevante")
    
    return relevantes

def analizar_todas_supervisiones_campo_sucursal():
    """Analizar TODAS las supervisiones que tienen campo Sucursal"""
    
    print(f"\n📋 ANÁLISIS COMPLETO CAMPO 'Sucursal'")
    print("=" * 70)
    
    df_seg = pd.read_excel("CONTROL_OPERATIVO_DE_SEGURIDAD_CAS_11_REV_25012025_Submissions-2025-12-18__1232CST-1766104009.xlsx")
    
    # Supervisiones con campo Sucursal no vacío
    con_sucursal = df_seg[df_seg['Sucursal'].notna()].copy()
    
    print(f"📊 Total supervisiones con campo Sucursal: {len(con_sucursal)}")
    
    # Agrupar por valor de campo Sucursal
    valores_sucursal = con_sucursal['Sucursal'].value_counts()
    
    print(f"\n📊 VALORES EN CAMPO 'Sucursal' (Top 20):")
    print(f"{'Valor':<30} {'Count':<6} {'Observación'}")
    print("-" * 60)
    
    for valor, count in valores_sucursal.head(20).items():
        observacion = ""
        valor_lower = str(valor).lower()
        
        if 'gomez' in valor_lower or 'morin' in valor_lower:
            observacion = "🎯 GÓMEZ MORÍN"
        elif 'centrito' in valor_lower or 'valle' in valor_lower:
            observacion = "🎯 CENTRITO VALLE"
        elif 'sc' in valor_lower or 'santa catarina' in valor_lower:
            observacion = "📍 SANTA CATARINA"
        elif 'lh' in valor_lower or 'huasteca' in valor_lower:
            observacion = "📍 LA HUASTECA"
        elif 'gc' in valor_lower or 'garcia' in valor_lower:
            observacion = "📍 GARCIA"
        
        valor_str = str(valor)[:29]
        print(f"{valor_str:<30} {count:<6} {observacion}")
    
    # Buscar específicamente errores de asignación
    print(f"\n🔍 ERRORES POTENCIALES DE ASIGNACIÓN:")
    print("=" * 60)
    
    errores_potenciales = []
    
    for idx, row in con_sucursal.iterrows():
        location = str(row.get('Location', ''))
        sucursal_campo = str(row.get('Sucursal', ''))
        
        # Detectar inconsistencias
        inconsistencia = False
        razon = ""
        
        # Caso 1: Location dice una cosa, campo Sucursal dice otra
        if 'centrito' in location.lower() and ('gomez' in sucursal_campo.lower() or 'morin' in sucursal_campo.lower()):
            inconsistencia = True
            razon = "Location=Centrito, Sucursal=Gómez Morín"
        elif 'gomez' in location.lower() and 'centrito' in sucursal_campo.lower():
            inconsistencia = True
            razon = "Location=Gómez Morín, Sucursal=Centrito"
        
        if inconsistencia:
            fecha_dt = pd.to_datetime(row['Date Submitted'])
            
            errores_potenciales.append({
                'index': idx,
                'fecha': fecha_dt.strftime('%Y-%m-%d %H:%M'),
                'usuario': row['Submitted By'],
                'location': location,
                'sucursal_campo': sucursal_campo,
                'razon': razon
            })
    
    if errores_potenciales:
        print(f"🚨 ERRORES POTENCIALES DETECTADOS: {len(errores_potenciales)}")
        print(f"\n{'#':<3} {'Fecha':<17} {'Usuario':<15} {'Location':<20} {'Campo Sucursal':<20} {'Index'}")
        print("-" * 95)
        
        for i, error in enumerate(errores_potenciales, 1):
            location_short = error['location'][:19]
            sucursal_short = error['sucursal_campo'][:19]
            print(f"{i:<3} {error['fecha']:<17} {error['usuario']:<15} {location_short:<20} {sucursal_short:<20} {error['index']}")
            
        return errores_potenciales
    else:
        print("✅ No se detectaron errores obvios de asignación")
        return []

def buscar_supervisiones_israel_gomez_morin():
    """Buscar supervisiones de Israel Garcia con campo Sucursal = Gómez Morín"""
    
    print(f"\n🎯 SUPERVISIONES ISRAEL GARCIA - CAMPO SUCURSAL = GÓMEZ MORÍN")
    print("=" * 70)
    
    df_seg = pd.read_excel("CONTROL_OPERATIVO_DE_SEGURIDAD_CAS_11_REV_25012025_Submissions-2025-12-18__1232CST-1766104009.xlsx")
    
    # Filtrar por Israel Garcia Y campo Sucursal con Gómez Morín
    filtro = (
        (df_seg['Submitted By'] == 'Israel Garcia') &
        (df_seg['Sucursal'].notna()) &
        (df_seg['Sucursal'].str.contains('Gomez|Morin|gomez|morin', na=False, case=False))
    )
    
    israel_gomez = df_seg[filtro].copy()
    
    if len(israel_gomez) > 0:
        print(f"✅ Encontradas {len(israel_gomez)} supervisiones de Israel Garcia con Sucursal = Gómez Morín")
        
        print(f"\n{'#':<3} {'Fecha':<17} {'Location Asignado':<20} {'Campo Sucursal':<20} {'Index'}")
        print("-" * 85)
        
        candidatos = []
        
        for i, (idx, row) in enumerate(israel_gomez.iterrows(), 1):
            fecha_dt = pd.to_datetime(row['Date Submitted'])
            fecha_str = fecha_dt.strftime('%Y-%m-%d %H:%M')
            location = str(row.get('Location', 'SIN_LOCATION'))[:19]
            sucursal_campo = str(row['Sucursal'])[:19]
            
            print(f"{i:<3} {fecha_str:<17} {location:<20} {sucursal_campo:<20} {idx}")
            
            # Si NO tiene location asignado pero campo Sucursal dice Gómez Morín
            if pd.isna(row['Location']):
                candidatos.append({
                    'index': idx,
                    'fecha': fecha_dt,
                    'fecha_str': fecha_str,
                    'sucursal_campo': row['Sucursal'],
                    'es_candidato': True,
                    'razon': 'SIN_LOCATION pero campo Sucursal = Gómez Morín'
                })
        
        print(f"\n🎯 CANDIDATOS PARA ASIGNAR A GÓMEZ MORÍN:")
        if candidatos:
            for i, candidato in enumerate(candidatos, 1):
                print(f"   {i}. Index {candidato['index']} - {candidato['fecha_str']} - {candidato['razon']}")
        else:
            print("   ❌ No hay candidatos (todos ya tienen Location asignado)")
            
        return candidatos
    else:
        print("❌ No se encontraron supervisiones de Israel Garcia con campo Sucursal = Gómez Morín")
        return []

def main():
    """Función principal"""
    
    print("🔍 ANÁLISIS CAMPO 'Sucursal' EN EXCEL DE SEGURIDAD")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Objetivo: Detectar supervisiones mal asignadas usando campo Sucursal")
    print("=" * 80)
    
    # 1. Analizar supervisiones de Centrito Valle y Gómez Morín
    relevantes = analizar_campo_sucursal_centrito_gomez()
    
    # 2. Análisis completo de campo Sucursal
    errores_potenciales = analizar_todas_supervisiones_campo_sucursal()
    
    # 3. Buscar candidatos específicos de Israel Garcia para Gómez Morín
    candidatos_israel = buscar_supervisiones_israel_gomez_morin()
    
    # 4. Guardar análisis
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    import json
    with open(f"ANALISIS_CAMPO_SUCURSAL_{timestamp}.json", 'w', encoding='utf-8') as f:
        json.dump({
            'timestamp': timestamp,
            'supervisiones_relevantes': relevantes if relevantes else [],
            'errores_potenciales': errores_potenciales,
            'candidatos_israel_gomez': candidatos_israel
        }, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"\n📁 ANÁLISIS GUARDADO: ANALISIS_CAMPO_SUCURSAL_{timestamp}.json")
    
    # Resumen final
    print(f"\n📊 RESUMEN FINAL:")
    if relevantes:
        print(f"   📍 Supervisiones relevantes encontradas: {len(relevantes)}")
    if errores_potenciales:
        print(f"   🚨 Errores potenciales detectados: {len(errores_potenciales)}")
    if candidatos_israel:
        print(f"   🎯 Candidatos Israel Garcia para Gómez Morín: {len(candidatos_israel)}")
        print(f"   💡 Estos podrían ser los que realmente pertenecen a Gómez Morín")
    
    return relevantes, errores_potenciales, candidatos_israel

if __name__ == "__main__":
    main()