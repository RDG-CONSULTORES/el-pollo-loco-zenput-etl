#!/usr/bin/env python3
"""
🔧 NORMALIZAR SUCURSALES RARAS Y CENTRITO VALLE
1. GC → Garcia, LH → La Huasteca, SC → Santa Catarina
2. Centrito Valle: mover 2 supervisiones específicas a Gómez Morín
"""

import pandas as pd
from datetime import datetime

def normalizar_sucursales_raras():
    """Normalizar GC, LH, SC a nombres completos"""
    
    print("🔧 NORMALIZANDO SUCURSALES RARAS (GC, LH, SC)")
    print("=" * 60)
    
    df_asignaciones = pd.read_csv("ASIGNACIONES_FINALES_CORREGIDAS_20251218_140924.csv")
    print(f"📊 Asignaciones cargadas: {len(df_asignaciones)}")
    
    # Mapeo de normalización
    normalizaciones = {
        'Sucursal GC - Garcia': '6 - Garcia',
        'Sucursal LH - La Huasteca': '7 - La Huasteca', 
        'Sucursal SC - Santa Catarina': '4 - Santa Catarina'
    }
    
    print(f"\n🔄 APLICANDO NORMALIZACIONES:")
    normalizaciones_aplicadas = 0
    
    for sucursal_rara, sucursal_correcta in normalizaciones.items():
        mask = df_asignaciones['sucursal_asignada'] == sucursal_rara
        count_encontrados = len(df_asignaciones[mask])
        
        if count_encontrados > 0:
            df_asignaciones.loc[mask, 'sucursal_asignada'] = sucursal_correcta
            df_asignaciones.loc[mask, 'metodo'] = 'NORMALIZADO_CODIGO'
            normalizaciones_aplicadas += count_encontrados
            print(f"   ✅ {sucursal_rara} → {sucursal_correcta} ({count_encontrados} casos)")
        else:
            print(f"   ❌ {sucursal_rara}: No encontrado")
    
    print(f"\n📊 Total normalizaciones aplicadas: {normalizaciones_aplicadas}")
    return df_asignaciones, normalizaciones_aplicadas

def identificar_centrito_para_gomez_morin():
    """Identificar las 2 supervisiones de Centrito Valle que van a Gómez Morín"""
    
    print(f"\n🎯 IDENTIFICANDO SUPERVISIONES CENTRITO → GÓMEZ MORÍN")
    print("=" * 70)
    
    # Del análisis anterior sabemos que son Index 103 y 145 del Excel de seguridad
    # Que tienen campo Sucursal = "Gómez Morín" pero Location = "Centrito Valle"
    
    df_seg = pd.read_excel("CONTROL_OPERATIVO_DE_SEGURIDAD_CAS_11_REV_25012025_Submissions-2025-12-18__1232CST-1766104009.xlsx")
    
    # Buscar supervisiones de Centrito Valle con campo Sucursal = Gómez Morín
    centrito_supervisiones = df_seg[df_seg['Location'] == '71 - Centrito Valle'].copy()
    
    candidatos_gomez = []
    
    print(f"📋 SUPERVISIONES CENTRITO VALLE:")
    print(f"{'#':<3} {'Index':<6} {'Fecha':<12} {'Usuario':<15} {'Campo Sucursal':<20} {'Candidato'}")
    print("-" * 80)
    
    for i, (idx, row) in enumerate(centrito_supervisiones.iterrows(), 1):
        fecha_dt = pd.to_datetime(row['Date Submitted'])
        fecha_str = fecha_dt.strftime('%Y-%m-%d')
        usuario = row['Submitted By']
        sucursal_campo = str(row.get('Sucursal', 'N/A'))
        
        # Verificar si el campo Sucursal indica Gómez Morín
        es_candidato = 'gomez' in sucursal_campo.lower() or 'morin' in sucursal_campo.lower()
        candidato_str = "✅ SÍ" if es_candidato else "❌ No"
        
        print(f"{i:<3} {idx:<6} {fecha_str:<12} {usuario:<15} {sucursal_campo[:19]:<20} {candidato_str}")
        
        if es_candidato:
            candidatos_gomez.append({
                'index_excel': idx,
                'fecha': fecha_dt,
                'usuario': usuario,
                'sucursal_campo': sucursal_campo
            })
    
    print(f"\n🎯 CANDIDATOS PARA MOVER A GÓMEZ MORÍN: {len(candidatos_gomez)}")
    
    if candidatos_gomez:
        print(f"\n📋 DETALLES DE CANDIDATOS:")
        for i, cand in enumerate(candidatos_gomez, 1):
            print(f"   {i}. Index {cand['index_excel']}: {cand['fecha'].strftime('%Y-%m-%d')} - {cand['usuario']}")
            print(f"      Campo Sucursal: '{cand['sucursal_campo']}'")
    
    return candidatos_gomez

def aplicar_correccion_centrito_valle(candidatos_gomez):
    """Aplicar la corrección de Centrito Valle → Gómez Morín"""
    
    print(f"\n🔧 APLICANDO CORRECCIÓN CENTRITO VALLE")
    print("=" * 50)
    
    if not candidatos_gomez:
        print("❌ No hay candidatos confirmados para mover")
        return None
    
    print(f"🎯 Candidatos a mover: {len(candidatos_gomez)}")
    
    # En nuestro contexto, estas supervisiones ya están en el Excel con Location asignado
    # No están en el archivo de asignaciones porque ya tenían Location
    # Necesitamos crear un registro de la corrección conceptual
    
    correcciones_centrito = []
    
    for cand in candidatos_gomez:
        correcciones_centrito.append({
            'index_excel_seguridad': cand['index_excel'],
            'fecha': cand['fecha'],
            'usuario': cand['usuario'],
            'sucursal_origen': '71 - Centrito Valle',
            'sucursal_destino': '38 - Gomez Morin',
            'razon': f"Campo Sucursal manual: '{cand['sucursal_campo']}'",
            'tipo_correccion': 'CENTRITO_VALLE_A_GOMEZ_MORIN'
        })
    
    print(f"📊 CORRECCIONES DE CENTRITO VALLE:")
    print(f"{'#':<3} {'Index':<6} {'Fecha':<12} {'Usuario':<15} {'Razón'}")
    print("-" * 70)
    
    for i, corr in enumerate(correcciones_centrito, 1):
        fecha_str = corr['fecha'].strftime('%Y-%m-%d')
        razon_short = corr['razon'][:25] + "..." if len(corr['razon']) > 25 else corr['razon']
        print(f"{i:<3} {corr['index_excel_seguridad']:<6} {fecha_str:<12} {corr['usuario']:<15} {razon_short}")
    
    return correcciones_centrito

def verificar_distribucion_actualizada():
    """Verificar cómo queda la distribución después de las normalizaciones"""
    
    print(f"\n📊 VERIFICACIÓN DISTRIBUCIÓN ACTUALIZADA")
    print("=" * 60)
    
    # Cargar datos actualizados
    df_ops = pd.read_excel("SUPERVISION_OPERATIVA_CAS_11_REV_250125_Submissions-2025-12-18__1228CST-1766141816.xlsx")
    df_seg = pd.read_excel("CONTROL_OPERATIVO_DE_SEGURIDAD_CAS_11_REV_25012025_Submissions-2025-12-18__1232CST-1766104009.xlsx")
    df_asignaciones_actualizadas = pd.read_csv("ASIGNACIONES_NORMALIZADAS_TEMP.csv")
    
    # Contar operativas
    ops_count = df_ops[df_ops['Location'].notna()]['Location'].value_counts()
    
    # Contar seguridad Excel (aplicando corrección conceptual de Centrito)
    seg_excel = df_seg[df_seg['Location'].notna()].copy()
    seg_excel_count = seg_excel['Location'].value_counts().to_dict()
    
    # Aplicar corrección conceptual Centrito Valle
    seg_excel_count['71 - Centrito Valle'] = seg_excel_count.get('71 - Centrito Valle', 0) - 2
    seg_excel_count['38 - Gomez Morin'] = seg_excel_count.get('38 - Gomez Morin', 0) + 2
    
    # Contar seguridad asignaciones (ya normalizadas)
    seg_asig_count = df_asignaciones_actualizadas['sucursal_asignada'].value_counts()
    
    # Combinar todas
    sucursales_importantes = [
        '71 - Centrito Valle', '38 - Gomez Morin',
        '4 - Santa Catarina', '6 - Garcia', '7 - La Huasteca'
    ]
    
    print(f"📊 IMPACTO EN SUCURSALES CLAVE:")
    print(f"{'Sucursal':<35} {'Ops':<4} {'Seg':<4} {'Tot':<4} {'Estado'}")
    print("-" * 60)
    
    for sucursal in sucursales_importantes:
        ops = ops_count.get(sucursal, 0)
        seg_excel_val = seg_excel_count.get(sucursal, 0)
        seg_asig = seg_asig_count.get(sucursal, 0)
        seg_total = seg_excel_val + seg_asig
        total = ops + seg_total
        
        # Estado según tipo
        if sucursal in ['71 - Centrito Valle', '38 - Gomez Morin', '4 - Santa Catarina', '6 - Garcia', '7 - La Huasteca']:
            esperado = 8  # Todas son LOCAL
            estado = "✅" if total == esperado else f"⚠️({total}/8)"
        else:
            estado = f"📊{total}"
        
        print(f"{sucursal[:34]:<35} {ops:<4} {seg_total:<4} {total:<4} {estado}")

def main():
    """Función principal"""
    
    print("🔧 NORMALIZAR SUCURSALES RARAS Y CENTRITO VALLE")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 1. Normalizar GC→Garcia, LH→La Huasteca, SC→Santa Catarina")
    print("🎯 2. Identificar Centrito Valle → Gómez Morín según campo manual")
    print("=" * 80)
    
    # 1. Normalizar sucursales raras
    df_asignaciones_actualizadas, normalizaciones_aplicadas = normalizar_sucursales_raras()
    
    # 2. Identificar candidatos Centrito Valle
    candidatos_gomez = identificar_centrito_para_gomez_morin()
    
    # 3. Aplicar corrección Centrito Valle
    correcciones_centrito = aplicar_correccion_centrito_valle(candidatos_gomez)
    
    # 4. Guardar datos actualizados temporalmente
    df_asignaciones_actualizadas.to_csv("ASIGNACIONES_NORMALIZADAS_TEMP.csv", index=False, encoding='utf-8')
    print(f"\n💾 Datos temporales guardados: ASIGNACIONES_NORMALIZADAS_TEMP.csv")
    
    # 5. Verificar distribución
    verificar_distribucion_actualizada()
    
    # Resumen final
    print(f"\n🎯 RESUMEN DE NORMALIZACIONES:")
    print(f"   ✅ Códigos normalizados: {normalizaciones_aplicadas}")
    print(f"   🎯 Centrito → Gómez Morín: {len(correcciones_centrito or [])}")
    print(f"   📊 Datos listos para validación 5 en 5 con Roberto")
    
    print(f"\n💡 PRÓXIMO PASO:")
    print(f"   📋 Validar distribución actualizada de 5 en 5")
    print(f"   🎯 Usar fechas coincidentes para normalizar el resto")
    
    return df_asignaciones_actualizadas, correcciones_centrito

if __name__ == "__main__":
    main()