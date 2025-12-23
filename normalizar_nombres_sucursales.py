#!/usr/bin/env python3
"""
🔧 NORMALIZAR NOMBRES SUCURSALES
Encontrar y corregir nombres como SC, LH, GC y asignarlos correctamente
"""

import pandas as pd
from datetime import datetime

def cargar_datos():
    """Cargar dataset final"""
    
    print("📁 CARGAR DATASET FINAL")
    print("=" * 40)
    
    df = pd.read_csv("DATASET_FINAL_API_20251218_153922.csv")
    
    print(f"✅ Dataset cargado: {len(df)} registros")
    
    return df

def encontrar_nombres_normalizados(df):
    """Encontrar todos los nombres que necesitan normalización"""
    
    print(f"\n🔍 BUSCAR NOMBRES NORMALIZADOS")
    print("=" * 50)
    
    # Buscar patrones sospechosos
    location_counts = df['location_asignado'].value_counts()
    
    # Filtrar nombres que contengan siglas o patrones sospechosos
    nombres_sospechosos = []
    
    for location, count in location_counts.items():
        if pd.notna(location):
            location_str = str(location).lower()
            
            # Buscar patrones: SC, LH, GC, sucursal + sigla, nombres cortos
            if (
                'sucursal sc' in location_str or
                'sucursal lh' in location_str or 
                'sucursal gc' in location_str or
                location_str in ['sc', 'lh', 'gc', 'santa catarina', 'huasteca', 'garcia'] or
                'sucursal ' in location_str
            ):
                nombres_sospechosos.append({
                    'location_original': location,
                    'count': count,
                    'pattern': 'sospechoso'
                })
    
    print(f"📋 NOMBRES SOSPECHOSOS ENCONTRADOS:")
    for item in nombres_sospechosos:
        print(f"   '{item['location_original']}': {item['count']} registros")
    
    return nombres_sospechosos

def mostrar_registros_sospechosos(df, nombres_sospechosos):
    """Mostrar registros detallados de nombres sospechosos"""
    
    print(f"\n📊 DETALLES DE REGISTROS SOSPECHOSOS")
    print("=" * 60)
    
    for item in nombres_sospechosos:
        location = item['location_original']
        registros = df[df['location_asignado'] == location]
        
        print(f"\n🔍 '{location}' ({item['count']} registros):")
        print(f"{'Tipo':<12} {'Fecha':<12} {'Submission ID'}")
        print("-" * 50)
        
        for _, reg in registros.head(5).iterrows():
            tipo = reg['tipo']
            fecha = reg['fecha_str']
            sub_id = str(reg['submission_id'])[:15]
            print(f"{tipo:<12} {fecha:<12} {sub_id}")
        
        if len(registros) > 5:
            print(f"... y {len(registros) - 5} más")

def proponer_normalizaciones():
    """Proponer normalizaciones según indicaciones de Roberto"""
    
    print(f"\n🔧 NORMALIZACIONES PROPUESTAS")
    print("=" * 50)
    
    normalizaciones = {
        # Según Roberto: SC → Santa Catarina, LH → La Huasteca, GC → García
        'SC': '4 - Santa Catarina',
        'sc': '4 - Santa Catarina', 
        'Santa Catarina': '4 - Santa Catarina',
        'santa catarina': '4 - Santa Catarina',
        'Sucursal SC - Santa Catarina': '4 - Santa Catarina',
        
        'LH': '7 - La Huasteca',
        'lh': '7 - La Huasteca',
        'La Huasteca': '7 - La Huasteca', 
        'la huasteca': '7 - La Huasteca',
        'Huasteca': '7 - La Huasteca',
        'huasteca': '7 - La Huasteca',
        'Sucursal LH - La Huasteca': '7 - La Huasteca',
        
        'GC': '6 - Garcia',
        'gc': '6 - Garcia',
        'Garcia': '6 - Garcia',
        'garcia': '6 - Garcia',
        'Sucursal GC - Garcia': '6 - Garcia'
    }
    
    print(f"📋 TABLA DE NORMALIZACIONES:")
    for original, normalizado in normalizaciones.items():
        print(f"   '{original}' → '{normalizado}'")
    
    return normalizaciones

def aplicar_normalizaciones(df, normalizaciones):
    """Aplicar normalizaciones al dataset"""
    
    print(f"\n⚙️ APLICAR NORMALIZACIONES")
    print("=" * 40)
    
    df_normalizado = df.copy()
    cambios_realizados = []
    
    for original, normalizado in normalizaciones.items():
        # Buscar registros que coincidan exactamente
        mask = df_normalizado['location_asignado'] == original
        registros_afectados = mask.sum()
        
        if registros_afectados > 0:
            df_normalizado.loc[mask, 'location_asignado'] = normalizado
            cambios_realizados.append({
                'original': original,
                'normalizado': normalizado, 
                'registros': registros_afectados
            })
            print(f"✅ '{original}' → '{normalizado}' ({registros_afectados} registros)")
    
    print(f"\n📊 RESUMEN NORMALIZACIONES:")
    total_cambios = sum([c['registros'] for c in cambios_realizados])
    print(f"   🔄 Total cambios: {total_cambios}")
    print(f"   📝 Normalizaciones aplicadas: {len(cambios_realizados)}")
    
    return df_normalizado, cambios_realizados

def verificar_nuevos_conteos(df_normalizado):
    """Verificar nuevos conteos después de normalización"""
    
    print(f"\n✅ VERIFICAR NUEVOS CONTEOS")
    print("=" * 40)
    
    # Contar por sucursal después de normalización
    ops = df_normalizado[df_normalizado['tipo'] == 'operativas']['location_asignado'].value_counts()
    seg = df_normalizado[df_normalizado['tipo'] == 'seguridad']['location_asignado'].value_counts()
    
    # Enfocarse en las sucursales normalizadas
    sucursales_objetivo = ['4 - Santa Catarina', '7 - La Huasteca', '6 - Garcia']
    
    print(f"📊 NUEVOS CONTEOS SUCURSALES NORMALIZADAS:")
    print(f"{'Sucursal':<25} {'Ops':<4} {'Seg':<4} {'Total':<6} {'Estado'}")
    print("-" * 60)
    
    for sucursal in sucursales_objetivo:
        ops_count = ops.get(sucursal, 0)
        seg_count = seg.get(sucursal, 0)
        total = ops_count + seg_count
        
        # Estado según reglas: LOCAL 4+4
        if total == 8:
            estado = "✅ PERFECTO"
        elif total > 8:
            estado = f"⚠️ EXCESO (+{total-8})"
        else:
            estado = f"❌ DEFICIT (-{8-total})"
        
        print(f"{sucursal:<25} {ops_count:<4} {seg_count:<4} {total:<6} {estado}")

def main():
    """Función principal"""
    
    print("🔧 NORMALIZAR NOMBRES SUCURSALES")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Objetivo: Normalizar SC→Santa Catarina, LH→La Huasteca, GC→García")
    print("=" * 80)
    
    # 1. Cargar datos
    df = cargar_datos()
    
    # 2. Encontrar nombres normalizados
    nombres_sospechosos = encontrar_nombres_normalizados(df)
    
    # 3. Mostrar detalles
    mostrar_registros_sospechosos(df, nombres_sospechosos)
    
    # 4. Proponer normalizaciones
    normalizaciones = proponer_normalizaciones()
    
    # 5. Aplicar normalizaciones
    df_normalizado, cambios = aplicar_normalizaciones(df, normalizaciones)
    
    # 6. Verificar nuevos conteos
    verificar_nuevos_conteos(df_normalizado)
    
    # 7. Guardar dataset normalizado
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archivo_normalizado = f"DATASET_NORMALIZADO_{timestamp}.csv"
    
    df_normalizado.to_csv(archivo_normalizado, index=False, encoding='utf-8')
    
    print(f"\n📁 DATASET NORMALIZADO GUARDADO:")
    print(f"   ✅ Archivo: {archivo_normalizado}")
    print(f"   📊 Total registros: {len(df_normalizado)}")
    print(f"   🔄 Cambios aplicados: {len(cambios)}")
    
    print(f"\n✅ NORMALIZACIÓN COMPLETADA")
    print(f"🔧 Próximo: Revisar cumplimiento con nombres normalizados")
    
    return df_normalizado, cambios

if __name__ == "__main__":
    main()