#!/usr/bin/env python3
"""
🔍 ANALIZAR EXCESOS DE CLASIFICACIÓN
Revisar 16 sucursales con exceso +4 - posible mala clasificación FORÁNEA vs LOCAL
"""

import pandas as pd
from datetime import datetime

def cargar_datos_finales():
    """Cargar dataset final y catálogo de sucursales"""
    
    df = pd.read_csv("DATASET_FINAL_COMPLETO.csv")
    df_sucursales = pd.read_csv('SUCURSALES_MASTER_20251218_110913.csv')
    
    return df, df_sucursales

def crear_catalogo_reglas():
    """Crear catálogo con reglas actuales"""
    
    df_sucursales = pd.read_csv('SUCURSALES_MASTER_20251218_110913.csv')
    
    # Reglas especiales confirmadas por Roberto
    reglas_especiales = {
        '1 - Pino Suarez': {'ops': 3, 'seg': 3, 'total': 6, 'tipo': 'ESPECIAL_3_3'},
        '5 - Felix U. Gomez': {'ops': 3, 'seg': 3, 'total': 6, 'tipo': 'ESPECIAL_3_3'},
        '2 - Madero': {'ops': 3, 'seg': 3, 'total': 6, 'tipo': 'ESPECIAL_3_3'},
        '3 - Matamoros': {'ops': 3, 'seg': 3, 'total': 6, 'tipo': 'ESPECIAL_3_3'}
    }
    
    catalogo = {}
    
    for _, row in df_sucursales.iterrows():
        if pd.notna(row['numero']) and pd.notna(row['nombre']):
            numero = int(row['numero'])
            nombre = row['nombre']
            location_key = f"{numero} - {nombre}"
            
            # Aplicar reglas específicas
            if location_key in reglas_especiales:
                regla = reglas_especiales[location_key]
                ops_esperadas = regla['ops']
                seg_esperadas = regla['seg'] 
                total_esperado = regla['total']
                tipo_regla = regla['tipo']
            else:
                # Reglas por tipo
                tipo_original = row.get('tipo', 'LOCAL')
                if tipo_original == 'FORANEA':
                    ops_esperadas = 2
                    seg_esperadas = 2
                    total_esperado = 4
                    tipo_regla = 'FORANEA_2_2'
                else:  # LOCAL
                    ops_esperadas = 4
                    seg_esperadas = 4
                    total_esperado = 8
                    tipo_regla = 'LOCAL_4_4'
            
            catalogo[location_key] = {
                'tipo_original': row.get('tipo', 'LOCAL'),
                'grupo': row.get('grupo', ''),
                'ops_esperadas': ops_esperadas,
                'seg_esperadas': seg_esperadas,
                'total_esperado': total_esperado,
                'tipo_regla': tipo_regla
            }
    
    return catalogo

def encontrar_sucursales_exceso_4(df, catalogo):
    """Encontrar sucursales con exceso exacto de +4"""
    
    print("🚨 SUCURSALES CON EXCESO +4")
    print("=" * 70)
    
    # Contar supervisiones actuales
    ops_por_sucursal = df[df['tipo'] == 'operativas']['location_asignado'].value_counts()
    seg_por_sucursal = df[df['tipo'] == 'seguridad']['location_asignado'].value_counts()
    
    sucursales_exceso_4 = []
    
    for location_key, reglas in catalogo.items():
        ops_actuales = ops_por_sucursal.get(location_key, 0)
        seg_actuales = seg_por_sucursal.get(location_key, 0)
        total_actual = ops_actuales + seg_actuales
        total_esperado = reglas['total_esperado']
        diferencia = total_actual - total_esperado
        
        if diferencia == 4:
            sucursales_exceso_4.append({
                'location_key': location_key,
                'tipo_original': reglas['tipo_original'],
                'tipo_regla_actual': reglas['tipo_regla'],
                'ops_actuales': ops_actuales,
                'seg_actuales': seg_actuales,
                'total_actual': total_actual,
                'total_esperado': total_esperado,
                'diferencia': diferencia,
                'grupo': reglas['grupo']
            })
    
    print(f"📊 ENCONTRADAS: {len(sucursales_exceso_4)} sucursales con exceso exacto +4")
    print()
    print(f"{'#':<3} {'Sucursal':<30} {'Actual':<8} {'Esperado':<8} {'Tipo':<12} {'Grupo'}")
    print("-" * 85)
    
    for i, suc in enumerate(sucursales_exceso_4, 1):
        actual_str = f"{suc['ops_actuales']}+{suc['seg_actuales']}={suc['total_actual']}"
        print(f"{i:<3} {suc['location_key']:<30} {actual_str:<8} {suc['total_esperado']:<8} {suc['tipo_original']:<12} {suc['grupo']}")
    
    return sucursales_exceso_4

def analizar_patrones_clasificacion(sucursales_exceso_4):
    """Analizar patrones en las clasificaciones incorrectas"""
    
    print(f"\n🔍 ANÁLISIS DE PATRONES DE CLASIFICACIÓN")
    print("=" * 70)
    
    # Análisis por tipo original
    por_tipo = {}
    for suc in sucursales_exceso_4:
        tipo = suc['tipo_original']
        if tipo not in por_tipo:
            por_tipo[tipo] = []
        por_tipo[tipo].append(suc)
    
    print(f"📊 DISTRIBUCIÓN POR TIPO ORIGINAL:")
    for tipo, lista in por_tipo.items():
        print(f"   {tipo}: {len(lista)} sucursales")
    
    # Análisis por grupo
    por_grupo = {}
    for suc in sucursales_exceso_4:
        grupo = suc['grupo'] or 'SIN_GRUPO'
        if grupo not in por_grupo:
            por_grupo[grupo] = []
        por_grupo[grupo].append(suc)
    
    print(f"\n📍 DISTRIBUCIÓN POR GRUPO:")
    for grupo, lista in sorted(por_grupo.items()):
        print(f"   {grupo}: {len(lista)} sucursales")
        for suc in lista:
            print(f"      • {suc['location_key']}")
    
    return por_tipo, por_grupo

def proponer_reclasificaciones(sucursales_exceso_4):
    """Proponer reclasificaciones basadas en el patrón 4+4=8"""
    
    print(f"\n💡 PROPUESTA DE RECLASIFICACIONES")
    print("=" * 70)
    
    print(f"🎯 PATRÓN IDENTIFICADO:")
    print(f"   Todas tienen 4 operativas + 4 seguridad = 8 total")
    print(f"   Esperado FORÁNEA: 2+2 = 4 total")
    print(f"   Esperado LOCAL: 4+4 = 8 total")
    print(f"   💡 TODAS deberían ser LOCALES, no FORÁNEAS")
    
    print(f"\n📋 RECLASIFICACIONES PROPUESTAS:")
    print(f"{'Sucursal':<35} {'Actual':<12} {'Propuesta':<12} {'Motivo'}")
    print("-" * 85)
    
    reclasificaciones = []
    
    for suc in sucursales_exceso_4:
        if suc['tipo_original'] == 'FORANEA' and suc['total_actual'] == 8:
            reclasificaciones.append({
                'location_key': suc['location_key'],
                'tipo_actual': 'FORANEA',
                'tipo_propuesto': 'LOCAL',
                'motivo': '4+4=8 coincide con LOCAL 4+4'
            })
            
            print(f"{suc['location_key']:<35} {'FORANEA_2_2':<12} {'LOCAL_4_4':<12} {'4+4=8 perfecta'}")
    
    return reclasificaciones

def revisar_otras_sucursales_similares(df, catalogo):
    """Revisar otras sucursales que podrían tener el mismo problema"""
    
    print(f"\n🔍 REVISAR OTRAS SUCURSALES CON PATRÓN SIMILAR")
    print("=" * 70)
    
    # Buscar sucursales FORÁNEAS con 8 supervisiones
    ops_por_sucursal = df[df['tipo'] == 'operativas']['location_asignado'].value_counts()
    seg_por_sucursal = df[df['tipo'] == 'seguridad']['location_asignado'].value_counts()
    
    similares = []
    
    for location_key, reglas in catalogo.items():
        if reglas['tipo_regla'] == 'FORANEA_2_2':  # Clasificadas como FORÁNEAS
            ops_actuales = ops_por_sucursal.get(location_key, 0)
            seg_actuales = seg_por_sucursal.get(location_key, 0)
            total_actual = ops_actuales + seg_actuales
            
            if total_actual >= 7:  # 7 o más supervisiones en FORÁNEA
                diferencia = total_actual - 4  # Diferencia vs esperado FORÁNEA
                similares.append({
                    'location_key': location_key,
                    'ops': ops_actuales,
                    'seg': seg_actuales,
                    'total': total_actual,
                    'diferencia': diferencia,
                    'grupo': reglas['grupo']
                })
    
    if similares:
        print(f"📊 FORÁNEAS CON ≥7 SUPERVISIONES (posibles LOCALES):")
        print(f"{'Sucursal':<35} {'Ops+Seg':<8} {'Total':<6} {'Exceso':<6} {'Grupo'}")
        print("-" * 75)
        
        for suc in sorted(similares, key=lambda x: x['total'], reverse=True):
            ops_seg = f"{suc['ops']}+{suc['seg']}"
            print(f"{suc['location_key']:<35} {ops_seg:<8} {suc['total']:<6} +{suc['diferencia']:<5} {suc['grupo']}")
    
    return similares

def main():
    """Función principal"""
    
    print("🔍 ANALIZAR EXCESOS DE CLASIFICACIÓN")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Objetivo: Revisar sucursales con exceso +4 para reclasificación")
    print("=" * 80)
    
    # 1. Cargar datos
    df, df_sucursales = cargar_datos_finales()
    catalogo = crear_catalogo_reglas()
    
    # 2. Encontrar sucursales con exceso +4
    sucursales_exceso_4 = encontrar_sucursales_exceso_4(df, catalogo)
    
    # 3. Analizar patrones
    por_tipo, por_grupo = analizar_patrones_clasificacion(sucursales_exceso_4)
    
    # 4. Proponer reclasificaciones
    reclasificaciones = proponer_reclasificaciones(sucursales_exceso_4)
    
    # 5. Revisar otras sucursales similares
    similares = revisar_otras_sucursales_similares(df, catalogo)
    
    # 6. Resumen final
    print(f"\n🎯 RESUMEN FINAL:")
    print(f"   🚨 Sucursales con exceso exacto +4: {len(sucursales_exceso_4)}")
    print(f"   💡 Reclasificaciones propuestas: {len(reclasificaciones)}")
    print(f"   🔍 Otras similares encontradas: {len(similares)}")
    
    total_afectadas = len(set([s['location_key'] for s in sucursales_exceso_4] + [s['location_key'] for s in similares]))
    print(f"   📊 Total sucursales afectadas: {total_afectadas}")
    
    if len(reclasificaciones) > 0:
        print(f"\n💡 RECOMENDACIÓN:")
        print(f"   Reclasificar {len(reclasificaciones)} sucursales de FORÁNEA → LOCAL")
        print(f"   Esto resolvería automáticamente {len(reclasificaciones)} problemas de exceso")
    
    print(f"\n✅ ANÁLISIS DE CLASIFICACIÓN COMPLETADO")
    
    return sucursales_exceso_4, reclasificaciones, similares

if __name__ == "__main__":
    main()