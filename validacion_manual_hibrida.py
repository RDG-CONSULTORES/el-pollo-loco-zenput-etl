#!/usr/bin/env python3
"""
🧪 VALIDACIÓN MANUAL HÍBRIDA - EXCEL + API
Aplicar reglas de normalización específicas de Roberto y procesar submissions
"""

import pandas as pd
import numpy as np
from datetime import datetime
import json
import math

def cargar_datos_excel():
    """Cargar ambos archivos Excel con datos reales"""
    
    print("📊 CARGANDO DATOS EXCEL REALES")
    print("=" * 50)
    
    try:
        # Cargar OPERATIVA
        df_ops = pd.read_excel("SUPERVISION_OPERATIVA_CAS_11_REV_250125_Submissions-2025-12-18__1228CST-1766141816.xlsx")
        df_ops['form_type'] = 'OPERATIVA'
        
        # Cargar SEGURIDAD  
        df_seg = pd.read_excel("CONTROL_OPERATIVO_DE_SEGURIDAD_CAS_11_REV_25012025_Submissions-2025-12-18__1232CST-1766104009.xlsx")
        df_seg['form_type'] = 'SEGURIDAD'
        
        print(f"✅ OPERATIVA: {len(df_ops)} submissions")
        print(f"✅ SEGURIDAD: {len(df_seg)} submissions")
        
        # Combinar y extraer campos clave
        campos_clave = ['Location', 'Location External Key', 'Submitted By', 'Date Submitted', 'Sucursal', 'form_type']
        
        df_ops_clean = df_ops[campos_clave].copy()
        df_seg_clean = df_seg[campos_clave].copy()
        
        df_combined = pd.concat([df_ops_clean, df_seg_clean], ignore_index=True)
        
        print(f"📊 TOTAL COMBINADO: {len(df_combined)} submissions")
        
        return df_combined
        
    except Exception as e:
        print(f"❌ Error cargando Excel: {e}")
        return None

def aplicar_reglas_normalizacion(df):
    """Aplicar reglas específicas de normalización de Roberto"""
    
    print(f"\n🔄 APLICANDO REGLAS DE NORMALIZACIÓN")
    print("=" * 50)
    
    df_norm = df.copy()
    
    # REGLA 1: Normalizar locations sin formato estándar
    normalizaciones = {
        'Sucursal SC - Santa Catarina': '80 - Santa Catarina',
        'Sucursal LH - La Huasteca': '81 - La Huasteca', 
        'Sucursal GC - Garcia': '82 - Garcia'
    }
    
    print(f"📝 REGLA 1: Normalizar locations sin formato")
    for old_location, new_location in normalizaciones.items():
        mask = df_norm['Location'] == old_location
        count = mask.sum()
        if count > 0:
            df_norm.loc[mask, 'Location'] = new_location
            df_norm.loc[mask, 'Location External Key'] = int(new_location.split(' - ')[0])
            print(f"   ✅ {old_location} → {new_location} ({count} submissions)")
    
    return df_norm

def analizar_distribuciones_por_location(df):
    """Analizar distribución actual por location"""
    
    print(f"\n📊 ANÁLISIS DE DISTRIBUCIONES POR LOCATION")
    print("=" * 60)
    
    # Agrupar por location y form_type
    distribuciones = df.groupby(['Location', 'form_type']).size().unstack(fill_value=0)
    distribuciones['TOTAL'] = distribuciones.sum(axis=1)
    
    print(f"📋 DISTRIBUCIONES ACTUALES:")
    print(f"{'Location':<30} {'OPERATIVA':<12} {'SEGURIDAD':<12} {'TOTAL':<8} {'Regla':<15}")
    print("-" * 80)
    
    problemas = []
    
    for location in distribuciones.index:
        ops = distribuciones.loc[location, 'OPERATIVA'] if 'OPERATIVA' in distribuciones.columns else 0
        seg = distribuciones.loc[location, 'SEGURIDAD'] if 'SEGURIDAD' in distribuciones.columns else 0
        total = ops + seg
        
        # Determinar regla esperada
        if total >= 8:
            regla = "LOCAL (4+4)"
        elif total >= 4:
            regla = "FORÁNEA (2+2)"
        else:
            regla = "INCOMPLETO"
            
        print(f"{location:<30} {ops:<12} {seg:<12} {total:<8} {regla:<15}")
        
        # Detectar problemas específicos
        if location == '71 - Centrito Valle' and total > 8:
            problemas.append({
                'location': location,
                'problema': 'Exceso de supervisiones - redistribuir a Gómez Morín',
                'ops': ops,
                'seg': seg,
                'accion': 'Mapear 1 supervision por coordenadas'
            })
    
    # Casos especiales identificados por Roberto
    print(f"\n🎯 CASOS ESPECIALES IDENTIFICADOS:")
    
    centrito = distribuciones.loc[distribuciones.index.str.contains('Centrito Valle', na=False)]
    if not centrito.empty:
        ops = centrito['OPERATIVA'].iloc[0] if 'OPERATIVA' in centrito.columns else 0
        seg = centrito['SEGURIDAD'].iloc[0] if 'SEGURIDAD' in centrito.columns else 0
        print(f"   🔍 Centrito Valle: {ops} ops + {seg} seg = {ops+seg} total")
        if ops + seg > 8:
            print(f"      ⚠️ PROBLEMA: Exceso - redistribuir a Gómez Morín")
    
    # Verificar 4+4 esperados
    esperados_4_4 = ['8 - Gonzalitos', '21 - Chapultepec', '20 - Tecnológico']
    for location_name in esperados_4_4:
        location_match = distribuciones.index[distribuciones.index.str.contains(location_name.split(' - ')[1], na=False)]
        if not location_match.empty:
            location = location_match[0]
            ops = distribuciones.loc[location, 'OPERATIVA'] if 'OPERATIVA' in distribuciones.columns else 0
            seg = distribuciones.loc[location, 'SEGURIDAD'] if 'SEGURIDAD' in distribuciones.columns else 0
            print(f"   ✅ {location}: {ops} ops + {seg} seg = {ops+seg} total {'✅' if ops+seg==8 else '⚠️'}")
    
    return distribuciones, problemas

def procesar_submissions_sin_location(df):
    """Procesar submissions que no tienen location asignado"""
    
    print(f"\n🎯 PROCESANDO SUBMISSIONS SIN LOCATION")
    print("=" * 50)
    
    # Identificar submissions sin location
    sin_location = df[df['Location'].isna() | (df['Location'] == '')]
    con_location = df[df['Location'].notna() & (df['Location'] != '')]
    
    print(f"📊 ESTADÍSTICAS:")
    print(f"   ✅ CON location: {len(con_location)}")
    print(f"   ❌ SIN location: {len(sin_location)}")
    
    if len(sin_location) > 0:
        print(f"\n📋 SUBMISSIONS SIN LOCATION:")
        print(f"{'Fecha':<12} {'Usuario':<15} {'Tipo':<12} {'Sucursal':<20}")
        print("-" * 60)
        
        for idx, row in sin_location.head(10).iterrows():
            fecha = str(row['Date Submitted'])[:10] if pd.notna(row['Date Submitted']) else 'N/A'
            usuario = str(row['Submitted By'])[:14] if pd.notna(row['Submitted By']) else 'N/A'
            tipo = str(row['form_type'])
            sucursal = str(row['Sucursal'])[:19] if pd.notna(row['Sucursal']) else 'N/A'
            
            print(f"{fecha:<12} {usuario:<15} {tipo:<12} {sucursal:<20}")
        
        if len(sin_location) > 10:
            print(f"... y {len(sin_location) - 10} más")
    
    return sin_location, con_location

def cargar_sucursales_master():
    """Cargar catálogo de 86 sucursales para mapeo por coordenadas"""
    
    print(f"\n📂 CARGANDO CATÁLOGO SUCURSALES MASTER")
    print("=" * 40)
    
    try:
        df_master = pd.read_csv('SUCURSALES_MASTER_20251218_110913.csv')
        print(f"✅ Cargadas {len(df_master)} sucursales del catálogo master")
        
        # Crear diccionario para búsqueda rápida
        sucursales_dict = {}
        for _, row in df_master.iterrows():
            if pd.notna(row['numero']) and pd.notna(row['nombre']):
                numero = int(row['numero'])
                nombre = str(row['nombre'])
                key = f"{numero} - {nombre}"
                
                sucursales_dict[key] = {
                    'numero': numero,
                    'nombre': nombre,
                    'grupo': row.get('grupo', ''),
                    'tipo': row.get('tipo', ''),
                    'lat': float(row['lat']) if pd.notna(row['lat']) else None,
                    'lon': float(row['lon']) if pd.notna(row['lon']) else None
                }
        
        print(f"📊 Diccionario creado con {len(sucursales_dict)} entradas")
        return sucursales_dict
        
    except Exception as e:
        print(f"❌ Error cargando catálogo: {e}")
        return {}

def mapear_submissions_sin_location_por_sucursal(sin_location, sucursales_master):
    """Mapear submissions sin location usando campo Sucursal"""
    
    print(f"\n🗺️ MAPEO POR CAMPO SUCURSAL")
    print("=" * 40)
    
    mapeadas = []
    no_mapeadas = []
    
    for idx, row in sin_location.iterrows():
        sucursal_campo = str(row['Sucursal']).strip() if pd.notna(row['Sucursal']) else ''
        
        if not sucursal_campo or sucursal_campo == 'nan':
            no_mapeadas.append(row)
            continue
        
        # Buscar coincidencia en sucursales master
        location_encontrado = None
        
        # Búsqueda por nombre parcial
        for location_key, datos in sucursales_master.items():
            nombre_master = datos['nombre'].lower().strip()
            sucursal_lower = sucursal_campo.lower().strip()
            
            # Coincidencia directa
            if nombre_master == sucursal_lower:
                location_encontrado = location_key
                break
            
            # Coincidencia parcial (contiene)
            if sucursal_lower in nombre_master or nombre_master in sucursal_lower:
                location_encontrado = location_key
                break
        
        if location_encontrado:
            row_mapped = row.copy()
            row_mapped['Location'] = location_encontrado
            row_mapped['Location External Key'] = sucursales_master[location_encontrado]['numero']
            row_mapped['metodo_mapeo'] = 'CAMPO_SUCURSAL'
            mapeadas.append(row_mapped)
        else:
            row_mapped = row.copy()
            row_mapped['metodo_mapeo'] = 'NO_MAPEADO'
            no_mapeadas.append(row_mapped)
    
    print(f"📊 RESULTADOS MAPEO:")
    print(f"   ✅ Mapeadas por sucursal: {len(mapeadas)}")
    print(f"   ❌ No mapeadas: {len(no_mapeadas)}")
    
    if mapeadas:
        print(f"\n📋 MUESTRAS MAPEADAS:")
        for row in mapeadas[:5]:
            sucursal = str(row['Sucursal'])[:15]
            location = str(row['Location'])[:25]
            print(f"   '{sucursal}' → '{location}'")
    
    if no_mapeadas:
        print(f"\n📋 NO MAPEADAS:")
        for row in no_mapeadas[:5]:
            sucursal = str(row['Sucursal'])[:20] if pd.notna(row['Sucursal']) else 'N/A'
            usuario = str(row['Submitted By'])[:15]
            print(f"   '{sucursal}' ({usuario})")
    
    return mapeadas, no_mapeadas

def generar_validacion_manual(df_normalizado, distribuciones, problemas):
    """Generar lista para validación manual con Roberto"""
    
    print(f"\n🧪 GENERANDO VALIDACIÓN MANUAL")
    print("=" * 50)
    
    # Agrupar por location para análisis
    validacion_items = []
    
    for location in distribuciones.index:
        location_data = df_normalizado[df_normalizado['Location'] == location]
        
        if len(location_data) == 0:
            continue
        
        ops_count = len(location_data[location_data['form_type'] == 'OPERATIVA'])
        seg_count = len(location_data[location_data['form_type'] == 'SEGURIDAD'])
        
        # Obtener datos adicionales
        usuarios = location_data['Submitted By'].unique()
        fechas = location_data['Date Submitted'].dt.strftime('%Y-%m-%d').unique() if location_data['Date Submitted'].dtype != 'object' else location_data['Date Submitted'].unique()
        
        item = {
            'location': location,
            'operativas': ops_count,
            'seguridad': seg_count,
            'total': ops_count + seg_count,
            'usuarios': list(usuarios),
            'fechas_muestra': list(fechas)[:3],
            'problema_identificado': None,
            'recomendacion': 'OK' if ops_count + seg_count in [4, 8] else 'REVISAR',
            'tipo_esperado': 'LOCAL' if ops_count + seg_count >= 8 else 'FORÁNEA' if ops_count + seg_count >= 4 else 'INCOMPLETO'
        }
        
        # Marcar problemas específicos
        for problema in problemas:
            if problema['location'] == location:
                item['problema_identificado'] = problema['problema']
                item['recomendacion'] = problema['accion']
        
        validacion_items.append(item)
    
    # Ordenar por total descendente para priorizar casos problemáticos
    validacion_items.sort(key=lambda x: x['total'], reverse=True)
    
    return validacion_items

def mostrar_validacion_interactiva(validacion_items):
    """Mostrar validación interactiva caso por caso"""
    
    print(f"\n🎯 VALIDACIÓN MANUAL INTERACTIVA")
    print("=" * 60)
    print(f"📋 Total items para validar: {len(validacion_items)}")
    print("=" * 60)
    
    casos_importantes = []
    
    for i, item in enumerate(validacion_items[:10], 1):  # Primeros 10 casos
        location = item['location']
        ops = item['operativas']
        seg = item['seguridad'] 
        total = item['total']
        usuarios = ', '.join(item['usuarios'])
        tipo = item['tipo_esperado']
        problema = item['problema_identificado']
        
        print(f"\n📍 CASO {i}: {location}")
        print(f"   📊 Supervisiones: {ops} operativas + {seg} seguridad = {total} total")
        print(f"   👤 Usuarios: {usuarios}")
        print(f"   📅 Fechas muestra: {', '.join(item['fechas_muestra'])}")
        print(f"   🏷️ Tipo esperado: {tipo}")
        
        if problema:
            print(f"   ⚠️ PROBLEMA: {problema}")
            print(f"   💡 Acción: {item['recomendacion']}")
            casos_importantes.append(item)
        else:
            if total in [4, 8]:
                print(f"   ✅ Estado: CORRECTO")
            else:
                print(f"   ⚠️ Estado: REVISAR (total {total} no estándar)")
                casos_importantes.append(item)
    
    return casos_importantes

def main():
    """Función principal - Validación Manual Híbrida"""
    
    print("🧪 VALIDACIÓN MANUAL HÍBRIDA - EXCEL + REGLAS ROBERTO")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Objetivo: Aplicar reglas de normalización y procesar submissions")
    print("=" * 80)
    
    # 1. Cargar datos Excel
    df_excel = cargar_datos_excel()
    if df_excel is None:
        print("❌ Error: No se pudieron cargar los datos Excel")
        return
    
    # 2. Aplicar reglas de normalización
    df_normalizado = aplicar_reglas_normalizacion(df_excel)
    
    # 3. Analizar distribuciones
    distribuciones, problemas = analizar_distribuciones_por_location(df_normalizado)
    
    # 4. Procesar submissions sin location
    sin_location, con_location = procesar_submissions_sin_location(df_normalizado)
    
    # 5. Cargar sucursales master para mapeo
    sucursales_master = cargar_sucursales_master()
    
    # 6. Mapear submissions sin location
    if len(sin_location) > 0 and sucursales_master:
        mapeadas, no_mapeadas = mapear_submissions_sin_location_por_sucursal(sin_location, sucursales_master)
        
        # Agregar mapeadas al dataset principal
        if mapeadas:
            df_mapeadas = pd.DataFrame(mapeadas)
            df_final = pd.concat([con_location, df_mapeadas], ignore_index=True)
        else:
            df_final = con_location
    else:
        df_final = con_location
        mapeadas, no_mapeadas = [], list(sin_location.to_dict('records'))
    
    # 7. Generar validación manual
    validacion_items = generar_validacion_manual(df_final, distribuciones, problemas)
    
    # 8. Mostrar validación interactiva
    casos_importantes = mostrar_validacion_interactiva(validacion_items)
    
    # 9. Guardar resultados
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Guardar dataset procesado
    df_final.to_csv(f"SUBMISSIONS_NORMALIZADAS_{timestamp}.csv", index=False, encoding='utf-8')
    
    # Guardar validación manual
    with open(f"VALIDACION_MANUAL_{timestamp}.json", 'w', encoding='utf-8') as f:
        json.dump({
            'timestamp': timestamp,
            'total_submissions': len(df_final),
            'sin_location_originales': len(sin_location),
            'mapeadas_por_sucursal': len(mapeadas),
            'no_mapeadas': len(no_mapeadas),
            'casos_validacion': validacion_items,
            'casos_importantes': casos_importantes,
            'problemas_identificados': problemas
        }, f, indent=2, ensure_ascii=False, default=str)
    
    # RESUMEN FINAL
    print(f"\n" + "=" * 80)
    print(f"🎉 VALIDACIÓN MANUAL COMPLETADA")
    print("=" * 80)
    
    print(f"📊 ESTADÍSTICAS FINALES:")
    print(f"   📋 Total submissions procesadas: {len(df_final)}")
    print(f"   ✅ Con location original: {len(con_location)}")
    print(f"   🗺️ Mapeadas por sucursal: {len(mapeadas)}")
    print(f"   ❌ No mapeadas: {len(no_mapeadas)}")
    
    print(f"\n📁 ARCHIVOS GENERADOS:")
    print(f"   📄 Dataset normalizado: SUBMISSIONS_NORMALIZADAS_{timestamp}.csv")
    print(f"   🧪 Validación manual: VALIDACION_MANUAL_{timestamp}.json")
    
    print(f"\n🔜 SIGUIENTE PASO:")
    print(f"   🧪 Revisar casos importantes identificados")
    print(f"   🗺️ Confirmar clasificación LOCAL/FORÁNEA")
    print(f"   📊 Procesar {len(no_mapeadas)} submissions no mapeadas")
    
    if casos_importantes:
        print(f"\n⚠️ CASOS IMPORTANTES A REVISAR: {len(casos_importantes)}")
        for caso in casos_importantes[:3]:
            print(f"   • {caso['location']}: {caso['total']} supervisiones")
    
    return df_final, validacion_items, casos_importantes

if __name__ == "__main__":
    main()