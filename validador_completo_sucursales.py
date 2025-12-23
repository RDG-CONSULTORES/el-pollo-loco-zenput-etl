#!/usr/bin/env python3
"""
📋 VALIDADOR COMPLETO DE SUCURSALES
Listado de todas las sucursales con sus supervisiones para validación manual
"""

import pandas as pd
import numpy as np
import json
from datetime import datetime

def cargar_datos_completos():
    """Cargar todos los datos necesarios para validación"""
    
    print("📂 CARGANDO DATOS PARA VALIDACIÓN")
    print("=" * 50)
    
    # 1. Asignaciones finales Opción C
    df_asignaciones = pd.read_csv("ASIGNACIONES_FINALES_OPCION_C_20251218_135441.csv")
    print(f"✅ Asignaciones Opción C: {len(df_asignaciones)}")
    
    # 2. Submissions normalizadas originales
    df_normalizadas = pd.read_csv("SUBMISSIONS_NORMALIZADAS_20251218_130301.csv")
    print(f"✅ Submissions normalizadas: {len(df_normalizadas)}")
    
    # 3. Excel de operativas
    df_ops = pd.read_excel("SUPERVISION_OPERATIVA_CAS_11_REV_250125_Submissions-2025-12-18__1228CST-1766141816.xlsx")
    df_ops['form_type'] = 'OPERATIVA'
    print(f"✅ Excel operativas: {len(df_ops)}")
    
    # 4. Excel de seguridad
    df_seg = pd.read_excel("CONTROL_OPERATIVO_DE_SEGURIDAD_CAS_11_REV_25012025_Submissions-2025-12-18__1232CST-1766104009.xlsx")
    df_seg['form_type'] = 'SEGURIDAD'
    print(f"✅ Excel seguridad: {len(df_seg)}")
    
    # 5. Coordenadas sucursales
    df_sucursales = pd.read_csv('SUCURSALES_MASTER_20251218_110913.csv')
    print(f"✅ Sucursales master: {len(df_sucursales)}")
    
    return df_asignaciones, df_normalizadas, df_ops, df_seg, df_sucursales

def consolidar_todas_supervisiones(df_normalizadas, df_asignaciones, df_ops, df_seg):
    """Consolidar todas las supervisiones (operativas + seguridad asignadas)"""
    
    print(f"\n🔄 CONSOLIDANDO TODAS LAS SUPERVISIONES")
    print("=" * 50)
    
    supervisiones_consolidadas = []
    
    # 1. OPERATIVAS (todas tienen location asignado)
    for _, row in df_ops.iterrows():
        if pd.notna(row['Location']):
            supervisiones_consolidadas.append({
                'index_excel': row.name,
                'submission_id': row.get('Submission ID', f'ops_{row.name}'),
                'fecha': row['Date Submitted'],
                'usuario': row['Submitted By'],
                'location': row['Location'],
                'form_type': 'OPERATIVA',
                'lat_entrega': None,  # No disponible en Excel
                'lon_entrega': None,
                'metodo_asignacion': 'EXCEL_DIRECTO',
                'confianza': 1.0
            })
    
    print(f"✅ Operativas agregadas: {len([s for s in supervisiones_consolidadas if s['form_type'] == 'OPERATIVA'])}")
    
    # 2. SEGURIDAD CON LOCATION (del Excel original)
    seg_con_location = df_seg[df_seg['Location'].notna()]
    
    for _, row in seg_con_location.iterrows():
        supervisiones_consolidadas.append({
            'index_excel': row.name,
            'submission_id': row.get('Submission ID', f'seg_{row.name}'),
            'fecha': row['Date Submitted'],
            'usuario': row['Submitted By'],
            'location': row['Location'],
            'form_type': 'SEGURIDAD',
            'lat_entrega': None,
            'lon_entrega': None,
            'metodo_asignacion': 'EXCEL_DIRECTO',
            'confianza': 1.0
        })
    
    print(f"✅ Seguridad con location: {len([s for s in supervisiones_consolidadas if s['form_type'] == 'SEGURIDAD'])}")
    
    # 3. SEGURIDAD SIN LOCATION (de Opción C)
    for _, row in df_asignaciones.iterrows():
        # Obtener datos del Excel original
        try:
            excel_row = df_seg.iloc[row['index_original']]
            
            supervisiones_consolidadas.append({
                'index_excel': row['index_original'],
                'submission_id': excel_row.get('Submission ID', f'seg_{row["index_original"]}'),
                'fecha': row['fecha'],
                'usuario': row['usuario'],
                'location': row['sucursal_asignada'],
                'form_type': 'SEGURIDAD',
                'lat_entrega': row['lat_entrega'] if pd.notna(row['lat_entrega']) else None,
                'lon_entrega': row['lon_entrega'] if pd.notna(row['lon_entrega']) else None,
                'metodo_asignacion': row['metodo'],
                'confianza': row['confianza']
            })
        except:
            print(f"⚠️ Error procesando asignación index {row['index_original']}")
    
    total_seguridad = len([s for s in supervisiones_consolidadas if s['form_type'] == 'SEGURIDAD'])
    print(f"✅ Total seguridad consolidada: {total_seguridad}")
    
    print(f"\n📊 RESUMEN CONSOLIDACIÓN:")
    print(f"   📋 Total supervisiones: {len(supervisiones_consolidadas)}")
    print(f"   🏗️ Operativas: {len([s for s in supervisiones_consolidadas if s['form_type'] == 'OPERATIVA'])}")
    print(f"   🛡️ Seguridad: {total_seguridad}")
    
    return supervisiones_consolidadas

def generar_reporte_por_sucursal(supervisiones_consolidadas, df_sucursales):
    """Generar reporte detallado por sucursal para validación"""
    
    print(f"\n📋 GENERANDO REPORTE POR SUCURSAL")
    print("=" * 60)
    
    # Agrupar por sucursal
    sucursales_dict = {}
    
    for supervision in supervisiones_consolidadas:
        location = supervision['location']
        
        if location not in sucursales_dict:
            sucursales_dict[location] = {
                'operativas': [],
                'seguridad': []
            }
        
        if supervision['form_type'] == 'OPERATIVA':
            sucursales_dict[location]['operativas'].append(supervision)
        else:
            sucursales_dict[location]['seguridad'].append(supervision)
    
    # Obtener coordenadas de sucursales
    coords_sucursales = {}
    for _, row in df_sucursales.iterrows():
        if pd.notna(row['numero']) and pd.notna(row['lat']) and pd.notna(row['lon']):
            numero = int(row['numero'])
            nombre = row['nombre']
            location_key = f"{numero} - {nombre}"
            coords_sucursales[location_key] = {
                'lat': float(row['lat']),
                'lon': float(row['lon']),
                'grupo': row.get('grupo', ''),
                'tipo': row.get('tipo', 'LOCAL')
            }
    
    # Generar reporte
    print(f"🏪 REPORTE DETALLADO POR SUCURSAL")
    print(f"{'='*100}")
    print(f"{'Sucursal':<35} {'Ops':<4} {'Seg':<4} {'Tot':<4} {'Regla':<6} {'Estado':<10} {'Confianza':<10} {'Métodos':<20}")
    print(f"{'-'*100}")
    
    sucursales_validacion = []
    
    for location in sorted(sucursales_dict.keys()):
        data = sucursales_dict[location]
        
        ops_count = len(data['operativas'])
        seg_count = len(data['seguridad'])
        total_count = ops_count + seg_count
        
        # Determinar regla esperada (LOCAL=4+4, FORÁNEA=2+2)
        coords = coords_sucursales.get(location, {})
        tipo_sucursal = coords.get('tipo', 'LOCAL')
        
        if tipo_sucursal == 'FORÁNEA':
            regla_esperada = '2+2'
            ops_esperadas = 2
            seg_esperadas = 2
        else:
            regla_esperada = '4+4'
            ops_esperadas = 4
            seg_esperadas = 4
        
        # Estado
        if ops_count == ops_esperadas and seg_count == seg_esperadas:
            estado = "✅ PERFECTO"
        elif ops_count == ops_esperadas and seg_count < seg_esperadas:
            estado = f"⚠️ -{seg_esperadas - seg_count} SEG"
        elif ops_count < ops_esperadas:
            estado = f"❌ -{ops_esperadas - ops_count} OPS"
        elif total_count > (ops_esperadas + seg_esperadas):
            estado = f"🔄 EXCESO"
        else:
            estado = "⚠️ REVISAR"
        
        # Confianza promedio de seguridad
        seg_confianzas = [s['confianza'] for s in data['seguridad']]
        confianza_promedio = np.mean(seg_confianzas) if seg_confianzas else 1.0
        
        # Métodos utilizados en seguridad
        metodos = list(set([s['metodo_asignacion'] for s in data['seguridad']]))
        metodos_str = ','.join([m.split('_')[0] for m in metodos])[:19]
        
        # Mostrar línea
        location_short = location[:34]
        conf_str = f"{confianza_promedio:.2f}"
        
        print(f"{location_short:<35} {ops_count:<4} {seg_count:<4} {total_count:<4} {regla_esperada:<6} {estado:<10} {conf_str:<10} {metodos_str:<20}")
        
        # Guardar para validación detallada
        sucursal_info = {
            'location': location,
            'operativas_count': ops_count,
            'seguridad_count': seg_count,
            'total_count': total_count,
            'regla_esperada': regla_esperada,
            'estado': estado,
            'confianza_promedio': confianza_promedio,
            'coordenadas_sucursal': coords,
            'operativas_detalles': data['operativas'],
            'seguridad_detalles': data['seguridad'],
            'metodos_utilizados': metodos
        }
        
        sucursales_validacion.append(sucursal_info)
    
    print(f"{'-'*100}")
    
    # Estadísticas generales
    total_sucursales = len(sucursales_validacion)
    perfectas = len([s for s in sucursales_validacion if "✅" in s['estado']])
    con_deficit = len([s for s in sucursales_validacion if "⚠️" in s['estado'] or "❌" in s['estado']])
    con_exceso = len([s for s in sucursales_validacion if "🔄" in s['estado']])
    
    print(f"\n📊 ESTADÍSTICAS GENERALES:")
    print(f"   🏪 Total sucursales: {total_sucursales}")
    print(f"   ✅ Perfectas (4+4 o 2+2): {perfectas}")
    print(f"   ⚠️ Con déficit: {con_deficit}")
    print(f"   🔄 Con exceso: {con_exceso}")
    print(f"   📈 Porcentaje completo: {(perfectas/total_sucursales)*100:.1f}%")
    
    return sucursales_validacion

def mostrar_detalles_deficit_default(sucursales_validacion):
    """Mostrar detalles de las supervisiones asignadas por DEFAULT_DEFICIT"""
    
    print(f"\n🔍 ANÁLISIS DETALLADO - ASIGNACIONES DEFAULT_DEFICIT")
    print("=" * 70)
    
    deficit_encontradas = []
    
    for sucursal in sucursales_validacion:
        for supervision in sucursal['seguridad_detalles']:
            if 'DEFAULT_DEFICIT' in supervision['metodo_asignacion']:
                deficit_encontradas.append({
                    'sucursal': sucursal['location'],
                    'index_excel': supervision['index_excel'],
                    'submission_id': supervision['submission_id'],
                    'fecha': supervision['fecha'],
                    'usuario': supervision['usuario'],
                    'confianza': supervision['confianza']
                })
    
    if deficit_encontradas:
        print(f"🚨 ENCONTRADAS {len(deficit_encontradas)} ASIGNACIONES DEFAULT_DEFICIT:")
        print(f"{'Index':<6} {'Submission ID':<15} {'Fecha':<12} {'Usuario':<15} {'Sucursal':<25}")
        print("-" * 80)
        
        for asig in deficit_encontradas:
            index_str = str(asig['index_excel'])
            sub_id = str(asig['submission_id'])[:14]
            fecha_str = str(asig['fecha'])[:10] if asig['fecha'] else 'N/A'
            usuario_str = str(asig['usuario'])[:14] if asig['usuario'] else 'N/A'
            sucursal_str = str(asig['sucursal'])[:24]
            
            print(f"{index_str:<6} {sub_id:<15} {fecha_str:<12} {usuario_str:<15} {sucursal_str:<25}")
        
        print(f"\n💡 PARA VALIDAR MANUALMENTE:")
        print(f"   1. Buscar estos indices en el Excel de seguridad")
        print(f"   2. Revisar coordenadas en Location Map si están disponibles")
        print(f"   3. Confirmar sucursal correcta basada en proximidad geográfica")
    else:
        print("✅ No se encontraron asignaciones DEFAULT_DEFICIT")
    
    return deficit_encontradas

def mostrar_fechas_coincidentes_sucursal(sucursales_validacion):
    """Mostrar análisis de fechas coincidentes por sucursal"""
    
    print(f"\n📅 ANÁLISIS DE FECHAS COINCIDENTES")
    print("=" * 50)
    
    print(f"🎯 SUCURSALES CON COINCIDENCIAS PERFECTAS MISMO DÍA:")
    print(f"{'Sucursal':<35} {'Fechas Coincidentes':<20} {'Detalles'}")
    print("-" * 75)
    
    coincidencias_perfectas = 0
    
    for sucursal in sucursales_validacion:
        if sucursal['operativas_count'] > 0 and sucursal['seguridad_count'] > 0:
            
            # Obtener fechas (solo fecha, sin hora)
            fechas_ops = set()
            for op in sucursal['operativas_detalles']:
                if op['fecha']:
                    fecha = pd.to_datetime(op['fecha']).date()
                    fechas_ops.add(fecha)
            
            fechas_seg = set()
            for seg in sucursal['seguridad_detalles']:
                if seg['fecha']:
                    fecha = pd.to_datetime(seg['fecha']).date()
                    fechas_seg.add(fecha)
            
            # Encontrar coincidencias
            coincidencias = fechas_ops & fechas_seg
            
            if coincidencias:
                coincidencias_perfectas += 1
                location_short = sucursal['location'][:34]
                coincidencias_count = len(coincidencias)
                detalles = f"Ops:{len(fechas_ops)}, Seg:{len(fechas_seg)}"
                
                print(f"{location_short:<35} {coincidencias_count:<20} {detalles}")
                
                # Mostrar fechas específicas para las primeras 3
                if coincidencias_perfectas <= 3:
                    fechas_str = ', '.join([str(f) for f in sorted(list(coincidencias))[:3]])
                    if len(coincidencias) > 3:
                        fechas_str += "..."
                    print(f"   📅 {fechas_str}")
    
    print(f"\n📊 RESUMEN COINCIDENCIAS:")
    print(f"   ✅ Sucursales con fechas coincidentes: {coincidencias_perfectas}")
    print(f"   📈 Porcentaje con coincidencias: {(coincidencias_perfectas/len([s for s in sucursales_validacion if s['operativas_count'] > 0 and s['seguridad_count'] > 0]))*100:.1f}%")

def validacion_interactiva(deficit_encontradas, sucursales_validacion):
    """Función para validación interactiva manual"""
    
    print(f"\n🔧 VALIDACIÓN INTERACTIVA DISPONIBLE")
    print("=" * 50)
    
    if deficit_encontradas:
        print(f"📋 HAY {len(deficit_encontradas)} ASIGNACIONES PARA VALIDAR:")
        print(f"\n💡 COMANDOS DISPONIBLES:")
        print(f"   • Para ver detalles de una sucursal: sucursal <nombre>")
        print(f"   • Para ver Excel index específico: excel <index>")
        print(f"   • Para corregir asignación: corregir <index> <nueva_sucursal>")
        print(f"   • Para validar coordenadas: coords <sucursal>")
        print(f"\n🚨 ASIGNACIONES A REVISAR:")
        
        for i, asig in enumerate(deficit_encontradas, 1):
            print(f"   {i}. Index {asig['index_excel']} → {asig['sucursal']} ({asig['fecha'][:10]})")
    else:
        print(f"✅ TODAS LAS ASIGNACIONES TIENEN ALTA CONFIANZA")
        print(f"🎉 No se requiere validación manual adicional")
    
    print(f"\n📊 ESTADO FINAL:")
    perfectas = len([s for s in sucursales_validacion if "✅" in s['estado']])
    total = len(sucursales_validacion)
    print(f"   🏪 {perfectas}/{total} sucursales completas ({(perfectas/total)*100:.1f}%)")

def main():
    """Función principal"""
    
    print("📋 VALIDADOR COMPLETO DE SUCURSALES")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Objetivo: Validar todas las asignaciones y permitir corrección manual")
    print("=" * 80)
    
    # 1. Cargar datos
    df_asignaciones, df_normalizadas, df_ops, df_seg, df_sucursales = cargar_datos_completos()
    
    # 2. Consolidar supervisiones
    supervisiones_consolidadas = consolidar_todas_supervisiones(
        df_normalizadas, df_asignaciones, df_ops, df_seg
    )
    
    # 3. Generar reporte por sucursal
    sucursales_validacion = generar_reporte_por_sucursal(
        supervisiones_consolidadas, df_sucursales
    )
    
    # 4. Análisis de DEFAULT_DEFICIT
    deficit_encontradas = mostrar_detalles_deficit_default(sucursales_validacion)
    
    # 5. Análisis de fechas coincidentes
    mostrar_fechas_coincidentes_sucursal(sucursales_validacion)
    
    # 6. Guardar para referencia
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Guardar reporte completo
    with open(f"VALIDACION_SUCURSALES_COMPLETA_{timestamp}.json", 'w', encoding='utf-8') as f:
        json.dump({
            'timestamp': timestamp,
            'total_supervisiones': len(supervisiones_consolidadas),
            'total_sucursales': len(sucursales_validacion),
            'deficit_encontradas': deficit_encontradas,
            'sucursales_detalle': sucursales_validacion
        }, f, indent=2, ensure_ascii=False, default=str)
    
    # 7. Validación interactiva
    validacion_interactiva(deficit_encontradas, sucursales_validacion)
    
    print(f"\n📁 REPORTE GUARDADO: VALIDACION_SUCURSALES_COMPLETA_{timestamp}.json")
    
    return sucursales_validacion, deficit_encontradas

if __name__ == "__main__":
    main()