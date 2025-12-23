#!/usr/bin/env python3
"""
📅 ANÁLISIS DE FECHAS COINCIDENTES PARA CASOS DEFICIT
Analizar operativas del mismo día para encontrar sucursales correctas
"""

import pandas as pd
import numpy as np
from datetime import datetime

def cargar_datos_deficit():
    """Cargar datos para análisis de déficit"""
    
    print("📂 CARGANDO DATOS PARA ANÁLISIS DÉFICIT")
    print("=" * 50)
    
    # Excel de operativas
    df_ops = pd.read_excel("SUPERVISION_OPERATIVA_CAS_11_REV_250125_Submissions-2025-12-18__1228CST-1766141816.xlsx")
    print(f"✅ Operativas: {len(df_ops)}")
    
    # Excel de seguridad
    df_seg = pd.read_excel("CONTROL_OPERATIVO_DE_SEGURIDAD_CAS_11_REV_25012025_Submissions-2025-12-18__1232CST-1766104009.xlsx")
    print(f"✅ Seguridad: {len(df_seg)}")
    
    return df_ops, df_seg

def analizar_casos_deficit_especificos(df_ops, df_seg):
    """Analizar los 3 casos específicos con déficit default"""
    
    print(f"\n🔍 ANÁLISIS CASOS DEFICIT DEFAULT")
    print("=" * 60)
    
    # Los 3 casos problemáticos
    indices_deficit = [157, 161, 162]
    
    casos_analizados = []
    
    for idx in indices_deficit:
        print(f"\n📍 CASO {idx} - ANÁLISIS DETALLADO")
        print("-" * 40)
        
        try:
            # Obtener datos del Excel de seguridad
            row_seg = df_seg.iloc[idx]
            
            fecha_seg = row_seg['Date Submitted']
            usuario_seg = row_seg['Submitted By']
            fecha_seg_date = pd.to_datetime(fecha_seg).date()
            
            print(f"📅 Fecha: {fecha_seg}")
            print(f"👤 Usuario: {usuario_seg}")
            print(f"📋 Index Excel: {idx}")
            
            # Buscar operativas del mismo día
            df_ops['fecha_date'] = pd.to_datetime(df_ops['Date Submitted']).dt.date
            operativas_mismo_dia = df_ops[df_ops['fecha_date'] == fecha_seg_date]
            
            print(f"\n🔍 OPERATIVAS DEL MISMO DÍA ({fecha_seg_date}):")
            print(f"   📊 Total encontradas: {len(operativas_mismo_dia)}")
            
            if len(operativas_mismo_dia) > 0:
                print(f"   {'Hora':<8} {'Usuario':<15} {'Location':<30} {'Match Usuario'}")
                print(f"   {'-'*70}")
                
                for _, op in operativas_mismo_dia.iterrows():
                    hora_op = pd.to_datetime(op['Date Submitted']).strftime('%H:%M')
                    usuario_op = op['Submitted By']
                    location_op = op['Location']
                    match_usuario = "✅ SÍ" if usuario_op == usuario_seg else "❌ NO"
                    
                    usuario_short = str(usuario_op)[:14]
                    location_short = str(location_op)[:29]
                    
                    print(f"   {hora_op:<8} {usuario_short:<15} {location_short:<30} {match_usuario}")
            
            # Buscar operativas del mismo usuario en fechas cercanas
            print(f"\n👤 OPERATIVAS DEL MISMO USUARIO EN FECHAS CERCANAS:")
            ops_mismo_usuario = df_ops[df_ops['Submitted By'] == usuario_seg]
            
            # Filtrar por fechas cercanas (±7 días)
            fecha_limite_inicio = fecha_seg_date - pd.Timedelta(days=7)
            fecha_limite_fin = fecha_seg_date + pd.Timedelta(days=7)
            
            ops_cercanas = ops_mismo_usuario[
                (pd.to_datetime(ops_mismo_usuario['Date Submitted']).dt.date >= fecha_limite_inicio) & 
                (pd.to_datetime(ops_mismo_usuario['Date Submitted']).dt.date <= fecha_limite_fin)
            ].sort_values('Date Submitted')
            
            print(f"   📊 Operativas del usuario en ±7 días: {len(ops_cercanas)}")
            
            if len(ops_cercanas) > 0:
                print(f"   {'Fecha':<12} {'Hora':<8} {'Location':<30} {'Días Diff'}")
                print(f"   {'-'*70}")
                
                for _, op in ops_cercanas.head(10).iterrows():
                    fecha_op = pd.to_datetime(op['Date Submitted'])
                    fecha_op_date = fecha_op.date()
                    hora_op = fecha_op.strftime('%H:%M')
                    location_op = str(op['Location'])[:29]
                    
                    dias_diff = (fecha_op_date - fecha_seg_date).days
                    dias_str = f"{dias_diff:+d}" if dias_diff != 0 else "MISMO"
                    
                    print(f"   {fecha_op_date:<12} {hora_op:<8} {location_op:<30} {dias_str}")
            
            # Verificar si hay Location Map disponible
            location_map = row_seg.get('Location Map', None)
            sucursal_manual = row_seg.get('Sucursal', None)
            
            print(f"\n📊 DATOS ADICIONALES DISPONIBLES:")
            print(f"   🗺️ Location Map: {'✅ SÍ' if pd.notna(location_map) else '❌ NO'}")
            print(f"   🏪 Sucursal manual: {'✅ ' + str(sucursal_manual) if pd.notna(sucursal_manual) else '❌ NO'}")
            
            if pd.notna(location_map):
                location_map_str = str(location_map)[:100] + "..." if len(str(location_map)) > 100 else str(location_map)
                print(f"   🔗 Link: {location_map_str}")
            
            # Recomendación
            print(f"\n💡 RECOMENDACIÓN:")
            
            if len(operativas_mismo_dia) > 0:
                # Hay operativas el mismo día
                mismo_usuario_mismo_dia = operativas_mismo_dia[operativas_mismo_dia['Submitted By'] == usuario_seg]
                
                if len(mismo_usuario_mismo_dia) > 0:
                    # Mismo usuario, mismo día - alta confianza
                    location_recomendada = mismo_usuario_mismo_dia.iloc[0]['Location']
                    print(f"   🎯 ALTA CONFIANZA: Mismo usuario mismo día → {location_recomendada}")
                    recomendacion = location_recomendada
                    confianza = "ALTA"
                else:
                    # Diferente usuario mismo día - buscar patrón
                    locations_mismo_dia = operativas_mismo_dia['Location'].value_counts()
                    if len(locations_mismo_dia) == 1:
                        location_recomendada = locations_mismo_dia.index[0]
                        print(f"   🎯 MEDIA CONFIANZA: Solo una location ese día → {location_recomendada}")
                        recomendacion = location_recomendada
                        confianza = "MEDIA"
                    else:
                        print(f"   ⚠️ BAJA CONFIANZA: Múltiples locations ese día, revisar manualmente")
                        recomendacion = "REVISAR_MANUAL"
                        confianza = "BAJA"
            
            elif len(ops_cercanas) > 0:
                # No hay operativas mismo día, usar patrón de fechas cercanas
                locations_cercanas = ops_cercanas['Location'].value_counts()
                location_mas_frecuente = locations_cercanas.index[0]
                frecuencia = locations_cercanas.iloc[0]
                
                print(f"   🎯 MEDIA CONFIANZA: Patrón fechas cercanas → {location_mas_frecuente} ({frecuencia} veces)")
                recomendacion = location_mas_frecuente
                confianza = "MEDIA"
            
            elif pd.notna(location_map):
                print(f"   🗺️ USAR COORDENADAS: Extraer de Location Map para proximidad geográfica")
                recomendacion = "USAR_COORDENADAS"
                confianza = "MEDIA"
            
            else:
                print(f"   ❌ SIN DATOS: Requiere validación manual completa")
                recomendacion = "VALIDACION_MANUAL"
                confianza = "BAJA"
            
            # Guardar caso analizado
            caso = {
                'index_excel': idx,
                'fecha': fecha_seg,
                'usuario': usuario_seg,
                'operativas_mismo_dia': len(operativas_mismo_dia),
                'operativas_mismo_usuario_cercanas': len(ops_cercanas),
                'tiene_location_map': pd.notna(location_map),
                'tiene_sucursal_manual': pd.notna(sucursal_manual),
                'recomendacion': recomendacion,
                'confianza': confianza,
                'operativas_mismo_dia_detalle': operativas_mismo_dia[['Date Submitted', 'Submitted By', 'Location']].to_dict('records') if len(operativas_mismo_dia) > 0 else [],
                'operativas_cercanas_detalle': ops_cercanas[['Date Submitted', 'Submitted By', 'Location']].head(5).to_dict('records') if len(ops_cercanas) > 0 else []
            }
            
            casos_analizados.append(caso)
            
        except Exception as e:
            print(f"❌ Error analizando caso {idx}: {e}")
    
    return casos_analizados

def generar_recomendaciones_finales(casos_analizados):
    """Generar recomendaciones finales para los casos déficit"""
    
    print(f"\n🎯 RECOMENDACIONES FINALES")
    print("=" * 50)
    
    casos_alta_confianza = [c for c in casos_analizados if c['confianza'] == 'ALTA']
    casos_media_confianza = [c for c in casos_analizados if c['confianza'] == 'MEDIA']
    casos_baja_confianza = [c for c in casos_analizados if c['confianza'] == 'BAJA']
    
    print(f"📊 RESUMEN CONFIANZA:")
    print(f"   ✅ Alta confianza: {len(casos_alta_confianza)}")
    print(f"   ⚠️ Media confianza: {len(casos_media_confianza)}")
    print(f"   ❌ Baja confianza: {len(casos_baja_confianza)}")
    
    print(f"\n📋 ACCIONES RECOMENDADAS:")
    
    for caso in casos_analizados:
        idx = caso['index_excel']
        fecha = str(caso['fecha'])[:10]
        usuario = caso['usuario']
        recomendacion = caso['recomendacion']
        confianza = caso['confianza']
        
        print(f"\n🔸 CASO {idx} ({fecha} - {usuario}):")
        
        if confianza == 'ALTA':
            print(f"   ✅ APLICAR AUTOMÁTICAMENTE: {recomendacion}")
            print(f"   💡 Razón: Mismo usuario, mismo día con operativa")
            
        elif confianza == 'MEDIA':
            if recomendacion == 'USAR_COORDENADAS':
                print(f"   🗺️ EXTRAER COORDENADAS de Location Map")
                print(f"   🎯 Buscar sucursal más cercana geográficamente")
            else:
                print(f"   ⚠️ REVISAR Y CONFIRMAR: {recomendacion}")
                print(f"   💡 Razón: Patrón basado en fechas cercanas")
                
        else:
            print(f"   ❌ VALIDACIÓN MANUAL REQUERIDA")
            print(f"   💡 Razón: Datos insuficientes para recomendación automática")
    
    return casos_analizados

def mostrar_correciones_sugeridas(casos_analizados):
    """Mostrar correcciones específicas sugeridas"""
    
    print(f"\n🔧 CORRECCIONES SUGERIDAS PARA APLICAR")
    print("=" * 50)
    
    correcciones = []
    
    for caso in casos_analizados:
        if caso['confianza'] in ['ALTA', 'MEDIA'] and caso['recomendacion'] not in ['USAR_COORDENADAS', 'VALIDACION_MANUAL', 'REVISAR_MANUAL']:
            correcciones.append({
                'index_excel': caso['index_excel'],
                'sucursal_actual': '8 - Gonzalitos',  # Todas están asignadas a Gonzalitos
                'sucursal_sugerida': caso['recomendacion'],
                'confianza': caso['confianza'],
                'razon': 'Coincidencia fechas operativas'
            })
    
    if correcciones:
        print(f"📋 CORRECCIONES A APLICAR:")
        print(f"{'Index':<6} {'Actual':<20} {'→ Sugerida':<25} {'Confianza':<10} {'Razón'}")
        print("-" * 80)
        
        for corr in correcciones:
            idx = str(corr['index_excel'])
            actual = str(corr['sucursal_actual'])[:19]
            sugerida = str(corr['sucursal_sugerida'])[:24]
            confianza = corr['confianza']
            razon = str(corr['razon'])[:15]
            
            print(f"{idx:<6} {actual:<20} → {sugerida:<25} {confianza:<10} {razon}")
        
        print(f"\n💡 Para aplicar estas correcciones, necesitas:")
        print(f"   1. Confirmar cada recomendación")
        print(f"   2. Actualizar el archivo de asignaciones")
        print(f"   3. Regenerar las distribuciones finales")
    else:
        print(f"⚠️ No hay correcciones automáticas disponibles")
        print(f"📋 Todos los casos requieren validación manual o extracción de coordenadas")
    
    return correcciones

def main():
    """Función principal"""
    
    print("📅 ANÁLISIS DE FECHAS COINCIDENTES PARA CASOS DEFICIT")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Objetivo: Encontrar sucursales correctas usando fechas de operativas")
    print("=" * 80)
    
    # 1. Cargar datos
    df_ops, df_seg = cargar_datos_deficit()
    
    # 2. Analizar casos específicos
    casos_analizados = analizar_casos_deficit_especificos(df_ops, df_seg)
    
    # 3. Generar recomendaciones
    casos_con_recomendaciones = generar_recomendaciones_finales(casos_analizados)
    
    # 4. Mostrar correcciones sugeridas
    correcciones = mostrar_correciones_sugeridas(casos_con_recomendaciones)
    
    # 5. Guardar análisis
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    import json
    with open(f"ANALISIS_DEFICIT_FECHAS_{timestamp}.json", 'w', encoding='utf-8') as f:
        json.dump({
            'timestamp': timestamp,
            'casos_analizados': casos_con_recomendaciones,
            'correcciones_sugeridas': correcciones,
            'resumen': {
                'total_casos': len(casos_analizados),
                'alta_confianza': len([c for c in casos_analizados if c['confianza'] == 'ALTA']),
                'media_confianza': len([c for c in casos_analizados if c['confianza'] == 'MEDIA']),
                'baja_confianza': len([c for c in casos_analizados if c['confianza'] == 'BAJA']),
                'correcciones_aplicables': len(correcciones)
            }
        }, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"\n📁 ANÁLISIS GUARDADO: ANALISIS_DEFICIT_FECHAS_{timestamp}.json")
    
    return casos_con_recomendaciones, correcciones

if __name__ == "__main__":
    main()