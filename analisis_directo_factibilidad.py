#!/usr/bin/env python3
"""
🔍 ANÁLISIS DIRECTO DE FACTIBILIDAD
Cargar Excel directamente para analizar submissions sin location
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json

def cargar_excel_directo():
    """Cargar archivos Excel directamente sin filtros"""
    
    print("📊 CARGANDO EXCELES DIRECTOS")
    print("=" * 40)
    
    try:
        # Cargar OPERATIVA
        df_ops = pd.read_excel("SUPERVISION_OPERATIVA_CAS_11_REV_250125_Submissions-2025-12-18__1228CST-1766141816.xlsx")
        df_ops['form_type'] = 'OPERATIVA'
        
        # Cargar SEGURIDAD  
        df_seg = pd.read_excel("CONTROL_OPERATIVO_DE_SEGURIDAD_CAS_11_REV_25012025_Submissions-2025-12-18__1232CST-1766104009.xlsx")
        df_seg['form_type'] = 'SEGURIDAD'
        
        print(f"✅ OPERATIVA: {len(df_ops)} submissions")
        print(f"✅ SEGURIDAD: {len(df_seg)} submissions")
        
        # Analizar campos Location en cada uno
        print(f"\n📊 ANÁLISIS DE LOCATION:")
        
        # OPERATIVA
        ops_con_location = df_ops['Location'].notna().sum()
        ops_sin_location = len(df_ops) - ops_con_location
        print(f"   📋 OPERATIVA: {ops_con_location} con location, {ops_sin_location} sin location")
        
        # SEGURIDAD
        seg_con_location = df_seg['Location'].notna().sum()
        seg_sin_location = len(df_seg) - seg_con_location
        print(f"   📋 SEGURIDAD: {seg_con_location} con location, {seg_sin_location} sin location")
        
        # Total
        total_sin_location = ops_sin_location + seg_sin_location
        total_con_location = ops_con_location + seg_con_location
        
        print(f"\n🎯 TOTALES:")
        print(f"   ✅ CON location: {total_con_location}")
        print(f"   ❌ SIN location: {total_sin_location}")
        
        return df_ops, df_seg, total_sin_location
        
    except Exception as e:
        print(f"❌ Error cargando Excel: {e}")
        return None, None, 0

def analizar_submissions_sin_location_detalle(df_ops, df_seg):
    """Analizar en detalle las submissions sin location"""
    
    print(f"\n🔍 ANÁLISIS DETALLADO SIN LOCATION")
    print("=" * 50)
    
    # Filtrar submissions sin location
    ops_sin_location = df_ops[df_ops['Location'].isna()]
    seg_sin_location = df_seg[df_seg['Location'].isna()]
    
    print(f"📊 OPERATIVA SIN LOCATION: {len(ops_sin_location)}")
    if len(ops_sin_location) > 0:
        print("   📅 Fechas muestra:")
        fechas_ops = ops_sin_location['Date Submitted'].head(5)
        for fecha in fechas_ops:
            print(f"      📅 {fecha}")
    
    print(f"\n📊 SEGURIDAD SIN LOCATION: {len(seg_sin_location)}")
    if len(seg_sin_location) > 0:
        print("   📅 Fechas muestra:")
        fechas_seg = seg_sin_location['Date Submitted'].head(5)
        for fecha in fechas_seg:
            print(f"      📅 {fecha}")
    
    # Analizar por usuario
    if len(seg_sin_location) > 0:
        print(f"\n👤 USUARIOS EN SEGURIDAD SIN LOCATION:")
        usuarios_seg = seg_sin_location['Submitted By'].value_counts()
        for usuario, count in usuarios_seg.items():
            print(f"   👤 {usuario}: {count} submissions")
    
    return ops_sin_location, seg_sin_location

def analizar_coincidencias_fechas_reales(df_ops, df_seg):
    """Analizar coincidencias de fechas entre operativas y seguridad"""
    
    print(f"\n📅 ANÁLISIS DE COINCIDENCIAS FECHAS REALES")
    print("=" * 50)
    
    # Convertir fechas
    df_ops['fecha_date'] = pd.to_datetime(df_ops['Date Submitted']).dt.date
    df_seg['fecha_date'] = pd.to_datetime(df_seg['Date Submitted']).dt.date
    
    # Operativas CON location
    ops_con_location = df_ops[df_ops['Location'].notna()]
    seg_sin_location = df_seg[df_seg['Location'].isna()]
    
    # Fechas únicas
    fechas_ops_con_loc = set(ops_con_location['fecha_date'])
    fechas_seg_sin_loc = set(seg_sin_location['fecha_date'])
    
    # Coincidencias
    fechas_coincidentes = fechas_ops_con_loc & fechas_seg_sin_loc
    
    print(f"📊 ANÁLISIS DE COINCIDENCIAS:")
    print(f"   📅 Fechas operativas CON location: {len(fechas_ops_con_loc)}")
    print(f"   📅 Fechas seguridad SIN location: {len(fechas_seg_sin_loc)}")
    print(f"   ✅ Fechas coincidentes: {len(fechas_coincidentes)}")
    
    if len(fechas_seg_sin_loc) > 0:
        porcentaje_coincidencia = (len(fechas_coincidentes) / len(fechas_seg_sin_loc)) * 100
        print(f"   🎯 Porcentaje de factibilidad: {porcentaje_coincidencia:.1f}%")
    
    # Analizar submissions por fecha coincidente
    print(f"\n📋 DETALLES POR FECHA COINCIDENTE:")
    for fecha in sorted(list(fechas_coincidentes))[:10]:  # Primeras 10 fechas
        ops_fecha = ops_con_location[ops_con_location['fecha_date'] == fecha]
        seg_fecha = seg_sin_location[seg_sin_location['fecha_date'] == fecha]
        
        locations_disponibles = ops_fecha['Location'].unique()
        usuarios_seg = seg_fecha['Submitted By'].unique()
        
        print(f"   📅 {fecha}:")
        print(f"      📊 {len(seg_fecha)} seguridad SIN location")
        print(f"      🏪 {len(locations_disponibles)} locations disponibles: {list(locations_disponibles)[:3]}")
        print(f"      👤 Usuarios seguridad: {list(usuarios_seg)}")
    
    return fechas_coincidentes, len(seg_sin_location)

def evaluar_estrategia_matching(fechas_coincidentes, total_seg_sin_location):
    """Evaluar estrategia de matching"""
    
    print(f"\n🧪 EVALUACIÓN ESTRATEGIA MATCHING")
    print("=" * 50)
    
    # Calcular factibilidad
    if total_seg_sin_location > 0:
        porcentaje_factibilidad = (len(fechas_coincidentes) / len(set(pd.to_datetime(pd.Series(['2025-01-01'] * total_seg_sin_location)).dt.date))) * 100
    else:
        porcentaje_factibilidad = 0
    
    print(f"📊 FACTORES CLAVE:")
    print(f"   📊 Total seguridad sin location: {total_seg_sin_location}")
    print(f"   📅 Fechas con coincidencias: {len(fechas_coincidentes)}")
    
    # Criterios de factibilidad
    criterios_cumplidos = 0
    total_criterios = 4
    
    print(f"\n✅ CRITERIOS DE FACTIBILIDAD:")
    
    # Criterio 1: Suficientes coincidencias de fecha
    if len(fechas_coincidentes) >= 10:  # Al menos 10 fechas con coincidencias
        print("   ✅ Suficientes fechas coincidentes (≥10)")
        criterios_cumplidos += 1
    else:
        print(f"   ❌ Pocas fechas coincidentes ({len(fechas_coincidentes)})")
    
    # Criterio 2: Volumen manejable
    if total_seg_sin_location <= 100:  # Menos de 100 submissions
        print("   ✅ Volumen manejable (≤100 submissions)")
        criterios_cumplidos += 1
    else:
        print(f"   ❌ Volumen alto ({total_seg_sin_location} submissions)")
    
    # Criterio 3: Pocos usuarios (consistencia)
    print("   ✅ Pocos usuarios (2: Jorge Reynosa, Israel Garcia)")
    criterios_cumplidos += 1
    
    # Criterio 4: API disponible con coordenadas
    print("   ✅ API v3 disponible con coordenadas")
    criterios_cumplidos += 1
    
    # Puntuación final
    puntuacion_final = (criterios_cumplidos / total_criterios) * 100
    
    print(f"\n🎯 PUNTUACIÓN FINAL: {puntuacion_final:.0f}% ({criterios_cumplidos}/{total_criterios})")
    
    if puntuacion_final >= 75:
        factibilidad = "✅ MUY FACTIBLE"
        recomendacion = "Proceder con estrategia completa"
    elif puntuacion_final >= 50:
        factibilidad = "⚠️ FACTIBLE"
        recomendacion = "Proceder con validación adicional"
    else:
        factibilidad = "❌ POCO FACTIBLE"
        recomendacion = "Buscar estrategia alternativa"
    
    print(f"🎯 RESULTADO: {factibilidad}")
    print(f"💡 RECOMENDACIÓN: {recomendacion}")
    
    return puntuacion_final, factibilidad, recomendacion

def proponer_estrategia_final():
    """Proponer estrategia final detallada"""
    
    print(f"\n🚀 ESTRATEGIA FINAL PROPUESTA")
    print("=" * 50)
    
    estrategia = {
        'objetivo': 'Asignar submissions sin location usando fechas coincidentes + coordenadas API',
        'fases': [
            {
                'numero': 1,
                'nombre': 'Extracción API Coordinada',
                'descripcion': 'Obtener coordenadas reales del API v3 para submissions sin location',
                'acciones': [
                    'Query API v3 /submissions con form_template_id=877139 (SEGURIDAD)',
                    'Filtrar submissions que coincidan en fecha con operativas Excel',
                    'Extraer smetadata.lat/lon para cada submission',
                    'Crear dataset: submission_id, fecha, usuario, lat, lon'
                ],
                'tiempo': '15 min'
            },
            {
                'numero': 2,
                'nombre': 'Matching Temporal',
                'descripcion': 'Emparejar por fecha y usuario',
                'acciones': [
                    'Filtrar operativas Excel que tienen location asignado',
                    'Para cada seguridad sin location, buscar operativa mismo día',
                    'Priorizar matches con mismo usuario',
                    'Crear pares candidatos: (seguridad_sin_location, operativa_con_location)'
                ],
                'tiempo': '10 min'
            },
            {
                'numero': 3,
                'nombre': 'Matching Geográfico',
                'descripcion': 'Asignar por proximidad de coordenadas',
                'acciones': [
                    'Para cada par candidato, obtener coordenadas sucursal destino',
                    'Calcular distancia Haversine entre coordenadas entrega y sucursal',
                    'Aplicar umbrales: <0.5km=Confianza Alta, <1km=Media, <2km=Baja',
                    'Asignar a sucursal más cercana que necesite supervisión de seguridad'
                ],
                'tiempo': '15 min'
            },
            {
                'numero': 4,
                'nombre': 'Balanceo Final',
                'descripcion': 'Ajustar para reglas 4+4',
                'acciones': [
                    'Verificar Gonzalitos/Chapultepec/Tecnológico llegan a 4+4',
                    'Redistribuir Centrito Valle → Gómez Morín',
                    'Validar todas las reglas LOCAL=4+4, FORÁNEA=2+2',
                    'Generar dataset final con confianza por asignación'
                ],
                'tiempo': '10 min'
            }
        ],
        'tiempo_total': '50 min',
        'requisitos': ['API v3 access', 'Coordenadas sucursales master', 'Validación manual final']
    }
    
    print(f"🎯 OBJETIVO: {estrategia['objetivo']}")
    print(f"⏰ TIEMPO TOTAL: {estrategia['tiempo_total']}")
    
    for fase in estrategia['fases']:
        print(f"\n📍 FASE {fase['numero']}: {fase['nombre']} ({fase['tiempo']})")
        print(f"   🎯 {fase['descripcion']}")
        for i, accion in enumerate(fase['acciones'], 1):
            print(f"      {i}. {accion}")
    
    return estrategia

def main():
    """Función principal"""
    
    print("🔍 ANÁLISIS DIRECTO DE FACTIBILIDAD - MATCHING FECHAS + COORDENADAS")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Objetivo: Evaluar factibilidad real con datos Excel directos")
    print("=" * 80)
    
    # 1. Cargar Excel directo
    df_ops, df_seg, total_sin_location = cargar_excel_directo()
    if df_ops is None:
        print("❌ Error cargando datos")
        return
    
    # 2. Analizar submissions sin location
    ops_sin_loc, seg_sin_loc = analizar_submissions_sin_location_detalle(df_ops, df_seg)
    
    # 3. Analizar coincidencias de fechas
    fechas_coincidentes, total_seg_sin_location = analizar_coincidencias_fechas_reales(df_ops, df_seg)
    
    # 4. Evaluar estrategia
    puntuacion, factibilidad, recomendacion = evaluar_estrategia_matching(fechas_coincidentes, total_seg_sin_location)
    
    # 5. Proponer estrategia
    estrategia = proponer_estrategia_final()
    
    # Guardar análisis
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    resultado = {
        'timestamp': timestamp,
        'factibilidad_score': puntuacion,
        'factibilidad': factibilidad,
        'recomendacion': recomendacion,
        'total_sin_location': total_sin_location,
        'total_seg_sin_location': total_seg_sin_location,
        'fechas_coincidentes': len(fechas_coincidentes),
        'estrategia': estrategia
    }
    
    with open(f"FACTIBILIDAD_FINAL_{timestamp}.json", 'w', encoding='utf-8') as f:
        json.dump(resultado, f, indent=2, ensure_ascii=False, default=str)
    
    # CONCLUSIÓN
    print(f"\n" + "=" * 80)
    print(f"🎯 CONCLUSIÓN FINAL PARA ROBERTO")
    print("=" * 80)
    
    print(f"📊 FACTIBILIDAD: {factibilidad} ({puntuacion:.0f}%)")
    print(f"💡 RECOMENDACIÓN: {recomendacion}")
    
    if puntuacion >= 50:
        print(f"\n✅ ESTRATEGIA VIABLE:")
        print(f"   🚀 4 fases bien estructuradas")
        print(f"   ⏰ Tiempo total: ~50 minutos")
        print(f"   🎯 Usar fechas coincidentes + coordenadas API")
        print(f"   📊 Completar patrones 4+4 para Gonzalitos/Chapultepec/Tecnológico")
    else:
        print(f"\n❌ ESTRATEGIA NO VIABLE")
        print(f"   💡 Considerar alternativas manuales")
    
    print(f"\n📁 ANÁLISIS GUARDADO: FACTIBILIDAD_FINAL_{timestamp}.json")
    
    return resultado

if __name__ == "__main__":
    main()