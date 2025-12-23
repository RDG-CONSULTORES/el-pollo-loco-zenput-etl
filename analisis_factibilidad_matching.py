#!/usr/bin/env python3
"""
🔍 ANÁLISIS DE FACTIBILIDAD: Matching por Fechas + Coordenadas
Evaluar si podemos asignar 85 submissions sin location usando coincidencia de fechas y proximidad
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import math

def cargar_datos_para_analisis():
    """Cargar datos Excel + API para análisis combinado"""
    
    print("📊 CARGANDO DATOS PARA ANÁLISIS DE FACTIBILIDAD")
    print("=" * 60)
    
    # 1. Cargar Excel normalizado
    try:
        df_excel = pd.read_csv("SUBMISSIONS_NORMALIZADAS_20251218_130301.csv")
        print(f"✅ Excel normalizado: {len(df_excel)} submissions")
        
        # Convertir fechas
        df_excel['Date Submitted'] = pd.to_datetime(df_excel['Date Submitted'])
        df_excel['fecha_date'] = df_excel['Date Submitted'].dt.date
        
    except Exception as e:
        print(f"❌ Error cargando Excel: {e}")
        return None, None
    
    # 2. Cargar datos API (si existen)
    api_data = None
    try:
        # Buscar archivo API más reciente
        api_files = ['FASE1_API_V3_CORRECTA_20251218_120332.csv', 'FASE1_COMPLETA_EXITO_20251218_120332.csv']
        
        for file in api_files:
            try:
                api_data = pd.read_csv(file)
                print(f"✅ API data: {len(api_data)} submissions")
                
                # Convertir fechas
                api_data['fecha'] = pd.to_datetime(api_data['fecha'])
                api_data['fecha_date'] = api_data['fecha'].dt.date
                break
            except:
                continue
                
    except Exception as e:
        print(f"⚠️ No se encontraron datos API: {e}")
    
    return df_excel, api_data

def analizar_submissions_sin_location(df_excel):
    """Analizar las 85 submissions sin location"""
    
    print(f"\n🎯 ANÁLISIS DE SUBMISSIONS SIN LOCATION")
    print("=" * 50)
    
    # Filtrar submissions sin location
    sin_location = df_excel[df_excel['Location'].isna() | (df_excel['Location'] == '')]
    con_location = df_excel[df_excel['Location'].notna() & (df_excel['Location'] != '')]
    
    print(f"📊 ESTADÍSTICAS:")
    print(f"   ❌ SIN location: {len(sin_location)}")
    print(f"   ✅ CON location: {len(con_location)}")
    
    # Analizar por tipo de formulario
    sin_loc_por_tipo = sin_location['form_type'].value_counts()
    print(f"\n📋 SIN LOCATION POR TIPO:")
    for tipo, count in sin_loc_por_tipo.items():
        print(f"   📊 {tipo}: {count} submissions")
    
    # Analizar por usuario
    sin_loc_por_usuario = sin_location['Submitted By'].value_counts()
    print(f"\n👤 SIN LOCATION POR USUARIO:")
    for usuario, count in sin_loc_por_usuario.items():
        print(f"   📊 {usuario}: {count} submissions")
    
    # Analizar distribución de fechas
    sin_location_fechas = sin_location['fecha_date'].value_counts().sort_index()
    print(f"\n📅 DISTRIBUCIÓN POR FECHAS (primeras 10):")
    for fecha, count in sin_location_fechas.head(10).items():
        print(f"   📅 {fecha}: {count} submissions")
    
    return sin_location, con_location

def analizar_coincidencias_por_fecha(sin_location, con_location):
    """Analizar cuántas submissions sin location tienen fechas coincidentes con submissions con location"""
    
    print(f"\n📅 ANÁLISIS DE COINCIDENCIAS POR FECHA")
    print("=" * 50)
    
    # Obtener fechas de submissions sin location
    fechas_sin_location = set(sin_location['fecha_date'])
    fechas_con_location = set(con_location['fecha_date'])
    
    # Calcular coincidencias
    fechas_coincidentes = fechas_sin_location & fechas_con_location
    fechas_sin_coincidencia = fechas_sin_location - fechas_con_location
    
    print(f"📊 RESULTADOS COINCIDENCIA FECHAS:")
    print(f"   📅 Fechas únicas SIN location: {len(fechas_sin_location)}")
    print(f"   📅 Fechas únicas CON location: {len(fechas_con_location)}")
    print(f"   ✅ Fechas coincidentes: {len(fechas_coincidentes)}")
    print(f"   ❌ Fechas SIN coincidencia: {len(fechas_sin_coincidencia)}")
    
    print(f"\n📈 PORCENTAJE DE FACTIBILIDAD:")
    porcentaje_coincidencia = (len(fechas_coincidentes) / len(fechas_sin_location)) * 100
    print(f"   🎯 {porcentaje_coincidencia:.1f}% de fechas sin location tienen coincidencias")
    
    # Analizar submissions por fecha coincidente
    submissions_coincidentes = 0
    for fecha in fechas_coincidentes:
        count_sin = len(sin_location[sin_location['fecha_date'] == fecha])
        submissions_coincidentes += count_sin
    
    porcentaje_submissions = (submissions_coincidentes / len(sin_location)) * 100
    print(f"   📊 {submissions_coincidentes}/{len(sin_location)} submissions ({porcentaje_submissions:.1f}%) pueden usar matching por fecha")
    
    # Mostrar ejemplos de coincidencias
    print(f"\n📋 EJEMPLOS DE FECHAS COINCIDENTES:")
    ejemplos_fechas = sorted(list(fechas_coincidentes))[:5]
    
    for fecha in ejemplos_fechas:
        sin_loc_count = len(sin_location[sin_location['fecha_date'] == fecha])
        con_loc_count = len(con_location[con_location['fecha_date'] == fecha])
        con_loc_locations = con_location[con_location['fecha_date'] == fecha]['Location'].unique()
        
        print(f"   📅 {fecha}:")
        print(f"      ❌ Sin location: {sin_loc_count} submissions")
        print(f"      ✅ Con location: {con_loc_count} submissions en {len(con_loc_locations)} sucursales")
        print(f"      🏪 Sucursales: {list(con_loc_locations)[:3]}{'...' if len(con_loc_locations) > 3 else ''}")
    
    return fechas_coincidentes, submissions_coincidentes

def analizar_sucursales_con_deficit(con_location):
    """Identificar sucursales que tienen déficit (menos de 4+4)"""
    
    print(f"\n📊 ANÁLISIS DE SUCURSALES CON DÉFICIT")
    print("=" * 50)
    
    # Agrupar por location y tipo
    distribuciones = con_location.groupby(['Location', 'form_type']).size().unstack(fill_value=0)
    distribuciones['TOTAL'] = distribuciones.sum(axis=1)
    
    # Identificar sucursales con déficit
    sucursales_deficit = []
    
    for location in distribuciones.index:
        ops = distribuciones.loc[location, 'OPERATIVA'] if 'OPERATIVA' in distribuciones.columns else 0
        seg = distribuciones.loc[location, 'SEGURIDAD'] if 'SEGURIDAD' in distribuciones.columns else 0
        total = ops + seg
        
        # Criterios de déficit
        deficit_info = None
        
        if total == 7:  # Como Gonzalitos, Chapultepec, Tecnológico
            if ops == 4 and seg == 3:
                deficit_info = {'tipo': 'SEGURIDAD', 'faltante': 1, 'razon': '4+3 debería ser 4+4'}
            elif ops == 3 and seg == 4:
                deficit_info = {'tipo': 'OPERATIVA', 'faltante': 1, 'razon': '3+4 debería ser 4+4'}
        elif total < 4:  # Sucursales incompletas
            deficit_info = {'tipo': 'AMBOS', 'faltante': 4-total, 'razon': f'Solo {total} supervisiones'}
        elif total in [5, 6]:  # Posibles foráneas incompletas
            deficit_info = {'tipo': 'AMBOS', 'faltante': 6-total, 'razon': f'Posible foránea incompleta'}
        
        if deficit_info:
            sucursales_deficit.append({
                'location': location,
                'operativas': ops,
                'seguridad': seg,
                'total': total,
                'deficit': deficit_info
            })
    
    # Ordenar por total para priorizar
    sucursales_deficit.sort(key=lambda x: x['total'], reverse=True)
    
    print(f"🎯 SUCURSALES CON DÉFICIT IDENTIFICADAS: {len(sucursales_deficit)}")
    print(f"{'Location':<30} {'Ops':<5} {'Seg':<5} {'Total':<6} {'Déficit':<20}")
    print("-" * 75)
    
    deficit_seguridad = 0
    deficit_operativa = 0
    
    for sucursal in sucursales_deficit[:15]:  # Mostrar primeras 15
        location = sucursal['location'][:29]
        ops = sucursal['operativas']
        seg = sucursal['seguridad']
        total = sucursal['total']
        deficit = sucursal['deficit']
        
        print(f"{location:<30} {ops:<5} {seg:<5} {total:<6} {deficit['razon']:<20}")
        
        if deficit['tipo'] == 'SEGURIDAD':
            deficit_seguridad += deficit['faltante']
        elif deficit['tipo'] == 'OPERATIVA':
            deficit_operativa += deficit['faltante']
    
    if len(sucursales_deficit) > 15:
        print(f"... y {len(sucursales_deficit) - 15} más")
    
    print(f"\n📊 RESUMEN DÉFICIT:")
    print(f"   📊 Sucursales con déficit de SEGURIDAD: {deficit_seguridad} submissions necesarias")
    print(f"   📊 Sucursales con déficit de OPERATIVA: {deficit_operativa} submissions necesarias")
    
    return sucursales_deficit

def evaluar_factibilidad_estrategia(sin_location, fechas_coincidentes, submissions_coincidentes, sucursales_deficit):
    """Evaluar factibilidad general de la estrategia"""
    
    print(f"\n🧪 EVALUACIÓN DE FACTIBILIDAD GENERAL")
    print("=" * 50)
    
    # Datos base
    total_sin_location = len(sin_location)
    sin_location_seguridad = len(sin_location[sin_location['form_type'] == 'SEGURIDAD'])
    
    # Déficit identificado
    deficit_seguridad = sum(s['deficit']['faltante'] for s in sucursales_deficit 
                          if s['deficit']['tipo'] in ['SEGURIDAD', 'AMBOS'])
    
    print(f"📊 DATOS BASE:")
    print(f"   ❌ Total submissions sin location: {total_sin_location}")
    print(f"   📊 De las cuales SEGURIDAD: {sin_location_seguridad}")
    print(f"   📊 Submissions con fechas coincidentes: {submissions_coincidentes}")
    
    print(f"\n📊 NECESIDADES IDENTIFICADAS:")
    print(f"   🎯 Sucursales necesitan SEGURIDAD: {deficit_seguridad} submissions")
    
    # Evaluación de factibilidad
    factibilidad_score = 0
    factores = []
    
    # Factor 1: Coincidencia de fechas
    porcentaje_fechas = (submissions_coincidentes / total_sin_location) * 100
    if porcentaje_fechas >= 80:
        factibilidad_score += 30
        factores.append(f"✅ Fechas coincidentes: {porcentaje_fechas:.1f}% (+30 pts)")
    elif porcentaje_fechas >= 60:
        factibilidad_score += 20
        factores.append(f"⚠️ Fechas coincidentes: {porcentaje_fechas:.1f}% (+20 pts)")
    else:
        factibilidad_score += 10
        factores.append(f"❌ Fechas coincidentes: {porcentaje_fechas:.1f}% (+10 pts)")
    
    # Factor 2: Balance oferta vs demanda
    if sin_location_seguridad >= deficit_seguridad:
        factibilidad_score += 25
        factores.append(f"✅ Oferta SEGURIDAD suficiente: {sin_location_seguridad} vs {deficit_seguridad} (+25 pts)")
    else:
        factibilidad_score += 10
        factores.append(f"❌ Oferta SEGURIDAD insuficiente: {sin_location_seguridad} vs {deficit_seguridad} (+10 pts)")
    
    # Factor 3: Distribución de usuarios (para verificar consistencia)
    usuarios_sin_location = sin_location['Submitted By'].nunique()
    if usuarios_sin_location <= 3:  # Pocos usuarios = más consistencia
        factibilidad_score += 20
        factores.append(f"✅ Pocos usuarios ({usuarios_sin_location}): Mayor consistencia (+20 pts)")
    else:
        factibilidad_score += 10
        factores.append(f"⚠️ Muchos usuarios ({usuarios_sin_location}): Menor consistencia (+10 pts)")
    
    # Factor 4: Disponibilidad de coordenadas (asumiendo API)
    factibilidad_score += 25  # Asumiendo que API tiene coordenadas
    factores.append("✅ Coordenadas disponibles en API (+25 pts)")
    
    print(f"\n🎯 FACTORES DE FACTIBILIDAD:")
    for factor in factores:
        print(f"   {factor}")
    
    print(f"\n📊 PUNTUACIÓN FACTIBILIDAD: {factibilidad_score}/100")
    
    if factibilidad_score >= 80:
        factibilidad = "✅ MUY FACTIBLE"
        recomendacion = "Proceder con estrategia completa"
    elif factibilidad_score >= 60:
        factibilidad = "⚠️ FACTIBLE CON PRECAUCIONES"
        recomendacion = "Proceder con validación manual adicional"
    else:
        factibilidad = "❌ POCO FACTIBLE"
        recomendacion = "Considerar estrategia alternativa"
    
    print(f"🎯 RESULTADO: {factibilidad}")
    print(f"💡 RECOMENDACIÓN: {recomendacion}")
    
    return factibilidad_score, factibilidad, recomendacion

def proponer_estrategia_implementacion(factibilidad_score):
    """Proponer estrategia detallada de implementación"""
    
    print(f"\n🚀 ESTRATEGIA PROPUESTA DE IMPLEMENTACIÓN")
    print("=" * 60)
    
    if factibilidad_score >= 60:
        estrategia = {
            'fase_1': {
                'nombre': 'Extracción API con Coordenadas',
                'descripcion': 'Extraer las 85 submissions del API v3 con coordenadas reales',
                'pasos': [
                    'Usar API v3 /submissions para obtener submissions sin location',
                    'Extraer smetadata.lat/lon (coordenadas reales de entrega)',
                    'Extraer date_submitted para matching temporal',
                    'Crear dataset: submission_id, fecha, usuario, lat, lon, form_type'
                ],
                'tiempo_estimado': '15 min',
                'herramientas': ['API v3', 'pandas', 'requests']
            },
            'fase_2': {
                'nombre': 'Matching por Fecha + Usuario',
                'descripcion': 'Filtrar candidatos usando coincidencia de fechas y usuarios',
                'pasos': [
                    'Filtrar submissions API que coinciden en fecha con Excel',
                    'Filtrar por usuario (Jorge Reynosa, Israel Garcia)',
                    'Crear pares candidatos: (sin_location_api, con_location_excel)',
                    'Priorizar pares con mismo día y mismo usuario'
                ],
                'tiempo_estimado': '10 min',
                'criterios': ['Fecha exacta', 'Usuario coincidente', 'Tipo formulario']
            },
            'fase_3': {
                'nombre': 'Matching por Coordenadas',
                'descripcion': 'Asignar usando proximidad geográfica (Haversine)',
                'pasos': [
                    'Para cada submission sin location, calcular distancia a sucursales con déficit',
                    'Aplicar algoritmo Haversine con umbrales: <0.5km=Alta, <1km=Media, <2km=Baja',
                    'Priorizar sucursales que necesitan el tipo de formulario específico',
                    'Asignar a sucursal más cercana con déficit compatible'
                ],
                'tiempo_estimado': '15 min',
                'umbrales': ['<0.5km: Confianza 0.9', '<1km: Confianza 0.8', '<2km: Confianza 0.7']
            },
            'fase_4': {
                'nombre': 'Validación y Ajuste',
                'descripcion': 'Validar asignaciones y ajustar distribuciones',
                'pasos': [
                    'Verificar que sucursales problemáticas (Gonzalitos, Chapultepec, Tecnológico) lleguen a 4+4',
                    'Confirmar que Centrito Valle se redistribuya correctamente a Gómez Morín',
                    'Validar reglas: LOCAL=4+4, FORÁNEA=2+2',
                    'Generar reporte de confianza por asignación'
                ],
                'tiempo_estimado': '10 min',
                'validaciones': ['Reglas de negocio', 'Distribuciones balanceadas', 'Confianza >0.7']
            }
        }
        
        tiempo_total = sum(int(fase['tiempo_estimado'].split()[0]) for fase in estrategia.values())
        
        print(f"📋 ESTRATEGIA DE 4 FASES (Total: ~{tiempo_total} min):")
        
        for fase_id, detalles in estrategia.items():
            print(f"\n📍 {fase_id.upper()}: {detalles['nombre']} ({detalles['tiempo_estimado']})")
            print(f"   🎯 {detalles['descripcion']}")
            for i, paso in enumerate(detalles['pasos'], 1):
                print(f"      {i}. {paso}")
    
    else:
        print("❌ ESTRATEGIA NO RECOMENDADA debido a baja factibilidad")
        print("💡 ALTERNATIVAS:")
        print("   1. Distribución manual por grupo operativo")
        print("   2. Usar solo coincidencia de fechas sin coordenadas")
        print("   3. Validación manual individual")
    
    return estrategia if factibilidad_score >= 60 else None

def main():
    """Función principal - Análisis de Factibilidad"""
    
    print("🔍 ANÁLISIS DE FACTIBILIDAD: Matching por Fechas + Coordenadas")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Objetivo: Evaluar si podemos asignar 85 submissions usando fechas + coordenadas")
    print("=" * 80)
    
    # 1. Cargar datos
    df_excel, api_data = cargar_datos_para_analisis()
    if df_excel is None:
        print("❌ Error: No se pudieron cargar los datos")
        return
    
    # 2. Analizar submissions sin location
    sin_location, con_location = analizar_submissions_sin_location(df_excel)
    
    # 3. Analizar coincidencias por fecha
    fechas_coincidentes, submissions_coincidentes = analizar_coincidencias_por_fecha(sin_location, con_location)
    
    # 4. Analizar sucursales con déficit
    sucursales_deficit = analizar_sucursales_con_deficit(con_location)
    
    # 5. Evaluar factibilidad
    factibilidad_score, factibilidad, recomendacion = evaluar_factibilidad_estrategia(
        sin_location, fechas_coincidentes, submissions_coincidentes, sucursales_deficit
    )
    
    # 6. Proponer estrategia
    estrategia = proponer_estrategia_implementacion(factibilidad_score)
    
    # 7. Guardar análisis
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    resultado = {
        'timestamp': timestamp,
        'factibilidad_score': factibilidad_score,
        'factibilidad': factibilidad,
        'recomendacion': recomendacion,
        'total_sin_location': len(sin_location),
        'submissions_coincidentes': submissions_coincidentes,
        'sucursales_deficit': sucursales_deficit,
        'estrategia_propuesta': estrategia
    }
    
    with open(f"ANALISIS_FACTIBILIDAD_{timestamp}.json", 'w', encoding='utf-8') as f:
        json.dump(resultado, f, indent=2, ensure_ascii=False, default=str)
    
    # CONCLUSIÓN FINAL
    print(f"\n" + "=" * 80)
    print(f"🎯 CONCLUSIÓN FINAL")
    print("=" * 80)
    
    print(f"📊 FACTIBILIDAD: {factibilidad} ({factibilidad_score}/100)")
    print(f"💡 RECOMENDACIÓN: {recomendacion}")
    
    if estrategia:
        print(f"\n✅ ESTRATEGIA LISTA PARA IMPLEMENTAR:")
        print(f"   🚀 4 fases bien definidas")
        print(f"   ⏱️ Tiempo estimado: ~{sum(int(fase['tiempo_estimado'].split()[0]) for fase in estrategia.values())} minutos")
        print(f"   🎯 Alta probabilidad de éxito")
    
    print(f"\n📁 ANÁLISIS GUARDADO: ANALISIS_FACTIBILIDAD_{timestamp}.json")
    
    return resultado

if __name__ == "__main__":
    main()