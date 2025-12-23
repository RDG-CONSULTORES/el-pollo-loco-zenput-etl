#!/usr/bin/env python3
"""
🔍 REVISIÓN COMPLETA CAMPO SUCURSAL
Analizar TODAS las supervisiones para detectar inconsistencias y normalizar
"""

import pandas as pd
from datetime import datetime

def analizar_todas_supervisiones_con_sucursal():
    """Analizar todas las supervisiones que tienen campo Sucursal"""
    
    print("🔍 ANÁLISIS COMPLETO SUPERVISIONES CON CAMPO SUCURSAL")
    print("=" * 80)
    
    df_seg = pd.read_excel("CONTROL_OPERATIVO_DE_SEGURIDAD_CAS_11_REV_25012025_Submissions-2025-12-18__1232CST-1766104009.xlsx")
    
    # Todas las supervisiones con campo Sucursal
    con_sucursal = df_seg[df_seg['Sucursal'].notna()].copy()
    
    print(f"📊 Total supervisiones con campo Sucursal: {len(con_sucursal)}")
    
    # Categorizar por estado de Location
    con_location = con_sucursal[con_sucursal['Location'].notna()]
    sin_location = con_sucursal[con_sucursal['Location'].isna()]
    
    print(f"   ✅ Con Location asignado: {len(con_location)}")
    print(f"   ❌ Sin Location asignado: {len(sin_location)}")
    
    return con_sucursal, con_location, sin_location

def crear_mapeo_sucursal_completo():
    """Crear mapeo completo de valores del campo Sucursal"""
    
    print(f"\n🗺️ CREANDO MAPEO COMPLETO CAMPO SUCURSAL")
    print("=" * 60)
    
    # Cargar catálogo master
    df_sucursales = pd.read_csv('SUCURSALES_MASTER_20251218_110913.csv')
    
    # Mapeo básico de nombres
    mapeo_basico = {}
    for _, row in df_sucursales.iterrows():
        if pd.notna(row['numero']) and pd.notna(row['nombre']):
            numero = int(row['numero'])
            nombre = row['nombre']
            location_key = f"{numero} - {nombre}"
            
            # Mapeo directo por nombre
            mapeo_basico[nombre.lower().strip()] = location_key
    
    # Mapeos específicos detectados en el análisis
    mapeos_especificos = {
        # Variaciones de nombres
        'sc': '4 - Santa Catarina',
        'santa catarina': '4 - Santa Catarina',
        'lh': '7 - La Huasteca', 
        'la huasteca': '7 - La Huasteca',
        'huasteca': '7 - La Huasteca',
        'gc': '6 - Garcia',
        'garcia': '6 - Garcia',
        'gomez morin': '38 - Gomez Morin',
        'gómez morín': '38 - Gomez Morin',
        'centrito valle': '71 - Centrito Valle',
        'centrito': '71 - Centrito Valle',
        
        # Nombres comunes detectados
        'gonzalitos': '8 - Gonzalitos',
        'tecnologico': '20 - Tecnológico',
        'chapultepec': '21 - Chapultepec',
        'pino suarez': '1 - Pino Suarez',
        'madero': '2 - Madero',
        'matamoros': '3 - Matamoros',
        'felix u. gomez': '5 - Felix U. Gomez',
        'félix u. gomez': '5 - Felix U. Gomez',
        'concordia': '12 - Concordia',
        'solidaridad': '16 - Solidaridad',
        'linda vista': '18 - Linda Vista',
        'pablo livas': '29 - Pablo Livas',
        'las quintas': '31 - Las Quintas',
        'allende': '32 - Allende',
        'montemorelos': '34 - Montemorelos',
        'lincoln': '11 - Lincoln',
        'cadereyta': '26 - Cadereyta',
        'santiago': '27 - Santiago',
        'universidad': '58 - Universidad (Tampico)',
        'ochoa (saltillo)': '54 - Ramos Arizpe',
        
        # Usuarios que fueron mal capturados como Sucursal
        'israel garcía': None,  # Es usuario, no sucursal
        'jorge reynosa': None,  # Es usuario, no sucursal
    }
    
    mapeo_completo = {**mapeo_basico, **mapeos_especificos}
    
    print(f"✅ Creado mapeo con {len(mapeo_completo)} entradas")
    
    return mapeo_completo

def detectar_inconsistencias_location_vs_sucursal(con_location, mapeo_completo):
    """Detectar inconsistencias entre Location asignado y campo Sucursal"""
    
    print(f"\n🚨 DETECTANDO INCONSISTENCIAS LOCATION vs CAMPO SUCURSAL")
    print("=" * 80)
    
    inconsistencias = []
    
    print(f"{'#':<3} {'Index':<6} {'Fecha':<12} {'Usuario':<15} {'Location Asignado':<25} {'Campo Sucursal':<20}")
    print("-" * 90)
    
    for i, (idx, row) in enumerate(con_location.iterrows(), 1):
        location_asignado = str(row['Location'])
        sucursal_campo = str(row['Sucursal']).lower().strip()
        
        # Buscar a qué location debería mapear el campo Sucursal
        location_sugerido = mapeo_completo.get(sucursal_campo, None)
        
        # Detectar inconsistencia
        if location_sugerido and location_sugerido != location_asignado:
            fecha_str = pd.to_datetime(row['Date Submitted']).strftime('%Y-%m-%d')
            usuario = row['Submitted By']
            
            inconsistencias.append({
                'index_excel': idx,
                'fecha': row['Date Submitted'],
                'fecha_str': fecha_str,
                'usuario': usuario,
                'location_asignado': location_asignado,
                'sucursal_campo': row['Sucursal'],
                'location_sugerido': location_sugerido,
                'tipo_inconsistencia': 'LOCATION_INCORRECTO'
            })
            
            location_asignado_short = location_asignado[:24]
            sucursal_campo_short = row['Sucursal'][:19]
            
            print(f"{i:<3} {idx:<6} {fecha_str:<12} {usuario:<15} {location_asignado_short:<25} {sucursal_campo_short:<20}")
    
    print(f"\n📊 INCONSISTENCIAS DETECTADAS: {len(inconsistencias)}")
    
    if inconsistencias:
        print(f"\n🔧 CORRECCIONES SUGERIDAS:")
        for inc in inconsistencias:
            print(f"   📍 Index {inc['index_excel']}: {inc['location_asignado']} → {inc['location_sugerido']}")
            print(f"      📅 {inc['fecha_str']} - {inc['usuario']} - Campo: '{inc['sucursal_campo']}'")
    
    return inconsistencias

def analizar_valores_sucursal_sin_mapeo(con_sucursal, mapeo_completo):
    """Analizar valores del campo Sucursal que no tienen mapeo"""
    
    print(f"\n❓ VALORES CAMPO SUCURSAL SIN MAPEO")
    print("=" * 50)
    
    # Obtener valores únicos del campo Sucursal
    valores_unicos = con_sucursal['Sucursal'].value_counts()
    
    sin_mapeo = []
    
    for valor, count in valores_unicos.items():
        valor_normalizado = str(valor).lower().strip()
        
        if valor_normalizado not in mapeo_completo:
            sin_mapeo.append({
                'valor_original': valor,
                'valor_normalizado': valor_normalizado,
                'count': count
            })
    
    if sin_mapeo:
        print(f"📊 Valores sin mapeo encontrados: {len(sin_mapeo)}")
        print(f"{'Valor Original':<25} {'Count':<6} {'Tipo Probable'}")
        print("-" * 50)
        
        for item in sorted(sin_mapeo, key=lambda x: x['count'], reverse=True):
            valor = item['valor_original'][:24]
            count = item['count']
            
            # Clasificar tipo probable
            if any(name in str(valor).lower() for name in ['israel', 'jorge', 'garcia', 'reynosa']):
                tipo = "👤 USUARIO"
            elif len(str(valor)) < 3:
                tipo = "❓ CÓDIGO"
            else:
                tipo = "🏪 SUCURSAL"
            
            print(f"{valor:<25} {count:<6} {tipo}")
        
        return sin_mapeo
    else:
        print("✅ Todos los valores tienen mapeo")
        return []

def generar_plan_correccion_completo(inconsistencias):
    """Generar plan de corrección completo"""
    
    print(f"\n📋 PLAN DE CORRECCIÓN COMPLETO")
    print("=" * 60)
    
    if not inconsistencias:
        print("✅ No se requieren correcciones")
        return
    
    # Agrupar correcciones por tipo
    correcciones_por_tipo = {}
    
    for inc in inconsistencias:
        from_location = inc['location_asignado']
        to_location = inc['location_sugerido']
        correction_key = f"{from_location} → {to_location}"
        
        if correction_key not in correcciones_por_tipo:
            correcciones_por_tipo[correction_key] = []
        
        correcciones_por_tipo[correction_key].append(inc)
    
    print(f"🔧 TIPOS DE CORRECCIÓN NECESARIAS:")
    
    for i, (correction_type, casos) in enumerate(correcciones_por_tipo.items(), 1):
        print(f"\n{i}. {correction_type}")
        print(f"   📊 Casos: {len(casos)}")
        print(f"   📅 Fechas:")
        
        for caso in casos:
            print(f"      • {caso['fecha_str']} - {caso['usuario']} (Index: {caso['index_excel']})")
    
    # Guardar plan
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    import json
    with open(f"PLAN_CORRECCION_SUCURSAL_{timestamp}.json", 'w', encoding='utf-8') as f:
        json.dump({
            'timestamp': timestamp,
            'total_inconsistencias': len(inconsistencias),
            'tipos_correccion': list(correcciones_por_tipo.keys()),
            'inconsistencias_detalle': inconsistencias
        }, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"\n📁 Plan guardado: PLAN_CORRECCION_SUCURSAL_{timestamp}.json")
    
    return correcciones_por_tipo

def main():
    """Función principal"""
    
    print("🔍 REVISIÓN COMPLETA CAMPO SUCURSAL")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Objetivo: Detectar todas las inconsistencias usando campo Sucursal")
    print("=" * 80)
    
    # 1. Analizar todas las supervisiones con campo Sucursal
    con_sucursal, con_location, sin_location = analizar_todas_supervisiones_con_sucursal()
    
    # 2. Crear mapeo completo
    mapeo_completo = crear_mapeo_sucursal_completo()
    
    # 3. Detectar inconsistencias Location vs Campo Sucursal
    inconsistencias = detectar_inconsistencias_location_vs_sucursal(con_location, mapeo_completo)
    
    # 4. Analizar valores sin mapeo
    sin_mapeo = analizar_valores_sucursal_sin_mapeo(con_sucursal, mapeo_completo)
    
    # 5. Generar plan de corrección
    if inconsistencias:
        correcciones_por_tipo = generar_plan_correccion_completo(inconsistencias)
    
    # Resumen final
    print(f"\n🎯 RESUMEN FINAL:")
    print(f"   📊 Total supervisiones con campo Sucursal: {len(con_sucursal)}")
    print(f"   🚨 Inconsistencias detectadas: {len(inconsistencias)}")
    print(f"   ❓ Valores sin mapeo: {len(sin_mapeo)}")
    
    if inconsistencias:
        print(f"\n💡 PRÓXIMOS PASOS:")
        print(f"   1. Revisar inconsistencias detectadas")
        print(f"   2. Confirmar correcciones propuestas") 
        print(f"   3. Aplicar correcciones masivas")
        print(f"   4. Recalcular distribuciones finales")
    
    return inconsistencias, sin_mapeo

if __name__ == "__main__":
    main()