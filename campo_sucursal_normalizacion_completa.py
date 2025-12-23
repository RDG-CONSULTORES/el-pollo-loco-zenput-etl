#!/usr/bin/env python3
"""
🔧 CAMPO SUCURSAL PARA NORMALIZACIÓN COMPLETA
Usar campo Sucursal para normalizar todas las supervisiones restantes
"""

import pandas as pd
from datetime import datetime

def analizar_campo_sucursal_sin_location():
    """Analizar supervisiones SIN location que tienen campo Sucursal"""
    
    print("🔍 ANÁLISIS CAMPO SUCURSAL PARA NORMALIZACIÓN")
    print("=" * 80)
    
    # Cargar Excel de seguridad
    df_seg = pd.read_excel("CONTROL_OPERATIVO_DE_SEGURIDAD_CAS_11_REV_25012025_Submissions-2025-12-18__1232CST-1766104009.xlsx")
    
    # Supervisiones SIN location pero CON campo Sucursal
    sin_location_con_sucursal = df_seg[
        (df_seg['Location'].isna()) & 
        (df_seg['Sucursal'].notna())
    ].copy()
    
    print(f"📊 Supervisiones SIN location pero CON campo Sucursal: {len(sin_location_con_sucursal)}")
    
    if len(sin_location_con_sucursal) > 0:
        print(f"\n📋 LISTADO COMPLETO:")
        print(f"{'#':<3} {'Index':<6} {'Fecha':<12} {'Usuario':<15} {'Campo Sucursal':<25}")
        print("-" * 70)
        
        casos_normalizacion = []
        
        for i, (idx, row) in enumerate(sin_location_con_sucursal.iterrows(), 1):
            fecha_dt = pd.to_datetime(row['Date Submitted'])
            fecha_str = fecha_dt.strftime('%Y-%m-%d')
            usuario = row['Submitted By']
            sucursal_campo = str(row['Sucursal'])
            
            print(f"{i:<3} {idx:<6} {fecha_str:<12} {usuario:<15} {sucursal_campo:<25}")
            
            casos_normalizacion.append({
                'numero': i,
                'index_excel': idx,
                'fecha': fecha_dt,
                'fecha_str': fecha_str,
                'usuario': usuario,
                'sucursal_campo': sucursal_campo
            })
        
        return casos_normalizacion
    else:
        print("❌ No hay supervisiones sin location con campo Sucursal")
        return []

def mapear_campo_sucursal_a_locations():
    """Crear mapeo de campo Sucursal a locations estándar"""
    
    print(f"\n🗺️ MAPEO CAMPO SUCURSAL → LOCATIONS ESTÁNDAR")
    print("=" * 60)
    
    # Cargar catálogo master para mapeo
    df_sucursales = pd.read_csv('SUCURSALES_MASTER_20251218_110913.csv')
    
    # Crear diccionario de mapeo
    mapeo_sucursales = {}
    
    for _, row in df_sucursales.iterrows():
        if pd.notna(row['numero']) and pd.notna(row['nombre']):
            numero = int(row['numero'])
            nombre = row['nombre']
            location_key = f"{numero} - {nombre}"
            
            # Mapeos directos
            mapeo_sucursales[nombre.lower()] = location_key
            
            # Mapeos con variaciones comunes
            if 'santa catarina' in nombre.lower():
                mapeo_sucursales['sc'] = location_key
                mapeo_sucursales['santa catarina'] = location_key
            
            if 'la huasteca' in nombre.lower():
                mapeo_sucursales['lh'] = location_key
                mapeo_sucursales['la huasteca'] = location_key
                mapeo_sucursales['huasteca'] = location_key
            
            if 'garcia' in nombre.lower():
                mapeo_sucursales['gc'] = location_key
                mapeo_sucursales['garcia'] = location_key
            
            if 'gomez morin' in nombre.lower():
                mapeo_sucursales['gomez morin'] = location_key
                mapeo_sucursales['gómez morín'] = location_key
            
            if 'centrito valle' in nombre.lower():
                mapeo_sucursales['centrito valle'] = location_key
                mapeo_sucursales['centrito'] = location_key
    
    # Mapeos adicionales específicos detectados
    mapeos_adicionales = {
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
        'ochoa (saltillo)': '54 - Ramos Arizpe'  # Aproximación
    }
    
    mapeo_sucursales.update(mapeos_adicionales)
    
    print(f"✅ Creado mapeo con {len(mapeo_sucursales)} entradas")
    
    return mapeo_sucursales

def aplicar_normalizacion_campo_sucursal(casos_normalizacion, mapeo_sucursales):
    """Aplicar normalización usando campo Sucursal"""
    
    print(f"\n🔧 APLICANDO NORMALIZACIÓN CAMPO SUCURSAL")
    print("=" * 70)
    
    normalizaciones_exitosas = []
    normalizaciones_fallidas = []
    
    print(f"{'#':<3} {'Index':<6} {'Campo Sucursal':<20} {'→ Location Sugerido':<30} {'Conf'}")
    print("-" * 80)
    
    for caso in casos_normalizacion:
        sucursal_campo = caso['sucursal_campo'].lower().strip()
        
        # Buscar mapeo
        location_encontrado = None
        confianza = 'BAJA'
        
        # Coincidencia exacta
        if sucursal_campo in mapeo_sucursales:
            location_encontrado = mapeo_sucursales[sucursal_campo]
            confianza = 'ALTA'
        else:
            # Coincidencia parcial
            for key, location in mapeo_sucursales.items():
                if key in sucursal_campo or sucursal_campo in key:
                    if len(key) > 3:  # Evitar coincidencias muy cortas
                        location_encontrado = location
                        confianza = 'MEDIA'
                        break
        
        if location_encontrado:
            normalizaciones_exitosas.append({
                'index_excel': caso['index_excel'],
                'fecha': caso['fecha'],
                'usuario': caso['usuario'],
                'sucursal_campo_original': caso['sucursal_campo'],
                'location_sugerido': location_encontrado,
                'confianza': confianza,
                'metodo': 'CAMPO_SUCURSAL_NORMALIZADO'
            })
            
            caso_num = caso['numero']
            index = caso['index_excel']
            campo_short = caso['sucursal_campo'][:19]
            location_short = location_encontrado[:29]
            
            print(f"{caso_num:<3} {index:<6} {campo_short:<20} → {location_short:<30} {confianza}")
        else:
            normalizaciones_fallidas.append(caso)
    
    print(f"\n📊 RESULTADO NORMALIZACIÓN:")
    print(f"   ✅ Exitosas: {len(normalizaciones_exitosas)}")
    print(f"   ❌ Fallidas: {len(normalizaciones_fallidas)}")
    
    if normalizaciones_fallidas:
        print(f"\n❌ CASOS SIN NORMALIZAR:")
        for caso in normalizaciones_fallidas:
            print(f"   📋 Index {caso['index_excel']}: '{caso['sucursal_campo']}'")
    
    return normalizaciones_exitosas, normalizaciones_fallidas

def validar_normalizaciones_propuestas(normalizaciones_exitosas):
    """Validar las normalizaciones propuestas"""
    
    print(f"\n✅ VALIDACIÓN DE NORMALIZACIONES PROPUESTAS")
    print("=" * 70)
    
    # Agrupar por location sugerido
    por_location = {}
    for norm in normalizaciones_exitosas:
        location = norm['location_sugerido']
        if location not in por_location:
            por_location[location] = []
        por_location[location].append(norm)
    
    print(f"📊 DISTRIBUCIÓN DE NUEVAS ASIGNACIONES:")
    print(f"{'Location':<35} {'Nuevas':<7} {'Confianza':<10} {'Usuarios'}")
    print("-" * 70)
    
    for location in sorted(por_location.keys()):
        asignaciones = por_location[location]
        count = len(asignaciones)
        
        # Confianza promedio
        confianzas = [a['confianza'] for a in asignaciones]
        if 'ALTA' in confianzas:
            confianza_promedio = 'ALTA'
        elif 'MEDIA' in confianzas:
            confianza_promedio = 'MEDIA'
        else:
            confianza_promedio = 'BAJA'
        
        # Usuarios únicos
        usuarios = list(set([a['usuario'] for a in asignaciones]))
        usuarios_str = ','.join([u.split()[0] for u in usuarios])[:15]
        
        location_short = location[:34]
        
        print(f"{location_short:<35} +{count:<6} {confianza_promedio:<10} {usuarios_str}")
    
    # Detectar posibles problemas
    print(f"\n⚠️ VALIDACIONES ADICIONALES:")
    
    # Sucursales que recibirán muchas asignaciones
    exceso_asignaciones = [loc for loc, asigs in por_location.items() if len(asigs) > 5]
    if exceso_asignaciones:
        print(f"   🔄 Sucursales con muchas nuevas asignaciones:")
        for loc in exceso_asignaciones:
            print(f"      📊 {loc}: +{len(por_location[loc])}")
    
    # Fechas muy concentradas
    fechas_todas = [norm['fecha'].date() for norm in normalizaciones_exitosas]
    from collections import Counter
    fechas_count = Counter(fechas_todas)
    fechas_concentradas = [(fecha, count) for fecha, count in fechas_count.items() if count > 3]
    
    if fechas_concentradas:
        print(f"   📅 Fechas con muchas asignaciones:")
        for fecha, count in fechas_concentradas:
            print(f"      📅 {fecha}: {count} asignaciones")
    
    return por_location

def generar_archivo_normalizaciones(normalizaciones_exitosas):
    """Generar archivo con normalizaciones para aplicar"""
    
    print(f"\n📁 GENERANDO ARCHIVO DE NORMALIZACIONES")
    print("=" * 50)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Crear DataFrame
    df_normalizaciones = pd.DataFrame(normalizaciones_exitosas)
    
    # Guardar CSV
    filename = f"NORMALIZACIONES_CAMPO_SUCURSAL_{timestamp}.csv"
    df_normalizaciones.to_csv(filename, index=False, encoding='utf-8')
    
    print(f"✅ Archivo guardado: {filename}")
    print(f"📊 {len(normalizaciones_exitosas)} normalizaciones listas para aplicar")
    
    return filename

def main():
    """Función principal"""
    
    print("🔧 CAMPO SUCURSAL PARA NORMALIZACIÓN COMPLETA")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Objetivo: Normalizar supervisiones usando campo Sucursal manual")
    print("=" * 80)
    
    # 1. Analizar casos sin location con campo Sucursal
    casos_normalizacion = analizar_campo_sucursal_sin_location()
    
    if not casos_normalizacion:
        print("❌ No hay casos para normalizar")
        return
    
    # 2. Crear mapeo de campo Sucursal a locations
    mapeo_sucursales = mapear_campo_sucursal_a_locations()
    
    # 3. Aplicar normalización
    normalizaciones_exitosas, normalizaciones_fallidas = aplicar_normalizacion_campo_sucursal(
        casos_normalizacion, mapeo_sucursales
    )
    
    # 4. Validar normalizaciones propuestas
    if normalizaciones_exitosas:
        por_location = validar_normalizaciones_propuestas(normalizaciones_exitosas)
        
        # 5. Generar archivo de normalizaciones
        filename = generar_archivo_normalizaciones(normalizaciones_exitosas)
        
        # Resumen final
        print(f"\n🎯 RESUMEN FINAL:")
        print(f"   📊 Total casos analizados: {len(casos_normalizacion)}")
        print(f"   ✅ Normalizaciones exitosas: {len(normalizaciones_exitosas)}")
        print(f"   ❌ Casos sin normalizar: {len(normalizaciones_fallidas)}")
        print(f"   📈 Tasa de éxito: {(len(normalizaciones_exitosas)/len(casos_normalizacion))*100:.1f}%")
        print(f"   🏪 Sucursales que recibirán asignaciones: {len(por_location)}")
        
        print(f"\n💡 PRÓXIMO PASO:")
        print(f"   1. Revisar normalizaciones propuestas")
        print(f"   2. Confirmar las que están correctas")
        print(f"   3. Aplicar correcciones masivas")
        
        return normalizaciones_exitosas, filename
    else:
        print(f"\n❌ No se pudieron generar normalizaciones exitosas")
        return [], None

if __name__ == "__main__":
    main()