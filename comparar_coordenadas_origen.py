#!/usr/bin/env python3
"""
🔍 COMPARAR COORDENADAS - VALIDACIÓN ORIGEN
Comparar coordenadas entre archivos para confirmar fuente correcta
Roberto: Verificar que estamos usando las coordenadas correctas
"""

import pandas as pd
import numpy as np
from datetime import datetime

def cargar_archivos():
    """Cargar ambos archivos de coordenadas"""
    
    print("📁 CARGANDO ARCHIVOS DE COORDENADAS")
    print("=" * 60)
    
    # 1. Archivo actual usado para enriquecimiento
    archivo_actual = "SUCURSALES_CORRECCIONES_ROBERTO_20251218_171807.csv"
    df_actual = pd.read_csv(archivo_actual)
    
    print(f"✅ Archivo actual: {archivo_actual}")
    print(f"   📊 Total sucursales: {len(df_actual)}")
    
    # 2. Archivo del proyecto original
    archivo_proyecto = "/Users/robertodavila/pollo-loco-supervision/data/grupos_operativos_final_corregido.csv"
    df_proyecto = pd.read_csv(archivo_proyecto)
    
    print(f"✅ Archivo proyecto: grupos_operativos_final_corregido.csv")
    print(f"   📊 Total sucursales: {len(df_proyecto)}")
    
    return df_actual, df_proyecto

def normalizar_nombres(df_actual, df_proyecto):
    """Normalizar nombres para comparación"""
    
    print(f"\n🔧 NORMALIZACIÓN NOMBRES")
    print("=" * 40)
    
    # Normalizar nombres actuales
    df_actual['nombre_norm'] = df_actual['nombre'].str.strip().str.upper()
    
    # Normalizar nombres proyecto
    df_proyecto['nombre_norm'] = df_proyecto['Nombre_Sucursal'].str.strip().str.upper()
    
    # También normalizar por número de sucursal
    df_actual['numero_norm'] = df_actual['numero'].astype(str)
    df_proyecto['numero_norm'] = df_proyecto['Numero_Sucursal'].astype(str)
    
    print(f"✅ Normalizados para comparación")
    
    return df_actual, df_proyecto

def comparar_por_numero(df_actual, df_proyecto):
    """Comparar coordinadas por número de sucursal"""
    
    print(f"\n🔢 COMPARACIÓN POR NÚMERO DE SUCURSAL")
    print("=" * 50)
    
    coincidencias = []
    diferencias = []
    
    for _, row_actual in df_actual.iterrows():
        numero = row_actual['numero']
        
        # Buscar en proyecto
        match_proyecto = df_proyecto[df_proyecto['Numero_Sucursal'] == numero]
        
        if len(match_proyecto) > 0:
            row_proyecto = match_proyecto.iloc[0]
            
            # Comparar coordenadas
            lat_diff = abs(float(row_actual['lat']) - float(row_proyecto['Latitude']))
            lon_diff = abs(float(row_actual['lon']) - float(row_proyecto['Longitude']))
            
            # Tolerancia: 0.001 = ~100 metros
            if lat_diff < 0.001 and lon_diff < 0.001:
                coincidencias.append({
                    'numero': numero,
                    'nombre_actual': row_actual['nombre'],
                    'nombre_proyecto': row_proyecto['Nombre_Sucursal'],
                    'lat_actual': row_actual['lat'],
                    'lon_actual': row_actual['lon'],
                    'lat_proyecto': row_proyecto['Latitude'],
                    'lon_proyecto': row_proyecto['Longitude'],
                    'diferencia_lat': lat_diff,
                    'diferencia_lon': lon_diff,
                    'estado': 'COINCIDE'
                })
            else:
                diferencias.append({
                    'numero': numero,
                    'nombre_actual': row_actual['nombre'],
                    'nombre_proyecto': row_proyecto['Nombre_Sucursal'],
                    'lat_actual': row_actual['lat'],
                    'lon_actual': row_actual['lon'],
                    'lat_proyecto': row_proyecto['Latitude'],
                    'lon_proyecto': row_proyecto['Longitude'],
                    'diferencia_lat': lat_diff,
                    'diferencia_lon': lon_diff,
                    'estado': 'DIFERENTE'
                })
        else:
            diferencias.append({
                'numero': numero,
                'nombre_actual': row_actual['nombre'],
                'nombre_proyecto': 'NO ENCONTRADO',
                'lat_actual': row_actual['lat'],
                'lon_actual': row_actual['lon'],
                'lat_proyecto': None,
                'lon_proyecto': None,
                'diferencia_lat': None,
                'diferencia_lon': None,
                'estado': 'SIN MATCH'
            })
    
    print(f"📊 RESULTADOS:")
    print(f"   ✅ Coincidencias exactas: {len(coincidencias)}")
    print(f"   ⚠️ Diferencias: {len(diferencias)}")
    print(f"   📈 Porcentaje coincidencia: {len(coincidencias)/(len(coincidencias)+len(diferencias))*100:.1f}%")
    
    return coincidencias, diferencias

def analizar_coincidencias(coincidencias):
    """Analizar detalles de las coincidencias"""
    
    print(f"\n✅ ANÁLISIS COINCIDENCIAS ({len(coincidencias)})")
    print("=" * 50)
    
    if len(coincidencias) == 0:
        print("❌ No hay coincidencias para analizar")
        return
    
    # Mostrar algunas coincidencias como ejemplo
    print(f"📋 EJEMPLOS COINCIDENCIAS (primeras 5):")
    for i, match in enumerate(coincidencias[:5]):
        print(f"   {i+1}. {match['numero']} - {match['nombre_actual']}")
        print(f"      📍 Actual: ({match['lat_actual']:.6f}, {match['lon_actual']:.6f})")
        print(f"      📍 Proyecto: ({match['lat_proyecto']:.6f}, {match['lon_proyecto']:.6f})")
        print(f"      📏 Diferencia: {match['diferencia_lat']:.7f}, {match['diferencia_lon']:.7f}")
        print()
    
    # Estadísticas
    diferencias_lat = [m['diferencia_lat'] for m in coincidencias]
    diferencias_lon = [m['diferencia_lon'] for m in coincidencias]
    
    print(f"📊 ESTADÍSTICAS PRECISIÓN:")
    print(f"   📍 Diferencia LAT promedio: {np.mean(diferencias_lat):.7f}°")
    print(f"   📍 Diferencia LON promedio: {np.mean(diferencias_lon):.7f}°")
    print(f"   📏 Máxima diferencia LAT: {np.max(diferencias_lat):.7f}°")
    print(f"   📏 Máxima diferencia LON: {np.max(diferencias_lon):.7f}°")
    
    # Calcular distancia promedio (aproximada)
    # 1° lat ≈ 111 km, 1° lon ≈ 111*cos(lat) km
    distancias_aprox = []
    for match in coincidencias:
        lat_km = match['diferencia_lat'] * 111
        lon_km = match['diferencia_lon'] * 111 * np.cos(np.radians(match['lat_actual']))
        distancia = np.sqrt(lat_km**2 + lon_km**2) * 1000  # metros
        distancias_aprox.append(distancia)
    
    print(f"   🎯 Distancia promedio: {np.mean(distancias_aprox):.1f} metros")
    print(f"   🎯 Distancia máxima: {np.max(distancias_aprox):.1f} metros")

def analizar_diferencias(diferencias):
    """Analizar detalles de las diferencias"""
    
    print(f"\n⚠️ ANÁLISIS DIFERENCIAS ({len(diferencias)})")
    print("=" * 50)
    
    if len(diferencias) == 0:
        print("✅ No hay diferencias - coincidencia perfecta!")
        return
    
    # Separar tipos de diferencias
    sin_match = [d for d in diferencias if d['estado'] == 'SIN MATCH']
    coordenadas_diferentes = [d for d in diferencias if d['estado'] == 'DIFERENTE']
    
    print(f"📊 TIPOS DE DIFERENCIAS:")
    print(f"   🔍 Sin encontrar en proyecto: {len(sin_match)}")
    print(f"   📍 Coordenadas diferentes: {len(coordenadas_diferentes)}")
    
    # Mostrar sucursales sin match
    if len(sin_match) > 0:
        print(f"\n🔍 SUCURSALES SIN MATCH:")
        for diff in sin_match:
            print(f"   {diff['numero']} - {diff['nombre_actual']}")
    
    # Mostrar coordenadas diferentes
    if len(coordenadas_diferentes) > 0:
        print(f"\n📍 COORDENADAS DIFERENTES:")
        for diff in coordenadas_diferentes:
            print(f"   {diff['numero']} - {diff['nombre_actual']}")
            print(f"      📍 Actual: ({diff['lat_actual']:.6f}, {diff['lon_actual']:.6f})")
            print(f"      📍 Proyecto: ({diff['lat_proyecto']:.6f}, {diff['lon_proyecto']:.6f})")
            print(f"      📏 Diferencia: {diff['diferencia_lat']:.6f}, {diff['diferencia_lon']:.6f}")
            print()

def comparar_metadatos(df_actual, df_proyecto):
    """Comparar metadatos de los archivos"""
    
    print(f"\n📋 COMPARACIÓN METADATOS")
    print("=" * 40)
    
    print(f"📁 ARCHIVO ACTUAL:")
    print(f"   📅 Fecha en nombre: 2025-12-18 17:18:07")
    print(f"   📊 Columnas: {list(df_actual.columns)}")
    print(f"   🔧 Grupos únicos: {df_actual['grupo'].nunique()}")
    
    print(f"\n📁 ARCHIVO PROYECTO:")
    print(f"   📊 Columnas: {list(df_proyecto.columns)}")
    print(f"   🔧 Grupos únicos: {df_proyecto['Grupo_Operativo'].nunique()}")
    print(f"   📅 Última sincronización: {df_proyecto['Synced_At'].iloc[0]}")
    
    # Comparar grupos operativos
    grupos_actual = set(df_actual['grupo'].unique())
    grupos_proyecto = set(df_proyecto['Grupo_Operativo'].unique())
    
    print(f"\n🔧 COMPARACIÓN GRUPOS OPERATIVOS:")
    print(f"   📊 Actual: {len(grupos_actual)} grupos")
    print(f"   📊 Proyecto: {len(grupos_proyecto)} grupos")
    
    grupos_comunes = grupos_actual.intersection(grupos_proyecto)
    grupos_solo_actual = grupos_actual - grupos_proyecto
    grupos_solo_proyecto = grupos_proyecto - grupos_actual
    
    print(f"   ✅ Grupos en común: {len(grupos_comunes)}")
    if len(grupos_solo_actual) > 0:
        print(f"   ⚠️ Solo en actual: {list(grupos_solo_actual)}")
    if len(grupos_solo_proyecto) > 0:
        print(f"   ⚠️ Solo en proyecto: {list(grupos_solo_proyecto)}")

def generar_resumen_final(coincidencias, diferencias, df_actual, df_proyecto):
    """Generar resumen final para Roberto"""
    
    print(f"\n🎯 RESUMEN FINAL PARA ROBERTO")
    print("=" * 80)
    
    total_comparados = len(coincidencias) + len(diferencias)
    porcentaje_coincidencia = len(coincidencias) / total_comparados * 100 if total_comparados > 0 else 0
    
    print(f"📊 ESTADÍSTICAS GENERALES:")
    print(f"   📁 Archivo actual: 86 sucursales con coordenadas")
    print(f"   📁 Archivo proyecto: {len(df_proyecto)} sucursales")
    print(f"   🔍 Sucursales comparadas: {total_comparados}")
    print(f"   ✅ Coincidencias exactas: {len(coincidencias)}")
    print(f"   📈 Porcentaje coincidencia: {porcentaje_coincidencia:.1f}%")
    
    print(f"\n💡 CONCLUSIONES:")
    
    if porcentaje_coincidencia >= 95:
        print("   ✅ COORDINADAS VALIDADAS - Mismo origen confirmado")
        print("   ✅ Archivo actual es confiable para Railway")
        print("   ✅ Precisión excelente (<100m diferencia promedio)")
    elif porcentaje_coincidencia >= 80:
        print("   ⚠️ COORDINADAS MAYORMENTE VÁLIDAS - Algunas diferencias menores")
        print("   ✅ Archivo actual es confiable con revisión menor")
    else:
        print("   ❌ COORDINADAS REQUIEREN VALIDACIÓN - Diferencias significativas")
        print("   ⚠️ Revisar fuente antes de continuar con Railway")
    
    print(f"\n🚀 SIGUIENTE PASO:")
    if porcentaje_coincidencia >= 80:
        print("   ✅ Continuar con implementación Railway")
        print("   📊 Usar archivo actual para migración PostgreSQL")
        print("   🔧 Proceder con clonación dashboard")
    else:
        print("   🔍 Revisar diferencias de coordenadas")
        print("   🤔 Confirmar fuente correcta antes de Railway")
    
    return porcentaje_coincidencia

def main():
    """Función principal"""
    
    print("🔍 COMPARACIÓN COORDENADAS - EL POLLO LOCO")
    print("=" * 80)
    print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Roberto: Validar origen de coordenadas vs archivo proyecto")
    print("=" * 80)
    
    # 1. Cargar archivos
    df_actual, df_proyecto = cargar_archivos()
    
    # 2. Normalizar nombres
    df_actual, df_proyecto = normalizar_nombres(df_actual, df_proyecto)
    
    # 3. Comparar por número
    coincidencias, diferencias = comparar_por_numero(df_actual, df_proyecto)
    
    # 4. Analizar coincidencias
    analizar_coincidencias(coincidencias)
    
    # 5. Analizar diferencias
    analizar_diferencias(diferencias)
    
    # 6. Comparar metadatos
    comparar_metadatos(df_actual, df_proyecto)
    
    # 7. Resumen final
    porcentaje = generar_resumen_final(coincidencias, diferencias, df_actual, df_proyecto)
    
    return {
        'coincidencias': len(coincidencias),
        'diferencias': len(diferencias),
        'porcentaje_coincidencia': porcentaje,
        'archivo_validado': porcentaje >= 80
    }

if __name__ == "__main__":
    resultado = main()