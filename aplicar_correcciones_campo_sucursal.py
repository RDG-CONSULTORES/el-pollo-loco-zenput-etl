#!/usr/bin/env python3
"""
🔧 APLICAR CORRECCIONES DETECTADAS EN CAMPO SUCURSAL
Aplicar las 5 correcciones identificadas donde campo Sucursal contradice Location asignado
"""

import pandas as pd
from datetime import datetime

def aplicar_correcciones_sucursal():
    """Aplicar las 5 correcciones detectadas por el análisis del campo Sucursal"""
    
    print("🔧 APLICANDO CORRECCIONES CAMPO SUCURSAL")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Objetivo: Corregir 5 inconsistencias detectadas en campo Sucursal")
    print("=" * 80)
    
    # Cargar datos actuales
    df_asignaciones = pd.read_csv("ASIGNACIONES_FINALES_CORREGIDAS_20251218_140924.csv")
    print(f"📊 Cargadas {len(df_asignaciones)} asignaciones actuales")
    
    # Correcciones identificadas del análisis
    correcciones_sucursal = [
        # Venustiano Carranza → Ramos Arizpe (Jorge Reynosa dice "Ochoa (Saltillo)")
        {'index_excel': 4, 'sucursal_actual': '52 - Venustiano Carranza', 'sucursal_nueva': '54 - Ramos Arizpe', 'razon': 'Campo Sucursal: Ochoa (Saltillo)', 'usuario': 'Jorge Reynosa'},
        {'index_excel': 69, 'sucursal_actual': '52 - Venustiano Carranza', 'sucursal_nueva': '54 - Ramos Arizpe', 'razon': 'Campo Sucursal: Ochoa (Saltillo)', 'usuario': 'Jorge Reynosa'},
        {'index_excel': 128, 'sucursal_actual': '52 - Venustiano Carranza', 'sucursal_nueva': '54 - Ramos Arizpe', 'razon': 'Campo Sucursal: Ochoa (Saltillo)', 'usuario': 'Jorge Reynosa'},
        
        # Anahuac → Universidad (Tampico) (Jorge Reynosa dice "Universidad")
        {'index_excel': 89, 'sucursal_actual': '9 - Anahuac', 'sucursal_nueva': '58 - Universidad (Tampico)', 'razon': 'Campo Sucursal: Universidad', 'usuario': 'Jorge Reynosa'},
        {'index_excel': 134, 'sucursal_actual': '9 - Anahuac', 'sucursal_nueva': '58 - Universidad (Tampico)', 'razon': 'Campo Sucursal: Universidad', 'usuario': 'Jorge Reynosa'}
    ]
    
    print(f"\n🔧 APLICANDO {len(correcciones_sucursal)} CORRECCIONES:")
    print(f"{'#':<3} {'Index':<6} {'Actual':<25} → {'Nueva':<25} {'Razón'}")
    print("-" * 95)
    
    correcciones_aplicadas = 0
    
    for i, corr in enumerate(correcciones_sucursal, 1):
        index_excel = corr['index_excel']
        sucursal_actual = corr['sucursal_actual']
        sucursal_nueva = corr['sucursal_nueva']
        razon = corr['razon']
        usuario = corr['usuario']
        
        # Buscar la fila correspondiente
        mask = df_asignaciones['index_original'] == index_excel
        fila_encontrada = df_asignaciones[mask]
        
        if len(fila_encontrada) > 0:
            # Verificar que la sucursal actual coincida
            sucursal_en_datos = fila_encontrada.iloc[0]['sucursal_asignada']
            
            if sucursal_en_datos == sucursal_actual:
                # Aplicar corrección
                df_asignaciones.loc[mask, 'sucursal_asignada'] = sucursal_nueva
                df_asignaciones.loc[mask, 'metodo'] = f"CAMPO_SUCURSAL_CORREGIDO"
                df_asignaciones.loc[mask, 'confianza'] = 1.0  # Máxima confianza por campo manual
                
                correcciones_aplicadas += 1
                
                print(f"{i:<3} {index_excel:<6} {sucursal_actual[:24]:<25} → {sucursal_nueva[:24]:<25} {razon}")
            else:
                print(f"{i:<3} {index_excel:<6} ⚠️ CONFLICTO: Esperado {sucursal_actual}, encontrado {sucursal_en_datos}")
        else:
            print(f"{i:<3} {index_excel:<6} ❌ NO ENCONTRADO en asignaciones")
    
    print(f"\n📊 RESULTADO:")
    print(f"   ✅ Correcciones aplicadas: {correcciones_aplicadas}/{len(correcciones_sucursal)}")
    
    return df_asignaciones, correcciones_aplicadas

def generar_reporte_impacto(df_asignaciones):
    """Generar reporte del impacto de las correcciones"""
    
    print(f"\n📊 REPORTE DE IMPACTO DE CORRECCIONES")
    print("=" * 60)
    
    # Contar por sucursal
    distribucion_actual = df_asignaciones['sucursal_asignada'].value_counts().sort_index()
    
    # Mostrar cambios específicos
    sucursales_afectadas = ['52 - Venustiano Carranza', '54 - Ramos Arizpe', '9 - Anahuac', '58 - Universidad (Tampico)']
    
    print(f"📊 DISTRIBUCIÓN ACTUALIZADA EN SUCURSALES AFECTADAS:")
    print(f"{'Sucursal':<35} {'Count':<8} {'Observación'}")
    print("-" * 70)
    
    for sucursal in sucursales_afectadas:
        count = distribucion_actual.get(sucursal, 0)
        
        if 'Venustiano Carranza' in sucursal:
            obs = f"📉 Reducida (-3)"
        elif 'Ramos Arizpe' in sucursal:
            obs = f"📈 Aumentada (+3)"
        elif 'Anahuac' in sucursal:
            obs = f"📉 Reducida (-2)"
        elif 'Universidad (Tampico)' in sucursal:
            obs = f"📈 Aumentada (+2)"
        else:
            obs = ""
        
        print(f"{sucursal[:34]:<35} {count:<8} {obs}")
    
    # Verificar distribuciones por regla 4+4 / 2+2
    print(f"\n🎯 VERIFICACIÓN REGLAS 4+4 / 2+2:")
    
    # Cargar catálogo para verificar tipos
    df_sucursales = pd.read_csv('SUCURSALES_MASTER_20251218_110913.csv')
    tipo_map = {}
    for _, row in df_sucursales.iterrows():
        if pd.notna(row['numero']) and pd.notna(row['nombre']):
            numero = int(row['numero'])
            nombre = row['nombre']
            location_key = f"{numero} - {nombre}"
            tipo_map[location_key] = row.get('tipo', 'DESCONOCIDO')
    
    for sucursal in sucursales_afectadas:
        count = distribucion_actual.get(sucursal, 0)
        tipo = tipo_map.get(sucursal, 'DESCONOCIDO')
        
        if tipo == 'LOCAL':
            esperado = 8  # 4+4
            status = "✅ CORRECTO" if count == esperado else f"⚠️ INCORRECTO (esperado {esperado})"
        elif tipo == 'FORANEA':
            esperado = 4  # 2+2
            status = "✅ CORRECTO" if count == esperado else f"⚠️ INCORRECTO (esperado {esperado})"
        else:
            status = f"❓ TIPO {tipo}"
        
        print(f"   {sucursal[:34]:<35}: {count} supervisiones - {status}")

def guardar_datos_corregidos(df_asignaciones):
    """Guardar datos con correcciones aplicadas"""
    
    print(f"\n💾 GUARDANDO DATOS CORREGIDOS")
    print("=" * 40)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Guardar CSV principal
    filename_csv = f"ASIGNACIONES_FINALES_CORREGIDAS_{timestamp}.csv"
    df_asignaciones.to_csv(filename_csv, index=False, encoding='utf-8')
    print(f"✅ CSV guardado: {filename_csv}")
    
    # Estadísticas finales
    total_asignaciones = len(df_asignaciones)
    metodos_count = df_asignaciones['metodo'].value_counts()
    
    print(f"\n📊 ESTADÍSTICAS FINALES:")
    print(f"   📊 Total asignaciones: {total_asignaciones}")
    print(f"   🔧 Métodos utilizados:")
    for metodo, count in metodos_count.items():
        print(f"      {metodo}: {count}")
    
    return filename_csv

def main():
    """Función principal"""
    
    print("🔧 APLICAR CORRECCIONES DETECTADAS EN CAMPO SUCURSAL")
    print("=" * 80)
    print("🎯 Aplicar 5 correcciones donde campo manual Sucursal contradice Location")
    print("=" * 80)
    
    # 1. Aplicar correcciones
    df_corregido, correcciones_aplicadas = aplicar_correcciones_sucursal()
    
    if correcciones_aplicadas > 0:
        # 2. Generar reporte de impacto
        generar_reporte_impacto(df_corregido)
        
        # 3. Guardar datos corregidos
        filename = guardar_datos_corregidos(df_corregido)
        
        print(f"\n🎯 RESUMEN FINAL:")
        print(f"   ✅ Correcciones aplicadas: {correcciones_aplicadas}")
        print(f"   📁 Archivo actualizado: {filename}")
        print(f"   🎯 Campo Sucursal manual usado como fuente autoritativa")
        
        print(f"\n💡 IMPACTO:")
        print(f"   📉 Venustiano Carranza: -3 supervisiones")
        print(f"   📈 Ramos Arizpe: +3 supervisiones")
        print(f"   📉 Anahuac: -2 supervisiones") 
        print(f"   📈 Universidad (Tampico): +2 supervisiones")
        
        return df_corregido, filename
    else:
        print(f"❌ No se aplicaron correcciones")
        return None, None

if __name__ == "__main__":
    main()