#!/usr/bin/env python3
"""
🔍 VERIFICAR INCONSISTENCIAS EN EXCEL ORIGINAL DE SEGURIDAD
Verificar las 5 inconsistencias detectadas y mostrar el estado actual
"""

import pandas as pd
from datetime import datetime

def verificar_inconsistencias_en_excel():
    """Verificar las 5 inconsistencias detectadas en el Excel original"""
    
    print("🔍 VERIFICACIÓN INCONSISTENCIAS EN EXCEL ORIGINAL")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Verificar 5 inconsistencias: Location asignado ≠ Campo Sucursal")
    print("=" * 80)
    
    # Cargar Excel de seguridad
    df_seg = pd.read_excel("CONTROL_OPERATIVO_DE_SEGURIDAD_CAS_11_REV_25012025_Submissions-2025-12-18__1232CST-1766104009.xlsx")
    
    print(f"📊 Excel seguridad cargado: {len(df_seg)} submissions")
    
    # Índices específicos detectados
    indices_inconsistencias = [4, 69, 89, 128, 134]
    
    print(f"\n🔍 VERIFICANDO INCONSISTENCIAS DETECTADAS:")
    print(f"{'Index':<6} {'Fecha':<12} {'Usuario':<15} {'Location':<25} {'Campo Sucursal':<20} {'Problema'}")
    print("-" * 100)
    
    inconsistencias_confirmadas = []
    
    for index_excel in indices_inconsistencias:
        if index_excel < len(df_seg):
            row = df_seg.iloc[index_excel]
            
            fecha_dt = pd.to_datetime(row['Date Submitted'])
            fecha_str = fecha_dt.strftime('%Y-%m-%d')
            usuario = row['Submitted By']
            location_actual = str(row.get('Location', 'N/A'))
            sucursal_campo = str(row.get('Sucursal', 'N/A'))
            
            # Determinar problema
            problema = ""
            if 'Venustiano Carranza' in location_actual and 'ochoa' in sucursal_campo.lower():
                problema = "Debería ser Ramos Arizpe"
            elif 'Anahuac' in location_actual and 'universidad' in sucursal_campo.lower():
                problema = "Debería ser Universidad"
            else:
                problema = "Otro tipo"
            
            print(f"{index_excel:<6} {fecha_str:<12} {usuario:<15} {location_actual[:24]:<25} {sucursal_campo[:19]:<20} {problema}")
            
            inconsistencias_confirmadas.append({
                'index': index_excel,
                'fecha': fecha_dt,
                'usuario': usuario,
                'location_actual': location_actual,
                'sucursal_campo': sucursal_campo,
                'problema': problema
            })
        else:
            print(f"{index_excel:<6} ❌ ÍNDICE FUERA DE RANGO")
    
    print(f"\n📊 INCONSISTENCIAS CONFIRMADAS: {len(inconsistencias_confirmadas)}")
    
    return inconsistencias_confirmadas, df_seg

def mostrar_estadisticas_actuales(df_seg):
    """Mostrar estadísticas actuales de distribución"""
    
    print(f"\n📊 ESTADÍSTICAS ACTUALES DEL EXCEL ORIGINAL")
    print("=" * 60)
    
    # Supervisiones con Location asignado
    con_location = df_seg[df_seg['Location'].notna()]
    sin_location = df_seg[df_seg['Location'].isna()]
    
    print(f"📊 ESTADO GENERAL:")
    print(f"   ✅ Con Location asignado: {len(con_location)}")
    print(f"   ❌ Sin Location asignado: {len(sin_location)}")
    
    # Top sucursales asignadas
    if len(con_location) > 0:
        top_sucursales = con_location['Location'].value_counts().head(10)
        
        print(f"\n📊 TOP 10 SUCURSALES CON LOCATION ASIGNADO:")
        print(f"{'Sucursal':<35} {'Count'}")
        print("-" * 45)
        
        for sucursal, count in top_sucursales.items():
            print(f"{str(sucursal)[:34]:<35} {count}")
        
        # Verificar las sucursales específicas afectadas
        sucursales_afectadas = [
            '52 - Venustiano Carranza',
            '54 - Ramos Arizpe', 
            '9 - Anahuac',
            '58 - Universidad (Tampico)'
        ]
        
        print(f"\n🔍 SUCURSALES AFECTADAS POR CORRECCIONES:")
        print(f"{'Sucursal':<35} {'Count Actual':<12} {'Cambio Propuesto'}")
        print("-" * 70)
        
        for sucursal in sucursales_afectadas:
            count_actual = len(con_location[con_location['Location'] == sucursal])
            
            if 'Venustiano Carranza' in sucursal:
                cambio = "-3 → Ramos Arizpe"
            elif 'Ramos Arizpe' in sucursal:
                cambio = "+3 ← Venustiano Carranza"
            elif 'Anahuac' in sucursal:
                cambio = "-2 → Universidad"
            elif 'Universidad (Tampico)' in sucursal:
                cambio = "+2 ← Anahuac"
            else:
                cambio = ""
            
            print(f"{sucursal[:34]:<35} {count_actual:<12} {cambio}")

def generar_plan_correccion_excel(inconsistencias_confirmadas):
    """Generar plan para corrección directa en Excel"""
    
    print(f"\n📋 PLAN DE CORRECCIÓN PARA EXCEL ORIGINAL")
    print("=" * 60)
    
    if not inconsistencias_confirmadas:
        print("❌ No hay inconsistencias confirmadas para corregir")
        return
    
    # Agrupar correcciones
    correcciones = {
        'venustiano_a_ramos': [],
        'anahuac_a_universidad': []
    }
    
    for inc in inconsistencias_confirmadas:
        if 'Venustiano Carranza' in inc['location_actual']:
            correcciones['venustiano_a_ramos'].append(inc)
        elif 'Anahuac' in inc['location_actual']:
            correcciones['anahuac_a_universidad'].append(inc)
    
    print(f"🔧 CORRECCIONES AGRUPADAS:")
    
    print(f"\n1. VENUSTIANO CARRANZA → RAMOS ARIZPE ({len(correcciones['venustiano_a_ramos'])} casos):")
    for inc in correcciones['venustiano_a_ramos']:
        print(f"   📍 Index {inc['index']}: {inc['fecha'].strftime('%Y-%m-%d')} - {inc['usuario']}")
        print(f"      Campo dice: '{inc['sucursal_campo']}'")
    
    print(f"\n2. ANAHUAC → UNIVERSIDAD (TAMPICO) ({len(correcciones['anahuac_a_universidad'])} casos):")
    for inc in correcciones['anahuac_a_universidad']:
        print(f"   📍 Index {inc['index']}: {inc['fecha'].strftime('%Y-%m-%d')} - {inc['usuario']}")
        print(f"      Campo dice: '{inc['sucursal_campo']}'")
    
    print(f"\n💡 PRÓXIMOS PASOS:")
    print(f"   1. Confirmar que estas correcciones son correctas")
    print(f"   2. Aplicar cambios directamente en campo Location del Excel")
    print(f"   3. Recalcular distribuciones finales")
    print(f"   4. Verificar cumplimiento reglas 4+4 / 2+2")
    
    return correcciones

def main():
    """Función principal"""
    
    print("🔍 VERIFICAR INCONSISTENCIAS EN EXCEL ORIGINAL DE SEGURIDAD")
    print("=" * 80)
    
    # 1. Verificar inconsistencias en Excel original
    inconsistencias, df_seg = verificar_inconsistencias_en_excel()
    
    # 2. Mostrar estadísticas actuales
    mostrar_estadisticas_actuales(df_seg)
    
    # 3. Generar plan de corrección
    correcciones = generar_plan_correccion_excel(inconsistencias)
    
    # 4. Resumen final
    print(f"\n🎯 RESUMEN:")
    print(f"   📊 Excel original: {len(df_seg)} supervisiones")
    print(f"   🚨 Inconsistencias confirmadas: {len(inconsistencias)}")
    print(f"   🔧 Estas son supervisiones CON location pero campo Sucursal indica error")
    print(f"   ✅ Las correcciones son basadas en campo manual autoritativo")
    
    return inconsistencias, correcciones

if __name__ == "__main__":
    main()