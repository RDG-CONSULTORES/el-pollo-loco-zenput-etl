#!/usr/bin/env python3
"""
🔧 IMPLEMENTAR CORRECCIONES DE ROBERTO
Aplicar todas las clasificaciones validadas grupo por grupo
"""

import pandas as pd
from datetime import datetime

def aplicar_correcciones_completas():
    """Aplicar todas las correcciones validadas por Roberto"""
    
    print("🔧 IMPLEMENTAR CORRECCIONES VALIDADAS POR ROBERTO")
    print("=" * 70)
    
    # Cargar catálogo
    df_sucursales = pd.read_csv('SUCURSALES_MASTER_20251218_110913.csv')
    df_corregido = df_sucursales.copy()
    
    # Sucursales nuevas a excluir del proceso
    sucursales_nuevas = [
        '35 - Apodaca',
        '82 - Aeropuerto Nuevo Laredo', 
        '83 - Cerradas de Anahuac',
        '84 - Aeropuerto del Norte',
        '85 - Diego Diaz',
        '86 - Miguel de la Madrid'
    ]
    
    # CLASIFICACIONES VALIDADAS POR ROBERTO
    clasificaciones_roberto = {
        # ESPECIALES (3+3=6) - Ya confirmados
        '1 - Pino Suarez': 'ESPECIAL',
        '2 - Madero': 'ESPECIAL', 
        '3 - Matamoros': 'ESPECIAL',
        '5 - Felix U. Gomez': 'ESPECIAL',
        
        # LOCALES (4+4=8)
        '4 - Santa Catarina': 'LOCAL',
        '6 - Garcia': 'LOCAL',
        '7 - La Huasteca': 'LOCAL',
        '8 - Gonzalitos': 'LOCAL',
        '9 - Anahuac': 'LOCAL',
        '10 - Barragan': 'LOCAL',
        '11 - Lincoln': 'LOCAL',
        '12 - Concordia': 'LOCAL',
        '13 - Escobedo': 'LOCAL',
        '14 - Aztlan': 'LOCAL',
        '15 - Ruiz Cortinez': 'LOCAL',
        '16 - Solidaridad': 'LOCAL',  # Era FORANEA
        '17 - Romulo Garza': 'LOCAL',  # Era FORANEA
        '18 - Linda Vista': 'LOCAL',  # Era FORANEA
        '19 - Valle Soleado': 'LOCAL',  # Era FORANEA
        '20 - Tecnológico': 'LOCAL',
        '21 - Chapultepec': 'LOCAL',
        '22 - Satelite': 'LOCAL',
        '24 - Exposicion': 'LOCAL',  # Era FORANEA
        '25 - Juarez': 'LOCAL',  # Era FORANEA
        '26 - Cadereyta': 'LOCAL',  # Era FORANEA
        '27 - Santiago': 'LOCAL',  # Era FORANEA
        '29 - Pablo Livas': 'LOCAL',  # Era FORANEA
        '31 - Las Quintas': 'LOCAL',  # Era FORANEA
        '32 - Allende': 'LOCAL',  # Era FORANEA
        '33 - Eloy Cavazos': 'LOCAL',  # Era FORANEA
        '34 - Montemorelos': 'LOCAL',  # Era FORANEA
        '36 - Apodaca Centro': 'LOCAL',
        '37 - Stiva': 'LOCAL',
        '38 - Gomez Morin': 'LOCAL',  # PENDIENTE: tiene 3+3=6
        '39 - Lazaro Cardenas': 'LOCAL',
        '40 - Plaza 1500': 'LOCAL',
        '41 - Vasconcelos': 'LOCAL',
        '52 - Venustiano Carranza': 'LOCAL',
        '53 - Lienzo Charro': 'LOCAL',
        '54 - Ramos Arizpe': 'LOCAL',
        '55 - Eulalio Gutierrez': 'LOCAL',
        '56 - Luis Echeverria': 'LOCAL',
        '71 - Centrito Valle': 'LOCAL',  # PENDIENTE: tiene 5+5=10
        '72 - Sabinas Hidalgo': 'LOCAL',  # Era FORANEA
        
        # FORÁNEAS (2+2=4)
        '23 - Guasave': 'FORANEA',  # Era LOCAL
        '28 - Guerrero': 'FORANEA',
        '30 - Carrizo': 'FORANEA',
        '42 - Independencia': 'FORANEA',
        '43 - Revolucion': 'FORANEA',
        '44 - Senderos': 'FORANEA',
        '45 - Triana': 'FORANEA',
        '46 - Campestre': 'FORANEA',
        '47 - San Antonio': 'FORANEA',
        '48 - Refugio': 'FORANEA',
        '49 - Pueblito': 'FORANEA',
        '50 - Patio': 'FORANEA',
        '51 - Constituyentes': 'FORANEA',
        '57 - Harold R. Pape': 'FORANEA',  # Era LOCAL
        '58 - Universidad (Tampico)': 'FORANEA',
        '59 - Plaza 3601': 'FORANEA',
        '60 - Centro (Tampico)': 'FORANEA',
        '61 - Aeropuerto (Tampico)': 'FORANEA',
        '62 - Lazaro Cardenas (Morelia)': 'FORANEA',
        '63 - Madero (Morelia)': 'FORANEA',
        '64 - Huerta': 'FORANEA',
        '65 - Pedro Cardenas': 'FORANEA',
        '66 - Lauro Villar': 'FORANEA',
        '67 - Centro (Matamoros)': 'FORANEA',
        '68 - Avenida del Niño': 'FORANEA',  # PENDIENTE: tiene 1+1=2
        '69 - Puerto Rico': 'FORANEA',
        '70 - Coahuila Comidas': 'FORANEA',
        '73 - Anzalduas': 'FORANEA',
        '74 - Hidalgo (Reynosa)': 'FORANEA',
        '75 - Libramiento (Reynosa)': 'FORANEA',
        '76 - Aeropuerto (Reynosa)': 'FORANEA',
        '77 - Boulevard Morelos': 'FORANEA',
        '78 - Alcala': 'FORANEA',
        '79 - Rio Bravo': 'FORANEA',
        '80 - Guerrero 2 (Ruelas)': 'FORANEA',
        '81 - Reforma (Ruelas)': 'FORANEA'
    }
    
    print(f"📋 APLICANDO {len(clasificaciones_roberto)} CLASIFICACIONES:")
    
    cambios_aplicados = 0
    cambios_detalle = {'LOCAL_a_FORANEA': 0, 'FORANEA_a_LOCAL': 0, 'sin_cambio': 0}
    
    for _, row in df_corregido.iterrows():
        if pd.notna(row['numero']) and pd.notna(row['nombre']):
            numero = int(row['numero'])
            nombre = row['nombre']
            location_key = f"{numero} - {nombre}"
            
            # Excluir sucursales nuevas
            if location_key in sucursales_nuevas:
                continue
                
            if location_key in clasificaciones_roberto:
                tipo_actual = row['tipo']
                nuevo_tipo = clasificaciones_roberto[location_key]
                
                # Convertir ESPECIAL a LOCAL para efectos de catálogo
                if nuevo_tipo == 'ESPECIAL':
                    nuevo_tipo_catalogo = 'LOCAL'
                else:
                    nuevo_tipo_catalogo = nuevo_tipo
                
                if tipo_actual != nuevo_tipo_catalogo:
                    df_corregido.loc[df_corregido['numero'] == numero, 'tipo'] = nuevo_tipo_catalogo
                    print(f"   ✅ {location_key}: {tipo_actual} → {nuevo_tipo_catalogo}")
                    cambios_aplicados += 1
                    
                    if tipo_actual == 'LOCAL' and nuevo_tipo_catalogo == 'FORANEA':
                        cambios_detalle['LOCAL_a_FORANEA'] += 1
                    elif tipo_actual == 'FORANEA' and nuevo_tipo_catalogo == 'LOCAL':
                        cambios_detalle['FORANEA_a_LOCAL'] += 1
                else:
                    cambios_detalle['sin_cambio'] += 1
    
    print(f"\n📊 RESUMEN DE CAMBIOS:")
    print(f"   🔄 Total cambios aplicados: {cambios_aplicados}")
    print(f"   📈 FORANEA → LOCAL: {cambios_detalle['FORANEA_a_LOCAL']}")
    print(f"   📉 LOCAL → FORANEA: {cambios_detalle['LOCAL_a_FORANEA']}")
    print(f"   ✅ Sin cambio: {cambios_detalle['sin_cambio']}")
    
    return df_corregido, clasificaciones_roberto

def crear_catalogo_final_correcto(df_sucursales_correcto, clasificaciones_roberto):
    """Crear catálogo final con clasificaciones correctas"""
    
    print(f"\n📊 CATÁLOGO FINAL CON CLASIFICACIONES CORRECTAS")
    print("=" * 70)
    
    # Sucursales nuevas a excluir
    sucursales_nuevas = [
        '35 - Apodaca',
        '82 - Aeropuerto Nuevo Laredo', 
        '83 - Cerradas de Anahuac',
        '84 - Aeropuerto del Norte',
        '85 - Diego Diaz',
        '86 - Miguel de la Madrid'
    ]
    
    # Reglas especiales
    reglas_especiales = {
        '1 - Pino Suarez': {'ops': 3, 'seg': 3, 'total': 6, 'tipo': 'ESPECIAL_3_3'},
        '5 - Felix U. Gomez': {'ops': 3, 'seg': 3, 'total': 6, 'tipo': 'ESPECIAL_3_3'},
        '2 - Madero': {'ops': 3, 'seg': 3, 'total': 6, 'tipo': 'ESPECIAL_3_3'},
        '3 - Matamoros': {'ops': 3, 'seg': 3, 'total': 6, 'tipo': 'ESPECIAL_3_3'}
    }
    
    catalogo = {}
    contadores = {'LOCAL': 0, 'FORANEA': 0, 'ESPECIAL': 0}
    
    for _, row in df_sucursales_correcto.iterrows():
        if pd.notna(row['numero']) and pd.notna(row['nombre']):
            numero = int(row['numero'])
            nombre = row['nombre']
            location_key = f"{numero} - {nombre}"
            
            # Excluir sucursales nuevas
            if location_key in sucursales_nuevas:
                continue
            
            # Aplicar reglas específicas
            if location_key in reglas_especiales:
                regla = reglas_especiales[location_key]
                ops_esperadas = regla['ops']
                seg_esperadas = regla['seg'] 
                total_esperado = regla['total']
                tipo_regla = regla['tipo']
                contadores['ESPECIAL'] += 1
            else:
                # Usar tipo corregido
                tipo_corregido = row['tipo']
                if tipo_corregido == 'FORANEA':
                    ops_esperadas = 2
                    seg_esperadas = 2
                    total_esperado = 4
                    tipo_regla = 'FORANEA_2_2'
                    contadores['FORANEA'] += 1
                else:  # LOCAL
                    ops_esperadas = 4
                    seg_esperadas = 4
                    total_esperado = 8
                    tipo_regla = 'LOCAL_4_4'
                    contadores['LOCAL'] += 1
            
            catalogo[location_key] = {
                'tipo_final': row['tipo'],
                'grupo': row.get('grupo', ''),
                'ops_esperadas': ops_esperadas,
                'seg_esperadas': seg_esperadas,
                'total_esperado': total_esperado,
                'tipo_regla': tipo_regla
            }
    
    print(f"📊 DISTRIBUCIÓN FINAL:")
    print(f"   🏢 LOCAL: {contadores['LOCAL']} sucursales (4+4=8)")
    print(f"   🌍 FORÁNEA: {contadores['FORANEA']} sucursales (2+2=4)") 
    print(f"   ⭐ ESPECIAL: {contadores['ESPECIAL']} sucursales (3+3=6)")
    print(f"   📍 TOTAL ACTIVAS: {sum(contadores.values())}")
    
    return catalogo, contadores

def validar_con_supervisiones_actuales(df_dataset, catalogo):
    """Validar distribución actual vs esperada"""
    
    print(f"\n✅ VALIDACIÓN CON SUPERVISIONES ACTUALES")
    print("=" * 70)
    
    # Cargar dataset
    df_dataset = pd.read_csv("DATASET_EMPAREJADO_20251218_164319.csv")
    
    # Contar supervisiones actuales
    ops_por_sucursal = df_dataset[df_dataset['tipo'] == 'operativas']['location_asignado'].value_counts()
    seg_por_sucursal = df_dataset[df_dataset['tipo'] == 'seguridad']['location_asignado'].value_counts()
    
    resultados = []
    
    for location_key, reglas in catalogo.items():
        ops_actuales = ops_por_sucursal.get(location_key, 0)
        seg_actuales = seg_por_sucursal.get(location_key, 0)
        total_actual = ops_actuales + seg_actuales
        total_esperado = reglas['total_esperado']
        diferencia = total_actual - total_esperado
        
        if diferencia == 0:
            estado = "✅ PERFECTO"
        elif diferencia > 0:
            estado = f"⚠️ EXCESO (+{diferencia})"
        else:
            estado = f"❌ DEFICIT ({diferencia})"
        
        resultados.append({
            'location_key': location_key,
            'tipo_final': reglas['tipo_final'],
            'tipo_regla': reglas['tipo_regla'],
            'ops_actuales': ops_actuales,
            'seg_actuales': seg_actuales,
            'total_actual': total_actual,
            'total_esperado': total_esperado,
            'diferencia': diferencia,
            'estado': estado
        })
    
    # Resumen
    perfectos = len([r for r in resultados if r['diferencia'] == 0])
    excesos = len([r for r in resultados if r['diferencia'] > 0])
    deficits = len([r for r in resultados if r['diferencia'] < 0])
    
    print(f"📊 RESULTADOS CON CLASIFICACIONES FINALES:")
    print(f"   ✅ PERFECTOS: {perfectos}/{len(resultados)} ({perfectos/len(resultados)*100:.1f}%)")
    print(f"   ⚠️ EXCESOS: {excesos}")
    print(f"   ❌ DÉFICITS: {deficits}")
    
    # Casos pendientes identificados por Roberto
    casos_pendientes = []
    
    for r in resultados:
        if r['location_key'] == '38 - Gomez Morin' and r['total_actual'] == 6:
            casos_pendientes.append(f"🔍 Gomez Morin: {r['ops_actuales']}+{r['seg_actuales']}=6 (esperado 8, falta 1 pareja)")
        elif r['location_key'] == '68 - Avenida del Niño' and r['total_actual'] == 2:
            casos_pendientes.append(f"🔍 Avenida del Niño: {r['ops_actuales']}+{r['seg_actuales']}=2 (esperado 4, falta 1 pareja)")
        elif r['location_key'] == '71 - Centrito Valle' and r['total_actual'] == 10:
            casos_pendientes.append(f"🔍 Centrito Valle: {r['ops_actuales']}+{r['seg_actuales']}=10 (esperado 8, sobra 1 pareja)")
    
    if casos_pendientes:
        print(f"\n⚠️ CASOS PENDIENTES IDENTIFICADOS:")
        for caso in casos_pendientes:
            print(f"   {caso}")
    
    return resultados, casos_pendientes

def main():
    """Función principal"""
    
    print("🔧 IMPLEMENTAR CORRECCIONES DE ROBERTO")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Aplicar todas las clasificaciones validadas grupo por grupo")
    print("=" * 80)
    
    # 1. Aplicar correcciones
    df_sucursales_correcto, clasificaciones_roberto = aplicar_correcciones_completas()
    
    # 2. Crear catálogo final
    catalogo_final, contadores = crear_catalogo_final_correcto(df_sucursales_correcto, clasificaciones_roberto)
    
    # 3. Validar con supervisiones actuales
    resultados, casos_pendientes = validar_con_supervisiones_actuales(None, catalogo_final)
    
    # 4. Guardar resultados
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Guardar catálogo corregido
    archivo_sucursales = f"SUCURSALES_CORRECCIONES_ROBERTO_{timestamp}.csv"
    df_sucursales_correcto.to_csv(archivo_sucursales, index=False, encoding='utf-8')
    
    # Guardar análisis final
    archivo_analisis = f"ANALISIS_CORRECCIONES_ROBERTO_{timestamp}.csv"
    df_resultados = pd.DataFrame(resultados)
    df_resultados.to_csv(archivo_analisis, index=False, encoding='utf-8')
    
    print(f"\n📁 ARCHIVOS GUARDADOS:")
    print(f"   ✅ Catálogo final: {archivo_sucursales}")
    print(f"   ✅ Análisis final: {archivo_analisis}")
    
    print(f"\n🎯 RESUMEN FINAL:")
    print(f"   🏢 LOCAL: {contadores['LOCAL']} sucursales")
    print(f"   🌍 FORÁNEA: {contadores['FORANEA']} sucursales") 
    print(f"   ⭐ ESPECIAL: {contadores['ESPECIAL']} sucursales")
    
    perfectos = len([r for r in resultados if r['diferencia'] == 0])
    print(f"   ✅ {perfectos}/{len(resultados)} sucursales perfectamente balanceadas")
    print(f"   ⚠️ {len(casos_pendientes)} casos pendientes para revisar")
    
    print(f"\n✅ CORRECCIONES DE ROBERTO IMPLEMENTADAS")
    
    return df_sucursales_correcto, catalogo_final, resultados, casos_pendientes

if __name__ == "__main__":
    main()