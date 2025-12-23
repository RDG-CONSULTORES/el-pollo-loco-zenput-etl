#!/usr/bin/env python3
"""
📋 LISTADO COMPLETO 80 SUCURSALES
Para que Roberto valide UNA por UNA si es LOCAL o FORÁNEA
"""

import pandas as pd
from datetime import datetime

def cargar_datos():
    """Cargar datos actuales"""
    
    df_sucursales = pd.read_csv('SUCURSALES_MASTER_20251218_110913.csv')
    df_dataset = pd.read_csv("DATASET_EMPAREJADO_20251218_164319.csv")
    
    return df_sucursales, df_dataset

def obtener_listado_completo(df_sucursales, df_dataset):
    """Obtener listado completo para validación de Roberto"""
    
    # Sucursales nuevas a excluir
    sucursales_nuevas = [
        '35 - Apodaca',
        '82 - Aeropuerto Nuevo Laredo', 
        '83 - Cerradas de Anahuac',
        '84 - Aeropuerto del Norte',
        '85 - Diego Diaz',
        '86 - Miguel de la Madrid'
    ]
    
    # Contar supervisiones por sucursal
    ops_por_sucursal = df_dataset[df_dataset['tipo'] == 'operativas']['location_asignado'].value_counts()
    seg_por_sucursal = df_dataset[df_dataset['tipo'] == 'seguridad']['location_asignado'].value_counts()
    
    listado = []
    
    for _, row in df_sucursales.iterrows():
        if pd.notna(row['numero']) and pd.notna(row['nombre']):
            numero = int(row['numero'])
            nombre = row['nombre']
            location_key = f"{numero} - {nombre}"
            
            # Excluir sucursales nuevas
            if location_key in sucursales_nuevas:
                continue
            
            tipo_actual = row.get('tipo', 'LOCAL')
            grupo = row.get('grupo', '')
            
            # Contar supervisiones
            ops_count = ops_por_sucursal.get(location_key, 0)
            seg_count = seg_por_sucursal.get(location_key, 0)
            total_count = ops_count + seg_count
            
            listado.append({
                'numero': numero,
                'nombre': nombre,
                'location_key': location_key,
                'tipo_actual': tipo_actual,
                'grupo': grupo,
                'ops_count': ops_count,
                'seg_count': seg_count,
                'total_count': total_count
            })
    
    # Ordenar por número
    listado.sort(key=lambda x: x['numero'])
    
    return listado

def mostrar_listado_completo(listado):
    """Mostrar listado completo para validación de Roberto"""
    
    print("📋 LISTADO COMPLETO 80 SUCURSALES PARA VALIDACIÓN")
    print("=" * 100)
    print("Roberto: Valida cada una como LOCAL (4+4) o FORÁNEA (2+2)")
    print("=" * 100)
    
    print(f"{'#':<3} {'Sucursal':<40} {'Tipo Actual':<12} {'Supervisiones':<15} {'Grupo'}")
    print("-" * 100)
    
    for i, suc in enumerate(listado, 1):
        supervisiones_str = f"{suc['ops_count']}+{suc['seg_count']}={suc['total_count']}"
        
        print(f"{i:<3} {suc['location_key']:<40} {suc['tipo_actual']:<12} {supervisiones_str:<15} {suc['grupo']}")
    
    print("-" * 100)
    print(f"📊 TOTAL: {len(listado)} sucursales activas")
    
    # Resumen por tipo actual
    locales = len([s for s in listado if s['tipo_actual'] == 'LOCAL'])
    foraneas = len([s for s in listado if s['tipo_actual'] == 'FORANEA'])
    
    print(f"\n📊 DISTRIBUCIÓN ACTUAL:")
    print(f"   🏢 LOCAL: {locales} sucursales")
    print(f"   🌍 FORÁNEA: {foraneas} sucursales")
    
    # Supervisiones totales
    total_ops = sum(s['ops_count'] for s in listado)
    total_seg = sum(s['seg_count'] for s in listado)
    
    print(f"\n📊 SUPERVISIONES DISTRIBUIDAS:")
    print(f"   🔧 Operativas: {total_ops}")
    print(f"   🛡️ Seguridad: {total_seg}")
    print(f"   📊 Total: {total_ops + total_seg}")

def crear_template_validacion(listado):
    """Crear template para que Roberto complete"""
    
    print(f"\n\n📝 TEMPLATE PARA ROBERTO:")
    print("=" * 60)
    print("Copia y pega, cambiando LOCAL/FORANEA según corresponda:")
    print("=" * 60)
    
    for i, suc in enumerate(listado, 1):
        print(f"{i:2}. {suc['location_key']:<40} → LOCAL")

def main():
    """Función principal"""
    
    print("📋 LISTADO COMPLETO 80 SUCURSALES")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Roberto valida UNA por UNA: LOCAL o FORÁNEA")
    print("=" * 80)
    
    # 1. Cargar datos
    df_sucursales, df_dataset = cargar_datos()
    
    # 2. Obtener listado
    listado = obtener_listado_completo(df_sucursales, df_dataset)
    
    # 3. Mostrar listado completo
    mostrar_listado_completo(listado)
    
    # 4. Crear template
    crear_template_validacion(listado)
    
    # 5. Guardar para referencia
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archivo_listado = f"LISTADO_80_SUCURSALES_VALIDACION_{timestamp}.csv"
    
    df_listado = pd.DataFrame(listado)
    df_listado.to_csv(archivo_listado, index=False, encoding='utf-8')
    
    print(f"\n📁 ARCHIVO GUARDADO:")
    print(f"   ✅ {archivo_listado}")
    print(f"   📊 {len(listado)} sucursales para validar")
    
    print(f"\n✅ LISTADO COMPLETO LISTO PARA ROBERTO")
    
    return listado

if __name__ == "__main__":
    main()