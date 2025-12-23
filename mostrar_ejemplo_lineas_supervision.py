#!/usr/bin/env python3
"""
👀 MOSTRAR EJEMPLO DE LÍNEAS DE SUPERVISIÓN
Ver exactamente cómo se ven las áreas por cada línea de supervisión
"""

import pandas as pd

def mostrar_ejemplo_operativas():
    """Mostrar ejemplo de líneas de supervisiones operativas"""
    
    print("🔧 EJEMPLO LÍNEAS OPERATIVAS")
    print("=" * 80)
    
    try:
        # Leer Excel
        df_ops = pd.read_excel("SUPERVISIONES_OPERATIVAS_COMPLETO_CON_AREAS_20251218_190749.xlsx", 
                               sheet_name='Operativas_Solo_Calificaciones')
        
        print(f"📊 Total supervisiones: {len(df_ops)}")
        print(f"📋 Total columnas: {len(df_ops.columns)}")
        
        # Mostrar estructura de columnas
        columnas_areas = [col for col in df_ops.columns if col.startswith('AREA_')]
        
        print(f"\n📋 ESTRUCTURA DE COLUMNAS:")
        print(f"   🔧 Datos básicos: submission_id, sucursal_nombre, date_submitted")
        print(f"   🎯 Calificación general: calificacion_general_zenput")
        print(f"   🏢 {len(columnas_areas)} áreas con calificaciones")
        
        # Mostrar 5 líneas completas como ejemplo
        print(f"\n📊 EJEMPLO DE 5 SUPERVISIONES COMPLETAS:")
        print("=" * 120)
        
        muestra = df_ops.head(5)
        
        for i, (_, row) in enumerate(muestra.iterrows(), 1):
            print(f"\n📋 SUPERVISIÓN {i}:")
            print(f"   ID: {row['submission_id']}")
            print(f"   Sucursal: {row['sucursal_nombre']}")
            print(f"   Fecha: {row['date_submitted']}")
            print(f"   🎯 CALIFICACIÓN GENERAL: {row['calificacion_general_zenput']}")
            
            print(f"\n   🏢 CALIFICACIONES POR ÁREA:")
            areas_con_valores = []
            for col in columnas_areas[:10]:  # Mostrar primeras 10 áreas
                area_name = col.replace('AREA_', '').replace('_', ' ')
                valor = row[col]
                if pd.notna(valor):
                    areas_con_valores.append(f"{area_name}: {valor}")
            
            # Mostrar en filas de 3 áreas
            for j in range(0, len(areas_con_valores), 3):
                areas_fila = areas_con_valores[j:j+3]
                print(f"      {' | '.join(areas_fila)}")
            
            if len(columnas_areas) > 10:
                print(f"      ... y {len(columnas_areas)-10} áreas más")
            
            print("-" * 80)
        
        return df_ops
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def mostrar_formato_tabla_operativas():
    """Mostrar formato tipo tabla de las operativas"""
    
    print(f"\n\n📊 FORMATO TABLA - OPERATIVAS")
    print("=" * 120)
    
    try:
        df_ops = pd.read_excel("SUPERVISIONES_OPERATIVAS_COMPLETO_CON_AREAS_20251218_190749.xlsx", 
                               sheet_name='Operativas_Solo_Calificaciones')
        
        # Seleccionar columnas clave + primeras 8 áreas para mostrar formato
        columnas_mostrar = ['submission_id', 'sucursal_nombre', 'calificacion_general_zenput']
        columnas_areas = [col for col in df_ops.columns if col.startswith('AREA_')][:8]
        
        columnas_finales = columnas_mostrar + columnas_areas
        
        # Crear tabla de muestra
        muestra_tabla = df_ops[columnas_finales].head(3)
        
        # Acortar nombres de columnas para mostrar
        muestra_tabla_display = muestra_tabla.copy()
        
        # Renombrar columnas para display
        rename_dict = {
            'submission_id': 'ID',
            'sucursal_nombre': 'Sucursal',
            'calificacion_general_zenput': 'Cal_Gral'
        }
        
        for col in columnas_areas:
            area_corta = col.replace('AREA_', '').replace('_', ' ')[:15]
            rename_dict[col] = area_corta
        
        muestra_tabla_display = muestra_tabla_display.rename(columns=rename_dict)
        
        print("FORMATO COMO TABLA (muestra de primeras 8 áreas):")
        print(muestra_tabla_display.to_string(index=False))
        
        print(f"\n💡 NOTA: Cada fila es una supervisión con:")
        print(f"   • ID de la supervisión")
        print(f"   • Sucursal asignada") 
        print(f"   • Calificación general")
        print(f"   • {len(columnas_areas)} calificaciones de áreas (una por columna)")
        
    except Exception as e:
        print(f"❌ Error en tabla: {e}")

def mostrar_ejemplo_seguridad():
    """Mostrar ejemplo de líneas de supervisiones de seguridad"""
    
    print(f"\n\n🛡️ EJEMPLO LÍNEAS SEGURIDAD")
    print("=" * 80)
    
    try:
        df_seg = pd.read_excel("SUPERVISIONES_SEGURIDAD_COMPLETO_CON_AREAS_20251218_190749.xlsx", 
                               sheet_name='Seguridad_Solo_Calificaciones')
        
        columnas_areas = [col for col in df_seg.columns if col.startswith('AREA_')]
        
        print(f"📊 Total supervisiones: {len(df_seg)}")
        print(f"🏢 {len(columnas_areas)} áreas evaluadas")
        
        # Mostrar 3 supervisiones completas
        muestra = df_seg.head(3)
        
        for i, (_, row) in enumerate(muestra.iterrows(), 1):
            print(f"\n📋 SUPERVISIÓN {i}:")
            print(f"   ID: {row['submission_id']}")
            print(f"   Sucursal: {row['sucursal_nombre']}")
            print(f"   🎯 CALIFICACIÓN GENERAL: {row['calificacion_general_zenput']}")
            
            print(f"   🏢 TODAS LAS ÁREAS DE SEGURIDAD:")
            areas_valores = []
            for col in columnas_areas:
                area_name = col.replace('AREA_', '').replace('_', ' ')
                valor = row[col]
                if pd.notna(valor):
                    areas_valores.append(f"{area_name}: {valor}")
            
            # Mostrar todas las áreas (son solo 11)
            for j in range(0, len(areas_valores), 3):
                areas_fila = areas_valores[j:j+3]
                print(f"      {' | '.join(areas_fila)}")
            
            print("-" * 60)
        
        # Mostrar formato tabla para seguridad
        print(f"\n📊 FORMATO TABLA SEGURIDAD (todas las 11 áreas):")
        columnas_mostrar = ['submission_id', 'sucursal_nombre', 'calificacion_general_zenput'] + columnas_areas
        tabla_seg = df_seg[columnas_mostrar].head(2)
        
        # Renombrar para display
        rename_dict = {
            'submission_id': 'ID',
            'sucursal_nombre': 'Sucursal', 
            'calificacion_general_zenput': 'Cal_Gral'
        }
        
        for col in columnas_areas:
            area_corta = col.replace('AREA_', '')[:10]
            rename_dict[col] = area_corta
        
        tabla_display = tabla_seg.rename(columns=rename_dict)
        print(tabla_display.to_string(index=False))
        
    except Exception as e:
        print(f"❌ Error en seguridad: {e}")

def main():
    """Función principal"""
    
    print("👀 EJEMPLO LÍNEAS DE SUPERVISIÓN CON ÁREAS")
    print("=" * 90)
    print("🎯 Roberto: Ver cómo están las áreas por cada línea de supervisión")
    print("=" * 90)
    
    # 1. Mostrar operativas
    df_ops = mostrar_ejemplo_operativas()
    
    # 2. Mostrar formato tabla operativas
    mostrar_formato_tabla_operativas()
    
    # 3. Mostrar seguridad
    mostrar_ejemplo_seguridad()
    
    print(f"\n🎯 EXPLICACIÓN PARA ROBERTO:")
    print(f"   📊 Cada FILA = Una supervisión completa")
    print(f"   📋 Cada COLUMNA = Una calificación de área")
    print(f"   🔧 Operativas: 238 filas × 47 columnas (29 áreas)")
    print(f"   🛡️ Seguridad: 238 filas × 29 columnas (11 áreas)")
    print(f"   ✅ Formato perfecto para Dashboard y PostgreSQL")

if __name__ == "__main__":
    main()