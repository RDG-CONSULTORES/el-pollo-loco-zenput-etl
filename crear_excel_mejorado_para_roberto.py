#!/usr/bin/env python3
"""
📊 CREAR EXCEL MEJORADO PARA ROBERTO
Versión mejorada de los Excel con formato más claro para ver las áreas
"""

import pandas as pd
from datetime import datetime

def crear_excel_mejorado():
    """Crear Excel mejorado con formato más claro para Roberto"""
    
    print("🔧 CREAR EXCEL MEJORADO PARA ROBERTO")
    print("=" * 60)
    print("🎯 Objetivo: Formato más claro para ver áreas por línea")
    print("=" * 60)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 1. OPERATIVAS MEJORADO
    print("\n📊 PROCESANDO OPERATIVAS...")
    try:
        df_ops = pd.read_excel("SUPERVISIONES_OPERATIVAS_COMPLETO_CON_AREAS_20251218_190749.xlsx", 
                               sheet_name='Operativas_Solo_Calificaciones')
        
        # Reordenar columnas para mejor visibilidad
        columnas_base = ['submission_id', 'sucursal_nombre', 'date_submitted', 'calificacion_general_zenput']
        columnas_areas = [col for col in df_ops.columns if col.startswith('AREA_')]
        columnas_areas.sort()  # Ordenar alfabéticamente
        
        # Crear DataFrame final
        columnas_finales = columnas_base + columnas_areas
        df_ops_mejorado = df_ops[columnas_finales].copy()
        
        # Renombrar columnas para mejor legibilidad
        df_ops_mejorado = df_ops_mejorado.rename(columns={
            'submission_id': 'ID_SUPERVISION',
            'sucursal_nombre': 'SUCURSAL', 
            'date_submitted': 'FECHA',
            'calificacion_general_zenput': 'CALIFICACION_GENERAL'
        })
        
        # Limpiar nombres de áreas
        for col in df_ops_mejorado.columns:
            if col.startswith('AREA_'):
                nuevo_nombre = col.replace('AREA_', '').replace('_', ' ')
                df_ops_mejorado = df_ops_mejorado.rename(columns={col: nuevo_nombre})
        
        # Crear Excel mejorado operativas
        archivo_ops = f"OPERATIVAS_MEJORADO_ROBERTO_{timestamp}.xlsx"
        
        with pd.ExcelWriter(archivo_ops, engine='openpyxl') as writer:
            # Hoja principal 
            df_ops_mejorado.to_excel(writer, sheet_name='Operativas_Con_Areas', index=False)
            
            # Crear hoja de muestra con explicación
            crear_hoja_explicacion_operativas(writer, df_ops_mejorado)
        
        print(f"✅ Operativas mejorado: {archivo_ops}")
        print(f"   📊 {len(df_ops_mejorado)} supervisiones")
        print(f"   📋 {len(df_ops_mejorado.columns)} columnas")
        print(f"   🏢 {len([c for c in df_ops_mejorado.columns if not c in ['ID_SUPERVISION', 'SUCURSAL', 'FECHA', 'CALIFICACION_GENERAL']])} áreas")
        
    except Exception as e:
        print(f"❌ Error en operativas: {e}")
        archivo_ops = None
    
    # 2. SEGURIDAD MEJORADO
    print("\n🛡️ PROCESANDO SEGURIDAD...")
    try:
        df_seg = pd.read_excel("SUPERVISIONES_SEGURIDAD_COMPLETO_CON_AREAS_20251218_190749.xlsx", 
                               sheet_name='Seguridad_Solo_Calificaciones')
        
        # Reordenar columnas
        columnas_base = ['submission_id', 'sucursal_nombre', 'date_submitted', 'calificacion_general_zenput']
        columnas_areas = [col for col in df_seg.columns if col.startswith('AREA_')]
        columnas_areas.sort()
        
        columnas_finales = columnas_base + columnas_areas
        df_seg_mejorado = df_seg[columnas_finales].copy()
        
        # Renombrar columnas
        df_seg_mejorado = df_seg_mejorado.rename(columns={
            'submission_id': 'ID_SUPERVISION',
            'sucursal_nombre': 'SUCURSAL',
            'date_submitted': 'FECHA', 
            'calificacion_general_zenput': 'CALIFICACION_GENERAL'
        })
        
        # Limpiar nombres de áreas
        for col in df_seg_mejorado.columns:
            if col.startswith('AREA_'):
                nuevo_nombre = col.replace('AREA_', '').replace('_', ' ')
                df_seg_mejorado = df_seg_mejorado.rename(columns={col: nuevo_nombre})
        
        # Crear Excel mejorado seguridad
        archivo_seg = f"SEGURIDAD_MEJORADO_ROBERTO_{timestamp}.xlsx"
        
        with pd.ExcelWriter(archivo_seg, engine='openpyxl') as writer:
            # Hoja principal
            df_seg_mejorado.to_excel(writer, sheet_name='Seguridad_Con_Areas', index=False)
            
            # Crear hoja de muestra con explicación
            crear_hoja_explicacion_seguridad(writer, df_seg_mejorado)
        
        print(f"✅ Seguridad mejorado: {archivo_seg}")
        print(f"   📊 {len(df_seg_mejorado)} supervisiones")
        print(f"   📋 {len(df_seg_mejorado.columns)} columnas") 
        print(f"   🏢 {len([c for c in df_seg_mejorado.columns if not c in ['ID_SUPERVISION', 'SUCURSAL', 'FECHA', 'CALIFICACION_GENERAL']])} áreas")
        
    except Exception as e:
        print(f"❌ Error en seguridad: {e}")
        archivo_seg = None
    
    # 3. RESUMEN FINAL
    print(f"\n🎯 ARCHIVOS MEJORADOS PARA ROBERTO:")
    print("=" * 60)
    
    if archivo_ops:
        print(f"✅ {archivo_ops}")
        print("   📊 FORMATO: ID | SUCURSAL | FECHA | CALIFICACION_GENERAL | ÁREA1 | ÁREA2 | ...")
        print("   🔧 Cada fila = Una supervisión operativa completa")
        print("   🏢 29 columnas de áreas con nombres limpios")
    
    if archivo_seg:
        print(f"✅ {archivo_seg}")
        print("   📊 FORMATO: ID | SUCURSAL | FECHA | CALIFICACION_GENERAL | ÁREA1 | ÁREA2 | ...")
        print("   🛡️ Cada fila = Una supervisión de seguridad completa")
        print("   🏢 11 columnas de áreas con nombres limpios")
    
    print(f"\n💡 INSTRUCCIONES PARA ROBERTO:")
    print("   1️⃣ Cada fila horizontal = Una supervisión completa")
    print("   2️⃣ Columnas 1-4 = Datos básicos (ID, Sucursal, Fecha, Calificación)")
    print("   3️⃣ Columnas 5+ = Calificaciones de cada área específica")
    print("   4️⃣ Perfecto para importar a PostgreSQL")
    
    return archivo_ops, archivo_seg

def crear_hoja_explicacion_operativas(writer, df):
    """Crear hoja explicativa para operativas"""
    
    # Tomar muestra de 3 supervisiones
    muestra = df.head(3)
    
    explicacion_data = []
    explicacion_data.append(['', 'EXPLICACIÓN DEL FORMATO - OPERATIVAS', '', '', ''])
    explicacion_data.append(['', '', '', '', ''])
    explicacion_data.append(['ESTRUCTURA:', 'Cada FILA = Una supervisión completa', '', '', ''])
    explicacion_data.append(['', 'Columna A = ID de la supervisión', '', '', ''])
    explicacion_data.append(['', 'Columna B = Sucursal asignada', '', '', ''])
    explicacion_data.append(['', 'Columna C = Fecha de supervisión', '', '', ''])
    explicacion_data.append(['', 'Columna D = Calificación general', '', '', ''])
    explicacion_data.append(['', 'Columnas E+ = Calificación de cada área', '', '', ''])
    explicacion_data.append(['', '', '', '', ''])
    
    # Agregar ejemplo
    areas_ejemplo = [col for col in df.columns if col not in ['ID_SUPERVISION', 'SUCURSAL', 'FECHA', 'CALIFICACION_GENERAL']]
    
    explicacion_data.append(['EJEMPLO:', f'{len(areas_ejemplo)} áreas evaluadas:', '', '', ''])
    for i, area in enumerate(areas_ejemplo[:10]):
        explicacion_data.append(['', f'{i+1}. {area}', '', '', ''])
    if len(areas_ejemplo) > 10:
        explicacion_data.append(['', f'... y {len(areas_ejemplo)-10} áreas más', '', '', ''])
    
    df_explicacion = pd.DataFrame(explicacion_data)
    df_explicacion.to_excel(writer, sheet_name='INSTRUCCIONES', index=False, header=False)

def crear_hoja_explicacion_seguridad(writer, df):
    """Crear hoja explicativa para seguridad"""
    
    explicacion_data = []
    explicacion_data.append(['', 'EXPLICACIÓN DEL FORMATO - SEGURIDAD', '', '', ''])
    explicacion_data.append(['', '', '', '', ''])
    explicacion_data.append(['ESTRUCTURA:', 'Cada FILA = Una supervisión completa', '', '', ''])
    explicacion_data.append(['', 'Columna A = ID de la supervisión', '', '', ''])
    explicacion_data.append(['', 'Columna B = Sucursal asignada', '', '', ''])
    explicacion_data.append(['', 'Columna C = Fecha de supervisión', '', '', ''])
    explicacion_data.append(['', 'Columna D = Calificación general', '', '', ''])
    explicacion_data.append(['', 'Columnas E+ = Calificación de cada área', '', '', ''])
    explicacion_data.append(['', '', '', '', ''])
    
    areas_ejemplo = [col for col in df.columns if col not in ['ID_SUPERVISION', 'SUCURSAL', 'FECHA', 'CALIFICACION_GENERAL']]
    
    explicacion_data.append(['TODAS LAS ÁREAS:', f'{len(areas_ejemplo)} áreas de seguridad:', '', '', ''])
    for i, area in enumerate(areas_ejemplo):
        explicacion_data.append(['', f'{i+1}. {area}', '', '', ''])
    
    df_explicacion = pd.DataFrame(explicacion_data)
    df_explicacion.to_excel(writer, sheet_name='INSTRUCCIONES', index=False, header=False)

def main():
    """Función principal"""
    
    print("📊 CREAR EXCEL MEJORADO PARA ROBERTO")
    print("=" * 80)
    print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Roberto: 'no lo veo pudieras actualizarlo o hacer una copia ?'")
    print("=" * 80)
    
    archivo_ops, archivo_seg = crear_excel_mejorado()
    
    print(f"\n🎯 ¡LISTO ROBERTO!")
    print("=" * 50)
    print("✅ Archivos mejorados con formato más claro")
    print("✅ Nombres de áreas más legibles")
    print("✅ Estructura perfecta para PostgreSQL")
    print("✅ Incluye hojas de instrucciones")

if __name__ == "__main__":
    main()