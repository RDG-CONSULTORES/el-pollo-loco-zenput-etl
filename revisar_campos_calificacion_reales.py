#!/usr/bin/env python3
"""
🔍 REVISAR CAMPOS DE CALIFICACIÓN REALES
Ver qué campos ya tienen las calificaciones calculadas en los Excel originales
"""

import pandas as pd

def revisar_campos_calificacion():
    """Revisar campos de calificación en Excel originales"""
    
    print("🔍 REVISAR CAMPOS DE CALIFICACIÓN REALES")
    print("=" * 60)
    
    # Excel operativas
    try:
        print("\n🔧 EXCEL OPERATIVAS ORIGINAL:")
        df_ops = pd.read_excel("SUPERVISION_OPERATIVA_CAS_11_REV_250125_Submissions-2025-12-18__1228CST-1766141816.xlsx")
        print(f"   📊 Registros: {len(df_ops)}")
        print(f"   📋 Columnas: {len(df_ops.columns)}")
        
        # Buscar campos de calificación
        campos_calificacion_ops = []
        for col in df_ops.columns:
            col_lower = str(col).lower()
            if any(word in col_lower for word in ['porcentaje', 'calificacion', 'puntos', 'score', '%']):
                campos_calificacion_ops.append(col)
        
        print(f"\n   📈 CAMPOS DE CALIFICACIÓN ENCONTRADOS:")
        for i, campo in enumerate(campos_calificacion_ops):
            valores_unicos = df_ops[campo].nunique()
            rango_valores = f"{df_ops[campo].min()} - {df_ops[campo].max()}" if pd.api.types.is_numeric_dtype(df_ops[campo]) else "texto"
            print(f"      {i+1:2}. {campo:<40} | {valores_unicos} únicos | {rango_valores}")
        
        # Muestra de valores del campo principal
        if 'PORCENTAJE %' in df_ops.columns:
            print(f"\n   🎯 MUESTRA 'PORCENTAJE %':")
            muestra = df_ops[['Submission ID', 'Sucursal', 'PORCENTAJE %']].head(5)
            for _, row in muestra.iterrows():
                print(f"      • {row['Submission ID'][:12]}... | {row['Sucursal'][:20]:<20} | {row['PORCENTAJE %']}")
        
    except Exception as e:
        print(f"   ❌ Error con operativas: {e}")
    
    # Excel seguridad
    try:
        print("\n🛡️ EXCEL SEGURIDAD ORIGINAL:")
        df_seg = pd.read_excel("CONTROL_OPERATIVO_DE_SEGURIDAD_CAS_11_REV_25012025_Submissions-2025-12-18__1232CST-1766104009.xlsx")
        print(f"   📊 Registros: {len(df_seg)}")
        print(f"   📋 Columnas: {len(df_seg.columns)}")
        
        # Buscar campos de calificación
        campos_calificacion_seg = []
        for col in df_seg.columns:
            col_lower = str(col).lower()
            if any(word in col_lower for word in ['porcentaje', 'calificacion', 'puntos', 'score', '%']):
                campos_calificacion_seg.append(col)
        
        print(f"\n   📈 CAMPOS DE CALIFICACIÓN ENCONTRADOS:")
        for i, campo in enumerate(campos_calificacion_seg):
            valores_unicos = df_seg[campo].nunique()
            rango_valores = f"{df_seg[campo].min()} - {df_seg[campo].max()}" if pd.api.types.is_numeric_dtype(df_seg[campo]) else "texto"
            print(f"      {i+1:2}. {campo:<40} | {valores_unicos} únicos | {rango_valores}")
        
        # Muestra de valores del campo principal
        if 'CALIFICACION PORCENTAJE %' in df_seg.columns:
            print(f"\n   🎯 MUESTRA 'CALIFICACION PORCENTAJE %':")
            muestra = df_seg[['Submission ID', 'Sucursal', 'CALIFICACION PORCENTAJE %']].head(5)
            for _, row in muestra.iterrows():
                print(f"      • {row['Submission ID'][:12]}... | {row['Sucursal'][:20]:<20} | {row['CALIFICACION PORCENTAJE %']}")
        
    except Exception as e:
        print(f"   ❌ Error con seguridad: {e}")
    
    print(f"\n🎯 CONCLUSIÓN:")
    print(f"   ❌ Estoy calculando calificaciones cuando YA están calculadas")
    print(f"   ✅ Debo usar los campos 'PORCENTAJE %' y 'CALIFICACION PORCENTAJE %'")
    print(f"   📊 Estos son los valores OFICIALES de Zenput")

if __name__ == "__main__":
    revisar_campos_calificacion()