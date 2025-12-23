#!/usr/bin/env python3
"""
📊 ANÁLISIS COMPLETO DE EXCELES REALES DE ZENPUT
Analizar estructura, campos y datos de las 238 operativas + 238 seguridad
para crear ETL automatizado 2026
"""

import pandas as pd
import numpy as np
from datetime import datetime
import json

def analizar_excel_completo(archivo_path, tipo_formulario):
    """Analizar estructura completa del Excel de Zenput"""
    
    print(f"\n📊 ANALIZANDO: {tipo_formulario}")
    print("=" * 80)
    print(f"📁 Archivo: {archivo_path}")
    
    try:
        # Leer Excel
        df = pd.read_excel(archivo_path)
        
        print(f"\n✅ CARGADO EXITOSAMENTE:")
        print(f"   📊 Filas: {len(df)}")
        print(f"   📊 Columnas: {len(df.columns)}")
        
        # 1. ESTRUCTURA DE COLUMNAS
        print(f"\n🔍 COLUMNAS DISPONIBLES ({len(df.columns)} total):")
        for i, col in enumerate(df.columns, 1):
            tipo_dato = df[col].dtype
            no_nulos = df[col].notna().sum()
            print(f"   {i:2d}. {col:<30} | {str(tipo_dato):<15} | {no_nulos}/{len(df)} no-nulos")
        
        # 2. ANÁLISIS DE CAMPOS CLAVE
        print(f"\n🎯 ANÁLISIS DE CAMPOS CRÍTICOS:")
        
        # Buscar campos de ID/identificación
        campos_id = [col for col in df.columns if any(x in col.lower() for x in ['id', 'submission', 'identifier'])]
        if campos_id:
            print(f"   🔑 Campos ID encontrados: {campos_id}")
            for campo in campos_id:
                if not df[campo].isna().all():
                    muestra = df[campo].dropna().head(3).tolist()
                    print(f"      └─ {campo}: {muestra}")
        
        # Buscar campos de fecha
        campos_fecha = [col for col in df.columns if any(x in col.lower() for x in ['date', 'fecha', 'time', 'created', 'submitted'])]
        if campos_fecha:
            print(f"   📅 Campos Fecha encontrados: {campos_fecha}")
            for campo in campos_fecha:
                if not df[campo].isna().all():
                    muestra = df[campo].dropna().head(3).tolist()
                    print(f"      └─ {campo}: {muestra}")
        
        # Buscar campos de ubicación
        campos_location = [col for col in df.columns if any(x in col.lower() for x in ['location', 'lugar', 'sucursal', 'address', 'lat', 'lon', 'coord'])]
        if campos_location:
            print(f"   📍 Campos Ubicación encontrados: {campos_location}")
            for campo in campos_location:
                if not df[campo].isna().all():
                    muestra = df[campo].dropna().head(3).tolist()
                    print(f"      └─ {campo}: {muestra}")
        
        # Buscar campos de usuario
        campos_usuario = [col for col in df.columns if any(x in col.lower() for x in ['user', 'usuario', 'created_by', 'author', 'name'])]
        if campos_usuario:
            print(f"   👤 Campos Usuario encontrados: {campos_usuario}")
            for campo in campos_usuario:
                if not df[campo].isna().all():
                    muestra = df[campo].dropna().head(3).tolist()
                    print(f"      └─ {campo}: {muestra}")
        
        # 3. ANÁLISIS DE DATOS
        print(f"\n📈 ESTADÍSTICAS DE DATOS:")
        
        # Valores únicos en campos importantes
        for col in df.columns[:10]:  # Primeras 10 columnas
            unicos = df[col].nunique()
            if unicos <= 20:  # Si tiene pocos valores únicos, mostrarlos
                valores = df[col].value_counts().head(5).to_dict()
                print(f"   📊 {col}: {unicos} valores únicos - {valores}")
            else:
                print(f"   📊 {col}: {unicos} valores únicos")
        
        # 4. MUESTRA DE DATOS (primeras 3 filas)
        print(f"\n📋 MUESTRA DE DATOS (primeras 3 filas):")
        for i, (idx, row) in enumerate(df.head(3).iterrows()):
            print(f"   Fila {i+1}:")
            for col in df.columns[:8]:  # Primeras 8 columnas para no saturar
                valor = row[col]
                if pd.isna(valor):
                    valor = "NULL"
                elif isinstance(valor, str) and len(str(valor)) > 50:
                    valor = str(valor)[:50] + "..."
                print(f"      {col}: {valor}")
            if len(df.columns) > 8:
                print(f"      ... y {len(df.columns) - 8} columnas más")
            print()
        
        # 5. CALIDAD DE DATOS
        print(f"\n🔍 ANÁLISIS DE CALIDAD:")
        nulos_por_columna = df.isnull().sum()
        columnas_con_nulos = nulos_por_columna[nulos_por_columna > 0]
        
        if len(columnas_con_nulos) > 0:
            print(f"   ⚠️ Columnas con datos faltantes:")
            for col, nulos in columnas_con_nulos.items():
                porcentaje = (nulos / len(df)) * 100
                print(f"      {col}: {nulos} nulos ({porcentaje:.1f}%)")
        else:
            print(f"   ✅ No hay datos faltantes")
        
        return {
            'tipo_formulario': tipo_formulario,
            'archivo': archivo_path,
            'total_filas': len(df),
            'total_columnas': len(df.columns),
            'columnas': list(df.columns),
            'campos_id': campos_id,
            'campos_fecha': campos_fecha,
            'campos_location': campos_location,
            'campos_usuario': campos_usuario,
            'calidad_datos': nulos_por_columna.to_dict(),
            'muestra_datos': df.head(3).to_dict('records')
        }
        
    except Exception as e:
        print(f"❌ Error analizando {archivo_path}: {e}")
        return None

def comparar_con_api_data():
    """Comparar estructura Excel vs datos API extraídos"""
    
    print(f"\n🔄 COMPARACIÓN EXCEL vs API")
    print("=" * 60)
    
    try:
        # Cargar datos API (si existen)
        api_files = [
            'FASE1_API_V3_CORRECTA_20251218_120332.csv',
            'FASE1_COMPLETA_EXITO_20251218_120332.csv'
        ]
        
        for archivo in api_files:
            try:
                df_api = pd.read_csv(archivo)
                print(f"📊 API Data ({archivo}):")
                print(f"   📊 Filas: {len(df_api)}")
                print(f"   📊 Columnas: {list(df_api.columns)}")
                return df_api
            except:
                continue
                
    except Exception as e:
        print(f"   ⚠️ No se encontraron datos API para comparar")
        return None

def main():
    """Función principal"""
    
    print("📊 ANÁLISIS COMPLETO DE EXCELES REALES DE ZENPUT")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Objetivo: Entender estructura completa para ETL automatizado 2026")
    print("=" * 80)
    
    # Archivos a analizar
    archivos = [
        ("SUPERVISION_OPERATIVA_CAS_11_REV_250125_Submissions-2025-12-18__1228CST-1766141816.xlsx", "OPERATIVA"),
        ("CONTROL_OPERATIVO_DE_SEGURIDAD_CAS_11_REV_25012025_Submissions-2025-12-18__1232CST-1766104009.xlsx", "SEGURIDAD")
    ]
    
    resultados_analisis = []
    
    # Analizar cada archivo
    for archivo, tipo in archivos:
        resultado = analizar_excel_completo(archivo, tipo)
        if resultado:
            resultados_analisis.append(resultado)
    
    # Comparar con datos API
    df_api = comparar_con_api_data()
    
    # RESUMEN CONSOLIDADO
    print(f"\n" + "=" * 80)
    print(f"📋 RESUMEN CONSOLIDADO PARA ETL 2026")
    print("=" * 80)
    
    if resultados_analisis:
        print(f"✅ ARCHIVOS ANALIZADOS: {len(resultados_analisis)}")
        
        total_registros = sum(r['total_filas'] for r in resultados_analisis)
        print(f"📊 TOTAL REGISTROS: {total_registros}")
        
        for resultado in resultados_analisis:
            print(f"   {resultado['tipo_formulario']}: {resultado['total_filas']} submissions")
        
        # Campos comunes entre archivos
        if len(resultados_analisis) >= 2:
            columnas_ops = set(resultados_analisis[0]['columnas'])
            columnas_seg = set(resultados_analisis[1]['columnas'])
            columnas_comunes = columnas_ops & columnas_seg
            columnas_diferentes = (columnas_ops | columnas_seg) - columnas_comunes
            
            print(f"\n🔍 ANÁLISIS DE ESTRUCTURA:")
            print(f"   📊 Columnas comunes: {len(columnas_comunes)}")
            print(f"   📊 Columnas diferentes: {len(columnas_diferentes)}")
            
            if columnas_comunes:
                print(f"   ✅ Columnas comunes: {sorted(list(columnas_comunes))}")
            if columnas_diferentes:
                print(f"   ⚠️ Columnas diferentes: {sorted(list(columnas_diferentes))}")
    
    # Guardar análisis detallado
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if resultados_analisis:
        with open(f"ANALISIS_EXCELES_ZENPUT_{timestamp}.json", 'w', encoding='utf-8') as f:
            json.dump(resultados_analisis, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n📁 ANÁLISIS GUARDADO: ANALISIS_EXCELES_ZENPUT_{timestamp}.json")
    
    print(f"\n🚀 SIGUIENTE PASO:")
    print(f"   1. ✅ Estructura Excel analizada completamente")
    print(f"   2. 🔄 Crear mapeo Excel → API para automatización")
    print(f"   3. 🏗️ Diseñar ETL automatizado para 2026")
    print(f"   4. 🧪 Validar datos Excel vs API")
    
    return resultados_analisis

if __name__ == "__main__":
    main()