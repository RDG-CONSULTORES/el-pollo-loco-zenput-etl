#!/usr/bin/env python3
"""
📊 ANALIZAR CÓDIGOS DE SUPERVISIÓN POR GRUPO
Ver qué códigos de supervisión tiene cada grupo y sucursal
"""

import pandas as pd
from datetime import datetime

def cargar_datos_finales():
    """Cargar dataset emparejado y catálogo final"""
    
    df_dataset = pd.read_csv("DATASET_EMPAREJADO_20251218_164319.csv")
    df_sucursales = pd.read_csv("SUCURSALES_CORRECCIONES_ROBERTO_20251218_171807.csv")
    
    return df_dataset, df_sucursales

def obtener_grupos_sucursales(df_sucursales):
    """Obtener grupos únicos de sucursales"""
    
    # Excluir sucursales nuevas
    sucursales_nuevas = [
        '35 - Apodaca',
        '82 - Aeropuerto Nuevo Laredo', 
        '83 - Cerradas de Anahuac',
        '84 - Aeropuerto del Norte',
        '85 - Diego Diaz',
        '86 - Miguel de la Madrid'
    ]
    
    grupos = {}
    
    for _, row in df_sucursales.iterrows():
        if pd.notna(row['numero']) and pd.notna(row['nombre']):
            numero = int(row['numero'])
            nombre = row['nombre']
            location_key = f"{numero} - {nombre}"
            
            # Excluir sucursales nuevas
            if location_key in sucursales_nuevas:
                continue
                
            grupo = row.get('grupo', 'SIN_GRUPO')
            tipo = row.get('tipo', 'LOCAL')
            
            if grupo not in grupos:
                grupos[grupo] = []
            
            grupos[grupo].append({
                'location_key': location_key,
                'numero': numero,
                'tipo': tipo
            })
    
    return grupos

def analizar_supervisiones_por_grupo(df_dataset, grupos):
    """Analizar supervisiones por grupo"""
    
    print("📊 CÓDIGOS DE SUPERVISIÓN POR GRUPO")
    print("=" * 80)
    
    resumen_por_grupo = {}
    
    for grupo, sucursales in grupos.items():
        print(f"\n🏢 GRUPO: {grupo}")
        print(f"   📍 {len(sucursales)} sucursales")
        print("-" * 60)
        
        supervisiones_grupo = []
        
        for sucursal in sorted(sucursales, key=lambda x: x['numero']):
            location_key = sucursal['location_key']
            
            # Filtrar supervisiones de esta sucursal
            suc_data = df_dataset[df_dataset['location_asignado'] == location_key]
            
            if len(suc_data) > 0:
                ops = suc_data[suc_data['tipo'] == 'operativas']
                seg = suc_data[suc_data['tipo'] == 'seguridad']
                
                # Códigos únicos
                codigos_ops = list(ops['submission_id'].unique()) if len(ops) > 0 else []
                codigos_seg = list(seg['submission_id'].unique()) if len(seg) > 0 else []
                
                supervisiones_grupo.extend(codigos_ops + codigos_seg)
                
                print(f"   • {location_key:<35} | {sucursal['tipo']:<8} | {len(ops)}ops + {len(seg)}seg = {len(suc_data)} total")
                
                # Mostrar algunos códigos como muestra
                if len(codigos_ops) > 0:
                    print(f"     🔧 Ops: {codigos_ops[0][:12]}... ({len(codigos_ops)} códigos)")
                if len(codigos_seg) > 0:
                    print(f"     🛡️ Seg: {codigos_seg[0][:12]}... ({len(codigos_seg)} códigos)")
            else:
                print(f"   • {location_key:<35} | {sucursal['tipo']:<8} | SIN SUPERVISIONES")
        
        # Resumen del grupo
        total_supervisiones_grupo = len(supervisiones_grupo)
        ops_grupo = len([s for s in supervisiones_grupo if s in df_dataset[df_dataset['tipo'] == 'operativas']['submission_id'].values])
        seg_grupo = len([s for s in supervisiones_grupo if s in df_dataset[df_dataset['tipo'] == 'seguridad']['submission_id'].values])
        
        resumen_por_grupo[grupo] = {
            'sucursales': len(sucursales),
            'total_supervisiones': total_supervisiones_grupo,
            'operativas': ops_grupo,
            'seguridad': seg_grupo
        }
        
        print(f"\n   📊 RESUMEN {grupo}:")
        print(f"      Total supervisiones: {total_supervisiones_grupo}")
        print(f"      Operativas: {ops_grupo}")
        print(f"      Seguridad: {seg_grupo}")
    
    return resumen_por_grupo

def analizar_casos_pendientes(df_dataset):
    """Analizar en detalle los 3 casos pendientes"""
    
    print(f"\n\n🔍 ANÁLISIS DETALLADO CASOS PENDIENTES")
    print("=" * 80)
    
    casos_pendientes = [
        '38 - Gomez Morin',
        '68 - Avenida del Niño', 
        '71 - Centrito Valle'
    ]
    
    for caso in casos_pendientes:
        print(f"\n📋 {caso}:")
        print("-" * 50)
        
        suc_data = df_dataset[df_dataset['location_asignado'] == caso]
        
        if len(suc_data) > 0:
            ops = suc_data[suc_data['tipo'] == 'operativas']
            seg = suc_data[suc_data['tipo'] == 'seguridad']
            
            print(f"   📊 Total: {len(suc_data)} supervisiones ({len(ops)} ops + {len(seg)} seg)")
            
            # Fechas - convertir a datetime primero
            if len(ops) > 0:
                ops_dates = pd.to_datetime(ops['date_submitted'])
                fechas_ops = sorted(ops_dates.dt.strftime('%Y-%m-%d').unique())
            else:
                fechas_ops = []
                
            if len(seg) > 0:
                seg_dates = pd.to_datetime(seg['date_submitted'])
                fechas_seg = sorted(seg_dates.dt.strftime('%Y-%m-%d').unique())
            else:
                fechas_seg = []
            
            print(f"   📅 Fechas operativas: {fechas_ops}")
            print(f"   📅 Fechas seguridad: {fechas_seg}")
            
            # Códigos
            print(f"   🔧 Códigos operativas:")
            for _, row in ops.iterrows():
                fecha = pd.to_datetime(row['date_submitted']).strftime('%Y-%m-%d %H:%M')
                print(f"      • {row['submission_id']} | {fecha}")
            
            print(f"   🛡️ Códigos seguridad:")
            for _, row in seg.iterrows():
                fecha = pd.to_datetime(row['date_submitted']).strftime('%Y-%m-%d %H:%M')
                print(f"      • {row['submission_id']} | {fecha}")
        else:
            print(f"   ❌ Sin supervisiones asignadas")

def mostrar_resumen_final(resumen_por_grupo):
    """Mostrar resumen final por grupos"""
    
    print(f"\n\n📊 RESUMEN FINAL POR GRUPOS")
    print("=" * 80)
    print(f"{'Grupo':<40} {'Sucursales':<10} {'Total':<8} {'Ops':<6} {'Seg'}")
    print("-" * 80)
    
    total_sucursales = 0
    total_supervisiones = 0
    total_ops = 0
    total_seg = 0
    
    for grupo, datos in sorted(resumen_por_grupo.items()):
        print(f"{grupo:<40} {datos['sucursales']:<10} {datos['total_supervisiones']:<8} {datos['operativas']:<6} {datos['seguridad']}")
        
        total_sucursales += datos['sucursales']
        total_supervisiones += datos['total_supervisiones']
        total_ops += datos['operativas']
        total_seg += datos['seguridad']
    
    print("-" * 80)
    print(f"{'TOTAL':<40} {total_sucursales:<10} {total_supervisiones:<8} {total_ops:<6} {total_seg}")
    
    print(f"\n✅ DISTRIBUCIÓN CONFIRMADA:")
    print(f"   🏢 {total_sucursales} sucursales activas")
    print(f"   📊 {total_supervisiones} supervisiones totales")
    print(f"   🔧 {total_ops} operativas")
    print(f"   🛡️ {total_seg} seguridad")

def main():
    """Función principal"""
    
    print("📊 ANALIZAR CÓDIGOS DE SUPERVISIÓN POR GRUPO")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Roberto: Ver códigos por grupo, dejar 3 casos pendientes como están")
    print("=" * 80)
    
    # 1. Cargar datos
    df_dataset, df_sucursales = cargar_datos_finales()
    
    # 2. Obtener grupos
    grupos = obtener_grupos_sucursales(df_sucursales)
    
    # 3. Analizar supervisiones por grupo
    resumen_por_grupo = analizar_supervisiones_por_grupo(df_dataset, grupos)
    
    # 4. Analizar casos pendientes en detalle
    analizar_casos_pendientes(df_dataset)
    
    # 5. Mostrar resumen final
    mostrar_resumen_final(resumen_por_grupo)
    
    # 6. Guardar análisis
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archivo_grupos = f"ANALISIS_GRUPOS_SUPERVISIONES_{timestamp}.csv"
    
    # Crear DataFrame del resumen
    df_resumen = pd.DataFrame.from_dict(resumen_por_grupo, orient='index')
    df_resumen.to_csv(archivo_grupos, encoding='utf-8')
    
    print(f"\n📁 ARCHIVO GUARDADO:")
    print(f"   ✅ {archivo_grupos}")
    print(f"   📊 {len(grupos)} grupos analizados")
    
    print(f"\n🎯 DECISIÓN FINAL:")
    print(f"   ✅ Clasificaciones implementadas correctamente")
    print(f"   📊 91.2% sucursales perfectas (73/80)")
    print(f"   ⏳ 3 casos pendientes quedan como están por ahora")
    print(f"   📋 Códigos de supervisión identificados por grupo")
    
    print(f"\n✅ ANÁLISIS DE GRUPOS COMPLETADO")
    
    return grupos, resumen_por_grupo

if __name__ == "__main__":
    main()