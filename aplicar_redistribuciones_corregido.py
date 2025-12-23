#!/usr/bin/env python3
"""
🔄 APLICAR REDISTRIBUCIONES CORREGIDO
Implementar redistribuciones con IDs exactos encontrados
"""

import pandas as pd
from datetime import datetime

def cargar_dataset():
    """Cargar dataset normalizado"""
    df = pd.read_csv("DATASET_NORMALIZADO_20251218_155659.csv")
    return df

def encontrar_ids_exactos(df):
    """Encontrar los IDs exactos de las submissions a redistribuir"""
    
    print("🔍 BUSCAR IDs EXACTOS")
    print("=" * 40)
    
    # 1. Pablo Livas → Linda Vista (2025-06-16, 18:43)
    pablo_seg = df[(df['location_asignado'] == '29 - Pablo Livas') & 
                   (df['fecha_str'] == '2025-06-16') & 
                   (df['tipo'] == 'seguridad')]
    
    print(f"📍 Pablo Livas seguridad 2025-06-16:")
    for _, row in pablo_seg.iterrows():
        submitted = pd.to_datetime(row['date_submitted'])
        hora = submitted.strftime('%H:%M') if pd.notna(submitted) else 'N/A'
        print(f"   {row['submission_id']} - {hora}")
    
    # 2. Pedro Cardenas → Puerto Rico (2025-05-14, más cercana a 20:06)
    pedro_seg = df[(df['location_asignado'] == '65 - Pedro Cardenas') & 
                   (df['fecha_str'] == '2025-05-14') & 
                   (df['tipo'] == 'seguridad')]
    
    print(f"\n📍 Pedro Cardenas seguridad 2025-05-14:")
    for _, row in pedro_seg.iterrows():
        submitted = pd.to_datetime(row['date_submitted'])
        hora = submitted.strftime('%H:%M:%S') if pd.notna(submitted) else 'N/A'
        print(f"   {row['submission_id']} - {hora}")
    
    # 3. Centrito Valle → Plaza 1500 (2025-06-24)
    centrito_jun = df[(df['location_asignado'] == '71 - Centrito Valle') & 
                      (df['fecha_str'] == '2025-06-24') & 
                      (df['tipo'] == 'seguridad')]
    
    print(f"\n📍 Centrito Valle seguridad 2025-06-24:")
    for _, row in centrito_jun.iterrows():
        print(f"   {row['submission_id']}")
    
    # 4. Centrito Valle → Gomez Morin (2025-08-22)
    centrito_ago = df[(df['location_asignado'] == '71 - Centrito Valle') & 
                      (df['fecha_str'] == '2025-08-22') & 
                      (df['tipo'] == 'seguridad')]
    
    print(f"\n📍 Centrito Valle seguridad 2025-08-22:")
    for _, row in centrito_ago.iterrows():
        print(f"   {row['submission_id']}")

def aplicar_redistribuciones_exactas(df):
    """Aplicar redistribuciones con IDs exactos"""
    
    print(f"\n🔄 APLICAR REDISTRIBUCIONES CON IDs EXACTOS")
    print("=" * 60)
    
    df_redistribuido = df.copy()
    
    # IDs exactos basados en el análisis previo
    redistribuciones = [
        {
            'submission_id': '685065c72c100d805a446af4',  # ID completo encontrado
            'de': '29 - Pablo Livas',
            'a': '18 - Linda Vista',
            'fecha': '2025-06-16',
            'tipo': 'seguridad',
            'motivo': 'Completar par faltante Linda Vista (18:43)'
        },
        {
            'submission_id': '6824f8062b3388ec02cdd3bb',  # La de 20:07 (más cercana a 20:06)
            'de': '65 - Pedro Cardenas', 
            'a': '69 - Puerto Rico',
            'fecha': '2025-05-14',
            'tipo': 'seguridad',
            'motivo': 'Pedro Cardenas duplicada, Puerto Rico necesita par'
        }
        # Buscar los IDs de Centrito Valle
    ]
    
    # Buscar IDs de Centrito Valle
    centrito_jun = df[(df['location_asignado'] == '71 - Centrito Valle') & 
                      (df['fecha_str'] == '2025-06-24') & 
                      (df['tipo'] == 'seguridad')]
    
    if len(centrito_jun) > 0:
        redistribuciones.append({
            'submission_id': centrito_jun.iloc[0]['submission_id'],
            'de': '71 - Centrito Valle',
            'a': '40 - Plaza 1500',
            'fecha': '2025-06-24',
            'tipo': 'seguridad',
            'motivo': 'Centrito sobrante, Plaza 1500 necesita par'
        })
    
    centrito_ago = df[(df['location_asignado'] == '71 - Centrito Valle') & 
                      (df['fecha_str'] == '2025-08-22') & 
                      (df['tipo'] == 'seguridad')]
    
    if len(centrito_ago) > 0:
        redistribuciones.append({
            'submission_id': centrito_ago.iloc[0]['submission_id'],
            'de': '71 - Centrito Valle',
            'a': '38 - Gomez Morin',
            'fecha': '2025-08-22',
            'tipo': 'seguridad',
            'motivo': 'Centrito sobrante, Gomez Morin necesita par'
        })
    
    print(f"📋 REDISTRIBUCIONES A APLICAR:")
    aplicadas = 0
    
    for i, redist in enumerate(redistribuciones, 1):
        print(f"\n{i}. {redist['submission_id']} - {redist['fecha']}")
        print(f"   📤 DE: {redist['de']}")
        print(f"   📥 A:  {redist['a']}")
        print(f"   💡 Motivo: {redist['motivo']}")
        
        # Aplicar cambio
        mask = df_redistribuido['submission_id'] == redist['submission_id']
        registros_encontrados = mask.sum()
        
        if registros_encontrados > 0:
            df_redistribuido.loc[mask, 'location_asignado'] = redist['a']
            print(f"   ✅ Aplicado ({registros_encontrados} registro)")
            aplicadas += 1
        else:
            print(f"   ❌ ID no encontrado")
    
    print(f"\n✅ {aplicadas}/{len(redistribuciones)} REDISTRIBUCIONES APLICADAS")
    
    return df_redistribuido

def verificar_cambios(df_original, df_redistribuido):
    """Verificar que los cambios se aplicaron correctamente"""
    
    print(f"\n✅ VERIFICAR CAMBIOS APLICADOS")
    print("=" * 50)
    
    sucursales_verificar = [
        ('18 - Linda Vista', 'LOCAL', 8),
        ('29 - Pablo Livas', 'LOCAL', 8), 
        ('65 - Pedro Cardenas', 'FORANEA', 4),
        ('69 - Puerto Rico', 'FORANEA', 4),
        ('71 - Centrito Valle', 'LOCAL', 8),
        ('40 - Plaza 1500', 'LOCAL', 8),
        ('38 - Gomez Morin', 'LOCAL', 8)
    ]
    
    print(f"{'Sucursal':<25} {'Antes':<8} {'Después':<8} {'Esperado':<8} {'Estado'}")
    print("-" * 75)
    
    for sucursal, tipo, esperado in sucursales_verificar:
        # Conteos antes
        antes = len(df_original[df_original['location_asignado'] == sucursal])
        
        # Conteos después  
        despues = len(df_redistribuido[df_redistribuido['location_asignado'] == sucursal])
        
        # Estado
        if despues == esperado:
            estado = "✅ PERFECTO"
        elif despues > esperado:
            estado = f"⚠️ EXCESO (+{despues - esperado})"
        else:
            estado = f"❌ DEFICIT (-{esperado - despues})"
        
        print(f"{sucursal:<25} {antes:<8} {despues:<8} {esperado:<8} {estado}")

def main():
    """Función principal"""
    
    print("🔄 APLICAR REDISTRIBUCIONES CORREGIDO")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Objetivo: Aplicar redistribuciones con IDs exactos")
    print("=" * 80)
    
    # 1. Cargar datos
    df_original = cargar_dataset()
    
    # 2. Encontrar IDs exactos
    encontrar_ids_exactos(df_original)
    
    # 3. Aplicar redistribuciones
    df_redistribuido = aplicar_redistribuciones_exactas(df_original)
    
    # 4. Verificar cambios
    verificar_cambios(df_original, df_redistribuido)
    
    # 5. Guardar dataset final
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archivo_final = f"DATASET_REDISTRIBUIDO_CORRECTO_{timestamp}.csv"
    
    df_redistribuido.to_csv(archivo_final, index=False, encoding='utf-8')
    
    print(f"\n📁 DATASET FINAL GUARDADO:")
    print(f"   ✅ Archivo: {archivo_final}")
    print(f"   📊 Total registros: {len(df_redistribuido)}")
    
    print(f"\n✅ REDISTRIBUCIONES CORREGIDAS COMPLETADAS")
    
    return df_redistribuido

if __name__ == "__main__":
    main()