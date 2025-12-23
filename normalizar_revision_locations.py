#!/usr/bin/env python3
"""
🔧 NORMALIZACIÓN DE LOCATIONS
Mejorar el mapeo usando normalización de nombres de sucursales
"""

import pandas as pd
import re
from difflib import SequenceMatcher
import csv

def cargar_sucursales_master():
    """Cargar sucursales master con mapeo de nombres"""
    sucursales = {}
    
    with open('SUCURSALES_MASTER_20251218_110913.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            numero = int(row['numero'])
            nombre = row['nombre']
            
            # Crear múltiples variantes de búsqueda
            sucursales[nombre] = row
            sucursales[numero] = row
            sucursales[f"{numero} - {nombre}"] = row
            
            # Nombre normalizado sin acentos/caracteres especiales
            nombre_norm = normalizar_texto(nombre)
            sucursales[nombre_norm] = row
            sucursales[f"{numero} - {nombre_norm}"] = row
    
    return sucursales

def normalizar_texto(texto):
    """Normalizar texto removiendo acentos y caracteres especiales"""
    if not texto:
        return ""
    
    # Remover acentos
    replacements = {
        'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
        'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U',
        'ñ': 'n', 'Ñ': 'N'
    }
    
    for old, new in replacements.items():
        texto = texto.replace(old, new)
    
    # Convertir a lowercase y remover espacios extra
    return texto.lower().strip()

def extraer_nombre_limpio(location_name):
    """Extraer nombre limpio del location_name de Zenput"""
    if not location_name:
        return ""
    
    # Patrón: "número - nombre"
    match = re.match(r'^(\d+)\s*-\s*(.+)$', location_name.strip())
    if match:
        numero = int(match.group(1))
        nombre = match.group(2).strip()
        return numero, nombre, normalizar_texto(nombre)
    
    # Si no tiene número, usar el nombre tal cual
    return None, location_name.strip(), normalizar_texto(location_name)

def calcular_similitud(str1, str2):
    """Calcular similitud entre dos strings"""
    return SequenceMatcher(None, str1.lower(), str2.lower()).ratio()

def encontrar_mejor_match(location_name, sucursales_master):
    """Encontrar el mejor match para un location_name"""
    
    numero, nombre_original, nombre_norm = extraer_nombre_limpio(location_name)
    
    # 1. Match exacto por número - nombre
    if numero and f"{numero} - {nombre_original}" in sucursales_master:
        return sucursales_master[f"{numero} - {nombre_original}"], "EXACTO_NUMERO_NOMBRE", 1.0
    
    # 2. Match exacto por nombre original
    if nombre_original in sucursales_master:
        return sucursales_master[nombre_original], "EXACTO_NOMBRE", 1.0
    
    # 3. Match exacto por nombre normalizado
    if nombre_norm in sucursales_master:
        return sucursales_master[nombre_norm], "EXACTO_NORMALIZADO", 1.0
    
    # 4. Match por similitud
    mejor_match = None
    mejor_similitud = 0.0
    mejor_metodo = ""
    
    for key, datos in sucursales_master.items():
        if isinstance(key, str) and not key.isdigit():
            # Similitud con nombre original
            sim_original = calcular_similitud(nombre_original, key)
            if sim_original > mejor_similitud and sim_original >= 0.8:
                mejor_similitud = sim_original
                mejor_match = datos
                mejor_metodo = f"SIMILITUD_ORIGINAL_{sim_original:.2f}"
            
            # Similitud con nombre normalizado
            sim_norm = calcular_similitud(nombre_norm, normalizar_texto(key))
            if sim_norm > mejor_similitud and sim_norm >= 0.8:
                mejor_similitud = sim_norm
                mejor_match = datos
                mejor_metodo = f"SIMILITUD_NORMALIZADA_{sim_norm:.2f}"
    
    if mejor_match:
        return mejor_match, mejor_metodo, mejor_similitud
    
    return None, "NO_ENCONTRADO", 0.0

def procesar_revision_mejorada():
    """Procesar archivo de revisión con mapeo mejorado"""
    
    print("🔧 NORMALIZANDO MAPEO DE LOCATIONS")
    print("=" * 50)
    
    # Cargar datos
    sucursales_master = cargar_sucursales_master()
    df_revision = pd.read_csv('REQUIEREN_REVISION_20251218_110913.csv')
    
    print(f"📊 Procesando {len(df_revision)} submissions que requerían revisión...")
    
    # Procesar cada submission
    resultados = []
    resueltos_automaticamente = 0
    aun_requieren_revision = 0
    
    for index, row in df_revision.iterrows():
        if index % 50 == 0:
            print(f"   🔄 Procesadas: {index}/{len(df_revision)}")
        
        location_name = row['location_name']
        status_original = row['status']
        
        # Intentar mapeo mejorado
        mejor_match, metodo, similitud = encontrar_mejor_match(location_name, sucursales_master)
        
        if mejor_match and (metodo.startswith('EXACTO') or similitud >= 0.9):
            # ¡RESUELTO AUTOMÁTICAMENTE!
            resultado = row.to_dict()
            resultado.update({
                'status_nuevo': 'RESUELTO_AUTOMATICAMENTE',
                'sucursal_final_nueva': mejor_match['nombre'],
                'sucursal_numero_nuevo': mejor_match['numero'],
                'sucursal_tipo_nuevo': mejor_match['tipo'],
                'metodo_mapeo': metodo,
                'similitud_score': similitud,
                'requiere_revision_nueva': 'NO',
                'notas_nuevas': f'Resuelto por {metodo} - similitud: {similitud:.2f}'
            })
            resueltos_automaticamente += 1
        else:
            # AÚN REQUIERE REVISIÓN MANUAL
            resultado = row.to_dict()
            resultado.update({
                'status_nuevo': 'AUN_REQUIERE_REVISION',
                'sucursal_final_nueva': row['sucursal_final'],
                'sucursal_numero_nuevo': row['sucursal_numero'],
                'sucursal_tipo_nuevo': row['sucursal_tipo'],
                'metodo_mapeo': metodo if mejor_match else 'NO_MATCH',
                'similitud_score': similitud,
                'requiere_revision_nueva': 'SI',
                'notas_nuevas': f'Requiere revisión manual - mejor match: {metodo} ({similitud:.2f})'
            })
            aun_requieren_revision += 1
        
        resultados.append(resultado)
    
    # Crear DataFrame de resultados
    df_resultados = pd.DataFrame(resultados)
    
    # Guardar resultados
    timestamp = "20251218_111500"  # timestamp fijo para consistencia
    
    # 1. Todas las submissions con análisis
    df_resultados.to_csv(f'REVISION_NORMALIZADA_{timestamp}.csv', index=False, encoding='utf-8')
    
    # 2. Solo las resueltas automáticamente
    df_resueltas = df_resultados[df_resultados['status_nuevo'] == 'RESUELTO_AUTOMATICAMENTE']
    if not df_resueltas.empty:
        df_resueltas.to_csv(f'RESUELTAS_AUTOMATICAMENTE_{timestamp}.csv', index=False, encoding='utf-8')
    
    # 3. Solo las que aún requieren revisión
    df_pendientes = df_resultados[df_resultados['status_nuevo'] == 'AUN_REQUIERE_REVISION']
    if not df_pendientes.empty:
        df_pendientes.to_csv(f'AUN_REQUIEREN_REVISION_{timestamp}.csv', index=False, encoding='utf-8')
    
    # Estadísticas
    print(f"\n📊 RESULTADOS DE LA NORMALIZACIÓN:")
    print(f"   📋 Total procesadas: {len(df_revision)}")
    print(f"   ✅ Resueltas automáticamente: {resueltos_automaticamente} ({resueltos_automaticamente/len(df_revision)*100:.1f}%)")
    print(f"   ⚠️ Aún requieren revisión: {aun_requieren_revision} ({aun_requieren_revision/len(df_revision)*100:.1f}%)")
    
    # Ejemplos de resoluciones
    print(f"\n🎯 EJEMPLOS DE RESOLUCIONES AUTOMÁTICAS:")
    ejemplos = df_resueltas.head(5)
    for i, (_, row) in enumerate(ejemplos.iterrows(), 1):
        print(f"   {i}. '{row['location_name']}' → {row['sucursal_final_nueva']} ({row['metodo_mapeo']})")
    
    # Estadísticas por método
    if not df_resueltas.empty:
        metodos = df_resueltas['metodo_mapeo'].value_counts()
        print(f"\n📈 MÉTODOS DE RESOLUCIÓN:")
        for metodo, count in metodos.items():
            print(f"   {metodo}: {count}")
    
    print(f"\n📁 ARCHIVOS GENERADOS:")
    print(f"   📄 Análisis completo: REVISION_NORMALIZADA_{timestamp}.csv")
    if not df_resueltas.empty:
        print(f"   ✅ Resueltas: RESUELTAS_AUTOMATICAMENTE_{timestamp}.csv")
    if not df_pendientes.empty:
        print(f"   ⚠️ Pendientes: AUN_REQUIEREN_REVISION_{timestamp}.csv")
    
    return df_resultados, resueltos_automaticamente, aun_requieren_revision

def main():
    """Función principal"""
    print("🔧 NORMALIZACIÓN DE LOCATIONS - MEJORA DEL MAPEO")
    print("=" * 80)
    
    # Procesar
    resultados, resueltos, pendientes = procesar_revision_mejorada()
    
    print(f"\n🎉 NORMALIZACIÓN COMPLETADA")
    print(f"💡 DE {resueltos + pendientes} SUBMISSIONS DUDOSAS:")
    print(f"   ✅ {resueltos} RESUELTAS AUTOMÁTICAMENTE")
    print(f"   ⚠️ {pendientes} AÚN NECESITAN REVISIÓN MANUAL")
    
    if resueltos > 0:
        porcentaje_mejora = (resueltos / (resueltos + pendientes)) * 100
        print(f"   📈 MEJORA: {porcentaje_mejora:.1f}% reducción en revisión manual")

if __name__ == "__main__":
    main()