#!/usr/bin/env python3
"""
🏪 NORMALIZACIÓN DE SUCURSALES - EL POLLO LOCO MÉXICO
Normaliza nombres de sucursales y detecta cambios por número de sucursal
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.zenput_api import create_zenput_client
import json
import re
from datetime import datetime
from collections import defaultdict

def extract_sucursal_number(sucursal_name):
    """Extrae número de sucursal del nombre"""
    
    # Patrones comunes: "1 - Nombre", "Sucursal 1", "001 Nombre", etc.
    patterns = [
        r'^(\d+)\s*-\s*(.+)$',        # "53 - Lienzo Charro"
        r'^(\d+)\s+(.+)$',            # "53 Lienzo Charro"
        r'^\D*(\d+)\D*(.+)$',         # "Sucursal 53 Lienzo"
        r'^(.+?)\s*(\d+)$',           # "Lienzo Charro 53"
    ]
    
    for pattern in patterns:
        match = re.match(pattern, sucursal_name.strip())
        if match:
            try:
                # Intentar primer grupo como número
                numero = int(match.group(1))
                nombre = match.group(2).strip()
                if 1 <= numero <= 100:  # Rango válido
                    return numero, nombre
            except (ValueError, IndexError):
                try:
                    # Intentar segundo grupo como número
                    numero = int(match.group(2))
                    nombre = match.group(1).strip()
                    if 1 <= numero <= 100:
                        return numero, nombre
                except (ValueError, IndexError):
                    continue
    
    return None, sucursal_name

def detect_name_changes(supervisiones_data):
    """Detecta cambios de nombres por número de sucursal"""
    
    print("🔍 DETECTANDO CAMBIOS DE NOMBRES DE SUCURSALES")
    print("-" * 60)
    
    # Agrupar por número de sucursal
    sucursales_by_number = defaultdict(lambda: {
        'nombres_vistos': set(),
        'submissions': [],
        'fechas': []
    })
    
    for submission in supervisiones_data:
        metadata = submission.get('smetadata', {})
        location = metadata.get('location', {})
        sucursal_name = location.get('name', '')
        fecha = metadata.get('date_completed_local', '')
        
        if sucursal_name:
            numero, nombre_limpio = extract_sucursal_number(sucursal_name)
            
            if numero:
                sucursales_by_number[numero]['nombres_vistos'].add(sucursal_name)
                sucursales_by_number[numero]['submissions'].append(submission.get('id'))
                sucursales_by_number[numero]['fechas'].append(fecha)
    
    # Detectar cambios
    cambios_detectados = []
    sucursales_normalizadas = {}
    
    for numero, data in sucursales_by_number.items():
        nombres = list(data['nombres_vistos'])
        
        if len(nombres) > 1:
            # Hay cambio de nombres
            # Ordenar por frecuencia para determinar nombre principal
            nombre_principal = max(nombres, key=lambda x: len([s for s in data['submissions']]))
            
            cambios_detectados.append({
                'sucursal_numero': numero,
                'nombre_principal': nombre_principal,
                'nombres_alternativos': [n for n in nombres if n != nombre_principal],
                'total_supervisiones': len(data['submissions']),
                'fechas_rango': [min(data['fechas']), max(data['fechas'])] if data['fechas'] else []
            })
            
            print(f"🔄 Sucursal {numero:2d}: {len(nombres)} nombres detectados")
            print(f"   📍 Principal: {nombre_principal}")
            for alt in [n for n in nombres if n != nombre_principal]:
                print(f"   📋 Alternativo: {alt}")
            print()
        
        # Determinar nombre normalizado
        nombre_final = nombres[0] if nombres else f"Sucursal {numero}"
        sucursales_normalizadas[numero] = {
            'numero': numero,
            'nombre_normalizado': nombre_final,
            'nombres_historicos': nombres,
            'total_supervisiones': len(data['submissions'])
        }
    
    print(f"📊 RESUMEN DE NORMALIZACIÓN:")
    print(f"   ✅ Total sucursales identificadas: {len(sucursales_normalizadas)}")
    print(f"   🔄 Sucursales con cambios de nombre: {len(cambios_detectados)}")
    
    return sucursales_normalizadas, cambios_detectados

def classify_sucursales_tipo(sucursales_normalizadas):
    """Clasifica sucursales como LOCAL o FORANEA"""
    
    print("\n🏷️ CLASIFICANDO SUCURSALES POR TIPO")
    print("-" * 60)
    
    # Ciudades/zonas locales (Monterrey y área metropolitana)
    ZONAS_LOCALES = [
        'monterrey', 'guadalupe', 'san nicolas', 'apodaca', 'escobedo', 'santa catarina',
        'san pedro', 'garza garcia', 'lincoln', 'juarez', 'centro', 'cumbres',
        'satelite', 'anahuac', 'concordia', 'madero', 'felix gomez', 'barragan'
    ]
    
    for numero, data in sucursales_normalizadas.items():
        nombre = data['nombre_normalizado'].lower()
        
        # Determinar si es local o foránea
        es_local = any(zona in nombre for zona in ZONAS_LOCALES)
        tipo = 'LOCAL' if es_local else 'FORANEA'
        
        data['tipo_sucursal'] = tipo
        
        # Extraer ciudad (simplificado)
        if 'saltillo' in nombre:
            data['ciudad'] = 'Saltillo'
        elif 'sabinas' in nombre:
            data['ciudad'] = 'Sabinas Hidalgo'
        elif any(zona in nombre for zona in ['monterrey', 'guadalupe', 'apodaca', 'escobedo']):
            data['ciudad'] = 'Monterrey (Área Metropolitana)'
        else:
            data['ciudad'] = 'Por determinar'
    
    # Mostrar clasificación
    locales = [s for s in sucursales_normalizadas.values() if s['tipo_sucursal'] == 'LOCAL']
    foraneas = [s for s in sucursales_normalizadas.values() if s['tipo_sucursal'] == 'FORANEA']
    
    print(f"🏠 SUCURSALES LOCALES: {len(locales)}")
    for sucursal in sorted(locales, key=lambda x: x['numero'])[:5]:
        print(f"   {sucursal['numero']:2d}. {sucursal['nombre_normalizado']}")
    if len(locales) > 5:
        print(f"   ... y {len(locales) - 5} más")
    
    print(f"\n🚗 SUCURSALES FORÁNEAS: {len(foraneas)}")
    for sucursal in sorted(foraneas, key=lambda x: x['numero'])[:5]:
        print(f"   {sucursal['numero']:2d}. {sucursal['nombre_normalizado']}")
    if len(foraneas) > 5:
        print(f"   ... y {len(foraneas) - 5} más")
    
    return sucursales_normalizadas

def generate_sucursales_master_data(sucursales_normalizadas, cambios_detectados):
    """Genera datos para tabla sucursales_master"""
    
    print(f"\n💾 GENERANDO DATOS MAESTROS DE SUCURSALES")
    print("-" * 60)
    
    master_data = {
        'sucursales': [],
        'cambios_detectados': cambios_detectados,
        'estadisticas': {
            'total_sucursales': len(sucursales_normalizadas),
            'sucursales_locales': len([s for s in sucursales_normalizadas.values() if s['tipo_sucursal'] == 'LOCAL']),
            'sucursales_foraneas': len([s for s in sucursales_normalizadas.values() if s['tipo_sucursal'] == 'FORANEA']),
            'sucursales_con_cambios': len(cambios_detectados)
        },
        'generacion': {
            'timestamp': datetime.now().isoformat(),
            'total_supervisiones_analizadas': sum([s['total_supervisiones'] for s in sucursales_normalizadas.values()])
        }
    }
    
    # Convertir a formato para base de datos
    for numero, data in sorted(sucursales_normalizadas.items()):
        sucursal_record = {
            'sucursal_numero': numero,
            'nombre_actual': data['nombre_normalizado'],
            'nombres_historicos': list(data['nombres_historicos']),
            'tipo_sucursal': data['tipo_sucursal'],
            'ciudad': data['ciudad'],
            'estado': 'Nuevo León',  # Mayoría están en NL
            'activa': True,
            'total_supervisiones_historicas': data['total_supervisiones']
        }
        
        master_data['sucursales'].append(sucursal_record)
    
    print(f"✅ {len(master_data['sucursales'])} sucursales procesadas")
    print(f"   🏠 Locales: {master_data['estadisticas']['sucursales_locales']}")
    print(f"   🚗 Foráneas: {master_data['estadisticas']['sucursales_foraneas']}")
    print(f"   🔄 Con cambios: {master_data['estadisticas']['sucursales_con_cambios']}")
    
    return master_data

def main():
    """Función principal"""
    
    print("🏪 NORMALIZACIÓN DE SUCURSALES - EL POLLO LOCO MÉXICO")
    print("=" * 70)
    print("🎯 Objetivo: Normalizar nombres y detectar cambios por número de sucursal")
    print("=" * 70)
    
    client = create_zenput_client()
    
    if not client.validate_api_connection():
        print("❌ No se puede conectar a API Zenput")
        return False
    
    # Obtener todas las supervisiones para análisis
    print("📊 EXTRAYENDO TODAS LAS SUPERVISIONES PARA ANÁLISIS...")
    
    all_supervisiones = []
    for form_id in ['877138', '877139']:
        submissions = client.get_submissions_for_form(form_id, days_back=60)  # Más rango
        all_supervisiones.extend(submissions)
        print(f"   ✅ Form {form_id}: {len(submissions)} supervisiones")
    
    print(f"📋 Total supervisiones para análisis: {len(all_supervisiones)}")
    
    # Detectar cambios de nombres
    sucursales_normalizadas, cambios_detectados = detect_name_changes(all_supervisiones)
    
    # Clasificar por tipo
    sucursales_normalizadas = classify_sucursales_tipo(sucursales_normalizadas)
    
    # Generar datos maestros
    master_data = generate_sucursales_master_data(sucursales_normalizadas, cambios_detectados)
    
    # Guardar resultados
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"data/sucursales_master_data_{timestamp}.json"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(master_data, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"\n💾 Datos maestros guardados en: {output_file}")
    
    # Generar SQL de inserción
    generate_insert_sql(master_data, timestamp)
    
    # Mostrar recomendaciones
    show_recommendations(cambios_detectados)
    
    return True

def generate_insert_sql(master_data, timestamp):
    """Genera SQL para insertar datos maestros"""
    
    sql_file = f"sql/insert_sucursales_master_{timestamp}.sql"
    
    with open(sql_file, 'w', encoding='utf-8') as f:
        f.write("-- INSERCIÓN DE DATOS MAESTROS - SUCURSALES\n")
        f.write(f"-- Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("-- Limpiar datos existentes (opcional)\n")
        f.write("-- TRUNCATE TABLE sucursales_master RESTART IDENTITY CASCADE;\n\n")
        
        f.write("-- Insertar sucursales normalizadas\n")
        for sucursal in master_data['sucursales']:
            nombres_hist = "'{" + ",".join([f'"{n}"' for n in sucursal['nombres_historicos']]) + "}'"
            
            f.write(f"""INSERT INTO sucursales_master 
    (sucursal_numero, nombre_actual, nombres_historicos, tipo_sucursal, ciudad, estado, activa) 
VALUES 
    ({sucursal['sucursal_numero']}, '{sucursal['nombre_actual']}', {nombres_hist}, '{sucursal['tipo_sucursal']}', '{sucursal['ciudad']}', '{sucursal['estado']}', {sucursal['activa']});
""")
        
        f.write("\n-- Estadísticas de inserción\n")
        f.write(f"-- Total sucursales: {len(master_data['sucursales'])}\n")
        f.write(f"-- Locales: {master_data['estadisticas']['sucursales_locales']}\n")
        f.write(f"-- Foráneas: {master_data['estadisticas']['sucursales_foraneas']}\n")
    
    print(f"📄 SQL de inserción generado en: {sql_file}")

def show_recommendations(cambios_detectados):
    """Muestra recomendaciones para Roberto"""
    
    print(f"\n💡 RECOMENDACIONES PARA ROBERTO:")
    print("=" * 50)
    
    if cambios_detectados:
        print(f"🔄 CAMBIOS DE NOMBRES DETECTADOS:")
        for cambio in cambios_detectados[:3]:  # Mostrar solo 3 ejemplos
            print(f"   Sucursal {cambio['sucursal_numero']}: {len(cambio['nombres_alternativos'])} nombres diferentes")
        
        if len(cambios_detectados) > 3:
            print(f"   ... y {len(cambios_detectados) - 3} más con cambios")
        
        print(f"\n📋 ACCIONES REQUERIDAS:")
        print(f"   1. Revisar archivo JSON generado para validar normalizaciones")
        print(f"   2. Ajustar clasificación LOCAL/FORÁNEA si es necesario")
        print(f"   3. Ejecutar SQL de inserción en Railway PostgreSQL")
        print(f"   4. Configurar fechas de periodos T1-T4 para 2026")
    
    print(f"\n🚀 PRÓXIMOS PASOS:")
    print(f"   1. ✅ Base de datos lista para supervisiones")
    print(f"   2. 🔧 ETL automático con normalización de sucursales")
    print(f"   3. 📊 Dashboard con datos históricos y periodos T1-T4")
    print(f"   4. 📱 Sistema de alertas por área crítica")

if __name__ == "__main__":
    main()