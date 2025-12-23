#!/usr/bin/env python3
"""
🔍 ANÁLISIS COMPLETO DE CAMPOS DE SUBMISSIONS
Mostrar TODOS los campos disponibles en submissions de ambas formas
"""

import requests
import json
from datetime import datetime

ZENPUT_CONFIG = {
    'base_url': 'https://www.zenput.com/api/v3',
    'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}
}

FORMULARIOS = {
    '877138': 'SUPERVISION OPERATIVA',
    '877139': 'SUPERVISION SEGURIDAD'
}

def analizar_campos_recursivamente(obj, prefix="", nivel=0):
    """Analizar campos de manera recursiva"""
    campos_encontrados = []
    
    if isinstance(obj, dict):
        for key, value in obj.items():
            path_completo = f"{prefix}.{key}" if prefix else key
            
            if isinstance(value, (dict, list)):
                # Si es dict o list, seguir explorando
                campos_encontrados.extend(analizar_campos_recursivamente(value, path_completo, nivel + 1))
            else:
                # Es un valor final
                tipo_valor = type(value).__name__
                valor_str = str(value)[:100] + "..." if len(str(value)) > 100 else str(value)
                campos_encontrados.append({
                    'campo': path_completo,
                    'tipo': tipo_valor,
                    'valor': valor_str,
                    'nivel': nivel
                })
    
    elif isinstance(obj, list) and obj:
        # Si es una lista no vacía, analizar el primer elemento
        if len(obj) > 0:
            primer_elemento = obj[0]
            campos_encontrados.extend(analizar_campos_recursivamente(primer_elemento, f"{prefix}[0]", nivel + 1))
            
            # Si hay más elementos y son diferentes, mostrar algunos más
            for i, elemento in enumerate(obj[1:3], 1):  # Solo 2 elementos más
                if elemento != primer_elemento:
                    campos_encontrados.extend(analizar_campos_recursivamente(elemento, f"{prefix}[{i}]", nivel + 1))
    
    return campos_encontrados

def obtener_submission_completa_form(form_id, form_name):
    """Obtener una submission completa de un formulario específico"""
    
    print(f"\n🎯 ANALIZANDO FORMULARIO {form_id}: {form_name}")
    print("=" * 80)
    
    try:
        # 1. Obtener lista de submissions
        url = f"{ZENPUT_CONFIG['base_url']}/submissions"
        params = {
            'form_template_id': form_id,
            'created_after': '2025-01-01T00:00:00Z',
            'created_before': '2025-12-31T23:59:59Z',
            'page': 1,
            'page_size': 1  # Solo 1 submission
        }
        
        response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            submissions = data.get('data', [])
            
            if submissions:
                submission = submissions[0]
                submission_id = submission.get('id')
                
                print(f"📋 Submission ID: {submission_id}")
                print(f"📊 Total campos nivel raíz: {len(submission.keys())}")
                
                # 2. Analizar todos los campos recursivamente
                print(f"\n🔍 TODOS LOS CAMPOS DISPONIBLES:")
                print("-" * 80)
                
                campos = analizar_campos_recursivamente(submission)
                
                # Organizar campos por nivel y tipo
                campos_por_nivel = {}
                campos_importantes = []
                
                for campo in campos:
                    nivel = campo['nivel']
                    if nivel not in campos_por_nivel:
                        campos_por_nivel[nivel] = []
                    campos_por_nivel[nivel].append(campo)
                    
                    # Identificar campos potencialmente importantes
                    campo_lower = campo['campo'].lower()
                    if any(keyword in campo_lower for keyword in [
                        'lat', 'lon', 'coordinate', 'location', 'address', 'user', 'name', 
                        'date', 'time', 'created', 'submitted', 'updated', 'team', 'delivery'
                    ]):
                        campos_importantes.append(campo)
                
                # Mostrar campos por nivel
                for nivel in sorted(campos_por_nivel.keys()):
                    print(f"\n📂 NIVEL {nivel}:")
                    for campo in campos_por_nivel[nivel][:20]:  # Máximo 20 por nivel
                        indentacion = "  " * nivel
                        print(f"{indentacion}• {campo['campo']} ({campo['tipo']}): {campo['valor']}")
                    
                    if len(campos_por_nivel[nivel]) > 20:
                        print(f"{indentacion}... y {len(campos_por_nivel[nivel]) - 20} campos más")
                
                # Mostrar campos importantes
                print(f"\n🎯 CAMPOS POTENCIALMENTE IMPORTANTES ({len(campos_importantes)}):")
                print("-" * 60)
                for campo in campos_importantes:
                    print(f"• {campo['campo']} ({campo['tipo']}): {campo['valor']}")
                
                # Buscar específicamente campos de coordenadas/ubicación
                print(f"\n📍 BÚSQUEDA ESPECÍFICA DE COORDENADAS/UBICACIÓN:")
                print("-" * 60)
                
                coordenadas_encontradas = []
                ubicacion_encontrada = []
                
                for campo in campos:
                    campo_lower = campo['campo'].lower()
                    
                    if any(keyword in campo_lower for keyword in ['lat', 'lon', 'coordinate']):
                        coordenadas_encontradas.append(campo)
                    
                    if any(keyword in campo_lower for keyword in ['location', 'address', 'place']):
                        ubicacion_encontrada.append(campo)
                
                if coordenadas_encontradas:
                    print("✅ COORDENADAS ENCONTRADAS:")
                    for coord in coordenadas_encontradas:
                        print(f"   • {coord['campo']}: {coord['valor']}")
                else:
                    print("❌ NO se encontraron campos de coordenadas")
                
                if ubicacion_encontrada:
                    print("✅ UBICACIÓN ENCONTRADA:")
                    for ubi in ubicacion_encontrada:
                        print(f"   • {ubi['campo']}: {ubi['valor']}")
                else:
                    print("❌ NO se encontraron campos de ubicación")
                
                # Guardar análisis completo
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"analisis_completo_{form_id}_{timestamp}.json"
                
                analisis_completo = {
                    'form_id': form_id,
                    'form_name': form_name,
                    'submission_id': submission_id,
                    'timestamp': timestamp,
                    'total_campos': len(campos),
                    'campos_por_nivel': {str(k): len(v) for k, v in campos_por_nivel.items()},
                    'submission_raw': submission,
                    'todos_los_campos': campos,
                    'campos_importantes': campos_importantes,
                    'coordenadas_encontradas': coordenadas_encontradas,
                    'ubicacion_encontrada': ubicacion_encontrada
                }
                
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(analisis_completo, f, indent=2, ensure_ascii=False)
                
                print(f"\n💾 Análisis completo guardado en: {filename}")
                
                return analisis_completo
                
            else:
                print("❌ No se encontraron submissions")
                return None
        else:
            print(f"❌ Error {response.status_code}: {response.text[:200]}")
            return None
            
    except Exception as e:
        print(f"💥 Error: {e}")
        return None

def main():
    """Función principal"""
    
    print("🔍 ANÁLISIS COMPLETO DE CAMPOS DE SUBMISSIONS")
    print("=" * 100)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"API Token: {ZENPUT_CONFIG['headers']['X-API-TOKEN'][:20]}...")
    print("=" * 100)
    
    resultados = {}
    
    # Analizar cada formulario
    for form_id, form_name in FORMULARIOS.items():
        resultado = obtener_submission_completa_form(form_id, form_name)
        if resultado:
            resultados[form_id] = resultado
    
    print(f"\n" + "=" * 100)
    print(f"📊 RESUMEN FINAL:")
    print("=" * 100)
    
    for form_id, resultado in resultados.items():
        print(f"\n📋 FORMULARIO {form_id} ({resultado['form_name']}):")
        print(f"   • Total campos analizados: {resultado['total_campos']}")
        print(f"   • Campos importantes: {len(resultado['campos_importantes'])}")
        print(f"   • Coordenadas encontradas: {len(resultado['coordenadas_encontradas'])}")
        print(f"   • Ubicaciones encontradas: {len(resultado['ubicacion_encontrada'])}")
        
        if resultado['coordenadas_encontradas']:
            print(f"   🎯 Coordenadas disponibles:")
            for coord in resultado['coordenadas_encontradas']:
                print(f"      - {coord['campo']}")
        
        if resultado['ubicacion_encontrada']:
            print(f"   🎯 Ubicaciones disponibles:")
            for ubi in resultado['ubicacion_encontrada']:
                print(f"      - {ubi['campo']}")
    
    print(f"\n🎉 ANÁLISIS COMPLETADO")

if __name__ == "__main__":
    main()