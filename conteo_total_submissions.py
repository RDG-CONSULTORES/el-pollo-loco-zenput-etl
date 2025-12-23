#!/usr/bin/env python3
"""
📊 CONTEO TOTAL DE SUBMISSIONS 2025
Contar exactamente cuántas submissions hay sin procesarlas
"""

import requests
import json
from datetime import datetime

ZENPUT_CONFIG = {
    'base_url': 'https://www.zenput.com/api/v3',
    'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}
}

FORMULARIOS_2025 = {
    '877138': 'OPERATIVA',  
    '877139': 'SEGURIDAD'
}

def contar_submissions_por_formulario():
    """Contar total de submissions por formulario"""
    
    print("📊 CONTANDO SUBMISSIONS TOTALES 2025")
    print("=" * 60)
    
    resultados = {}
    total_general = 0
    
    for form_id, tipo_form in FORMULARIOS_2025.items():
        print(f"\n📋 Contando {tipo_form} (Form {form_id})...")
        
        try:
            url = f"{ZENPUT_CONFIG['base_url']}/submissions"
            params = {
                'form_template_id': form_id,
                'created_after': '2025-01-01T00:00:00Z',
                'created_before': '2025-12-31T23:59:59Z',
                'page': 1,
                'page_size': 1  # Solo para obtener el count
            }
            
            response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                total = data.get('count', 0)
                
                # Calcular páginas necesarias
                page_size = 100
                paginas_necesarias = (total + page_size - 1) // page_size  # Ceiling division
                
                resultados[form_id] = {
                    'tipo': tipo_form,
                    'total': total,
                    'paginas_necesarias': paginas_necesarias,
                    'tiempo_estimado_minutos': paginas_necesarias * 0.5  # 0.5 min por página
                }
                
                total_general += total
                
                print(f"   ✅ {tipo_form}: {total:,} submissions")
                print(f"   📄 Páginas necesarias: {paginas_necesarias}")
                print(f"   ⏱️ Tiempo estimado: {paginas_necesarias * 0.5:.1f} minutos")
                
            else:
                print(f"   ❌ Error {response.status_code}: {response.text[:100]}")
                
        except Exception as e:
            print(f"   💥 Error: {e}")
    
    print(f"\n📊 RESUMEN TOTAL:")
    print("=" * 60)
    print(f"📈 TOTAL GENERAL: {total_general:,} submissions")
    
    tiempo_total_estimado = sum(r.get('tiempo_estimado_minutos', 0) for r in resultados.values())
    print(f"⏱️ TIEMPO TOTAL ESTIMADO: {tiempo_total_estimado:.1f} minutos")
    
    if total_general > 1000:
        print(f"\n⚠️ RECOMENDACIÓN:")
        print(f"   El volumen de datos es alto ({total_general:,} submissions)")
        print(f"   Se recomienda procesar por lotes o limitar el rango de fechas")
    
    # Guardar conteo
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"conteo_submissions_{timestamp}.json"
    
    resultado = {
        'timestamp': timestamp,
        'total_general': total_general,
        'por_formulario': resultados,
        'tiempo_estimado_total_minutos': tiempo_total_estimado
    }
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(resultado, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 Conteo guardado en: {filename}")
    
    return resultados

def analizar_fechas_por_mes():
    """Analizar distribución de submissions por mes"""
    
    print(f"\n📅 ANALIZANDO DISTRIBUCIÓN POR MES")
    print("=" * 60)
    
    meses_2025 = [
        ('2025-01-01', '2025-01-31', 'Enero'),
        ('2025-02-01', '2025-02-28', 'Febrero'),
        ('2025-03-01', '2025-03-31', 'Marzo'),
        ('2025-04-01', '2025-04-30', 'Abril'),
        ('2025-05-01', '2025-05-31', 'Mayo'),
        ('2025-06-01', '2025-06-30', 'Junio'),
        ('2025-07-01', '2025-07-31', 'Julio'),
        ('2025-08-01', '2025-08-31', 'Agosto'),
        ('2025-09-01', '2025-09-30', 'Septiembre'),
        ('2025-10-01', '2025-10-31', 'Octubre'),
        ('2025-11-01', '2025-11-30', 'Noviembre'),
        ('2025-12-01', '2025-12-31', 'Diciembre')
    ]
    
    distribucion_mensual = {}
    
    for inicio, fin, nombre_mes in meses_2025:
        print(f"\n📆 {nombre_mes} ({inicio} - {fin}):")
        mes_data = {}
        
        for form_id, tipo_form in FORMULARIOS_2025.items():
            try:
                url = f"{ZENPUT_CONFIG['base_url']}/submissions"
                params = {
                    'form_template_id': form_id,
                    'created_after': f'{inicio}T00:00:00Z',
                    'created_before': f'{fin}T23:59:59Z',
                    'page': 1,
                    'page_size': 1
                }
                
                response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    total = data.get('count', 0)
                    mes_data[tipo_form] = total
                    print(f"   {tipo_form}: {total:,}")
                else:
                    print(f"   {tipo_form}: Error {response.status_code}")
                    
            except Exception as e:
                print(f"   {tipo_form}: Error - {e}")
        
        distribucion_mensual[nombre_mes] = mes_data
    
    # Mostrar resumen mensual
    print(f"\n📊 RESUMEN MENSUAL:")
    print("=" * 50)
    for mes, data in distribucion_mensual.items():
        ops = data.get('OPERATIVA', 0)
        segs = data.get('SEGURIDAD', 0)
        total_mes = ops + segs
        print(f"{mes:12} | Op: {ops:4,} | Seg: {segs:4,} | Total: {total_mes:4,}")
    
    return distribucion_mensual

def main():
    """Función principal"""
    
    print("📊 ANÁLISIS DE VOLUMEN DE SUBMISSIONS 2025")
    print("=" * 80)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # 1. Contar totales
    conteo_total = contar_submissions_por_formulario()
    
    # 2. Analizar por mes
    distribucion_mensual = analizar_fechas_por_mes()
    
    # 3. Guardar análisis completo
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"analisis_volumen_completo_{timestamp}.json"
    
    resultado_completo = {
        'timestamp': timestamp,
        'conteo_total': conteo_total,
        'distribucion_mensual': distribucion_mensual,
        'total_general': sum(r.get('total', 0) for r in conteo_total.values()),
        'recomendaciones': {
            'procesamiento_por_lotes': True,
            'tiempo_estimado_completo': sum(r.get('tiempo_estimado_minutos', 0) for r in conteo_total.values()),
            'estrategia_recomendada': 'Procesar por trimestres o semestres para evitar timeouts'
        }
    }
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(resultado_completo, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 Análisis completo guardado en: {filename}")
    print(f"🎉 ANÁLISIS DE VOLUMEN COMPLETADO")

if __name__ == "__main__":
    main()