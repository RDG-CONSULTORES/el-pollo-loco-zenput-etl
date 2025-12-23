#!/usr/bin/env python3
"""
🔄 FASE 1B: EXTRACCIÓN CON FILTROS DE FECHA ESPECÍFICOS
Usar filtros de fecha de la API para encontrar todas las 238+238 submissions
"""

import requests
import json
import pandas as pd
from datetime import datetime, date, timedelta
import time

ZENPUT_CONFIG = {
    'base_url': 'https://www.zenput.com/api/v3',
    'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}
}

FORMULARIOS_OBJETIVO = {
    '877138': 'OPERATIVA',
    '877139': 'SEGURIDAD'
}

def generar_rangos_fechas_2025():
    """Generar rangos de fechas específicos para 2025"""
    
    # Fecha inicial: 12 marzo 2025
    fecha_inicio = date(2025, 3, 12)
    fecha_fin = date(2025, 12, 31)
    
    # Generar rangos mensuales
    rangos = []
    
    current = fecha_inicio
    while current <= fecha_fin:
        # Primer día del mes (o fecha_inicio si es marzo)
        if current.month == 3:
            mes_inicio = fecha_inicio
        else:
            mes_inicio = current.replace(day=1)
        
        # Último día del mes
        if current.month == 12:
            mes_fin = fecha_fin
        else:
            # Último día del mes actual
            if current.month == 12:
                mes_fin = current.replace(day=31)
            else:
                next_month = current.replace(month=current.month + 1, day=1)
                mes_fin = next_month - timedelta(days=1)
        
        rangos.append({
            'inicio': mes_inicio,
            'fin': mes_fin,
            'nombre': current.strftime('%Y-%m')
        })
        
        # Siguiente mes
        if current.month == 12:
            break
        current = current.replace(month=current.month + 1)
    
    return rangos

def extraer_submissions_con_filtro_fecha(form_id, tipo_form, fecha_inicio, fecha_fin):
    """Extraer submissions con filtros de fecha específicos"""
    
    print(f"   📅 Rango: {fecha_inicio} a {fecha_fin}")
    
    submissions_unicas = {}
    page = 1
    max_pages = 20  # Límite de seguridad
    
    while page <= max_pages:
        try:
            url = f"{ZENPUT_CONFIG['base_url']}/submissions"
            params = {
                'form_template_id': form_id,
                'page': page,
                'page_size': 100,
                'start_date': fecha_inicio.strftime('%Y-%m-%d'),
                'end_date': fecha_fin.strftime('%Y-%m-%d')
            }
            
            response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                submissions = data.get('data', [])
                
                if not submissions:
                    break
                
                print(f"      📄 Página {page}: {len(submissions)} submissions", end="")
                
                nuevas = 0
                for submission in submissions:
                    submission_id = submission.get('id')
                    
                    if submission_id not in submissions_unicas:
                        submission['form_type'] = tipo_form
                        submissions_unicas[submission_id] = submission
                        nuevas += 1
                
                print(f" → {nuevas} nuevas")
                
                page += 1
                time.sleep(0.1)
                
            else:
                print(f"      ❌ Error HTTP {response.status_code}")
                break
                
        except Exception as e:
            print(f"      💥 Error: {e}")
            break
    
    return list(submissions_unicas.values())

def extraer_todas_submissions_por_rangos():
    """Extraer todas las submissions usando rangos de fechas"""
    
    print("🔄 FASE 1B: EXTRACCIÓN CON FILTROS DE FECHA")
    print("=" * 80)
    
    # Generar rangos de fechas
    rangos_fechas = generar_rangos_fechas_2025()
    print(f"📅 Rangos de fechas generados: {len(rangos_fechas)}")
    for rango in rangos_fechas:
        print(f"   {rango['nombre']}: {rango['inicio']} a {rango['fin']}")
    
    todas_submissions = {}  # Global por submission_id
    
    for form_id, tipo_form in FORMULARIOS_OBJETIVO.items():
        print(f"\n📋 EXTRAYENDO {tipo_form} (Form {form_id})")
        print("-" * 60)
        
        submissions_form = {}
        
        for rango in rangos_fechas:
            print(f"   📅 Procesando {rango['nombre']}:")
            submissions_rango = extraer_submissions_con_filtro_fecha(
                form_id, tipo_form, rango['inicio'], rango['fin']
            )
            
            for submission in submissions_rango:
                submission_id = submission.get('id')
                if submission_id not in submissions_form:
                    submissions_form[submission_id] = submission
            
            print(f"      ✅ Total únicas en {rango['nombre']}: {len(submissions_rango)}")
            print(f"      📊 Acumuladas {tipo_form}: {len(submissions_form)}")
        
        # Agregar al total global
        for submission_id, submission in submissions_form.items():
            todas_submissions[submission_id] = submission
        
        print(f"\n📊 RESUMEN {tipo_form}:")
        print(f"   ✅ Total únicas: {len(submissions_form)}")
    
    # Verificar fechas de submissions encontradas
    submissions_list = list(todas_submissions.values())
    
    if submissions_list:
        fechas_encontradas = []
        for submission in submissions_list:
            smetadata = submission.get('smetadata', {})
            fecha_submitted = smetadata.get('date_submitted')
            if fecha_submitted:
                try:
                    fecha_dt = datetime.fromisoformat(fecha_submitted.replace('Z', '+00:00'))
                    fechas_encontradas.append(fecha_dt)
                except:
                    pass
        
        if fechas_encontradas:
            fecha_min = min(fechas_encontradas)
            fecha_max = max(fechas_encontradas)
            print(f"\n📅 RANGO REAL DE FECHAS ENCONTRADAS:")
            print(f"   📅 Primera: {fecha_min.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   📅 Última: {fecha_max.strftime('%Y-%m-%d %H:%M:%S')}")
    
    return submissions_list

def investigar_api_parameters():
    """Investigar qué parámetros acepta la API de submissions"""
    
    print("\n🔍 INVESTIGANDO PARÁMETROS DE LA API")
    print("=" * 50)
    
    # Probar diferentes combinaciones de parámetros
    form_id = '877138'  # Operativa
    
    parametros_a_probar = [
        # Solo form
        {'form_template_id': form_id, 'page': 1, 'page_size': 5},
        
        # Con fechas
        {'form_template_id': form_id, 'page': 1, 'page_size': 5, 
         'start_date': '2025-03-01', 'end_date': '2025-03-31'},
        
        # Con fecha submitted
        {'form_template_id': form_id, 'page': 1, 'page_size': 5,
         'date_submitted_start': '2025-03-01', 'date_submitted_end': '2025-03-31'},
        
        # Con created_at
        {'form_template_id': form_id, 'page': 1, 'page_size': 5,
         'created_at_start': '2025-03-01', 'created_at_end': '2025-03-31'},
        
        # Con order by
        {'form_template_id': form_id, 'page': 1, 'page_size': 5,
         'order_by': 'created_at'},
        
        # Con sort
        {'form_template_id': form_id, 'page': 1, 'page_size': 5,
         'sort': 'created_at'},
    ]
    
    for i, params in enumerate(parametros_a_probar):
        print(f"\n🧪 Prueba {i+1}: {params}")
        
        try:
            url = f"{ZENPUT_CONFIG['base_url']}/submissions"
            response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                submissions = data.get('data', [])
                count = data.get('count', 'N/A')
                
                print(f"   ✅ Éxito: {len(submissions)} submissions, count: {count}")
                
                # Mostrar fechas de las submissions obtenidas
                if submissions:
                    fechas = []
                    for sub in submissions[:3]:
                        smetadata = sub.get('smetadata', {})
                        fecha = smetadata.get('date_submitted')
                        if fecha:
                            fechas.append(fecha[:10])  # Solo fecha, sin hora
                    print(f"   📅 Fechas muestra: {fechas}")
                
            else:
                print(f"   ❌ Error: {response.status_code} - {response.text[:100]}")
                
        except Exception as e:
            print(f"   💥 Excepción: {e}")
        
        time.sleep(0.5)

def buscar_submissions_mas_antiguas():
    """Buscar submissions más antiguas para entender el rango real"""
    
    print("\n🕰️ BUSCANDO SUBMISSIONS MÁS ANTIGUAS")
    print("=" * 50)
    
    form_id = '877138'
    
    # Probar con rangos de fechas más amplios hacia atrás
    rangos_a_probar = [
        ('2024-01-01', '2024-12-31', '2024 completo'),
        ('2025-01-01', '2025-02-28', 'Enero-Febrero 2025'),
        ('2025-03-01', '2025-03-31', 'Marzo 2025'),
        ('2025-11-01', '2025-12-31', 'Noviembre-Diciembre 2025'),
    ]
    
    for inicio, fin, descripcion in rangos_a_probar:
        print(f"\n📅 Probando {descripcion} ({inicio} a {fin}):")
        
        try:
            url = f"{ZENPUT_CONFIG['base_url']}/submissions"
            params = {
                'form_template_id': form_id,
                'page': 1,
                'page_size': 10,
                'start_date': inicio,
                'end_date': fin
            }
            
            response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                submissions = data.get('data', [])
                print(f"   ✅ Encontradas: {len(submissions)} submissions")
                
                if submissions:
                    # Mostrar fechas y IDs
                    for sub in submissions[:3]:
                        sub_id = sub.get('id')
                        smetadata = sub.get('smetadata', {})
                        fecha = smetadata.get('date_submitted', '')[:16]  # Sin segundos
                        print(f"      {sub_id}: {fecha}")
                        
            else:
                print(f"   ❌ Error: {response.status_code}")
                
        except Exception as e:
            print(f"   💥 Error: {e}")

def main():
    """Función principal - Fase 1B"""
    
    print("🔄 INICIANDO FASE 1B: EXTRACCIÓN CON FILTROS ESPECÍFICOS")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 1. Investigar parámetros de API
    investigar_api_parameters()
    
    # 2. Buscar submissions más antiguas
    buscar_submissions_mas_antiguas()
    
    # 3. Extraer con rangos de fechas
    print(f"\n" + "=" * 80)
    todas_submissions = extraer_todas_submissions_por_rangos()
    
    # 4. Resumen final
    operativas = [s for s in todas_submissions if s.get('form_type') == 'OPERATIVA']
    seguridad = [s for s in todas_submissions if s.get('form_type') == 'SEGURIDAD']
    
    print(f"\n🎉 FASE 1B COMPLETADA")
    print("=" * 50)
    print(f"📊 RESULTADOS FINALES:")
    print(f"   📋 Total submissions: {len(todas_submissions)}")
    print(f"   📊 Operativas: {len(operativas)}")
    print(f"   📊 Seguridad: {len(seguridad)}")
    
    # Comparar con objetivo
    objetivo = 476
    print(f"\n🎯 vs OBJETIVO (238+238=476):")
    if len(todas_submissions) == objetivo:
        print(f"   🎉 ¡PERFECTO! Encontramos exactamente las {objetivo} submissions")
    elif len(todas_submissions) < objetivo:
        print(f"   ⚠️ FALTANTES: {objetivo - len(todas_submissions)} submissions")
        print(f"      Necesitamos investigar otros períodos o parámetros")
    else:
        print(f"   ℹ️ EXTRAS: {len(todas_submissions) - objetivo} submissions adicionales")
    
    # Guardar resultados básicos
    if todas_submissions:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        datos_basicos = []
        for submission in todas_submissions:
            smetadata = submission.get('smetadata', {})
            location = smetadata.get('location', {})
            
            datos_basicos.append({
                'submission_id': submission.get('id'),
                'form_type': submission.get('form_type'),
                'location_name': location.get('name'),
                'tiene_location': bool(location.get('name')),
                'fecha_submitted': smetadata.get('date_submitted', '')[:16]
            })
        
        filename = f"FASE1B_INVESTIGACION_{timestamp}.csv"
        pd.DataFrame(datos_basicos).to_csv(filename, index=False, encoding='utf-8')
        print(f"\n📁 Resultados guardados en: {filename}")
    
    return todas_submissions

if __name__ == "__main__":
    main()