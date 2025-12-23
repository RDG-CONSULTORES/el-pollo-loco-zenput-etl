#!/usr/bin/env python3
"""
📋 CASOS PENDIENTES PARA VALIDACIÓN MANUAL
Listado de sucursales con déficit para normalización final con Roberto
"""

import pandas as pd
import numpy as np
from datetime import datetime

def analizar_casos_pendientes():
    """Analizar casos pendientes que necesitan validación manual"""
    
    print("📋 CASOS PENDIENTES PARA VALIDACIÓN MANUAL")
    print("=" * 70)
    
    # Cargar distribuciones finales
    df_distribuciones = pd.read_csv("DISTRIBUCIONES_FINALES_20251218_140924.csv")
    
    # Cargar datos para análisis detallado
    df_ops = pd.read_excel("SUPERVISION_OPERATIVA_CAS_11_REV_250125_Submissions-2025-12-18__1228CST-1766141816.xlsx")
    df_seg = pd.read_excel("CONTROL_OPERATIVO_DE_SEGURIDAD_CAS_11_REV_25012025_Submissions-2025-12-18__1232CST-1766104009.xlsx")
    df_asignaciones = pd.read_csv("ASIGNACIONES_FINALES_CORREGIDAS_20251218_140924.csv")
    df_sucursales = pd.read_csv('SUCURSALES_MASTER_20251218_110913.csv')
    
    print(f"✅ Datos cargados para análisis")
    
    # Identificar casos con déficit
    casos_pendientes = []
    
    for _, row in df_distribuciones.iterrows():
        location = row['location']
        ops = int(row['OPERATIVA'])
        seg = int(row['SEGURIDAD'])
        total = ops + seg
        
        # Determinar tipo de sucursal
        tipo_sucursal = 'LOCAL'  # Default
        for _, suc_row in df_sucursales.iterrows():
            if pd.notna(suc_row['numero']) and pd.notna(suc_row['nombre']):
                numero = int(suc_row['numero'])
                nombre = suc_row['nombre']
                location_key = f"{numero} - {nombre}"
                if location_key == location:
                    tipo_sucursal = suc_row.get('tipo', 'LOCAL')
                    break
        
        # Reglas esperadas
        if tipo_sucursal == 'FORÁNEA':
            ops_esperadas = 2
            seg_esperadas = 2
            regla = '2+2'
        else:
            ops_esperadas = 4
            seg_esperadas = 4
            regla = '4+4'
        
        # Identificar casos con déficit
        if ops < ops_esperadas or seg < seg_esperadas or total > (ops_esperadas + seg_esperadas):
            # Obtener detalles adicionales
            detalles = obtener_detalles_sucursal(location, df_ops, df_seg, df_asignaciones)
            
            caso = {
                'location': location,
                'ops_actual': ops,
                'seg_actual': seg,
                'total_actual': total,
                'ops_esperadas': ops_esperadas,
                'seg_esperadas': seg_esperadas,
                'total_esperado': ops_esperadas + seg_esperadas,
                'regla': regla,
                'tipo': tipo_sucursal,
                'deficit_ops': max(0, ops_esperadas - ops),
                'deficit_seg': max(0, seg_esperadas - seg),
                'exceso_total': max(0, total - (ops_esperadas + seg_esperadas)),
                'detalles': detalles
            }
            
            casos_pendientes.append(caso)
    
    return casos_pendientes

def obtener_detalles_sucursal(location, df_ops, df_seg, df_asignaciones):
    """Obtener detalles específicos de una sucursal"""
    
    detalles = {
        'fechas_operativas': [],
        'fechas_seguridad': [],
        'usuarios_operativas': [],
        'usuarios_seguridad': [],
        'metodos_seguridad': []
    }
    
    # Operativas
    ops_sucursal = df_ops[df_ops['Location'] == location]
    if len(ops_sucursal) > 0:
        detalles['fechas_operativas'] = [str(fecha)[:10] for fecha in ops_sucursal['Date Submitted'] if pd.notna(fecha)]
        detalles['usuarios_operativas'] = list(ops_sucursal['Submitted By'].unique())
    
    # Seguridad con location
    seg_con_location = df_seg[df_seg['Location'] == location]
    if len(seg_con_location) > 0:
        detalles['fechas_seguridad'].extend([str(fecha)[:10] for fecha in seg_con_location['Date Submitted'] if pd.notna(fecha)])
        detalles['usuarios_seguridad'].extend(list(seg_con_location['Submitted By'].unique()))
        detalles['metodos_seguridad'].append('EXCEL_DIRECTO')
    
    # Seguridad asignada
    seg_asignada = df_asignaciones[df_asignaciones['sucursal_asignada'] == location]
    if len(seg_asignada) > 0:
        detalles['fechas_seguridad'].extend([str(fecha)[:10] for fecha in seg_asignada['fecha'] if pd.notna(fecha)])
        detalles['usuarios_seguridad'].extend(list(seg_asignada['usuario'].unique()))
        detalles['metodos_seguridad'].extend(list(seg_asignada['metodo'].unique()))
    
    # Limpiar duplicados
    detalles['usuarios_seguridad'] = list(set(detalles['usuarios_seguridad']))
    detalles['metodos_seguridad'] = list(set(detalles['metodos_seguridad']))
    
    return detalles

def mostrar_casos_para_validacion(casos_pendientes):
    """Mostrar casos pendientes en formato para validación con Roberto"""
    
    print(f"\n🎯 CASOS PENDIENTES PARA VALIDACIÓN CON ROBERTO")
    print("=" * 80)
    print(f"📊 Total casos: {len(casos_pendientes)}")
    
    # Categorizar casos
    deficit_operativas = [c for c in casos_pendientes if c['deficit_ops'] > 0]
    deficit_seguridad = [c for c in casos_pendientes if c['deficit_seg'] > 0 and c['deficit_ops'] == 0]
    exceso_casos = [c for c in casos_pendientes if c['exceso_total'] > 0]
    deficit_ambos = [c for c in casos_pendientes if c['deficit_ops'] > 0 and c['deficit_seg'] > 0]
    
    print(f"\n📋 CATEGORÍAS:")
    print(f"   ❌ Déficit solo operativas: {len(deficit_operativas)}")
    print(f"   ⚠️ Déficit solo seguridad: {len(deficit_seguridad)}")
    print(f"   ❌ Déficit ambos tipos: {len(deficit_ambos)}")
    print(f"   🔄 Exceso total: {len(exceso_casos)}")
    
    print(f"\n" + "="*100)
    print(f"📋 LISTADO COMPLETO PARA VALIDACIÓN MANUAL")
    print("="*100)
    print(f"{'#':<3} {'Sucursal':<35} {'Actual':<8} {'Esperado':<8} {'Problema':<15} {'Fechas':<12} {'Usuarios'}")
    print("-"*100)
    
    for i, caso in enumerate(casos_pendientes, 1):
        location = caso['location'][:34]
        actual = f"{caso['ops_actual']}+{caso['seg_actual']}={caso['total_actual']}"
        esperado = f"{caso['regla']}"
        
        # Problema
        if caso['exceso_total'] > 0:
            problema = f"EXCESO +{caso['exceso_total']}"
        elif caso['deficit_ops'] > 0 and caso['deficit_seg'] > 0:
            problema = f"-{caso['deficit_ops']}ops -{caso['deficit_seg']}seg"
        elif caso['deficit_ops'] > 0:
            problema = f"-{caso['deficit_ops']} ops"
        elif caso['deficit_seg'] > 0:
            problema = f"-{caso['deficit_seg']} seg"
        else:
            problema = "REVISAR"
        
        # Fechas (primeras 2)
        fechas = caso['detalles']['fechas_operativas'] + caso['detalles']['fechas_seguridad']
        fechas_unique = list(set(fechas))[:2]
        fechas_str = ','.join(fechas_unique) if fechas_unique else 'N/A'
        
        # Usuarios
        usuarios = list(set(caso['detalles']['usuarios_operativas'] + caso['detalles']['usuarios_seguridad']))
        usuarios_str = ','.join([u.split()[0] if u else 'N/A' for u in usuarios])[:15]
        
        print(f"{i:<3} {location:<35} {actual:<8} {esperado:<8} {problema:<15} {fechas_str[:12]:<12} {usuarios_str}")
    
    return casos_pendientes

def mostrar_detalles_caso_especifico(casos_pendientes, numero_caso):
    """Mostrar detalles de un caso específico"""
    
    if 1 <= numero_caso <= len(casos_pendientes):
        caso = casos_pendientes[numero_caso - 1]
        
        print(f"\n🔍 DETALLES CASO #{numero_caso}")
        print("=" * 50)
        print(f"🏪 Sucursal: {caso['location']}")
        print(f"📊 Actual: {caso['ops_actual']}+{caso['seg_actual']}={caso['total_actual']}")
        print(f"🎯 Esperado: {caso['regla']} = {caso['total_esperado']}")
        print(f"🏷️ Tipo: {caso['tipo']}")
        
        detalles = caso['detalles']
        
        print(f"\n📅 FECHAS OPERATIVAS ({len(detalles['fechas_operativas'])}):")
        for fecha in detalles['fechas_operativas']:
            print(f"   📅 {fecha}")
        
        print(f"\n📅 FECHAS SEGURIDAD ({len(detalles['fechas_seguridad'])}):")
        fechas_seg_unique = list(set(detalles['fechas_seguridad']))
        for fecha in fechas_seg_unique:
            print(f"   📅 {fecha}")
        
        print(f"\n👤 USUARIOS:")
        usuarios_todos = list(set(detalles['usuarios_operativas'] + detalles['usuarios_seguridad']))
        for usuario in usuarios_todos:
            print(f"   👤 {usuario}")
        
        print(f"\n🔧 MÉTODOS SEGURIDAD:")
        for metodo in detalles['metodos_seguridad']:
            print(f"   🔧 {metodo}")
        
        print(f"\n💭 ANÁLISIS:")
        if caso['deficit_ops'] > 0:
            print(f"   ❌ Faltan {caso['deficit_ops']} operativas")
        if caso['deficit_seg'] > 0:
            print(f"   ⚠️ Faltan {caso['deficit_seg']} seguridad")
        if caso['exceso_total'] > 0:
            print(f"   🔄 Exceso de {caso['exceso_total']} supervisiones")
        
        return caso
    else:
        print(f"❌ Caso #{numero_caso} no existe")
        return None

def procesar_validacion_roberto():
    """Procesar validación interactiva con Roberto"""
    
    print(f"\n🔧 VALIDACIÓN INTERACTIVA CON ROBERTO")
    print("=" * 50)
    
    print(f"💡 COMANDOS DISPONIBLES:")
    print(f"   📋 'detalle <número>' - Ver detalles de un caso específico")
    print(f"   ✅ 'ok <número>' - Marcar caso como correcto (no necesita cambios)")
    print(f"   🔧 'normalizar <número> <motivo>' - Explicar por qué está bien así")
    print(f"   📊 'resumen' - Ver resumen de casos validados")
    print(f"   ❌ 'problema <número> <descripción>' - Marcar problema real")
    
    print(f"\n🚨 CASOS MÁS IMPORTANTES (déficit operativas):")
    # Mostrar solo los casos más críticos primero
    casos_criticos = [
        "1 - Pino Suarez: 3+3=6 (esperado 4+4) - Falta 1 operativa",
        "2 - Madero: 3+3=6 (esperado 4+4) - Falta 1 operativa", 
        "3 - Matamoros: 3+3=6 (esperado 4+4) - Falta 1 operativa",
        "71 - Centrito Valle: 5+5=10 (esperado 4+4) - Exceso de 2"
    ]
    
    for caso in casos_criticos:
        print(f"   🚨 {caso}")

def generar_comandos_normalizacion():
    """Generar comandos de normalización sugeridos"""
    
    print(f"\n📋 COMANDOS DE NORMALIZACIÓN SUGERIDOS")
    print("=" * 60)
    
    # Casos típicos de normalización
    normalizaciones_sugeridas = [
        {
            'tipo': 'FORÁNEAS_CORRECTAS',
            'descripcion': 'Sucursales foráneas con 2+2=4 están correctas',
            'casos': ['28 - Guerrero', '30 - Carrizo', '42 - Independencia', '43 - Revolucion']
        },
        {
            'tipo': 'AÑO_INCOMPLETO',
            'descripcion': 'Supervisiones iniciaron en marzo, año no completo',
            'casos': ['23 - Guasave', '44 - Senderos', '45 - Triana']
        },
        {
            'tipo': 'SUCURSAL_NUEVA',
            'descripcion': 'Sucursales abiertas durante 2025',
            'casos': ['68 - Avenida del Niño', '69 - Puerto Rico']
        },
        {
            'tipo': 'CENTRITO_ESPECIAL',
            'descripcion': 'Caso especial de redistribución',
            'casos': ['71 - Centrito Valle']
        }
    ]
    
    for norm in normalizaciones_sugeridas:
        print(f"\n🏷️ {norm['tipo']}: {norm['descripcion']}")
        for caso in norm['casos']:
            print(f"   📍 {caso}")

def main():
    """Función principal"""
    
    print("📋 VALIDADOR DE CASOS PENDIENTES PARA ROBERTO")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Objetivo: Listar casos pendientes para validación manual")
    print("=" * 80)
    
    # 1. Analizar casos pendientes
    casos_pendientes = analizar_casos_pendientes()
    
    # 2. Mostrar casos para validación
    casos_mostrados = mostrar_casos_para_validacion(casos_pendientes)
    
    # 3. Generar sugerencias de normalización
    generar_comandos_normalizacion()
    
    # 4. Configurar validación interactiva
    procesar_validacion_roberto()
    
    # 5. Guardar para referencia
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    import json
    with open(f"CASOS_PENDIENTES_VALIDACION_{timestamp}.json", 'w', encoding='utf-8') as f:
        json.dump({
            'timestamp': timestamp,
            'total_casos': len(casos_pendientes),
            'casos_detallados': casos_pendientes
        }, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"\n📁 CASOS GUARDADOS: CASOS_PENDIENTES_VALIDACION_{timestamp}.json")
    print(f"\n🎯 LISTO PARA VALIDACIÓN: {len(casos_pendientes)} casos esperando confirmación")
    
    return casos_pendientes

if __name__ == "__main__":
    main()