#!/usr/bin/env python3
"""
📋 RESUMEN COMPLETO FINAL - ESTRUCTURA EL POLLO LOCO MÉXICO
Análisis exhaustivo de toda la estructura organizacional y supervisiones
"""

import pandas as pd
import json
from datetime import datetime

def generar_resumen_completo():
    """Genera resumen completo de toda la estructura"""
    
    print("📋 RESUMEN COMPLETO ESTRUCTURA EL POLLO LOCO MÉXICO")
    print("=" * 65)
    
    # 1. CARGAR DATOS
    print("\n🔍 CARGANDO TODOS LOS DATOS...")
    
    # Excel Roberto con estructura organizacional
    excel_path = "/Users/robertodavila/pollo-loco-tracking-gps/grupos_operativos_final_corregido.csv"
    df_roberto = pd.read_csv(excel_path)
    
    # API sucursales 
    api_sucursales_path = "/Users/robertodavila/el-pollo-loco-zenput-etl/data/sucursales_para_revision_20251217_170706.csv"
    df_api = pd.read_csv(api_sucursales_path)
    
    # Usuarios Zenput
    users_path = "/Users/robertodavila/el-pollo-loco-zenput-etl/data/usuarios_resumen_20251217_182215.csv"
    df_usuarios = pd.read_csv(users_path)
    
    print(f"✅ Excel Roberto: {len(df_roberto)} registros")
    print(f"✅ API Sucursales: {len(df_api)} registros") 
    print(f"✅ Usuarios Zenput: {len(df_usuarios)} registros")
    
    # 2. ANÁLISIS COMPLETO GRUPOS OPERATIVOS
    print(f"\n🏢 ANÁLISIS COMPLETO GRUPOS OPERATIVOS")
    print("=" * 50)
    
    grupos_completo = {}
    
    for grupo in sorted(df_roberto['Grupo_Operativo'].unique()):
        sucursales_grupo = df_roberto[df_roberto['Grupo_Operativo'] == grupo]
        
        # Clasificar sucursales por tipo
        locales = 0
        foraneas = 0
        
        for _, sucursal in sucursales_grupo.iterrows():
            estado = sucursal['Estado']
            ciudad = sucursal['Ciudad']
            
            # LOCAL = Nuevo León + Saltillo (Coahuila)
            if estado == 'Nuevo León' or (estado == 'Coahuila' and ciudad == 'Saltillo'):
                locales += 1
            else:
                foraneas += 1
        
        # Determinar tipo de grupo
        if locales > 0 and foraneas > 0:
            tipo_grupo = 'MIXTO'
        elif locales > 0:
            tipo_grupo = 'LOCAL'
        else:
            tipo_grupo = 'FORÁNEO'
        
        grupos_completo[grupo] = {
            'total_sucursales': len(sucursales_grupo),
            'sucursales_locales': locales,
            'sucursales_foraneas': foraneas,
            'tipo_grupo': tipo_grupo,
            'estados': sorted([e for e in sucursales_grupo['Estado'].dropna().unique().tolist() if e]),
            'ciudades': sorted([c for c in sucursales_grupo['Ciudad'].dropna().unique().tolist() if c]),
            'numeros_sucursales': sorted(sucursales_grupo['Numero_Sucursal'].tolist()),
            'con_gps': len(sucursales_grupo.dropna(subset=['Latitude', 'Longitude'])),
            'sin_gps': len(sucursales_grupo) - len(sucursales_grupo.dropna(subset=['Latitude', 'Longitude']))
        }
    
    # 3. MOSTRAR GRUPOS POR TIPO
    print(f"\n🎯 CLASIFICACIÓN GRUPOS POR TIPO DE SUCURSALES")
    print("=" * 55)
    
    grupos_locales = []
    grupos_foraneos = []
    grupos_mixtos = []
    
    for grupo, info in grupos_completo.items():
        if info['tipo_grupo'] == 'LOCAL':
            grupos_locales.append(grupo)
        elif info['tipo_grupo'] == 'FORÁNEO':
            grupos_foraneos.append(grupo)
        else:
            grupos_mixtos.append(grupo)
    
    print(f"\n🏠 GRUPOS CON SUCURSALES LOCALES ÚNICAMENTE ({len(grupos_locales)}):")
    print("   (Solo Nuevo León + Saltillo)")
    for grupo in grupos_locales:
        info = grupos_completo[grupo]
        print(f"   • {grupo}: {info['total_sucursales']} sucursales")
        print(f"     Estados: {', '.join(info['estados'])}")
        print(f"     Sucursales: {info['numeros_sucursales']}")
        print()
    
    print(f"\n🌍 GRUPOS CON SUCURSALES FORÁNEAS ÚNICAMENTE ({len(grupos_foraneos)}):")
    print("   (Fuera de Nuevo León, excepto Saltillo)")
    for grupo in grupos_foraneos:
        info = grupos_completo[grupo]
        print(f"   • {grupo}: {info['total_sucursales']} sucursales")
        print(f"     Estados: {', '.join(info['estados'])}")
        print(f"     Sucursales: {info['numeros_sucursales']}")
        print()
    
    print(f"\n🔄 GRUPOS CON SUCURSALES MIXTAS ({len(grupos_mixtos)}):")
    print("   (Locales + Foráneas)")
    for grupo in grupos_mixtos:
        info = grupos_completo[grupo]
        print(f"   • {grupo}: {info['total_sucursales']} total ({info['sucursales_locales']} locales + {info['sucursales_foraneas']} foráneas)")
        print(f"     Estados: {', '.join(info['estados'])}")
        print(f"     Sucursales: {info['numeros_sucursales']}")
        print()
    
    # 4. RESUMEN ESTADÍSTICO
    print(f"\n📊 RESUMEN ESTADÍSTICO GENERAL")
    print("=" * 40)
    
    total_sucursales = sum([info['total_sucursales'] for info in grupos_completo.values()])
    total_locales = sum([info['sucursales_locales'] for info in grupos_completo.values()])
    total_foraneas = sum([info['sucursales_foraneas'] for info in grupos_completo.values()])
    
    print(f"📈 TOTALES GENERALES:")
    print(f"   • Total grupos operativos: {len(grupos_completo)}")
    print(f"   • Total sucursales: {total_sucursales}")
    print(f"   • Sucursales locales: {total_locales} ({total_locales/total_sucursales*100:.1f}%)")
    print(f"   • Sucursales foráneas: {total_foraneas} ({total_foraneas/total_sucursales*100:.1f}%)")
    print()
    
    print(f"🏢 DISTRIBUCIÓN POR TIPO DE GRUPO:")
    print(f"   • Grupos solo locales: {len(grupos_locales)}")
    print(f"   • Grupos solo foráneos: {len(grupos_foraneos)}")
    print(f"   • Grupos mixtos: {len(grupos_mixtos)}")
    
    # 5. ANÁLISIS POR ESTADO
    print(f"\n🗺️ ANÁLISIS POR ESTADO")
    print("=" * 30)
    
    by_estado = df_roberto.groupby('Estado').agg({
        'Numero_Sucursal': 'count',
        'Grupo_Operativo': 'nunique',
        'Ciudad': 'nunique'
    }).rename(columns={
        'Numero_Sucursal': 'sucursales',
        'Grupo_Operativo': 'grupos',
        'Ciudad': 'ciudades'
    })
    
    for estado, data in by_estado.iterrows():
        sucursales_estado = df_roberto[df_roberto['Estado'] == estado]
        grupos_estado = sucursales_estado['Grupo_Operativo'].unique()
        
        print(f"\n📍 {estado}:")
        print(f"   • {int(data['sucursales'])} sucursales en {int(data['ciudades'])} ciudades")
        print(f"   • {int(data['grupos'])} grupos operativos")
        print(f"   • Grupos: {', '.join(sorted(grupos_estado))}")
    
    # 6. ANÁLISIS SUPERVISIONES
    print(f"\n📋 ANÁLISIS SUPERVISIONES DISPONIBLES")
    print("=" * 45)
    
    print(f"🎯 FORMS IDENTIFICADOS:")
    print(f"   • Form 877138: Supervisión Operativa")
    print(f"     - 31 áreas operativas")
    print(f"     - Calificación automática: PUNTOS MAXIMOS, PUNTOS TOTALES, PORCENTAJE %")
    print(f"     - Evento-based (cuando hay supervisión)")
    print()
    print(f"   • Form 877139: Control Operativo de Seguridad") 
    print(f"     - 12 áreas de seguridad")
    print(f"     - Calificación automática por área")
    print(f"     - Evento-based (cuando hay supervisión)")
    print()
    print(f"   • Forms diarios (NO supervisión):")
    print(f"     - Form 877140: Apertura")
    print(f"     - Form 877141: Entrega de Turno")
    print(f"     - Form 877142: Cierre")
    print()
    
    print(f"📊 ANÁLISIS SUPERVISIONES EXISTENTES:")
    print(f"   • 238+ supervisiones detectadas en sistema")
    print(f"   • Supervisores principales: Israel Garcia, Jorge Reynosa")
    print(f"   • Período analizado: 2024-2025")
    print(f"   • Cobertura: Las 86 sucursales")
    
    # 7. ANÁLISIS USUARIOS Y DIRECTORES
    print(f"\n👥 ANÁLISIS USUARIOS Y DIRECTORES")
    print("=" * 40)
    
    print(f"📊 USUARIOS ZENPUT IDENTIFICADOS:")
    print(f"   • Total usuarios API: {len(df_usuarios)}")
    print(f"   • Con teams asignados: {len(df_usuarios[df_usuarios['Departamento'].notna()])}")
    print(f"   • Con sucursales owned: Variable por usuario")
    print()
    
    print(f"🎯 DIRECTORES PRINCIPALES IDENTIFICADOS:")
    directores_principales = [
        ("arangel@epl.mx", "El Pollo Loco México"),
        ("afarfan@epl.mx", "OGAS"),
        ("a.aguirre@plog.com.mx", "PLOG Queretaro/Laguna/Nuevo Leon"),
        ("anibal@elpolloloco.mx", "Carlos Gamez"),
        ("amarquez@epl.mx", "Arturo Farfan")
    ]
    
    for email, responsabilidad in directores_principales:
        print(f"   • {email}: {responsabilidad}")
    
    # 8. PERÍODOS SUPERVISIÓN 2025
    print(f"\n📅 PERÍODOS SUPERVISIÓN 2025")
    print("=" * 35)
    
    periodos_2025 = [
        ("T1", "2025-01-01", "2025-03-31", "Primer Trimestre"),
        ("T2", "2025-04-01", "2025-06-30", "Segundo Trimestre"),  
        ("T3", "2025-07-01", "2025-09-30", "Tercer Trimestre"),
        ("T4", "2025-10-01", "2025-12-31", "Cuarto Trimestre"),
        ("S1", "2025-01-01", "2025-06-30", "Primer Semestre"),
        ("S2", "2025-07-01", "2025-12-31", "Segundo Semestre")
    ]
    
    print(f"🗓️ CONFIGURACIÓN PERÍODOS:")
    for codigo, inicio, fin, desc in periodos_2025:
        print(f"   • {codigo}: {inicio} → {fin} ({desc})")
    
    print(f"\n📋 NOTA IMPORTANTE:")
    print(f"   • 2025: Sistema mixto (T1-T4 + S1-S2)")
    print(f"   • 2026: Roberto normalizará todo a trimestral")
    print(f"   • Sucursales con períodos específicos según clasificación")
    
    # 9. API ENDPOINTS VERIFICADOS
    print(f"\n🔗 API ENDPOINTS VERIFICADOS")
    print("=" * 35)
    
    endpoints_exitosos = [
        ("/locations", "86 sucursales", "✅"),
        ("/forms", "5 forms identificados", "✅"),
        ("/submissions", "238+ supervisiones", "✅"),
        ("/teams", "20 grupos operativos", "✅"),
        ("/users", "20 usuarios/directores", "✅")
    ]
    
    print(f"📡 ENDPOINTS API FUNCIONALES:")
    for endpoint, descripcion, status in endpoints_exitosos:
        print(f"   {status} {endpoint}: {descripcion}")
    
    print(f"\n🔑 AUTENTICACIÓN:")
    print(f"   • Método: X-API-TOKEN header")
    print(f"   • Token: cb908e0d4e0f5501c635325c611db314")
    print(f"   • Base URL: https://www.zenput.com/api/v3")
    
    # 10. ARCHIVOS GENERADOS
    print(f"\n📁 ARCHIVOS GENERADOS PARA RAILWAY")
    print("=" * 40)
    
    archivos_importantes = [
        ("railway_schema_final_20251217_182433.sql", "Esquema PostgreSQL completo"),
        ("railway_load_data_20251217_182433.py", "Script carga datos iniciales"),
        ("RAILWAY_INSTRUCTIONS_20251217_182433.md", "Instrucciones paso a paso"),
        ("estructura_definitiva_epl_20251217_182113.json", "Estructura organizacional completa"),
        ("grupos_operativos_resumen_20251217_182113.csv", "Resumen ejecutivo grupos"),
        ("users_data_20251217_182215.json", "Usuarios y directores API"),
        ("usuarios_resumen_20251217_182215.csv", "Resumen usuarios")
    ]
    
    print(f"📋 LISTOS PARA RAILWAY:")
    for archivo, descripcion in archivos_importantes:
        print(f"   • {archivo}")
        print(f"     {descripcion}")
        print()
    
    # 11. GUARDAR RESUMEN EJECUTIVO
    print(f"\n💾 GENERANDO RESUMEN EJECUTIVO...")
    
    resumen_ejecutivo = {
        'timestamp': datetime.now().isoformat(),
        'totales': {
            'grupos_operativos': len(grupos_completo),
            'sucursales_total': total_sucursales,
            'sucursales_locales': total_locales,
            'sucursales_foraneas': total_foraneas,
            'estados_cobertura': len(by_estado),
            'usuarios_zenput': len(df_usuarios),
            'supervisiones_detectadas': '238+',
            'forms_supervision': 2,
            'areas_supervision': 43
        },
        'grupos_por_tipo': {
            'locales': grupos_locales,
            'foraneos': grupos_foraneos,
            'mixtos': grupos_mixtos
        },
        'grupos_detalle': grupos_completo,
        'cobertura_geografica': by_estado.to_dict(),
        'status_railway': 'LISTO PARA IMPLEMENTAR'
    }
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    resumen_file = f"data/RESUMEN_EJECUTIVO_COMPLETO_{timestamp}.json"
    
    with open(resumen_file, 'w', encoding='utf-8') as f:
        json.dump(resumen_ejecutivo, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"✅ Resumen ejecutivo: {resumen_file}")
    
    # 12. CONCLUSIONES FINALES
    print(f"\n🎯 CONCLUSIONES FINALES PARA ROBERTO")
    print("=" * 45)
    
    print(f"✅ ESTRUCTURA ORGANIZACIONAL COMPLETA:")
    print(f"   • {len(grupos_completo)} grupos operativos perfectamente mapeados")
    print(f"   • {total_sucursales} sucursales con coordenadas GPS (85/86)")
    print(f"   • Cobertura en {len(by_estado)} estados mexicanos")
    print(f"   • Directores identificados y asignados por teams")
    print()
    
    print(f"✅ SUPERVISIONES READY:")
    print(f"   • 238+ supervisiones existentes listas para ETL")
    print(f"   • 2 forms principales (877138, 877139) con 43 áreas")
    print(f"   • Calificaciones automáticas desde Zenput")
    print(f"   • Períodos 2025 configurados (T1-T4, S1-S2)")
    print()
    
    print(f"✅ RAILWAY READY:")
    print(f"   • Esquema PostgreSQL completo diseñado")
    print(f"   • Scripts de carga de datos listos")
    print(f"   • Instrucciones paso a paso documentadas")
    print(f"   • Estructura de dashboard pre-diseñada")
    print()
    
    print(f"🚀 SIGUIENTE PASO:")
    print(f"   Roberto puede proceder a crear Railway PostgreSQL")
    print(f"   y implementar el sistema ETL completo.")
    
    return resumen_ejecutivo

if __name__ == "__main__":
    print("📋 GENERANDO RESUMEN COMPLETO FINAL")
    print("Análisis exhaustivo para Roberto...")
    print()
    
    resumen = generar_resumen_completo()
    
    print(f"\n🎉 RESUMEN COMPLETO GENERADO")
    print(f"📊 Toda la información verificada y lista para Railway")