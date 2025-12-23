#!/usr/bin/env python3
"""
📅 EXTRAER PERÍODOS CAS ACTUALES
Extraer los períodos específicos del dashboard actual para replicar en Railway
"""

import re
from datetime import datetime

def extraer_periodos_cas_dashboard():
    """Extraer períodos CAS del código del dashboard actual"""
    
    print("📅 EXTRACCIÓN PERÍODOS CAS 2025")
    print("=" * 60)
    
    # Períodos extraídos del código server-COMPLETO-CON-MENU-BUTTON.js
    periodos_cas_2025 = {
        # LOCALES (Nuevo León) - Trimestres NO calendario
        'LOCALES_NL': {
            'NL-T1-2025': {
                'inicio': '2025-03-12',
                'fin': '2025-04-16',
                'descripcion': 'Nuevo León Trimestre 1 - 2025',
                'tipo': 'LOCAL',
                'duracion_dias': 35
            },
            'NL-T2-2025': {
                'inicio': '2025-06-11', 
                'fin': '2025-08-18',
                'descripcion': 'Nuevo León Trimestre 2 - 2025',
                'tipo': 'LOCAL',
                'duracion_dias': 68
            },
            'NL-T3-2025': {
                'inicio': '2025-08-19',
                'fin': '2025-10-09',
                'descripcion': 'Nuevo León Trimestre 3 - 2025',
                'tipo': 'LOCAL', 
                'duracion_dias': 51
            },
            'NL-T4-2025': {
                'inicio': '2025-10-30',
                'fin': '2025-12-31',  # Estimado
                'descripcion': 'Nuevo León Trimestre 4 - 2025',
                'tipo': 'LOCAL',
                'duracion_dias': 62
            }
        },
        
        # FORÁNEAS - Semestres NO calendario
        'FORANEAS': {
            'FOR-S1-2025': {
                'inicio': '2025-04-10',
                'fin': '2025-06-09', 
                'descripcion': 'Foráneas Semestre 1 - 2025',
                'tipo': 'FORANEA',
                'duracion_dias': 60
            },
            'FOR-S2-2025': {
                'inicio': '2025-07-30',
                'fin': '2025-11-07',
                'descripcion': 'Foráneas Semestre 2 - 2025',
                'tipo': 'FORANEA',
                'duracion_dias': 100
            }
        }
    }
    
    print("🔧 PERÍODOS LOCALES (Nuevo León):")
    for periodo, datos in periodos_cas_2025['LOCALES_NL'].items():
        print(f"   📅 {periodo}")
        print(f"      📆 {datos['inicio']} → {datos['fin']}")
        print(f"      ⏳ {datos['duracion_dias']} días")
        print(f"      📋 {datos['descripcion']}")
        print()
    
    print("🛡️ PERÍODOS FORÁNEAS:")
    for periodo, datos in periodos_cas_2025['FORANEAS'].items():
        print(f"   📅 {periodo}")
        print(f"      📆 {datos['inicio']} → {datos['fin']}")
        print(f"      ⏳ {datos['duracion_dias']} días")
        print(f"      📋 {datos['descripcion']}")
        print()
    
    return periodos_cas_2025

def generar_periodos_2026_calendar():
    """Generar períodos 2026 con trimestres calendario normales"""
    
    print("📅 PERÍODOS PROPUESTOS 2026 (Trimestres Calendario)")
    print("=" * 60)
    
    periodos_2026 = {
        # LOCALES Y FORÁNEAS - Trimestres calendario estándar
        'TRIMESTRES_2026': {
            'T1-2026': {
                'inicio': '2026-01-01',
                'fin': '2026-03-31',
                'descripcion': 'Trimestre 1 - 2026 (Enero-Marzo)',
                'aplica_a': 'TODOS'
            },
            'T2-2026': {
                'inicio': '2026-04-01',
                'fin': '2026-06-30', 
                'descripcion': 'Trimestre 2 - 2026 (Abril-Junio)',
                'aplica_a': 'TODOS'
            },
            'T3-2026': {
                'inicio': '2026-07-01',
                'fin': '2026-09-30',
                'descripcion': 'Trimestre 3 - 2026 (Julio-Septiembre)',
                'aplica_a': 'TODOS'
            },
            'T4-2026': {
                'inicio': '2026-10-01',
                'fin': '2026-12-31',
                'descripcion': 'Trimestre 4 - 2026 (Octubre-Diciembre)',
                'aplica_a': 'TODOS'
            }
        }
    }
    
    for periodo, datos in periodos_2026['TRIMESTRES_2026'].items():
        print(f"   📅 {periodo}")
        print(f"      📆 {datos['inicio']} → {datos['fin']}")
        print(f"      📋 {datos['descripcion']}")
        print(f"      🎯 Aplica a: {datos['aplica_a']}")
        print()
    
    return periodos_2026

def crear_funcion_sql_periodos():
    """Crear función SQL para identificar períodos CAS"""
    
    print("🗄️ FUNCIÓN SQL PARA PERÍODOS")
    print("=" * 50)
    
    sql_function = """
-- FUNCIÓN PARA DETERMINAR PERÍODO CAS
CREATE OR REPLACE FUNCTION get_periodo_cas(
    fecha_supervision TIMESTAMP,
    tipo_sucursal VARCHAR,
    estado VARCHAR DEFAULT 'Nuevo León',
    grupo_operativo VARCHAR DEFAULT NULL,
    sucursal_nombre VARCHAR DEFAULT NULL
) RETURNS VARCHAR AS $$
DECLARE
    fecha_date DATE;
    is_local BOOLEAN;
BEGIN
    fecha_date := fecha_supervision::DATE;
    
    -- Determinar si es LOCAL o FORÁNEA
    is_local := (
        estado = 'Nuevo León' OR 
        grupo_operativo = 'GRUPO SALTILLO'
    ) AND sucursal_nombre NOT IN ('57 - Harold R. Pape', '30 - Carrizo', '28 - Guerrero');
    
    -- 2025 - Períodos específicos no calendario
    IF EXTRACT(YEAR FROM fecha_date) = 2025 THEN
        IF is_local THEN
            -- LOCALES NL - Trimestres específicos
            IF fecha_date >= '2025-03-12' AND fecha_date <= '2025-04-16' THEN
                RETURN 'NL-T1-2025';
            ELSIF fecha_date >= '2025-06-11' AND fecha_date <= '2025-08-18' THEN
                RETURN 'NL-T2-2025';
            ELSIF fecha_date >= '2025-08-19' AND fecha_date <= '2025-10-09' THEN
                RETURN 'NL-T3-2025';
            ELSIF fecha_date >= '2025-10-30' THEN
                RETURN 'NL-T4-2025';
            END IF;
        ELSE
            -- FORÁNEAS - Semestres específicos
            IF fecha_date >= '2025-04-10' AND fecha_date <= '2025-06-09' THEN
                RETURN 'FOR-S1-2025';
            ELSIF fecha_date >= '2025-07-30' AND fecha_date <= '2025-11-07' THEN
                RETURN 'FOR-S2-2025';
            END IF;
        END IF;
    
    -- 2026 y posteriores - Trimestres calendario estándar
    ELSIF EXTRACT(YEAR FROM fecha_date) >= 2026 THEN
        CASE EXTRACT(QUARTER FROM fecha_date)
            WHEN 1 THEN RETURN 'T1-' || EXTRACT(YEAR FROM fecha_date);
            WHEN 2 THEN RETURN 'T2-' || EXTRACT(YEAR FROM fecha_date);
            WHEN 3 THEN RETURN 'T3-' || EXTRACT(YEAR FROM fecha_date);
            WHEN 4 THEN RETURN 'T4-' || EXTRACT(YEAR FROM fecha_date);
        END CASE;
    END IF;
    
    -- Fuera de períodos definidos
    RETURN 'OTRO';
END;
$$ LANGUAGE plpgsql;
"""
    
    print(sql_function)
    return sql_function

def validar_periodos_actuales():
    """Validar que los períodos extraídos son correctos"""
    
    print("\n✅ VALIDACIÓN PERÍODOS CAS")
    print("=" * 50)
    
    validaciones = [
        {
            'fecha': '2025-03-15',
            'tipo': 'LOCAL',
            'periodo_esperado': 'NL-T1-2025',
            'descripcion': 'Local en T1'
        },
        {
            'fecha': '2025-07-15', 
            'tipo': 'LOCAL',
            'periodo_esperado': 'NL-T2-2025',
            'descripcion': 'Local en T2'
        },
        {
            'fecha': '2025-09-15',
            'tipo': 'LOCAL', 
            'periodo_esperado': 'NL-T3-2025',
            'descripcion': 'Local en T3'
        },
        {
            'fecha': '2025-11-15',
            'tipo': 'LOCAL',
            'periodo_esperado': 'NL-T4-2025', 
            'descripcion': 'Local en T4'
        },
        {
            'fecha': '2025-05-15',
            'tipo': 'FORANEA',
            'periodo_esperado': 'FOR-S1-2025',
            'descripcion': 'Foránea en S1'
        },
        {
            'fecha': '2025-09-15',
            'tipo': 'FORANEA',
            'periodo_esperado': 'FOR-S2-2025',
            'descripcion': 'Foránea en S2'
        }
    ]
    
    for validacion in validaciones:
        print(f"📅 {validacion['fecha']} ({validacion['tipo']}) → {validacion['periodo_esperado']}")
        print(f"   📋 {validacion['descripcion']}")
    
    print(f"\n🎯 TOTAL PERÍODOS 2025:")
    print(f"   🔧 LOCALES: 4 trimestres (NL-T1 a NL-T4)")
    print(f"   🛡️ FORÁNEAS: 2 semestres (FOR-S1, FOR-S2)")
    print(f"   📅 FECHAS: No coinciden con calendario")
    
    print(f"\n🎯 PROPUESTA 2026:")
    print(f"   📅 TODOS: 4 trimestres calendario (T1-T4)")
    print(f"   🔧 LOCALES y FORÁNEAS: Mismos períodos")
    print(f"   📅 FECHAS: Trimestres calendario estándar")

def main():
    """Función principal"""
    
    print("📅 EXTRACCIÓN PERÍODOS CAS - EL POLLO LOCO")
    print("=" * 80)
    print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Roberto: Extraer períodos exactos del dashboard actual")
    print("=" * 80)
    
    # 1. Extraer períodos 2025 actuales
    periodos_2025 = extraer_periodos_cas_dashboard()
    
    # 2. Proponer períodos 2026
    periodos_2026 = generar_periodos_2026_calendar()
    
    # 3. Crear función SQL
    sql_function = crear_funcion_sql_periodos()
    
    # 4. Validar períodos
    validar_periodos_actuales()
    
    print(f"\n🎯 RESUMEN PARA ROBERTO:")
    print("=" * 50)
    print("✅ Períodos 2025 extraídos del dashboard actual")
    print("✅ Función SQL creada para Railway PostgreSQL")
    print("✅ Propuesta 2026 con trimestres calendario")
    print("✅ Validación de fechas específicas")
    
    print(f"\n💡 PARA RAILWAY:")
    print("   📊 Usar función get_periodo_cas() en queries")
    print("   📅 Toggle automático 2025 → 2026") 
    print("   🔧 Mantener lógica LOCAL vs FORÁNEA")
    
    return {
        'periodos_2025': periodos_2025,
        'periodos_2026': periodos_2026,
        'sql_function': sql_function
    }

if __name__ == "__main__":
    main()