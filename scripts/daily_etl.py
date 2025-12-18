#!/usr/bin/env python3
"""
📅 ETL DIARIO - EL POLLO LOCO MÉXICO
Extracción diaria de submissions de los 5 formularios críticos
Ejecutar diario a las 6:00 AM
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.zenput_api import create_zenput_client
from datetime import datetime
import json

def run_daily_etl():
    """Ejecuta ETL diario para todos los formularios"""
    
    print("🌅 INICIANDO ETL DIARIO - EL POLLO LOCO MÉXICO")
    print("=" * 60)
    print(f"📅 Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 1. Crear cliente API
    client = create_zenput_client()
    
    # 2. Validar conexión
    if not client.validate_api_connection():
        print("❌ FALLO: No se puede conectar a API Zenput")
        return False
    
    # 3. Extraer submissions diarias
    print(f"\n📊 EXTRAYENDO SUBMISSIONS DE 5 FORMULARIOS...")
    daily_data = client.get_daily_submissions()
    
    # 4. Guardar datos raw
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"../data/daily_submissions_{timestamp}.json"
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(daily_data, f, indent=2, ensure_ascii=False, default=str)
    
    # 5. Verificar calidad de datos
    total_submissions = daily_data['total_submissions']
    forms_with_data = len([f for f in daily_data['forms_data'].values() if f['submissions_count'] > 0])
    
    print(f"\n📊 RESULTADOS ETL:")
    print(f"   ✅ Total submissions: {total_submissions}")
    print(f"   📝 Formularios con datos: {forms_with_data}/5")
    print(f"   💾 Datos guardados en: {filename}")
    
    # 6. Alertas si es necesario
    if total_submissions == 0:
        print("⚠️ ALERTA: No se encontraron submissions hoy")
    elif forms_with_data < 3:
        print("⚠️ ALERTA: Pocos formularios reportaron datos")
    
    # 7. Verificación de sucursales inactivas  
    print(f"\n🔍 VERIFICANDO SUCURSALES INACTIVAS...")
    inactive = client.check_inactive_locations(daily_data)
    
    if len(inactive) > 5:  # Más de 5 sucursales sin reportar
        print(f"🚨 ALERTA: {len(inactive)} sucursales no reportaron")
    
    print(f"\n✅ ETL DIARIO COMPLETADO")
    print("=" * 60)
    
    return True

def main():
    """Función principal"""
    
    try:
        success = run_daily_etl()
        if success:
            print("🎉 ETL ejecutado exitosamente")
            sys.exit(0)
        else:
            print("❌ ETL falló")
            sys.exit(1)
    except Exception as e:
        print(f"💥 Error crítico: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()