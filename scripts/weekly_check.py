#!/usr/bin/env python3
"""
🗓️ VERIFICACIÓN SEMANAL - EL POLLO LOCO MÉXICO  
Auto-detección ligera de cambios estructurales
Ejecutar domingos a las 8:00 AM
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.zenput_api import create_zenput_client
from datetime import datetime
import json

def run_weekly_checks():
    """Ejecuta verificaciones semanales"""
    
    print("🗓️ VERIFICACIÓN SEMANAL - EL POLLO LOCO MÉXICO")
    print("=" * 60)
    print(f"📅 Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    client = create_zenput_client()
    
    if not client.validate_api_connection():
        print("❌ No se puede conectar a API Zenput")
        return False
    
    changes_detected = False
    
    # 1. Verificar nuevas sucursales (87, 88, 89...)
    print(f"\n🏪 VERIFICANDO NUEVAS SUCURSALES...")
    new_locations = client.check_new_locations(known_count=86)
    
    if new_locations:
        changes_detected = True
        print(f"🆕 {len(new_locations)} NUEVAS SUCURSALES ENCONTRADAS!")
        
        for loc in new_locations:
            print(f"   📍 {loc.get('name')} - {loc.get('city')}, {loc.get('state')}")
            print(f"      ID: {loc.get('id')}, Coordenadas: {loc.get('lat')}, {loc.get('lon')}")
    
    # 2. Verificar nuevos formularios (próximamente)
    print(f"\n📝 VERIFICANDO NUEVOS FORMULARIOS...")
    # TODO: Implementar cuando sea necesario
    print("   ℹ️ Función pendiente de implementar")
    
    # 3. Resumen semanal
    print(f"\n📊 RESUMEN SEMANAL:")
    
    if changes_detected:
        print(f"   ⚠️ Se detectaron cambios estructurales")
        print(f"   💡 Acción requerida: Actualizar configuraciones")
        
        # Guardar cambios detectados
        changes_data = {
            'timestamp': datetime.now().isoformat(),
            'new_locations': new_locations,
            'action_required': True
        }
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        changes_file = f"../data/weekly_changes_{timestamp}.json"
        
        with open(changes_file, 'w', encoding='utf-8') as f:
            json.dump(changes_data, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"   💾 Cambios guardados en: {changes_file}")
        
        # TODO: Enviar alerta a Roberto/Eduardo
        print(f"   📧 [PENDIENTE] Enviar alerta a equipo")
        
    else:
        print(f"   ✅ No se detectaron cambios")
        print(f"   🎯 Sistema estable con 86 sucursales")
    
    print(f"\n✅ VERIFICACIÓN SEMANAL COMPLETADA")
    print("=" * 60)
    
    return True

def main():
    """Función principal"""
    
    try:
        success = run_weekly_checks()
        if success:
            print("🎉 Verificación semanal exitosa")
            sys.exit(0)
        else:
            print("❌ Verificación semanal falló")
            sys.exit(1)
    except Exception as e:
        print(f"💥 Error crítico: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()