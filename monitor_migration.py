#!/usr/bin/env python3
"""
🔄 MONITOR MIGRACIÓN RAILWAY
Verificar si la migración sigue funcionando
Roberto: Monitor automático del progreso
"""

import requests
import time
import json
from datetime import datetime

def monitor_migration():
    """Monitorear migración Railway"""
    
    print("🔄 MONITOR MIGRACIÓN RAILWAY")
    print("=" * 40)
    
    railway_url = "https://el-pollo-loco-zenput-etl-production.up.railway.app"
    
    # Obtener estado inicial
    try:
        response = requests.get(f"{railway_url}/api/stats", timeout=10)
        stats_inicial = response.json()
        
        print(f"📊 ESTADO ACTUAL:")
        print(f"   🔧 Operativas: {stats_inicial['operativas']}")
        print(f"   🛡️ Seguridad: {stats_inicial['seguridad']}")
        print(f"   📊 Total: {int(stats_inicial['operativas']) + int(stats_inicial['seguridad'])}")
        
        # Verificar si hay migración activa
        print(f"\n🔍 VERIFICANDO MIGRACIÓN ACTIVA...")
        
        for i in range(5):
            time.sleep(10)  # Esperar 10 segundos
            
            response = requests.get(f"{railway_url}/api/stats", timeout=10)
            stats_actual = response.json()
            
            op_actual = int(stats_actual['operativas'])
            seg_actual = int(stats_actual['seguridad'])
            total_actual = op_actual + seg_actual
            
            op_inicial = int(stats_inicial['operativas'])
            seg_inicial = int(stats_inicial['seguridad'])
            total_inicial = op_inicial + seg_inicial
            
            cambio_op = op_actual - op_inicial
            cambio_seg = seg_actual - seg_inicial
            cambio_total = total_actual - total_inicial
            
            print(f"   📈 Check {i+1}: Op:{op_actual}(+{cambio_op}) Seg:{seg_actual}(+{cambio_seg}) Total:{total_actual}(+{cambio_total})")
            
            if cambio_total > 0:
                print(f"\n✅ MIGRACIÓN ACTIVA DETECTADA")
                print(f"   📊 Incremento: +{cambio_total} supervisiones")
                return True
        
        print(f"\n❌ MIGRACIÓN DETENIDA")
        print(f"   📊 Sin cambios en 50 segundos")
        
        # Mostrar faltantes
        objetivo_op = 238
        objetivo_seg = 238
        objetivo_total = 476
        
        faltante_op = objetivo_op - op_actual
        faltante_seg = objetivo_seg - seg_actual
        faltante_total = objetivo_total - total_actual
        
        progreso = (total_actual / objetivo_total) * 100
        
        print(f"\n📊 PROGRESO ACTUAL:")
        print(f"   🎯 Completado: {total_actual}/{objetivo_total} ({progreso:.1f}%)")
        print(f"   🔧 Faltante operativas: {faltante_op}")
        print(f"   🛡️ Faltante seguridad: {faltante_seg}")
        
        if faltante_total > 0:
            print(f"\n🚀 REANUDAR MIGRACIÓN:")
            print(f"   💡 python3 continue_migration.py")
        else:
            print(f"\n🎉 MIGRACIÓN 100% COMPLETA")
        
        return False
        
    except Exception as e:
        print(f"❌ Error monitoreando: {str(e)}")
        return None

if __name__ == "__main__":
    monitor_migration()