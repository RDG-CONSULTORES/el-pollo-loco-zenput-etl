#!/usr/bin/env python3
"""
🧪 TEST LOCAL - Verificar que main.py funciona correctamente
"""

import subprocess
import time
import requests
import signal
import os
import sys
from threading import Thread

def test_flask_app():
    """Test de la aplicación Flask localmente"""
    
    print("🧪 INICIANDO TEST LOCAL DE FLASK APP")
    print("=" * 50)
    
    # Configurar variable de entorno para test
    os.environ['DATABASE_URL'] = 'postgresql://postgres:qGgdIUuKYKMKGtSNYzARpyapBWHsloOt@turntable.proxy.rlwy.net:24097/railway'
    
    # Iniciar Flask en background
    print("🚀 Iniciando Flask app en puerto 5000...")
    
    def run_flask():
        subprocess.run([sys.executable, 'main.py'], env={**os.environ, 'PORT': '5000'})
    
    flask_thread = Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()
    
    # Esperar a que Flask arranque
    print("⏳ Esperando que Flask arranque...")
    time.sleep(3)
    
    # Test endpoints
    base_url = "http://localhost:5000"
    endpoints = [
        "/",
        "/health", 
        "/database"
    ]
    
    print("🔍 PROBANDO ENDPOINTS:")
    print("-" * 30)
    
    for endpoint in endpoints:
        try:
            url = f"{base_url}{endpoint}"
            print(f"📡 Testing {endpoint}...")
            
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                print(f"✅ {endpoint} - OK ({response.status_code})")
                if endpoint == "/":
                    data = response.json()
                    print(f"   📊 Project: {data.get('project', 'N/A')}")
                    print(f"   📊 Status: {data.get('status', 'N/A')}")
            else:
                print(f"❌ {endpoint} - ERROR ({response.status_code})")
                
        except Exception as e:
            print(f"❌ {endpoint} - FAILED: {str(e)}")
            
        time.sleep(0.5)
    
    print("\n" + "=" * 50)
    print("🏁 TEST LOCAL COMPLETADO")
    
if __name__ == '__main__':
    test_flask_app()