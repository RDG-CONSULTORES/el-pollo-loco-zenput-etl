#!/usr/bin/env python3
"""
🚀 RAILWAY QUICK DEPLOY - ETL mínimo para verificar funcionamiento
Solo Flask básico para confirmar que Railway funciona
"""

from flask import Flask, jsonify
import os
from datetime import datetime

app = Flask(__name__)

@app.route('/')
def home():
    """Endpoint principal - confirmar Railway funciona"""
    return jsonify({
        'project': 'El Pollo Loco Zenput ETL - Quick Deploy',
        'status': 'RAILWAY_WORKING',
        'timestamp': datetime.now().isoformat(),
        'message': 'Railway deployment successful!',
        'port': os.environ.get('PORT', '8080'),
        'next_step': 'Add PostgreSQL and ETL functionality'
    })

@app.route('/health')
def health():
    """Health check básico"""
    return jsonify({
        'status': 'healthy',
        'service': 'Railway Flask App',
        'timestamp': datetime.now().isoformat()
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    print(f"🚀 Quick Deploy Flask starting on port {port}")
    app.run(host='0.0.0.0', port=port)