#!/usr/bin/env python3
"""
🚀 EL POLLO LOCO ZENPUT ETL - RAILWAY ENTRY POINT
Entry point para Railway deployment con endpoints web básicos
"""

from flask import Flask, jsonify
import os
import psycopg2
from datetime import datetime

app = Flask(__name__)

# Configuración desde variables de entorno Railway
DATABASE_URL = os.environ.get('DATABASE_URL')

@app.route('/')
def home():
    """Endpoint principal - status del sistema"""
    return jsonify({
        'project': 'El Pollo Loco Zenput ETL',
        'status': 'active',
        'timestamp': datetime.now().isoformat(),
        'description': 'ETL System for El Pollo Loco México - Zenput API Integration',
        'version': '1.0.0',
        'services': {
            'database': 'PostgreSQL on Railway',
            'api': 'Zenput API v3',
            'etl': 'Python Scripts'
        },
        'endpoints': {
            '/': 'System status',
            '/health': 'Health check',
            '/database': 'Database connection test',
            '/stats': 'ETL statistics'
        }
    })

@app.route('/health')
def health():
    """Health check endpoint"""
    try:
        # Test database connection
        if DATABASE_URL:
            conn = psycopg2.connect(DATABASE_URL)
            cursor = conn.cursor()
            cursor.execute("SELECT 1;")
            cursor.close()
            conn.close()
            db_status = "connected"
        else:
            db_status = "no_credentials"
        
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'database': db_status,
            'environment': 'Railway'
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/database')
def database_test():
    """Test database connection and show basic info"""
    if not DATABASE_URL:
        return jsonify({
            'error': 'DATABASE_URL not configured',
            'message': 'Configure PostgreSQL service in Railway'
        }), 400
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Test basic queries
        cursor.execute("SELECT version();")
        postgres_version = cursor.fetchone()[0]
        
        cursor.execute("SELECT current_database();")
        current_db = cursor.fetchone()[0]
        
        # Check if our tables exist
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'status': 'connected',
            'postgres_version': postgres_version,
            'database': current_db,
            'tables_count': len(tables),
            'tables': tables,
            'schema_ready': 'grupos_operativos' in tables,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/stats')
def etl_stats():
    """Show ETL statistics if database is ready"""
    if not DATABASE_URL:
        return jsonify({'error': 'DATABASE_URL not configured'}), 400
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        stats = {}
        
        # Check if tables exist
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name IN 
            ('grupos_operativos', 'sucursales', 'supervisions')
            ORDER BY table_name;
        """)
        existing_tables = [row[0] for row in cursor.fetchall()]
        
        if 'grupos_operativos' in existing_tables:
            cursor.execute("SELECT COUNT(*) FROM grupos_operativos;")
            stats['grupos_operativos'] = cursor.fetchone()[0]
        
        if 'sucursales' in existing_tables:
            cursor.execute("SELECT COUNT(*) FROM sucursales;")
            stats['sucursales'] = cursor.fetchone()[0]
        
        if 'supervisions' in existing_tables:
            cursor.execute("SELECT COUNT(*) FROM supervisions;")
            stats['total_supervisions'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM supervisions WHERE form_type = 'OPERATIVA';")
            stats['supervisions_operativa'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM supervisions WHERE form_type = 'SEGURIDAD';")
            stats['supervisions_seguridad'] = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'status': 'success',
            'existing_tables': existing_tables,
            'stats': stats,
            'schema_ready': len(existing_tables) == 3,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)