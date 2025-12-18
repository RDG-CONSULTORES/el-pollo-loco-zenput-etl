#!/usr/bin/env python3
"""
⚙️ CONFIGURACIÓN SISTEMA ETL - EL POLLO LOCO MÉXICO
Configuraciones centralizadas del sistema
"""

import os
from datetime import time

class ETLSettings:
    """Configuraciones del sistema ETL"""
    
    # API Zenput
    ZENPUT_API_TOKEN = os.getenv('ZENPUT_API_TOKEN', 'cb908e0d4e0f5501c635325c611db314')
    ZENPUT_BASE_URL = 'https://www.zenput.com/api/v3'
    
    # PostgreSQL Railway (cuando esté listo)
    DB_HOST = os.getenv('RAILWAY_DB_HOST', 'localhost')
    DB_PORT = os.getenv('RAILWAY_DB_PORT', '5432')
    DB_NAME = os.getenv('RAILWAY_DB_NAME', 'epl_zenput_data')
    DB_USER = os.getenv('RAILWAY_DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('RAILWAY_DB_PASSWORD', '')
    
    # WhatsApp Twilio (cuando esté listo)
    TWILIO_SID = os.getenv('TWILIO_SID', '')
    TWILIO_TOKEN = os.getenv('TWILIO_TOKEN', '')
    TWILIO_WHATSAPP = os.getenv('TWILIO_WHATSAPP', '+14155238886')
    
    # Formularios críticos
    FORMS_CONFIG = {
        '877138': {
            'name': 'Supervisión Operativa EPL CAS',
            'priority': 'high',
            'alert_on_missing': True
        },
        '877139': {
            'name': 'Control Operativo de Seguridad EPL CAS',
            'priority': 'high', 
            'alert_on_missing': True
        },
        '877140': {
            'name': 'Apertura EPL CAS',
            'priority': 'medium',
            'alert_on_missing': False
        },
        '877141': {
            'name': 'Entrega de Turno EPL CAS',
            'priority': 'medium',
            'alert_on_missing': False
        },
        '877142': {
            'name': 'Cierre EPL CAS',
            'priority': 'medium',
            'alert_on_missing': False
        }
    }
    
    # Configuración de ejecución
    DAILY_ETL_TIME = time(6, 0)  # 6:00 AM
    WEEKLY_CHECK_TIME = time(8, 0)  # 8:00 AM domingos
    
    # Configuración de alertas
    MAX_INACTIVE_LOCATIONS = 5  # Alertar si más de 5 sucursales no reportan
    INACTIVE_DAYS_THRESHOLD = 3  # Sucursal inactiva si no reporta en 3 días
    
    # Estructura organizacional
    TOTAL_LOCATIONS = 86
    TOTAL_OPERATIONAL_GROUPS = 20
    
    # Datos maestros
    LOCATIONS_MASTER_FILE = 'data/86_sucursales_master.csv'
    API_DATA_FILE = 'data/zenput_api_complete_data.json'
    
    # Contactos clave
    CONTACTS = {
        'director_operaciones': {
            'name': 'Eduardo Martínez',
            'email': 'emartinez@epl.mx',
            'phone': '+52xxxxxxxxxx'  # TODO: Configurar
        },
        'developer': {
            'name': 'Roberto Dávila',
            'email': 'robertodavilag@gmail.com',
            'phone': '+52xxxxxxxxxx'  # TODO: Configurar
        }
    }

# Configuración por defecto
settings = ETLSettings()