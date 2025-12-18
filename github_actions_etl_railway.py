#!/usr/bin/env python3
"""
🚀 GITHUB ACTIONS ETL TO RAILWAY
ETL que funciona en GitHub Actions y manda data a PostgreSQL Railway
Basado en tu ETL exitoso de 189 días
"""

import os
import requests
import psycopg2
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import time

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_errors.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ZenputETLRailway:
    """ETL Zenput optimizado para GitHub Actions → Railway PostgreSQL"""
    
    def __init__(self):
        """Inicializar configuración desde environment variables"""
        
        # Configuración Zenput API - URLs CORRECTAS con fallback
        self.zenput_config = {
            'base_urls': {
                'v3': 'https://www.zenput.com/api/v3',
                'v1': 'https://www.zenput.com/api/v1'
            },
            'headers': {'X-API-TOKEN': os.getenv('ZENPUT_API_TOKEN')},
            'timeout': 30,
            'preferred_version': 'v3'  # Intentar v3 primero, v1 como fallback
        }
        
        # Configuración Railway PostgreSQL
        self.railway_url = os.getenv('RAILWAY_DATABASE_URL')
        
        # Configuración ETL
        self.date_range_days = int(os.getenv('DATE_RANGE_DAYS', '1'))
        
        # Formularios El Pollo Loco
        self.forms = {
            'operativa': '877138',
            'seguridad': '877139'
        }
        
        # Resultados
        self.results = {
            'timestamp': datetime.now().isoformat(),
            'total_extracted': 0,
            'total_saved': 0,
            'errors': [],
            'forms_processed': {},
            'execution_time_seconds': 0
        }
        
        self._validate_config()
    
    def _validate_config(self):
        """Validar configuración requerida"""
        
        if not self.zenput_config['headers']['X-API-TOKEN']:
            raise ValueError("ZENPUT_API_TOKEN environment variable required")
        
        if not self.railway_url:
            raise ValueError("RAILWAY_DATABASE_URL environment variable required")
        
        logger.info(f"✅ Configuration validated - Date range: {self.date_range_days} days")
    
    def get_date_range(self) -> tuple:
        """Calcular rango de fechas para extracción"""
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=self.date_range_days)
        
        return (
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )
    
    def extract_submissions(self, form_id: str, form_name: str) -> List[Dict]:
        """Extraer submissions de Zenput API con paginación"""
        
        start_date, end_date = self.get_date_range()
        logger.info(f"🔍 Extracting {form_name} submissions from {start_date} to {end_date}")
        
        all_submissions = []
        page = 1
        max_pages = 50  # Límite de seguridad
        
        while page <= max_pages:
            try:
                # Construir parámetros
                params = {
                    'form_id': form_id,
                    'submitted_at_start': start_date,
                    'submitted_at_end': end_date,
                    'page': page,
                    'per_page': 20  # Tamaño conservador
                }
                
                # Hacer request con retry usando versión determinada
                if not hasattr(self, 'api_version'):
                    self.api_version = self._determine_best_api_version()
                
                base_url = self.zenput_config['base_urls'][self.api_version]
                response = self._make_request_with_retry(
                    'GET',
                    f"{base_url}/submissions",
                    params=params
                )
                
                if not response:
                    break
                
                # Procesar respuesta
                data = response.json()
                submissions = data.get('submissions', [])
                
                if not submissions:
                    logger.info(f"   📄 Page {page}: No more submissions found")
                    break
                
                # Agregar metadata a cada submission
                for submission in submissions:
                    submission['form_type'] = form_name.upper()
                    submission['extracted_at'] = datetime.now().isoformat()
                
                all_submissions.extend(submissions)
                logger.info(f"   📄 Page {page}: {len(submissions)} submissions extracted")
                
                page += 1
                time.sleep(0.5)  # Rate limiting preventivo
                
            except Exception as e:
                error_msg = f"Error extracting {form_name} page {page}: {e}"
                logger.error(error_msg)
                self.results['errors'].append(error_msg)
                break
        
        logger.info(f"✅ {form_name}: {len(all_submissions)} total submissions extracted")
        return all_submissions
    
    def _determine_best_api_version(self) -> str:
        """Determinar qué versión de API funciona mejor"""
        
        for version in ['v3', 'v1']:
            try:
                test_url = f"{self.zenput_config['base_urls'][version]}/forms"
                response = requests.get(
                    test_url,
                    headers=self.zenput_config['headers'],
                    timeout=10
                )
                
                if response.status_code == 200:
                    logger.info(f"✅ API {version} working - will use this version")
                    return version
                else:
                    logger.warning(f"⚠️ API {version} returned {response.status_code}")
                    
            except Exception as e:
                logger.warning(f"⚠️ API {version} failed: {e}")
        
        # Default fallback
        logger.warning("Using v3 as fallback")
        return 'v3'
    
    def _make_request_with_retry(self, method: str, endpoint: str, **kwargs) -> Optional[requests.Response]:
        """Hacer request con retry automático"""
        
        max_retries = 3
        backoff_factor = 1
        
        for attempt in range(max_retries):
            try:
                if method == 'GET':
                    response = requests.get(
                        endpoint,  # endpoint ya viene como URL completa
                        headers=self.zenput_config['headers'],
                        timeout=self.zenput_config['timeout'],
                        **kwargs
                    )
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
                
                # Verificar status code
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:  # Rate limited
                    wait_time = backoff_factor * (2 ** attempt)
                    logger.warning(f"⏳ Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"❌ HTTP {response.status_code}: {response.text}")
                    return None
                    
            except requests.exceptions.RequestException as e:
                wait_time = backoff_factor * (2 ** attempt)
                logger.warning(f"⏳ Request failed (attempt {attempt+1}/{max_retries}), retrying in {wait_time}s: {e}")
                
                if attempt < max_retries - 1:
                    time.sleep(wait_time)
                else:
                    logger.error(f"❌ Request failed after {max_retries} attempts: {e}")
                    return None
        
        return None
    
    def save_to_railway(self, submissions: List[Dict], form_name: str) -> int:
        """Guardar submissions en PostgreSQL Railway"""
        
        if not submissions:
            return 0
        
        saved_count = 0
        
        try:
            # Conectar a Railway PostgreSQL
            conn = psycopg2.connect(self.railway_url)
            cursor = conn.cursor()
            
            logger.info(f"💾 Saving {len(submissions)} {form_name} submissions to Railway PostgreSQL")
            
            for submission in submissions:
                try:
                    # Extraer datos principales
                    submission_id = submission.get('id')
                    form_id = submission.get('form_id')
                    location_id = submission.get('location_id')
                    user_id = submission.get('user_id')
                    submitted_at = submission.get('submitted_at')
                    form_type = submission.get('form_type')
                    
                    # Insertar en tabla supervisions
                    cursor.execute("""
                        INSERT INTO supervisions 
                        (submission_id, form_id, location_id, user_id, submitted_at, form_type, raw_data, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (submission_id) 
                        DO UPDATE SET 
                            raw_data = EXCLUDED.raw_data,
                            updated_at = NOW()
                    """, (
                        submission_id,
                        form_id,
                        location_id,
                        user_id,
                        submitted_at,
                        form_type,
                        json.dumps(submission)
                    ))
                    
                    saved_count += 1
                    
                except Exception as e:
                    error_msg = f"Error saving submission {submission.get('id', 'unknown')}: {e}"
                    logger.error(error_msg)
                    self.results['errors'].append(error_msg)
            
            # Commit cambios
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"✅ {saved_count} {form_name} submissions saved to Railway")
            
        except Exception as e:
            error_msg = f"Database error saving {form_name}: {e}"
            logger.error(error_msg)
            self.results['errors'].append(error_msg)
            return 0
        
        return saved_count
    
    def update_statistics(self):
        """Actualizar estadísticas en Railway"""
        
        try:
            conn = psycopg2.connect(self.railway_url)
            cursor = conn.cursor()
            
            # Insertar estadísticas de ejecución
            cursor.execute("""
                INSERT INTO etl_execution_log 
                (execution_date, total_extracted, total_saved, errors_count, execution_time_seconds, details)
                VALUES (NOW(), %s, %s, %s, %s, %s)
            """, (
                self.results['total_extracted'],
                self.results['total_saved'],
                len(self.results['errors']),
                self.results['execution_time_seconds'],
                json.dumps(self.results)
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info("📊 Statistics updated in Railway")
            
        except Exception as e:
            logger.error(f"Error updating statistics: {e}")
    
    def create_tables_if_not_exist(self):
        """Crear tablas necesarias si no existen"""
        
        try:
            conn = psycopg2.connect(self.railway_url)
            cursor = conn.cursor()
            
            # Tabla principal de supervisiones
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS supervisions (
                    id SERIAL PRIMARY KEY,
                    submission_id VARCHAR(100) UNIQUE NOT NULL,
                    form_id VARCHAR(50),
                    location_id VARCHAR(100),
                    user_id VARCHAR(100),
                    submitted_at TIMESTAMP,
                    form_type VARCHAR(20),
                    raw_data JSONB,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                );
                
                CREATE INDEX IF NOT EXISTS idx_supervisions_submitted_at ON supervisions(submitted_at);
                CREATE INDEX IF NOT EXISTS idx_supervisions_form_type ON supervisions(form_type);
                CREATE INDEX IF NOT EXISTS idx_supervisions_location_id ON supervisions(location_id);
            """)
            
            # Tabla de log de ejecuciones
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS etl_execution_log (
                    id SERIAL PRIMARY KEY,
                    execution_date TIMESTAMP DEFAULT NOW(),
                    total_extracted INTEGER,
                    total_saved INTEGER,
                    errors_count INTEGER,
                    execution_time_seconds NUMERIC,
                    details JSONB
                );
            """)
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info("✅ Database tables validated/created")
            
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    def run_etl(self):
        """Ejecutar ETL completo"""
        
        start_time = datetime.now()
        logger.info(f"🚀 Starting ETL execution at {start_time}")
        
        try:
            # Crear tablas si no existen
            self.create_tables_if_not_exist()
            
            # Procesar cada formulario
            for form_name, form_id in self.forms.items():
                logger.info(f"📋 Processing {form_name} (Form ID: {form_id})")
                
                # Extraer submissions
                submissions = self.extract_submissions(form_id, form_name)
                self.results['total_extracted'] += len(submissions)
                
                # Guardar en Railway
                saved_count = self.save_to_railway(submissions, form_name)
                self.results['total_saved'] += saved_count
                
                # Actualizar resultados por formulario
                self.results['forms_processed'][form_name] = {
                    'extracted': len(submissions),
                    'saved': saved_count,
                    'form_id': form_id
                }
            
            # Actualizar estadísticas
            self.update_statistics()
            
            # Calcular tiempo total
            end_time = datetime.now()
            self.results['execution_time_seconds'] = (end_time - start_time).total_seconds()
            
            # Log final
            logger.info(f"🎯 ETL completed successfully!")
            logger.info(f"   📊 Total extracted: {self.results['total_extracted']}")
            logger.info(f"   💾 Total saved: {self.results['total_saved']}")
            logger.info(f"   ⏱️ Execution time: {self.results['execution_time_seconds']:.1f}s")
            logger.info(f"   ❌ Errors: {len(self.results['errors'])}")
            
            # Guardar resultados en archivo
            with open('etl_results.json', 'w') as f:
                json.dump(self.results, f, indent=2)
            
        except Exception as e:
            error_msg = f"ETL execution failed: {e}"
            logger.error(error_msg)
            self.results['errors'].append(error_msg)
            
            # Guardar resultados incluso si hay error
            with open('etl_results.json', 'w') as f:
                json.dump(self.results, f, indent=2)
            
            raise

if __name__ == '__main__':
    """Punto de entrada para GitHub Actions"""
    
    try:
        etl = ZenputETLRailway()
        etl.run_etl()
        
        # Exit code 0 para éxito
        exit(0)
        
    except Exception as e:
        logger.error(f"💥 ETL failed: {e}")
        
        # Exit code 1 para error
        exit(1)