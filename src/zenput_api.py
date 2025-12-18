#!/usr/bin/env python3
"""
🔗 ZENPUT API CLIENT - El Pollo Loco México
Cliente optimizado para extracción diaria de submissions
"""

import requests
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import time

class ZenputAPIClient:
    """Cliente API Zenput optimizado para submissions diarias"""
    
    def __init__(self, api_token: str):
        self.api_token = api_token
        self.headers = {
            'X-API-TOKEN': api_token,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        self.base_url = 'https://www.zenput.com/api/v3'
        
        # Formularios configurados
        self.forms_config = {
            '877138': 'Supervisión Operativa EPL CAS',
            '877139': 'Control Operativo de Seguridad EPL CAS', 
            '877140': 'Apertura EPL CAS',
            '877141': 'Entrega de Turno EPL CAS',
            '877142': 'Cierre EPL CAS'
        }
    
    def get_all_locations(self) -> List[Dict]:
        """Obtiene todas las 86 sucursales"""
        
        print("🏪 Obteniendo las 86 sucursales...")
        
        try:
            response = requests.get(
                f"{self.base_url}/locations",
                headers=self.headers,
                params={'limit': 100, 'offset': 0},  # ✅ Parámetros correctos
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                locations = self._extract_data(data)
                print(f"   ✅ {len(locations)} sucursales obtenidas")
                return locations
            else:
                print(f"   ❌ Error {response.status_code}: {response.text}")
                return []
                
        except Exception as e:
            print(f"   💥 Error: {e}")
            return []
    
    def get_submissions_for_form(self, form_id: str, days_back: int = 1) -> List[Dict]:
        """Obtiene submissions de un formulario específico"""
        
        print(f"📋 Extrayendo submissions Form {form_id} ({self.forms_config.get(form_id, 'Unknown')})...")
        
        # Calcular fechas
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        params = {
            'form_template_id': form_id,
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'page': 1,
            'page_size': 100
        }
        
        all_submissions = []
        
        try:
            response = requests.get(
                f"{self.base_url}/submissions",
                headers=self.headers,
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                submissions = self._extract_data(data)
                
                print(f"   ✅ {len(submissions)} submissions encontradas")
                
                # Enriquecer submissions con metadata
                for submission in submissions:
                    submission['form_id'] = form_id
                    submission['form_name'] = self.forms_config.get(form_id)
                    submission['extracted_at'] = datetime.now().isoformat()
                
                all_submissions.extend(submissions)
            else:
                print(f"   ❌ Error {response.status_code}")
                
        except Exception as e:
            print(f"   💥 Error: {e}")
        
        return all_submissions
    
    def get_daily_submissions(self, target_date: Optional[str] = None) -> Dict[str, List[Dict]]:
        """Extrae submissions de todos los formularios para una fecha específica"""
        
        if target_date:
            print(f"📅 Extrayendo submissions para fecha: {target_date}")
        else:
            target_date = datetime.now().strftime('%Y-%m-%d')
            print(f"📅 Extrayendo submissions de hoy: {target_date}")
        
        daily_data = {
            'extraction_date': target_date,
            'timestamp': datetime.now().isoformat(),
            'total_submissions': 0,
            'forms_data': {}
        }
        
        for form_id, form_name in self.forms_config.items():
            print(f"\n🔄 Procesando {form_name}...")
            
            submissions = self.get_submissions_for_form(form_id, days_back=1)
            
            daily_data['forms_data'][form_id] = {
                'form_name': form_name,
                'submissions_count': len(submissions),
                'submissions': submissions
            }
            
            daily_data['total_submissions'] += len(submissions)
            
            time.sleep(0.5)  # Rate limiting
        
        print(f"\n📊 RESUMEN EXTRACCIÓN:")
        print(f"   📅 Fecha: {target_date}")
        print(f"   📋 Total submissions: {daily_data['total_submissions']}")
        
        for form_id, form_data in daily_data['forms_data'].items():
            count = form_data['submissions_count']
            name = form_data['form_name']
            print(f"   • {name}: {count} submissions")
        
        return daily_data
    
    def check_new_locations(self, known_count: int = 86) -> List[Dict]:
        """Verificación semanal de nuevas sucursales"""
        
        print(f"🔍 Verificando nuevas sucursales (conocidas: {known_count})...")
        
        current_locations = self.get_all_locations()
        
        if len(current_locations) > known_count:
            new_locations = current_locations[known_count:]
            print(f"🆕 {len(new_locations)} nuevas sucursales encontradas!")
            
            for loc in new_locations:
                print(f"   📍 {loc.get('name')} - {loc.get('city')}, {loc.get('state')}")
            
            return new_locations
        else:
            print(f"   ✅ No hay nuevas sucursales")
            return []
    
    def check_inactive_locations(self, submissions_data: Dict, days_threshold: int = 3) -> List[Dict]:
        """Identifica sucursales que no han reportado en X días"""
        
        print(f"⚠️ Verificando sucursales inactivas (sin reportar >{days_threshold} días)...")
        
        # Obtener todas las sucursales
        all_locations = self.get_all_locations()
        location_ids = {loc.get('id') for loc in all_locations}
        
        # Obtener IDs que sí reportaron
        active_location_ids = set()
        for form_data in submissions_data.get('forms_data', {}).values():
            for submission in form_data.get('submissions', []):
                location_id = submission.get('location', {}).get('id')
                if location_id:
                    active_location_ids.add(location_id)
        
        # Identificar inactivas
        inactive_ids = location_ids - active_location_ids
        inactive_locations = [loc for loc in all_locations if loc.get('id') in inactive_ids]
        
        if inactive_locations:
            print(f"⚠️ {len(inactive_locations)} sucursales sin reportar:")
            for loc in inactive_locations:
                print(f"   📍 {loc.get('name')} (ID: {loc.get('id')})")
        else:
            print(f"   ✅ Todas las sucursales activas")
        
        return inactive_locations
    
    def _extract_data(self, response_data: Any) -> List[Dict]:
        """Extrae datos de diferentes formatos de respuesta API"""
        
        if isinstance(response_data, list):
            return response_data
        elif isinstance(response_data, dict):
            # Buscar en estructuras comunes
            for key in ['results', 'data', 'submissions', 'locations', 'items']:
                if key in response_data and isinstance(response_data[key], list):
                    return response_data[key]
            
            # Si es un solo objeto
            if 'id' in response_data:
                return [response_data]
        
        return []
    
    def validate_api_connection(self) -> bool:
        """Valida que la conexión API funcione correctamente"""
        
        print("🔐 Validando conexión API Zenput...")
        
        try:
            response = requests.get(
                f"{self.base_url}/locations",
                headers=self.headers,
                params={'limit': 1, 'offset': 0},
                timeout=10
            )
            
            if response.status_code == 200:
                print("   ✅ Conexión API exitosa")
                return True
            else:
                print(f"   ❌ Error API {response.status_code}")
                return False
                
        except Exception as e:
            print(f"   💥 Error conexión: {e}")
            return False

# Función de utilidad
def create_zenput_client(api_token: str = "cb908e0d4e0f5501c635325c611db314") -> ZenputAPIClient:
    """Crea cliente Zenput configurado"""
    return ZenputAPIClient(api_token)

if __name__ == "__main__":
    # Test del cliente
    client = create_zenput_client()
    
    if client.validate_api_connection():
        print("\n🔄 Probando extracción diaria...")
        daily_data = client.get_daily_submissions()
        
        print(f"\n📋 Submissions extraídas: {daily_data['total_submissions']}")
    else:
        print("❌ No se pudo conectar a API Zenput")