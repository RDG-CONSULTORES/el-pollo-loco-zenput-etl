# 🎯 PLAN ETL COMPLETO EL POLLO LOCO
## Análisis y Diseño para PostgreSQL Railway + Dashboard

### 📊 **ESTADO ACTUAL**

#### Datos Disponibles:
✅ **476 supervisiones** (238 operativas + 238 seguridad)  
✅ **Calificaciones oficiales** Zenput + 40 áreas evaluadas  
✅ **Sucursales asignadas** 100% con coordenadas y grupos  
✅ **Excel mejorados** listos para PostgreSQL  

#### Datos que Faltan:
❌ **Estado** de cada sucursal (no tenemos campo estado)  
❌ **Coordenadas** en Excel (están en CSV separado)  
❌ **ETL automatizado** diario desde API Zenput  

---

## 🔧 **FASE 1: ENRIQUECER EXCEL ACTUAL**

### 1.1 Datos a Agregar:
```yaml
Coordenadas:
  - lat: De SUCURSALES_CORRECCIONES_ROBERTO.csv
  - lon: De SUCURSALES_CORRECCIONES_ROBERTO.csv
  - location_asignado: Clave sucursal-nombre

Grupo_Operativo:
  - TEPEYAC: 7 sucursales
  - OGAS: 22 sucursales  
  - CADEREYTA: 13 sucursales
  - HUASTECA: 21 sucursales
  - SOLIDARIDAD: 8 sucursales
  - APODACA: 9 sucursales

Estado:
  - Todas las sucursales están en Nuevo León
  - Estado: "Nuevo León"
  - País: "México"
```

### 1.2 Script de Enriquecimiento:
```python
# enriquecer_excel_completo.py
- Leer Excel operativas/seguridad actuales
- Hacer JOIN con catálogo sucursales
- Agregar: lat, lon, grupo, estado="Nuevo León"
- Regenerar Excel enriquecidos
```

---

## 🏗️ **FASE 2: DISEÑO BASE DE DATOS POSTGRESQL**

### 2.1 Esquema de Tablas:

```sql
-- TABLA SUCURSALES
CREATE TABLE sucursales (
    id SERIAL PRIMARY KEY,
    numero INTEGER UNIQUE NOT NULL,
    nombre VARCHAR(100) NOT NULL,
    grupo_operativo VARCHAR(50) NOT NULL,
    tipo VARCHAR(20) NOT NULL, -- LOCAL/FORANEA/ESPECIAL
    estado VARCHAR(50) DEFAULT 'Nuevo León',
    pais VARCHAR(50) DEFAULT 'México',
    lat DECIMAL(10,8),
    lon DECIMAL(11,8),
    activa BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- TABLA SUPERVISIONES
CREATE TABLE supervisiones (
    id UUID PRIMARY KEY,
    submission_id VARCHAR(50) UNIQUE NOT NULL,
    sucursal_id INTEGER REFERENCES sucursales(id),
    tipo_supervision VARCHAR(20) NOT NULL, -- operativas/seguridad
    fecha_supervision TIMESTAMP NOT NULL,
    usuario VARCHAR(100),
    calificacion_general DECIMAL(5,2),
    puntos_totales INTEGER,
    puntos_maximos INTEGER,
    tiene_calificacion_oficial BOOLEAN DEFAULT FALSE,
    areas_evaluadas INTEGER DEFAULT 0,
    location_asignado VARCHAR(100),
    lat_entrega DECIMAL(10,8),
    lon_entrega DECIMAL(11,8),
    fuente_datos VARCHAR(50) DEFAULT 'zenput_api',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- TABLA CALIFICACIONES_AREAS
CREATE TABLE calificaciones_areas (
    id SERIAL PRIMARY KEY,
    supervision_id UUID REFERENCES supervisiones(id),
    area_nombre VARCHAR(100) NOT NULL,
    calificacion DECIMAL(5,2),
    tipo_supervision VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- INDICES PARA PERFORMANCE
CREATE INDEX idx_supervisiones_fecha ON supervisiones(fecha_supervision);
CREATE INDEX idx_supervisiones_sucursal ON supervisiones(sucursal_id);
CREATE INDEX idx_supervisiones_tipo ON supervisiones(tipo_supervision);
CREATE INDEX idx_calificaciones_supervision ON calificaciones_areas(supervision_id);
```

### 2.2 Ventajas del Diseño:
- ✅ **Normalización** adecuada (3NF)
- ✅ **Escalabilidad** para millones de registros
- ✅ **Performance** con índices optimizados
- ✅ **Integridad** referencial garantizada
- ✅ **Flexibilidad** para agregar nuevas áreas

---

## 🔄 **FASE 3: ETL DIARIO AUTOMATIZADO**

### 3.1 Arquitectura ETL:

```yaml
Fuente: 
  - API Zenput v3
  - URL: https://www.zenput.com/api/v3/submissions
  - Token: cb908e0d4e0f5501c635325c611db314
  - Forms: 877138 (operativas), 877139 (seguridad)

Frecuencia:
  - Ejecución: Diaria 6:00 AM MX
  - Ventana: Últimas 48 horas
  - Reintento: 3 intentos con backoff exponencial

Procesos:
  1. Extracción API → submissions nuevas
  2. Transformación → normalización + asignación sucursales
  3. Validación → calidad datos + coordenadas
  4. Carga → PostgreSQL Railway con UPSERT
  5. Notificación → Slack/Email con métricas
```

### 3.2 GitHub Actions Workflow:

```yaml
# .github/workflows/etl-daily.yml
name: ETL Diario El Pollo Loco
on:
  schedule:
    - cron: '0 12 * * *'  # 6 AM Mexico City (UTC-6)
  workflow_dispatch:

jobs:
  etl_process:
    runs-on: ubuntu-latest
    environment: production
    
    steps:
    - name: Checkout
      uses: actions/checkout@v4
      
    - name: Setup Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'
        
    - name: Install Dependencies
      run: |
        pip install -r requirements.txt
        
    - name: Run ETL Process
      env:
        ZENPUT_API_TOKEN: ${{ secrets.ZENPUT_API_TOKEN }}
        RAILWAY_DB_URL: ${{ secrets.RAILWAY_DB_URL }}
        SLACK_WEBHOOK: ${{ secrets.SLACK_WEBHOOK }}
      run: |
        python etl/main_etl_daily.py
        
    - name: Upload Logs
      if: always()
      uses: actions/upload-artifact@v4
      with:
        name: etl-logs
        path: logs/
```

### 3.3 ETL Script Principal:

```python
# etl/main_etl_daily.py
import requests
import psycopg2
from datetime import datetime, timedelta
import logging

class ETLDailyPollo:
    def __init__(self):
        self.zenput_token = os.getenv('ZENPUT_API_TOKEN')
        self.db_url = os.getenv('RAILWAY_DB_URL')
        self.setup_logging()
    
    def extract_from_zenput(self):
        """Extraer submissions últimas 48 horas"""
        yesterday = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
        
        params = {
            'activity_template_ids': '877138,877139',
            'date_submitted_after': yesterday,
            'limit': 1000
        }
        
        response = requests.get(
            'https://www.zenput.com/api/v3/submissions',
            headers={'X-API-TOKEN': self.zenput_token},
            params=params
        )
        
        return response.json().get('data', [])
    
    def transform_submissions(self, submissions):
        """Transformar y asignar sucursales"""
        # Aplicar lógica de asignación automática
        # 4 pasos: coordenadas → normalización → redistribución → backup
        pass
    
    def load_to_postgresql(self, data):
        """Cargar a PostgreSQL con UPSERT"""
        conn = psycopg2.connect(self.db_url)
        cursor = conn.cursor()
        
        # UPSERT supervisiones
        upsert_query = """
        INSERT INTO supervisiones (submission_id, sucursal_id, ...)
        VALUES (%s, %s, ...)
        ON CONFLICT (submission_id) 
        DO UPDATE SET updated_at = NOW()
        """
        pass
    
    def notify_results(self, metrics):
        """Notificar resultados vía Slack"""
        pass
    
    def run(self):
        """Ejecutar ETL completo"""
        try:
            submissions = self.extract_from_zenput()
            transformed = self.transform_submissions(submissions)
            self.load_to_postgresql(transformed)
            self.notify_results({'success': True, 'count': len(submissions)})
        except Exception as e:
            self.notify_results({'success': False, 'error': str(e)})
```

---

## 📈 **FASE 4: PREPARACIÓN DASHBOARD**

### 4.1 Vistas Materializadas:

```sql
-- VISTA RESUMEN POR SUCURSAL
CREATE MATERIALIZED VIEW dashboard_sucursales AS
SELECT 
    s.nombre as sucursal,
    s.grupo_operativo,
    s.tipo,
    COUNT(sup.id) as total_supervisiones,
    COUNT(CASE WHEN sup.tipo_supervision = 'operativas' THEN 1 END) as operativas_count,
    COUNT(CASE WHEN sup.tipo_supervision = 'seguridad' THEN 1 END) as seguridad_count,
    AVG(sup.calificacion_general) as promedio_calificacion,
    MAX(sup.fecha_supervision) as ultima_supervision
FROM sucursales s
LEFT JOIN supervisiones sup ON s.id = sup.sucursal_id
GROUP BY s.id, s.nombre, s.grupo_operativo, s.tipo;

-- VISTA TENDENCIAS TEMPORALES
CREATE MATERIALIZED VIEW dashboard_tendencias AS
SELECT 
    DATE_TRUNC('day', fecha_supervision) as fecha,
    tipo_supervision,
    COUNT(*) as total_supervisiones,
    AVG(calificacion_general) as promedio_calificacion
FROM supervisiones
GROUP BY DATE_TRUNC('day', fecha_supervision), tipo_supervision
ORDER BY fecha DESC;
```

### 4.2 APIs para Dashboard:

```python
# api/dashboard_endpoints.py
from fastapi import FastAPI
from sqlalchemy import create_engine

app = FastAPI(title="El Pollo Loco Dashboard API")

@app.get("/sucursales/resumen")
async def get_sucursales_resumen():
    """Resumen de todas las sucursales"""
    pass

@app.get("/supervisiones/tendencias")
async def get_tendencias():
    """Tendencias temporales de supervisiones"""
    pass

@app.get("/areas/performance")
async def get_areas_performance():
    """Performance por áreas"""
    pass
```

---

## ⚡ **CRONOGRAMA DE IMPLEMENTACIÓN**

### Semana 1: Enriquecimiento Datos
- ✅ **Día 1-2**: Enriquecer Excel con coordenadas y estado
- ✅ **Día 3**: Validar datos completos para PostgreSQL
- ✅ **Día 4-5**: Crear scripts de migración inicial

### Semana 2: Base de Datos
- 🔄 **Día 1-2**: Configurar PostgreSQL en Railway
- 🔄 **Día 3-4**: Crear esquema y cargar datos históricos
- 🔄 **Día 5**: Optimizar performance y crear vistas

### Semana 3: ETL Automatizado
- ⏳ **Día 1-3**: Desarrollar ETL diario completo
- ⏳ **Día 4**: Configurar GitHub Actions
- ⏳ **Día 5**: Testing y validación ETL

### Semana 4: Dashboard
- ⏳ **Día 1-3**: APIs y vistas materializadas
- ⏳ **Día 4-5**: Frontend dashboard básico

---

## 🎯 **PRÓXIMOS PASOS INMEDIATOS**

### 1. **HOY** - Enriquecer Excel:
```bash
python enriquecer_excel_completo.py
# Output: Excel con coordenadas + estado + grupo
```

### 2. **MAÑANA** - Railway PostgreSQL:
```bash
# Configurar base datos Railway
# Crear esquema completo
# Migrar datos históricos
```

### 3. **Esta Semana** - ETL Diario:
```bash
# Desarrollar ETL automatizado
# Configurar GitHub Actions
# Testing completo
```

---

## ❓ **DECISIONES PENDIENTES**

### Roberto, necesito que decidas:

1. **🗄️ Railway Plan**: ¿Qué plan PostgreSQL? (Hobby $5/mes vs Pro $20/mes)

2. **🔄 Frecuencia ETL**: ¿Diario suficiente o necesitas tiempo real?

3. **📊 Dashboard Tech**: ¿Streamlit simple o React/Next.js completo?

4. **🚨 Alertas**: ¿Notificaciones por Slack, Email o WhatsApp?

5. **📈 Métricas**: ¿Qué KPIs específicos necesitas en dashboard?

---

## 📋 **RESUMEN EJECUTIVO**

**✅ TENEMOS:** 476 supervisiones completas con calificaciones oficiales  
**🔧 NECESITAMOS:** Enriquecer con coordenadas + ETL automatizado  
**🎯 OBJETIVO:** Base PostgreSQL + Dashboard funcional en 4 semanas  
**💰 COSTO:** ~$20/mes Railway + GitHub Actions gratuito  
**🚀 BENEFICIO:** Datos automáticos diarios + Dashboard tiempo real  

¿Empezamos con el enriquecimiento del Excel? ¿Tienes alguna preferencia específica sobre el diseño?