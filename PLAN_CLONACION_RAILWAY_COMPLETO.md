# 🚀 PLAN CLONACIÓN EXACTA + RAILWAY COMPLETO
## Dashboard iOS Native con Toggle + PostgreSQL Optimizado

---

## 🎯 **OBJETIVO FINAL**

✅ **Clonar exacto** tu dashboard iOS actual  
✅ **Toggle switch** Operativas ↔ Seguridad  
✅ **Railway completo**: PostgreSQL + Web App deploy  
✅ **Velocidad máxima**: Consultas optimizadas <200ms  
✅ **Datos normalizados**: 476 supervisiones + áreas  

---

## 📊 **ARQUITECTURA RAILWAY COMPLETA**

```
🚀 RAILWAY PROJECT: el-pollo-loco-dashboard
├── 🗄️ PostgreSQL Database (Hobby $5/mes)
│   ├── Tablas optimizadas con índices
│   ├── Vistas materializadas para speed
│   └── 476 supervisiones + 40 áreas
├── 🌐 Node.js Web Service ($5/mes)
│   ├── API Backend (Express.js)
│   ├── Dashboard Frontend (tu diseño iOS)
│   └── Toggle switch Operativas/Seguridad
└── 📡 Networking & Deploy automático
```

---

## 📋 **FASE 1: ESQUEMA POSTGRESQL OPTIMIZADO**

### **Tablas Principales:**
```sql
-- 1. SUCURSALES (Base)
CREATE TABLE sucursales (
    id SERIAL PRIMARY KEY,
    numero INTEGER UNIQUE NOT NULL,
    nombre VARCHAR(100) NOT NULL,
    grupo_operativo VARCHAR(50) NOT NULL,
    tipo_sucursal VARCHAR(20) NOT NULL, -- LOCAL/FORANEA
    estado VARCHAR(50) DEFAULT 'Nuevo León',
    latitud DECIMAL(10,8),
    longitud DECIMAL(11,8),
    activa BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- 2. SUPERVISIONES (Core)
CREATE TABLE supervisiones (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    submission_id VARCHAR(50) UNIQUE NOT NULL,
    sucursal_id INTEGER REFERENCES sucursales(id),
    tipo_supervision VARCHAR(20) NOT NULL, -- 'operativas' | 'seguridad'
    fecha_supervision TIMESTAMP NOT NULL,
    usuario VARCHAR(100),
    calificacion_general DECIMAL(5,2) NOT NULL,
    puntos_totales INTEGER,
    puntos_maximos INTEGER,
    areas_evaluadas JSONB, -- {area_name: score, ...}
    metadatos JSONB, -- Extra data flexible
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- 3. AREAS_CALIFICACIONES (Detalle)
CREATE TABLE areas_calificaciones (
    id SERIAL PRIMARY KEY,
    supervision_id UUID REFERENCES supervisiones(id) ON DELETE CASCADE,
    area_nombre VARCHAR(100) NOT NULL,
    calificacion DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT NOW()
);
```

### **Índices para Velocidad Máxima:**
```sql
-- ÍNDICES CRÍTICOS PARA PERFORMANCE <200ms
CREATE INDEX CONCURRENTLY idx_supervisiones_fecha_desc ON supervisiones(fecha_supervision DESC);
CREATE INDEX CONCURRENTLY idx_supervisiones_tipo ON supervisiones(tipo_supervision);
CREATE INDEX CONCURRENTLY idx_supervisiones_sucursal_fecha ON supervisiones(sucursal_id, fecha_supervision DESC);
CREATE INDEX CONCURRENTLY idx_supervisiones_calificacion ON supervisiones(calificacion_general);
CREATE INDEX CONCURRENTLY idx_sucursales_grupo ON sucursales(grupo_operativo);
CREATE INDEX CONCURRENTLY idx_sucursales_tipo ON sucursales(tipo_sucursal);
CREATE INDEX CONCURRENTLY idx_areas_supervision ON areas_calificaciones(supervision_id);
CREATE INDEX CONCURRENTLY idx_areas_nombre ON areas_calificaciones(area_nombre);

-- ÍNDICES COMPUESTOS PARA QUERIES COMPLEJAS  
CREATE INDEX CONCURRENTLY idx_sup_tipo_fecha_calif ON supervisiones(tipo_supervision, fecha_supervision DESC, calificacion_general);
CREATE INDEX CONCURRENTLY idx_suc_grupo_tipo ON sucursales(grupo_operativo, tipo_sucursal);
```

### **Vistas Materializadas (Cache Automático):**
```sql
-- VISTA DASHBOARD OPERATIVAS (Refresh cada hora)
CREATE MATERIALIZED VIEW dashboard_operativas AS
SELECT 
    s.grupo_operativo,
    s.tipo_sucursal,
    s.nombre as sucursal_nombre,
    COUNT(sup.id) as total_supervisiones,
    ROUND(AVG(sup.calificacion_general), 1) as promedio_calificacion,
    MIN(sup.calificacion_general) as min_calificacion,
    MAX(sup.calificacion_general) as max_calificacion,
    MAX(sup.fecha_supervision) as ultima_supervision
FROM sucursales s
LEFT JOIN supervisiones sup ON s.id = sup.sucursal_id 
WHERE sup.tipo_supervision = 'operativas' OR sup.tipo_supervision IS NULL
GROUP BY s.id, s.grupo_operativo, s.tipo_sucursal, s.nombre;

-- VISTA DASHBOARD SEGURIDAD  
CREATE MATERIALIZED VIEW dashboard_seguridad AS
SELECT 
    s.grupo_operativo,
    s.tipo_sucursal,
    s.nombre as sucursal_nombre,
    COUNT(sup.id) as total_supervisiones,
    ROUND(AVG(sup.calificacion_general), 1) as promedio_calificacion,
    MIN(sup.calificacion_general) as min_calificacion,
    MAX(sup.calificacion_general) as max_calificacion,
    MAX(sup.fecha_supervision) as ultima_supervision
FROM sucursales s
LEFT JOIN supervisiones sup ON s.id = sup.sucursal_id 
WHERE sup.tipo_supervision = 'seguridad' OR sup.tipo_supervision IS NULL
GROUP BY s.id, s.grupo_operativo, s.tipo_sucursal, s.nombre;

-- AUTO-REFRESH CADA HORA
CREATE OR REPLACE FUNCTION refresh_dashboard_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW dashboard_operativas;
    REFRESH MATERIALIZED VIEW dashboard_seguridad;
END;
$$ LANGUAGE plpgsql;

-- CRON JOB (Railway extension)
SELECT cron.schedule('refresh-dashboards', '0 * * * *', 'SELECT refresh_dashboard_views();');
```

---

## 🔄 **FASE 2: MIGRACIÓN DATOS COMPLETOS**

### **Script Migración desde Excel:**
```python
# migrate_to_railway.py
import pandas as pd
import psycopg2
from datetime import datetime
import json
import uuid

class RailwayMigrator:
    def __init__(self, railway_db_url):
        self.conn = psycopg2.connect(railway_db_url)
        self.cursor = self.conn.cursor()
    
    def migrate_sucursales(self):
        """Migrar catálogo sucursales"""
        df = pd.read_csv("SUCURSALES_CORRECCIONES_ROBERTO_20251218_171807.csv")
        
        for _, row in df.iterrows():
            self.cursor.execute("""
                INSERT INTO sucursales (numero, nombre, grupo_operativo, tipo_sucursal, latitud, longitud)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (numero) DO UPDATE SET
                    nombre = EXCLUDED.nombre,
                    grupo_operativo = EXCLUDED.grupo_operativo,
                    tipo_sucursal = EXCLUDED.tipo_sucursal,
                    latitud = EXCLUDED.latitud,
                    longitud = EXCLUDED.longitud
            """, (row['numero'], row['nombre'], row['grupo'], row['tipo'], row['lat'], row['lon']))
        
        self.conn.commit()
        print(f"✅ Migradas {len(df)} sucursales")
    
    def migrate_operativas(self):
        """Migrar supervisiones operativas con áreas"""
        df = pd.read_excel("OPERATIVAS_POSTGRESQL_20251223_113008.xlsx", 
                          sheet_name='Operativas_PostgreSQL')
        
        for _, row in df.iterrows():
            # 1. Buscar sucursal_id
            self.cursor.execute(
                "SELECT id FROM sucursales WHERE nombre = %s", 
                (row['SUCURSAL'],)
            )
            sucursal_result = self.cursor.fetchone()
            if not sucursal_result:
                continue
                
            sucursal_id = sucursal_result[0]
            
            # 2. Extraer áreas
            areas_dict = {}
            for col in df.columns:
                if not col.startswith(('ID_', 'SUCURSAL', 'FECHA', 'CALIFICACION_GENERAL', 
                                     'sucursal_', 'latitud', 'longitud', 'grupo_', 'tipo_', 
                                     'estado', 'pais', 'region', 'zona_')):
                    if pd.notna(row[col]):
                        areas_dict[col] = float(row[col])
            
            # 3. Insertar supervisión
            supervision_id = str(uuid.uuid4())
            
            self.cursor.execute("""
                INSERT INTO supervisiones 
                (id, submission_id, sucursal_id, tipo_supervision, fecha_supervision, 
                 calificacion_general, areas_evaluadas)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                supervision_id,
                row['ID_SUPERVISION'],
                sucursal_id,
                'operativas',
                row['FECHA'],
                row['CALIFICACION_GENERAL'],
                json.dumps(areas_dict)
            ))
            
            # 4. Insertar áreas individuales
            for area_nombre, calificacion in areas_dict.items():
                self.cursor.execute("""
                    INSERT INTO areas_calificaciones 
                    (supervision_id, area_nombre, calificacion)
                    VALUES (%s, %s, %s)
                """, (supervision_id, area_nombre, calificacion))
        
        self.conn.commit()
        print(f"✅ Migradas {len(df)} supervisiones operativas")
    
    def migrate_seguridad(self):
        """Migrar supervisiones seguridad (mismo proceso)"""
        df = pd.read_excel("SEGURIDAD_POSTGRESQL_20251223_113008.xlsx", 
                          sheet_name='Seguridad_PostgreSQL')
        
        # Proceso idéntico pero tipo_supervision = 'seguridad'
        # ... (código similar al de operativas)
        
    def create_performance_indexes(self):
        """Crear índices de performance después de la carga"""
        indexes = [
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_supervisiones_fecha_desc ON supervisiones(fecha_supervision DESC)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_supervisiones_tipo ON supervisiones(tipo_supervision)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_supervisiones_sucursal_fecha ON supervisiones(sucursal_id, fecha_supervision DESC)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_supervisiones_calificacion ON supervisiones(calificacion_general)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sucursales_grupo ON sucursales(grupo_operativo)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sucursales_tipo ON sucursales(tipo_sucursal)"
        ]
        
        for index_sql in indexes:
            self.cursor.execute(index_sql)
        
        self.conn.commit()
        print("✅ Índices de performance creados")
```

---

## 🌐 **FASE 3: CLONACIÓN EXACTA DASHBOARD**

### **Estructura Railway Web Service:**
```
📁 el-pollo-loco-dashboard/
├── 📁 public/
│   ├── index.html (tu diseño iOS exacto)
│   ├── styles.css (tus estilos)
│   └── app.js (JavaScript + toggle)
├── 📁 api/
│   ├── server.js (Express backend)
│   ├── routes/
│   │   ├── operativas.js
│   │   ├── seguridad.js
│   │   └── sucursales.js
│   └── database.js
├── package.json
└── railway.json
```

### **Backend API Optimizado:**
```javascript
// api/server.js
const express = require('express');
const { Pool } = require('pg');

const app = express();
const port = process.env.PORT || 3000;

// PostgreSQL Railway Connection
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

// MIDDLEWARE
app.use(express.static('public'));
app.use(express.json());

// API ENDPOINTS OPTIMIZADOS
// 🔧 OPERATIVAS - Dashboard principal
app.get('/api/operativas/dashboard', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT * FROM dashboard_operativas 
            ORDER BY promedio_calificacion DESC
        `);
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// 🛡️ SEGURIDAD - Dashboard principal  
app.get('/api/seguridad/dashboard', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT * FROM dashboard_seguridad 
            ORDER BY promedio_calificacion DESC
        `);
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// 📊 DRILL-DOWN por Grupo Operativo
app.get('/api/:tipo/grupo/:grupo', async (req, res) => {
    const { tipo, grupo } = req.params;
    
    try {
        const result = await pool.query(`
            SELECT s.nombre as sucursal_nombre,
                   AVG(sup.calificacion_general) as promedio,
                   COUNT(sup.id) as total_supervisiones,
                   MAX(sup.fecha_supervision) as ultima_supervision
            FROM sucursales s
            LEFT JOIN supervisiones sup ON s.id = sup.sucursal_id 
            WHERE s.grupo_operativo = $1 
            AND (sup.tipo_supervision = $2 OR sup.tipo_supervision IS NULL)
            GROUP BY s.id, s.nombre
            ORDER BY promedio DESC
        `, [grupo, tipo]);
        
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// 🗺️ MAPA - Datos geográficos
app.get('/api/:tipo/mapa', async (req, res) => {
    const { tipo } = req.params;
    
    try {
        const result = await pool.query(`
            SELECT s.nombre, s.latitud, s.longitud, s.grupo_operativo,
                   AVG(sup.calificacion_general) as promedio_calificacion,
                   COUNT(sup.id) as total_supervisiones
            FROM sucursales s
            LEFT JOIN supervisiones sup ON s.id = sup.sucursal_id
            WHERE sup.tipo_supervision = $1 OR sup.tipo_supervision IS NULL
            GROUP BY s.id, s.nombre, s.latitud, s.longitud, s.grupo_operativo
            HAVING s.latitud IS NOT NULL AND s.longitud IS NOT NULL
        `, [tipo]);
        
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// 📈 HISTÓRICO - Tendencias temporales
app.get('/api/:tipo/historico', async (req, res) => {
    const { tipo } = req.params;
    const { grupo, sucursal } = req.query;
    
    let whereClause = `WHERE sup.tipo_supervision = $1`;
    let params = [tipo];
    let paramCount = 1;
    
    if (grupo) {
        whereClause += ` AND s.grupo_operativo = $${++paramCount}`;
        params.push(grupo);
    }
    
    if (sucursal) {
        whereClause += ` AND s.nombre = $${++paramCount}`;
        params.push(sucursal);
    }
    
    try {
        const result = await pool.query(`
            SELECT DATE_TRUNC('week', sup.fecha_supervision) as semana,
                   AVG(sup.calificacion_general) as promedio_semanal,
                   COUNT(sup.id) as total_supervisiones
            FROM supervisiones sup
            JOIN sucursales s ON sup.sucursal_id = s.id
            ${whereClause}
            GROUP BY DATE_TRUNC('week', sup.fecha_supervision)
            ORDER BY semana DESC
            LIMIT 12
        `, params);
        
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.listen(port, () => {
    console.log(`🚀 Server running on port ${port}`);
});
```

### **Frontend con Toggle Switch:**
```html
<!-- public/index.html - Tu diseño exacto + toggle -->
<!DOCTYPE html>
<html lang="es">
<head>
    <!-- Tus meta tags exactos -->
    <title>El Pollo Loco CAS - Dashboard Completo</title>
    <!-- Tus CDNs exactos -->
</head>
<body>
    <!-- Tu navbar exacto -->
    <div class="nav-bar">
        <div class="nav-title">El Pollo Loco CAS</div>
        
        <!-- NUEVO: Toggle Switch -->
        <div class="toggle-switch">
            <input type="radio" id="operativas" name="supervision-type" value="operativas" checked>
            <label for="operativas">🔧 Operativas</label>
            
            <input type="radio" id="seguridad" name="supervision-type" value="seguridad">
            <label for="seguridad">🛡️ Seguridad</label>
        </div>
    </div>
    
    <!-- Tu layout exacto -->
    <div class="large-title-container">
        <div class="large-title" id="dashboard-title">Supervisiones Operativas</div>
    </div>
    
    <!-- Tus tabs exactos -->
    <div class="tab-container">
        <div class="tab active" data-tab="dashboard">Dashboard</div>
        <div class="tab" data-tab="map">Map</div>
        <div class="tab" data-tab="historic">Historic</div>
        <div class="tab" data-tab="alerts">Alerts</div>
    </div>
    
    <!-- Tu contenido exacto -->
    <div id="dashboard-content">
        <!-- Tu código actual -->
    </div>

    <style>
    /* Tus estilos exactos + toggle */
    .toggle-switch {
        display: flex;
        background: var(--ios-gray-6);
        border-radius: 8px;
        padding: 2px;
        position: relative;
    }
    
    .toggle-switch input[type="radio"] {
        display: none;
    }
    
    .toggle-switch label {
        padding: 6px 12px;
        border-radius: 6px;
        font-size: 14px;
        font-weight: 500;
        cursor: pointer;
        transition: all 0.2s ease;
        color: var(--ios-label);
    }
    
    .toggle-switch input[type="radio"]:checked + label {
        background: var(--ios-blue);
        color: white;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
    }
    </style>

    <script>
    // Tu JavaScript exacto + toggle logic
    let currentType = 'operativas';
    
    // Toggle switch handler
    document.querySelectorAll('input[name="supervision-type"]').forEach(radio => {
        radio.addEventListener('change', (e) => {
            currentType = e.target.value;
            document.getElementById('dashboard-title').textContent = 
                currentType === 'operativas' ? 'Supervisiones Operativas' : 'Supervisiones de Seguridad';
            
            // Recargar datos con nuevo tipo
            loadDashboardData();
        });
    });
    
    // Tu función loadDashboardData modificada
    async function loadDashboardData() {
        try {
            const response = await fetch(`/api/${currentType}/dashboard`);
            const data = await response.json();
            
            // Tu código de renderizado exacto
            renderDashboard(data);
            
        } catch (error) {
            console.error('Error loading data:', error);
        }
    }
    
    // Todo tu JavaScript actual...
    </script>
</body>
</html>
```

---

## ⚡ **FASE 4: OPTIMIZACIÓN VELOCIDAD MÁXIMA**

### **Configuración Railway para Performance:**
```json
// railway.json
{
  "build": {
    "builder": "nixpacks"
  },
  "deploy": {
    "numReplicas": 1,
    "restartPolicyType": "ON_FAILURE",
    "sleepApplication": false
  },
  "environment": {
    "NODE_ENV": "production",
    "DATABASE_URL": "${{Postgres.DATABASE_URL}}",
    "PORT": "3000"
  }
}
```

### **PostgreSQL Optimizations:**
```sql
-- CONFIGURACIÓN RAILWAY POSTGRES HOBBY
ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';
ALTER SYSTEM SET pg_stat_statements.track = 'all';

-- MEMORY & PERFORMANCE  
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET effective_cache_size = '1GB'; 
ALTER SYSTEM SET work_mem = '16MB';
ALTER SYSTEM SET maintenance_work_mem = '128MB';

-- CONNECTION POOLING
ALTER SYSTEM SET max_connections = '100';

-- QUERY OPTIMIZATION
ALTER SYSTEM SET random_page_cost = '1.1';
ALTER SYSTEM SET seq_page_cost = '1.0';

-- RESTART REQUIRED
SELECT pg_reload_conf();
```

### **Connection Pooling:**
```javascript
// api/database.js
const { Pool } = require('pg');

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false },
    // POOL OPTIMIZADO PARA RAILWAY
    max: 20,                    // Máximo 20 conexiones
    idleTimeoutMillis: 30000,   // 30 segundos timeout
    connectionTimeoutMillis: 2000, // 2 segundos para conectar
    query_timeout: 5000,        // 5 segundos max por query
});

module.exports = pool;
```

---

## 🚀 **FASE 5: DEPLOY RAILWAY COMPLETO**

### **Setup Railway Project:**
```bash
# 1. Instalar Railway CLI
npm install -g @railway/cli

# 2. Login y crear proyecto
railway login
railway init
railway add postgresql

# 3. Variables de entorno
railway variables set NODE_ENV=production
railway variables set DATABASE_URL=${{Postgres.DATABASE_URL}}

# 4. Deploy
railway up
```

### **Estructura Final Deploy:**
```
🚀 RAILWAY PROJECT: el-pollo-loco-dashboard
│
├── 🗄️ PostgreSQL Service
│   ├── URL: postgresql://user:pass@hostname:port/db
│   ├── 476 supervisiones + áreas
│   ├── Vistas materializadas
│   └── Índices optimizados
│
└── 🌐 Web Service  
    ├── URL: https://el-pollo-loco-dashboard.railway.app
    ├── API Backend (/api/*)
    ├── Frontend (tu diseño iOS)
    └── Toggle Operativas/Seguridad
```

---

## 📊 **CRONOGRAMA IMPLEMENTACIÓN**

### **SEMANA 1:**
```
Día 1: 🗄️ Setup PostgreSQL Railway + Migración datos
Día 2: 🔧 Clonación exacta tu dashboard 
Día 3: 🛡️ Implementar toggle switch
Día 4: ⚡ Optimización queries + índices
Día 5: 🚀 Deploy + testing completo
```

### **SEMANA 2:**
```
Día 1-2: 🐛 Debugging + refinamientos
Día 3: 📱 Testing móvil + performance
Día 4: 📊 Validación datos + métricas
Día 5: ✅ Entrega final + documentación
```

---

## 💰 **COSTOS RAILWAY**

```
🗄️ PostgreSQL Hobby: $5/mes
🌐 Web Service: $5/mes  
📡 Networking: $0
═══════════════════════
💵 TOTAL: $10/mes (vs $20+ Render + Neon)
```

---

## 🎯 **ENTREGABLES FINALES**

✅ **Dashboard iOS clonado exacto** con tu diseño  
✅ **Toggle switch** Operativas ↔ Seguridad  
✅ **PostgreSQL optimizado** en Railway  
✅ **API backend** con queries <200ms  
✅ **Deploy automático** funcionando  
✅ **Datos completos** 476 supervisiones + áreas  
✅ **Performance testing** completo  

---

**¿Te parece perfecto este plan Roberto? ¿Empiezo con la migración a PostgreSQL Railway y la clonación exacta de tu dashboard?**

**Con este setup tendrás todo en Railway, súper rápido, y mantienes tu diseño iOS perfecto con el toggle para separar operativas y seguridad.**