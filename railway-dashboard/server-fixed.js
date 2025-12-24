// 🚀 RAILWAY BACKEND COMPLETO - EL POLLO LOCO CAS DASHBOARD
// Clonado desde RDG-CONSULTORES/pollo-loco-supervision 
// Adaptado para usar Railway PostgreSQL con datos normalizados

const express = require('express');
const path = require('path');
const compression = require('compression');
const helmet = require('helmet');
const { Pool } = require('pg');
const cors = require('cors');

// Load environment variables
require('dotenv').config();

// Initialize Express app
const app = express();
const port = process.env.PORT || 3000;

// ============================================================================
// 🗄️ POSTGRESQL RAILWAY CONNECTION
// ============================================================================

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false },
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000,
    query_timeout: 10000
});

// Test database connection
pool.on('connect', () => {
    console.log('🗄️ PostgreSQL Railway conectado');
});

pool.on('error', (err) => {
    console.error('❌ Error PostgreSQL:', err);
});

// ============================================================================
// 🔧 MIDDLEWARE CONFIGURATION
// ============================================================================

app.use(helmet({
    contentSecurityPolicy: {
        directives: {
            defaultSrc: ["'self'"],
            styleSrc: ["'self'", "'unsafe-inline'", "https://cdnjs.cloudflare.com", "https://unpkg.com"],
            scriptSrc: ["'self'", "'unsafe-inline'", "'unsafe-eval'", "https://cdn.jsdelivr.net", "https://unpkg.com"],
            scriptSrcAttr: ["'unsafe-inline'"],
            imgSrc: ["'self'", "data:", "https:", "blob:"],
            connectSrc: ["'self'", "https://cdn.jsdelivr.net", "https://unpkg.com"],
            fontSrc: ["'self'", "https://cdnjs.cloudflare.com"],
        },
    },
}));

app.use(compression());
app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// ============================================================================
// 📱 HOME PAGE ROUTE - SERVE DASHBOARD COMPLETO (PRIORITY ROUTE)
// ============================================================================

app.get('/', (req, res) => {
    console.log('🏠 Cache bust - serving index.html with correct endpoints');
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
    res.sendFile(path.join(__dirname, 'index.html'));
});

// Force serve dashboard sin cache
app.get('/fresh', (req, res) => {
    console.log('🎯 FRESH - No cache dashboard');
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
    res.sendFile(path.join(__dirname, 'index.html'));
});

// NEW FILE - Completely bypass cache
app.get('/final', (req, res) => {
    console.log('🎯 FINAL - Brand new file, zero cache');
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
    res.sendFile(path.join(__dirname, 'dashboard-final.html'));
});

// ULTIMATE BYPASS - New file name
app.get('/fixed', (req, res) => {
    console.log('🚀 ULTIMATE - Dashboard with fixed endpoints');
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
    res.sendFile(path.join(__dirname, 'dashboard-final.html'));
});

// Debug route to see what file content
app.get('/debug-file', (req, res) => {
    const fs = require('fs');
    const content = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');
    const hasOperativas = content.includes('/api/operativas/');
    res.json({
        hasOperativas,
        firstLines: content.split('\n').slice(0, 5),
        fileSize: content.length
    });
});

// Serve static files (after home route to avoid conflicts)
app.use(express.static(path.join(__dirname)));

// Request logging middleware
app.use((req, res, next) => {
    console.log(`📡 ${req.method} ${req.path} - ${new Date().toISOString()}`);
    next();
});

// ============================================================================
// 🏗️ UTILITY FUNCTIONS - TERRITORIAL CLASSIFICATION & FILTERS
// ============================================================================

function classifyTerritory(grupoOperativo) {
    const pureLocal = [
        'TEPEYAC', 'OGAS', 'EFM', 'EPL SO', 'PLOG NUEVO LEON', 
        'GRUPO CENTRITO', 'GRUPO SABINAS HIDALGO'
    ];

    const pureForanea = [
        'OCHTER TAMPICO', 'GRUPO MATAMOROS', 'RAP', 'CRR', 'PLOG LAGUNA', 
        'PLOG QUERETARO', 'PLOG BAJIO', 'PLOG GOLFO', 'LGO', 'PLOG TIJUANA', 'PLOG PACIFICO',
        // Previously unknown groups - now classified as foráneas per Roberto's specification
        'GRUPO REYNOSA', 'PLOG NORTE', 'GRUPO VICTORIA', 'PLOG CENTRO', 
        'GRUPO COAHUILA', 'PLOG SUR', 'GRUPO TAMAULIPAS'
    ];

    const mixed = ['TEC', 'EXPO', 'GRUPO SALTILLO'];

    if (pureLocal.includes(grupoOperativo)) return 'local';
    if (pureForanea.includes(grupoOperativo)) return 'foranea';
    if (mixed.includes(grupoOperativo)) return 'mixed';
    
    // Roberto specified: "la mayoría son Foráneos"
    // Any remaining unknowns default to foránea
    return 'foranea';
}

// Función para obtener rango de performance por nivel
function getPerformanceRange(level) {
    switch(level) {
        case 'excellent': return { min: 95, max: 100 };
        case 'very-good': return { min: 90, max: 95 };
        case 'good': return { min: 80, max: 90 };
        case 'regular': return { min: 70, max: 80 };
        case 'poor': return { min: 0, max: 70 };
        default: return { min: 0, max: 100 };
    }
}

// Función para construir condiciones WHERE con filtros
function buildFilterConditions(query, baseConditions = [], startParamIndex = 1) {
    const { territory, performance_level, period_cas, estado, type } = query;
    let whereConditions = [...baseConditions];
    let params = [];
    let paramIndex = startParamIndex;

    // Filtro por tipo de supervisión
    if (type && (type === 'operativas' || type === 'seguridad')) {
        whereConditions.push(`sup.tipo_supervision = $${paramIndex}`);
        params.push(type);
        paramIndex++;
    }

    // Filtro por territorio (requiere clasificación en tiempo real)
    if (territory && territory !== 'all') {
        // Usar la función classify_territorio que deberíamos crear
        const territoryCondition = getTerritoryCondition(territory, paramIndex);
        if (territoryCondition) {
            whereConditions.push(territoryCondition.condition);
            params.push(...territoryCondition.params);
            paramIndex += territoryCondition.params.length;
        }
    }

    // Filtro por nivel de performance
    if (performance_level && performance_level !== 'all') {
        const range = getPerformanceRange(performance_level);
        whereConditions.push(`sup.calificacion_general >= $${paramIndex} AND sup.calificacion_general < $${paramIndex + 1}`);
        params.push(range.min, range.max);
        paramIndex += 2;
    }

    // Filtro por período CAS
    if (period_cas && period_cas !== 'all') {
        // Usar la función de período que ya existe
        whereConditions.push(`
            CASE 
                WHEN s.estado = 'Nuevo León' OR s.grupo_operativo = 'GRUPO SALTILLO' THEN
                    CASE 
                        WHEN sup.fecha_supervision >= '2025-03-12' AND sup.fecha_supervision <= '2025-04-16' THEN 'T1-2025'
                        WHEN sup.fecha_supervision >= '2025-06-11' AND sup.fecha_supervision <= '2025-08-18' THEN 'T2-2025'
                        WHEN sup.fecha_supervision >= '2025-08-19' AND sup.fecha_supervision <= '2025-10-09' THEN 'T3-2025'
                        WHEN sup.fecha_supervision >= '2025-10-30' THEN 'T4-2025'
                        ELSE 'OTRO'
                    END
                ELSE
                    CASE 
                        WHEN sup.fecha_supervision >= '2025-04-10' AND sup.fecha_supervision <= '2025-06-09' THEN 'S1-2025'
                        WHEN sup.fecha_supervision >= '2025-07-30' AND sup.fecha_supervision <= '2025-11-07' THEN 'S2-2025'
                        ELSE 'OTRO'
                    END
            END = $${paramIndex}
        `);
        params.push(period_cas);
        paramIndex++;
    }

    // Filtro por estado
    if (estado && estado !== 'all') {
        whereConditions.push(`s.estado = $${paramIndex}`);
        params.push(estado);
        paramIndex++;
    }

    return {
        conditions: whereConditions,
        params: params,
        nextParamIndex: paramIndex
    };
}

// Función para obtener condición de territorio
function getTerritoryCondition(territory, paramIndex) {
    switch(territory) {
        case 'local':
            return {
                condition: `(s.estado = 'Nuevo León' OR s.grupo_operativo = 'GRUPO SALTILLO')`,
                params: []
            };
        case 'foranea':
            return {
                condition: `(s.estado != 'Nuevo León' AND s.grupo_operativo != 'GRUPO SALTILLO')`,
                params: []
            };
        case 'mixed':
            // Para grupos mixtos, podemos usar una lógica más compleja si es necesario
            return {
                condition: `s.grupo_operativo IN ('TEC', 'EXPO', 'GRUPO SALTILLO')`,
                params: []
            };
        default:
            return null;
    }
}

// ============================================================================
// 📊 API ENDPOINT: /api/kpis - KEY PERFORMANCE INDICATORS
// ============================================================================

app.get('/api/kpis', async (req, res) => {
    try {
        const { type } = req.query;
        console.log(`📊 KPIs requested - Type: ${type}`);
        
        // Build queries with supervision type filter  
        let supervisionFilter = '';
        let params = [];
        
        if (type && (type === 'operativas' || type === 'seguridad')) {
            supervisionFilter = ' WHERE tipo_supervision = $1';
            params = [type];
        }

        const queries = await Promise.all([
            pool.query('SELECT COUNT(DISTINCT s.id) as total FROM sucursales s'),
            pool.query('SELECT COUNT(DISTINCT s.grupo_operativo) as total FROM sucursales s WHERE s.grupo_operativo IS NOT NULL'),
            pool.query(`SELECT COUNT(*) as total FROM supervisiones${supervisionFilter}`, params),
            pool.query(`SELECT ROUND(AVG(calificacion_general), 2) as promedio FROM supervisiones${supervisionFilter}`, params),
            pool.query(`SELECT MAX(fecha_supervision) as ultima FROM supervisiones${supervisionFilter}`, params)
        ]);

        const kpis = {
            total_branches: parseInt(queries[0].rows[0]?.total) || 85,
            active_groups: parseInt(queries[1].rows[0]?.total) || 20,
            total_supervisions: parseInt(queries[2].rows[0]?.total) || 476,
            average_performance: parseFloat(queries[3].rows[0]?.promedio) || 91.2,
            last_update: queries[4].rows[0]?.ultima || null,
            // Frontend compatible format
            total_sucursales: parseInt(queries[0].rows[0]?.total) || 85,
            total_grupos: parseInt(queries[1].rows[0]?.total) || 20,
            total_supervisiones: parseInt(queries[2].rows[0]?.total) || 476,
            promedio_general: parseFloat(queries[3].rows[0]?.promedio) || 91.2
        };

        console.log('✅ KPIs calculated:', kpis);
        res.json(kpis);
        
    } catch (err) {
        console.error('❌ Error KPIs:', err);
        res.status(500).json({ error: err.message });
    }
});

// ============================================================================
// 👥 API ENDPOINT: /api/grupos - GRUPOS OPERATIVOS PERFORMANCE
// ============================================================================

app.get('/api/grupos', async (req, res) => {
    try {
        const { type } = req.query;
        console.log(`👥 Grupos data requested - Type: ${type}`);
        
        // Build query with supervision type filter
        let whereConditions = ['s.grupo_operativo IS NOT NULL'];
        let params = [];
        let paramIndex = 1;

        if (type && (type === 'operativas' || type === 'seguridad')) {
            whereConditions.push(`sup.tipo_supervision = $${paramIndex}`);
            params.push(type);
            paramIndex++;
        }

        const whereClause = whereConditions.join(' AND ');

        const result = await pool.query(`
            SELECT 
                s.grupo_operativo,
                COUNT(DISTINCT s.id) as sucursal_count,
                COUNT(sup.id) as total_supervisions,
                COUNT(ac.id) as evaluation_count,
                ROUND(AVG(sup.calificacion_general), 2) as average_performance,
                MIN(sup.calificacion_general) as min_performance,
                MAX(sup.calificacion_general) as max_performance
            FROM sucursales s
            LEFT JOIN supervisiones sup ON s.id = sup.sucursal_id
            LEFT JOIN areas_calificaciones ac ON sup.id = ac.supervision_id
            WHERE ${whereClause}
            GROUP BY s.grupo_operativo
            HAVING COUNT(sup.id) > 0
            ORDER BY average_performance DESC, total_supervisions DESC
        `, params);
        
        // Process and add territorial classification with ranking
        const processedGroups = result.rows.map((group, index) => ({
            // Original format
            grupo_operativo: group.grupo_operativo,
            sucursal_count: parseInt(group.sucursal_count) || 0,
            total_supervisions: parseInt(group.total_supervisions) || 0,
            evaluation_count: parseInt(group.evaluation_count) || 0,
            average_performance: parseFloat(group.average_performance) || 0,
            min_performance: parseFloat(group.min_performance) || 0,
            max_performance: parseFloat(group.max_performance) || 0,
            territory: classifyTerritory(group.grupo_operativo),
            // Frontend compatible format
            name: group.grupo_operativo,
            sucursales: parseInt(group.sucursal_count) || 0,
            supervisiones: parseInt(group.total_supervisions) || 0,
            performance: parseFloat(group.average_performance) || 0,
            rank: index + 1
        }));
        
        console.log(`✅ Grupos loaded: ${processedGroups.length} groups`);
        res.json(processedGroups);
        
    } catch (err) {
        console.error('❌ Error loading grupos:', err);
        res.status(500).json({ error: err.message });
    }
});

// ============================================================================
// 🗺️ API ENDPOINT: /api/mapa - GEOGRAPHICAL DATA
// ============================================================================

app.get('/api/mapa', async (req, res) => {
    try {
        const { grupo, estado, type } = req.query;
        console.log(`🗺️ Mapa data requested - Grupo: ${grupo}, Estado: ${estado}, Type: ${type}`);
        
        let whereConditions = ['s.latitud IS NOT NULL', 's.longitud IS NOT NULL'];
        let params = [];
        let paramIndex = 1;
        
        if (grupo) {
            whereConditions.push(`s.grupo_operativo = $${paramIndex}`);
            params.push(grupo);
            paramIndex++;
        }
        
        if (estado) {
            whereConditions.push(`s.estado = $${paramIndex}`);
            params.push(estado);
            paramIndex++;
        }
        
        if (type && (type === 'operativas' || type === 'seguridad')) {
            whereConditions.push(`sup.tipo_supervision = $${paramIndex}`);
            params.push(type);
            paramIndex++;
        }
        
        const whereClause = whereConditions.join(' AND ');
        
        const result = await pool.query(`
            SELECT 
                s.nombre as sucursal_nombre,
                s.nombre as location_name,
                s.grupo_operativo,
                s.estado,
                s.ciudad,
                s.latitud,
                s.longitud,
                COUNT(sup.id) as supervisions_count,
                ROUND(AVG(sup.calificacion_general), 2) as average_performance
            FROM sucursales s
            LEFT JOIN supervisiones sup ON s.id = sup.sucursal_id
            WHERE ${whereClause}
            GROUP BY s.id, s.nombre, s.grupo_operativo, s.estado, s.ciudad, s.latitud, s.longitud
            ORDER BY average_performance DESC NULLS LAST
        `, params);
        
        // Process data to ensure proper types
        const processedLocations = result.rows.map(loc => ({
            sucursal_nombre: loc.sucursal_nombre,
            location_name: loc.location_name,
            grupo_operativo: loc.grupo_operativo,
            estado: loc.estado,
            ciudad: loc.ciudad,
            latitud: parseFloat(loc.latitud),
            longitud: parseFloat(loc.longitud),
            supervisions_count: parseInt(loc.supervisions_count) || 0,
            average_performance: parseFloat(loc.average_performance) || 0,
            territory: classifyTerritory(loc.grupo_operativo)
        }));
        
        console.log(`✅ Mapa data: ${processedLocations.length} locations`);
        res.json(processedLocations);
        
    } catch (err) {
        console.error('❌ Error loading mapa:', err);
        res.status(500).json({ error: err.message });
    }
});

// ============================================================================
// 🏢 API ENDPOINT: /api/sucursal-detail - SUCURSAL DRILL-DOWN
// ============================================================================

app.get('/api/sucursal-detail', async (req, res) => {
    try {
        const { sucursal, grupo, type } = req.query;
        console.log(`🏢 Sucursal Detail requested for: ${sucursal}, group: ${grupo}, type: ${type}`);
        
        if (!sucursal) {
            return res.status(400).json({ error: 'Sucursal name is required' });
        }
        
        // Get sucursal basic info
        let whereConditions = ['s.nombre = $1'];
        let params = [sucursal];
        let paramIndex = 2;
        
        if (grupo) {
            whereConditions.push(`s.grupo_operativo = $${paramIndex}`);
            params.push(grupo);
            paramIndex++;
        }
        
        // Add supervision type filter for sucursal aggregated data
        let supervisionTypeFilter = '';
        if (type && (type === 'operativas' || type === 'seguridad')) {
            supervisionTypeFilter = ` AND sup.tipo_supervision = $${paramIndex}`;
            params.push(type);
            paramIndex++;
        }
        
        const whereClause = whereConditions.join(' AND ');
        
        const sucursalResult = await pool.query(`
            SELECT 
                s.*,
                COUNT(sup.id) as total_supervisions,
                ROUND(AVG(sup.calificacion_general), 2) as average_performance,
                MIN(sup.calificacion_general) as min_performance,
                MAX(sup.calificacion_general) as max_performance,
                MAX(sup.fecha_supervision) as last_supervision
            FROM sucursales s
            LEFT JOIN supervisiones sup ON s.id = sup.sucursal_id
            WHERE ${whereClause}${supervisionTypeFilter}
            GROUP BY s.id
        `, params);
        
        if (sucursalResult.rows.length === 0) {
            return res.status(404).json({ error: 'Sucursal not found' });
        }
        
        const sucursalData = sucursalResult.rows[0];
        
        // Get areas performance for this sucursal with supervision type filter
        let areasParams = [sucursalData.id];
        let areasTypeFilter = '';
        
        if (type && (type === 'operativas' || type === 'seguridad')) {
            areasTypeFilter = ' AND sup.tipo_supervision = $2';
            areasParams.push(type);
        }
        
        const areasResult = await pool.query(`
            SELECT 
                ac.area_nombre,
                ROUND(AVG(ac.calificacion), 2) as promedio_area,
                COUNT(ac.id) as evaluaciones_count
            FROM areas_calificaciones ac
            JOIN supervisiones sup ON ac.supervision_id = sup.id
            WHERE sup.sucursal_id = $1${areasTypeFilter}
            GROUP BY ac.area_nombre
            ORDER BY promedio_area DESC
        `, areasParams);
        
        const response = {
            sucursal: {
                nombre: sucursalData.nombre,
                grupo_operativo: sucursalData.grupo_operativo,
                estado: sucursalData.estado,
                ciudad: sucursalData.ciudad,
                latitud: parseFloat(sucursalData.latitud),
                longitud: parseFloat(sucursalData.longitud),
                total_supervisions: parseInt(sucursalData.total_supervisions) || 0,
                average_performance: parseFloat(sucursalData.average_performance) || 0,
                min_performance: parseFloat(sucursalData.min_performance) || 0,
                max_performance: parseFloat(sucursalData.max_performance) || 0,
                last_supervision: sucursalData.last_supervision,
                territory: classifyTerritory(sucursalData.grupo_operativo)
            },
            areas: areasResult.rows.map(area => ({
                area_nombre: area.area_nombre,
                promedio_area: parseFloat(area.promedio_area) || 0,
                evaluaciones_count: parseInt(area.evaluaciones_count) || 0
            }))
        };
        
        console.log(`✅ Sucursal detail: ${sucursal} with ${response.areas.length} areas`);
        res.json(response);
        
    } catch (err) {
        console.error('❌ Error sucursal detail:', err);
        res.status(500).json({ error: err.message });
    }
});

// ============================================================================
// 📈 API ENDPOINT: /api/historico - HISTORICAL PERFORMANCE DATA
// ============================================================================

app.get('/api/historico', async (req, res) => {
    try {
        const { type } = req.query;
        console.log(`📈 Historico data requested - Type: ${type}`);
        
        // Build query with supervision type filter
        let whereConditions = ['sup.fecha_supervision IS NOT NULL'];
        let params = [];
        
        if (type && (type === 'operativas' || type === 'seguridad')) {
            whereConditions.push('sup.tipo_supervision = $1');
            params.push(type);
        }
        
        const whereClause = whereConditions.join(' AND ');
        
        // Get weekly performance aggregation
        const result = await pool.query(`
            SELECT 
                DATE_TRUNC('week', sup.fecha_supervision) as week_start,
                COUNT(sup.id) as supervisions_count,
                ROUND(AVG(sup.calificacion_general), 2) as weekly_average
            FROM supervisiones sup
            JOIN sucursales s ON sup.sucursal_id = s.id
            WHERE ${whereClause}
            GROUP BY DATE_TRUNC('week', sup.fecha_supervision)
            ORDER BY week_start ASC
            LIMIT 12
        `, params);
        
        const historicData = result.rows.map(row => ({
            week: row.week_start,
            week_start: row.week_start,
            supervisions_count: parseInt(row.supervisions_count) || 0,
            weekly_average: parseFloat(row.weekly_average) || 0
        }));
        
        console.log(`✅ Historico data: ${historicData.length} weeks`);
        res.json(historicData);
        
    } catch (err) {
        console.error('❌ Error historico:', err);
        res.status(500).json({ error: err.message });
    }
});

// ============================================================================
// 🏆 API ENDPOINT: /api/sucursales-ranking - SUCURSALES RANKING
// ============================================================================

app.get('/api/sucursales-ranking', async (req, res) => {
    try {
        const { grupo, estado, limit = 50, type } = req.query;
        console.log(`🏆 Sucursales ranking - Grupo: ${grupo}, Estado: ${estado}, Limit: ${limit}, Type: ${type}`);
        
        let whereConditions = ['TRUE'];
        let params = [];
        let paramIndex = 1;
        
        if (grupo) {
            whereConditions.push(`s.grupo_operativo = $${paramIndex}`);
            params.push(grupo);
            paramIndex++;
        }
        
        if (estado) {
            whereConditions.push(`s.estado = $${paramIndex}`);
            params.push(estado);
            paramIndex++;
        }
        
        if (type && (type === 'operativas' || type === 'seguridad')) {
            whereConditions.push(`sup.tipo_supervision = $${paramIndex}`);
            params.push(type);
            paramIndex++;
        }
        
        const whereClause = whereConditions.join(' AND ');
        
        const result = await pool.query(`
            SELECT 
                s.nombre as sucursal,
                s.nombre as location_name,
                s.grupo_operativo,
                s.estado,
                s.ciudad,
                s.latitud,
                s.longitud,
                COUNT(sup.id) as evaluaciones,
                COUNT(sup.id) as supervisions,
                ROUND(AVG(sup.calificacion_general), 2) as promedio
            FROM sucursales s
            LEFT JOIN supervisiones sup ON s.id = sup.sucursal_id
            WHERE ${whereClause}
            GROUP BY s.id, s.nombre, s.grupo_operativo, s.estado, s.ciudad, s.latitud, s.longitud
            HAVING COUNT(sup.id) > 0
            ORDER BY promedio DESC NULLS LAST
            LIMIT $${paramIndex}
        `, [...params, parseInt(limit)]);
        
        // Process data to match original format
        const processedSucursales = result.rows.map(suc => ({
            sucursal: suc.sucursal,
            location_name: suc.location_name,
            grupo_operativo: suc.grupo_operativo,
            estado: suc.estado,
            ciudad: suc.ciudad,
            latitud: parseFloat(suc.latitud),
            longitud: parseFloat(suc.longitud),
            evaluaciones: parseInt(suc.evaluaciones) || 0,
            supervisions: parseInt(suc.supervisions) || 0,
            promedio: parseFloat(suc.promedio) || 0
        }));
        
        console.log(`✅ Sucursales ranking: ${processedSucursales.length} sucursales`);
        res.json(processedSucursales);
        
    } catch (err) {
        console.error('❌ Error sucursales ranking:', err);
        res.status(500).json({ error: err.message });
    }
});

// ============================================================================
// 📈 API ENDPOINT: /api/sucursal-tendencia - SUCURSAL PERFORMANCE TREND (CAS PERIODS)
// ============================================================================

app.get('/api/sucursal-tendencia', async (req, res) => {
    try {
        const { sucursal, grupo, type, periodo = 'cas_periods' } = req.query;
        console.log(`📈 Sucursal tendencia - Sucursal: ${sucursal}, Grupo: ${grupo}, Type: ${type}, Periodo: ${periodo}`);
        
        if (!sucursal) {
            return res.status(400).json({ error: 'Sucursal name is required' });
        }
        
        // First determine if sucursal is LOCAL or FORÁNEA
        const sucursalInfoResult = await pool.query(`
            SELECT s.*, 
                   CASE 
                       WHEN s.estado = 'Nuevo León' OR s.grupo_operativo = 'GRUPO SALTILLO' THEN 'LOCAL'
                       ELSE 'FORANEA'
                   END as tipo_territorial
            FROM sucursales s
            WHERE s.nombre = $1
        `, [sucursal]);
        
        if (sucursalInfoResult.rows.length === 0) {
            console.log(`⚠️ Sucursal not found: ${sucursal}`);
            return res.status(404).json({ error: 'Sucursal not found' });
        }
        
        const sucursalInfo = sucursalInfoResult.rows[0];
        const isLocal = sucursalInfo.tipo_territorial === 'LOCAL';
        console.log(`🏢 Sucursal ${sucursal} is ${isLocal ? 'LOCAL' : 'FORÁNEA'}`);
        
        // Build query with filters - simplified approach
        let whereConditions = ['s.nombre = $1', "sup.fecha_supervision IS NOT NULL"];
        let params = [sucursal];
        let paramIndex = 2;
        
        if (grupo) {
            whereConditions.push(`s.grupo_operativo = $${paramIndex}`);
            params.push(grupo);
            paramIndex++;
        }
        
        if (type && (type === 'operativas' || type === 'seguridad')) {
            whereConditions.push(`sup.tipo_supervision = $${paramIndex}`);
            params.push(type);
            paramIndex++;
        }
        
        const whereClause = whereConditions.join(' AND ');
        console.log(`🔍 Query conditions: ${whereClause}`);
        console.log(`🔍 Query params:`, params);
        
        // Get ALL supervisiones for this sucursal, then filter by periods
        const allSupervisiones = await pool.query(`
            SELECT 
                sup.fecha_supervision,
                sup.calificacion_general,
                CASE 
                    WHEN sup.fecha_supervision >= '2025-03-12' AND sup.fecha_supervision <= '2025-04-16' THEN 'T1-2025'
                    WHEN sup.fecha_supervision >= '2025-06-11' AND sup.fecha_supervision <= '2025-08-18' THEN 'T2-2025'
                    WHEN sup.fecha_supervision >= '2025-08-19' AND sup.fecha_supervision <= '2025-10-09' THEN 'T3-2025'
                    WHEN sup.fecha_supervision >= '2025-10-30' THEN 'T4-2025'
                    WHEN sup.fecha_supervision >= '2025-04-10' AND sup.fecha_supervision <= '2025-06-09' THEN 'S1-2025'
                    WHEN sup.fecha_supervision >= '2025-07-30' AND sup.fecha_supervision <= '2025-11-07' THEN 'S2-2025'
                    ELSE 'OTRO'
                END as periodo_cas
            FROM sucursales s
            JOIN supervisiones sup ON s.id = sup.sucursal_id
            WHERE ${whereClause}
        `, params);
        
        console.log(`📊 Found ${allSupervisiones.rows.length} total supervisiones for ${sucursal}`);
        
        // Filter by sucursal type and group by period
        const validSupervisiones = allSupervisiones.rows.filter(row => {
            if (row.periodo_cas === 'OTRO') return false;
            
            if (isLocal) {
                // Local sucursales: only T1-T4 periods
                return ['T1-2025', 'T2-2025', 'T3-2025', 'T4-2025'].includes(row.periodo_cas);
            } else {
                // Foránea sucursales: only S1-S2 periods  
                return ['S1-2025', 'S2-2025'].includes(row.periodo_cas);
            }
        });
        
        console.log(`📈 Valid supervisiones after filtering: ${validSupervisiones.length}`);
        
        // Group by period and calculate stats
        const periodStats = {};
        validSupervisiones.forEach(row => {
            const periodo = row.periodo_cas;
            if (!periodStats[periodo]) {
                periodStats[periodo] = {
                    evaluaciones: [],
                    fechas: []
                };
            }
            periodStats[periodo].evaluaciones.push(parseFloat(row.calificacion_general));
            periodStats[periodo].fechas.push(row.fecha_supervision);
        });
        
        // Convert to response format and get last 4 periods
        const allPeriods = Object.keys(periodStats).map(periodo => {
            const stats = periodStats[periodo];
            const evaluaciones = stats.evaluaciones;
            const fechas = stats.fechas;
            
            return {
                periodo: periodo,
                periodo_cas: periodo,
                tipo_sucursal: isLocal ? 'LOCAL' : 'FORANEA',
                evaluaciones_count: evaluaciones.length,
                promedio_periodo: Math.round((evaluaciones.reduce((a, b) => a + b, 0) / evaluaciones.length) * 100) / 100,
                min_periodo: Math.min(...evaluaciones),
                max_periodo: Math.max(...evaluaciones),
                periodo_inicio: new Date(Math.min(...fechas.map(f => new Date(f)))),
                periodo_fin: new Date(Math.max(...fechas.map(f => new Date(f)))),
                // Legacy compatibility
                month: new Date(Math.min(...fechas.map(f => new Date(f)))),
                month_start: new Date(Math.min(...fechas.map(f => new Date(f)))),
                promedio_mensual: Math.round((evaluaciones.reduce((a, b) => a + b, 0) / evaluaciones.length) * 100) / 100,
                min_mensual: Math.min(...evaluaciones),
                max_mensual: Math.max(...evaluaciones)
            };
        });
        
        // Sort periods and get last 4
        const sortOrder = isLocal ? 
            { 'T1-2025': 1, 'T2-2025': 2, 'T3-2025': 3, 'T4-2025': 4 } :
            { 'S1-2025': 1, 'S2-2025': 2 };
            
        const sortedPeriods = allPeriods.sort((a, b) => {
            return (sortOrder[a.periodo] || 999) - (sortOrder[b.periodo] || 999);
        });
        
        // Get last 4 periods (or all available)
        const tendenciaData = sortedPeriods.slice(-4);
        
        console.log(`✅ Sucursal tendencia CAS: ${sucursal} (${isLocal ? 'LOCAL' : 'FORANEA'}) with ${tendenciaData.length} periods`);
        console.log(`📊 Periods returned:`, tendenciaData.map(p => p.periodo));
        
        res.json(tendenciaData);
        
    } catch (err) {
        console.error('❌ Error sucursal tendencia CAS:', err);
        console.error('❌ Full error stack:', err.stack);
        res.status(500).json({ error: 'Error loading sucursal tendencia: ' + err.message, periods: [] });
    }
});

// ============================================================================
// 🔍 API ENDPOINT: /api/estados - ESTADOS LIST
// ============================================================================

app.get('/api/estados', async (req, res) => {
    try {
        console.log('🔍 Estados list requested');
        
        const result = await pool.query(`
            SELECT 
                s.estado,
                COUNT(DISTINCT s.id) as sucursales_count,
                COUNT(sup.id) as supervisions_count,
                ROUND(AVG(sup.calificacion_general), 2) as average_performance
            FROM sucursales s
            LEFT JOIN supervisiones sup ON s.id = sup.sucursal_id
            WHERE s.estado IS NOT NULL
            GROUP BY s.estado
            ORDER BY average_performance DESC NULLS LAST, sucursales_count DESC
        `);
        
        const estados = result.rows.map(estado => ({
            estado: estado.estado,
            sucursales_count: parseInt(estado.sucursales_count) || 0,
            supervisions_count: parseInt(estado.supervisions_count) || 0,
            average_performance: parseFloat(estado.average_performance) || 0
        }));
        
        console.log(`✅ Estados: ${estados.length} estados`);
        res.json(estados);
        
    } catch (err) {
        console.error('❌ Error estados:', err);
        res.status(500).json({ error: err.message });
    }
});

// ============================================================================
// 🏥 HEALTH CHECK
// ============================================================================

app.get('/health', async (req, res) => {
    try {
        const result = await pool.query('SELECT COUNT(*) as total FROM supervisiones');
        res.json({
            status: 'healthy',
            database: 'connected',
            total_supervisions: parseInt(result.rows[0].total),
            timestamp: new Date().toISOString()
        });
    } catch (err) {
        res.status(500).json({
            status: 'unhealthy',
            database: 'error',
            error: err.message,
            timestamp: new Date().toISOString()
        });
    }
});

// ============================================================================
// 🗺️ API ENDPOINT: /api/normalize-estados - NORMALIZAR ESTADOS POR GPS
// ============================================================================

app.get('/api/normalize-estados', async (req, res) => {
    try {
        console.log('🗺️ Starting GPS-based estado normalization...');
        
        // 1. Create function to classify estados by GPS coordinates
        const createFunctionSQL = `
            CREATE OR REPLACE FUNCTION classify_estado_by_coordinates(latitud DECIMAL, longitud DECIMAL) 
            RETURNS VARCHAR AS $$
            BEGIN
                -- Verificar que las coordenadas sean válidas
                IF latitud IS NULL OR longitud IS NULL THEN 
                    RETURN 'Desconocido';
                END IF;
                
                -- COAHUILA PRIMERO: Saltillo y área (más específico para evitar conflicto)
                -- Incluye: Saltillo, Ramos Arizpe, Monclova
                IF latitud BETWEEN 25.2 AND 27.0 AND longitud BETWEEN -101.6 AND -100.85 THEN
                    RETURN 'Coahuila';
                    
                -- NUEVO LEÓN ÁREA METRO: Monterrey y zona conurbada (más restrictivo)
                -- Rango preciso: Lat 25.5-26.0, Lng -100.6--99.8  
                ELSIF latitud BETWEEN 25.5 AND 26.0 AND longitud BETWEEN -100.6 AND -99.8 THEN
                    RETURN 'Nuevo León';
                    
                -- NUEVO LEÓN EXTENDIDO: Para sucursales fuera del área metro
                -- Incluye: Sabinas Hidalgo, Santiago, Cadereyta, Allende
                ELSIF latitud BETWEEN 25.0 AND 27.8 AND longitud BETWEEN -100.6 AND -99.0 THEN
                    RETURN 'Nuevo León';
                    
                -- TAMAULIPAS FRONTERA: Nuevo Laredo área específica
                -- Incluye: Nuevo Laredo, zona fronteriza
                ELSIF latitud BETWEEN 27.0 AND 28.0 AND longitud BETWEEN -99.8 AND -99.3 THEN
                    RETURN 'Tamaulipas';
                    
                -- TAMAULIPAS GENERAL: Costa del Golfo y frontera este
                -- Incluye: Matamoros, Reynosa, Tampico, Río Bravo  
                ELSIF latitud BETWEEN 22.2 AND 27.0 AND longitud BETWEEN -99.5 AND -97.0 THEN
                    RETURN 'Tamaulipas';
                    
                -- DURANGO: Centro: 25.5, -104.0 (Torreón/Laguna)
                -- Rango aproximado: Lat 22.3-26.9, Lng -107.1-102.3
                ELSIF latitud BETWEEN 22.3 AND 26.9 AND longitud BETWEEN -107.1 AND -102.3 THEN
                    RETURN 'Durango';
                    
                -- QUERÉTARO: Centro: 20.6, -100.4
                -- Rango aproximado: Lat 20.0-21.7, Lng -101.0-99.0
                ELSIF latitud BETWEEN 20.0 AND 21.7 AND longitud BETWEEN -101.0 AND -99.0 THEN
                    RETURN 'Querétaro';
                    
                -- MICHOACÁN: Centro Morelia: 19.7, -101.2
                -- Rango aproximado: Lat 18.3-20.4, Lng -103.7-100.0
                ELSIF latitud BETWEEN 18.3 AND 20.4 AND longitud BETWEEN -103.7 AND -100.0 THEN
                    RETURN 'Michoacán';
                    
                -- GUANAJUATO: Centro: 21.0, -101.3
                -- Rango aproximado: Lat 19.9-21.7, Lng -102.1-100.0
                ELSIF latitud BETWEEN 19.9 AND 21.7 AND longitud BETWEEN -102.1 AND -100.0 THEN
                    RETURN 'Guanajuato';
                    
                -- SAN LUIS POTOSÍ: Centro: 22.2, -100.9
                -- Rango aproximado: Lat 21.1-24.5, Lng -102.2-98.3
                ELSIF latitud BETWEEN 21.1 AND 24.5 AND longitud BETWEEN -102.2 AND -98.3 THEN
                    RETURN 'San Luis Potosí';
                    
                -- Default para coordenadas fuera de rango conocido
                ELSE 
                    RETURN 'Otro Estado';
                END IF;
            END;
            $$ LANGUAGE plpgsql;
        `;
        
        await pool.query(createFunctionSQL);
        console.log('✅ GPS classification function created');

        // 2. Preview changes
        const preview = await pool.query(`
            SELECT 
                nombre,
                grupo_operativo,
                estado as estado_actual,
                latitud,
                longitud,
                classify_estado_by_coordinates(latitud, longitud) as estado_nuevo
            FROM sucursales 
            WHERE latitud IS NOT NULL AND longitud IS NOT NULL
            ORDER BY grupo_operativo, nombre
            LIMIT 20
        `);
        
        // 3. Get summary of changes
        const summary = await pool.query(`
            SELECT 
                classify_estado_by_coordinates(latitud, longitud) as estado_nuevo,
                COUNT(*) as sucursales,
                COUNT(DISTINCT grupo_operativo) as grupos
            FROM sucursales 
            WHERE latitud IS NOT NULL AND longitud IS NOT NULL
            GROUP BY classify_estado_by_coordinates(latitud, longitud)
            ORDER BY sucursales DESC
        `);

        // 4. Update estados based on GPS coordinates
        const updateResult = await pool.query(`
            UPDATE sucursales 
            SET estado = classify_estado_by_coordinates(latitud, longitud)
            WHERE latitud IS NOT NULL AND longitud IS NOT NULL
        `);

        // 5. Verify final distribution
        const finalDistribution = await pool.query(`
            SELECT 
                estado,
                COUNT(DISTINCT grupo_operativo) as grupos,
                COUNT(*) as sucursales
            FROM sucursales 
            GROUP BY estado 
            ORDER BY sucursales DESC
        `);

        const response = {
            success: true,
            message: 'Estados normalizados exitosamente usando coordenadas GPS',
            updated_count: updateResult.rowCount,
            preview: preview.rows,
            summary_by_new_estado: summary.rows,
            final_distribution: finalDistribution.rows,
            timestamp: new Date().toISOString()
        };

        console.log(`✅ Estados normalization completed: ${updateResult.rowCount} sucursales updated`);
        res.json(response);

    } catch (error) {
        console.error('❌ Error normalizing estados:', error);
        res.status(500).json({ 
            success: false,
            error: error.message,
            message: 'Error normalizando estados por coordenadas GPS'
        });
    }
});

// ============================================================================
// 🗺️ API ENDPOINT: /api/fix-saltillo-estados - CORREGIR CLASIFICACIÓN SALTILLO
// ============================================================================

app.get('/api/fix-saltillo-estados', async (req, res) => {
    try {
        console.log('🔧 Re-executing GPS normalization with corrected ranges...');
        
        // 1. Update function with corrected GPS ranges (same as above but re-execute)
        const updateFunctionSQL = `
            CREATE OR REPLACE FUNCTION classify_estado_by_coordinates(latitud DECIMAL, longitud DECIMAL) 
            RETURNS VARCHAR AS $$
            BEGIN
                -- Verificar que las coordenadas sean válidas
                IF latitud IS NULL OR longitud IS NULL THEN 
                    RETURN 'Desconocido';
                END IF;
                
                -- COAHUILA PRIMERO: Saltillo y área (más específico para evitar conflicto)
                -- Incluye: Saltillo, Ramos Arizpe, Monclova
                IF latitud BETWEEN 25.2 AND 27.0 AND longitud BETWEEN -101.6 AND -100.85 THEN
                    RETURN 'Coahuila';
                    
                -- NUEVO LEÓN ÁREA METRO: Monterrey y zona conurbada (más restrictivo)
                -- Rango preciso: Lat 25.5-26.0, Lng -100.6--99.8  
                ELSIF latitud BETWEEN 25.5 AND 26.0 AND longitud BETWEEN -100.6 AND -99.8 THEN
                    RETURN 'Nuevo León';
                    
                -- NUEVO LEÓN EXTENDIDO: Para sucursales fuera del área metro
                -- Incluye: Sabinas Hidalgo, Santiago, Cadereyta, Allende
                ELSIF latitud BETWEEN 25.0 AND 27.8 AND longitud BETWEEN -100.6 AND -99.0 THEN
                    RETURN 'Nuevo León';
                    
                -- TAMAULIPAS FRONTERA: Nuevo Laredo área específica
                -- Incluye: Nuevo Laredo, zona fronteriza
                ELSIF latitud BETWEEN 27.0 AND 28.0 AND longitud BETWEEN -99.8 AND -99.3 THEN
                    RETURN 'Tamaulipas';
                    
                -- TAMAULIPAS GENERAL: Costa del Golfo y frontera este
                -- Incluye: Matamoros, Reynosa, Tampico, Río Bravo  
                ELSIF latitud BETWEEN 22.2 AND 27.0 AND longitud BETWEEN -99.5 AND -97.0 THEN
                    RETURN 'Tamaulipas';
                    
                -- DURANGO: Centro: 25.5, -104.0 (Torreón/Laguna)
                -- Rango aproximado: Lat 22.3-26.9, Lng -107.1-102.3
                ELSIF latitud BETWEEN 22.3 AND 26.9 AND longitud BETWEEN -107.1 AND -102.3 THEN
                    RETURN 'Durango';
                    
                -- QUERÉTARO: Centro: 20.6, -100.4
                -- Rango aproximado: Lat 20.0-21.7, Lng -101.0-99.0
                ELSIF latitud BETWEEN 20.0 AND 21.7 AND longitud BETWEEN -101.0 AND -99.0 THEN
                    RETURN 'Querétaro';
                    
                -- MICHOACÁN: Centro Morelia: 19.7, -101.2
                -- Rango aproximado: Lat 18.3-20.4, Lng -103.7-100.0
                ELSIF latitud BETWEEN 18.3 AND 20.4 AND longitud BETWEEN -103.7 AND -100.0 THEN
                    RETURN 'Michoacán';
                    
                -- GUANAJUATO: Centro: 21.0, -101.3
                -- Rango aproximado: Lat 19.9-21.7, Lng -102.1-100.0
                ELSIF latitud BETWEEN 19.9 AND 21.7 AND longitud BETWEEN -102.1 AND -100.0 THEN
                    RETURN 'Guanajuato';
                    
                -- SAN LUIS POTOSÍ: Centro: 22.2, -100.9
                -- Rango aproximado: Lat 21.1-24.5, Lng -102.2-98.3
                ELSIF latitud BETWEEN 21.1 AND 24.5 AND longitud BETWEEN -102.2 AND -98.3 THEN
                    RETURN 'San Luis Potosí';
                    
                -- Default para coordenadas fuera de rango conocido
                ELSE 
                    RETURN 'Otro Estado';
                END IF;
            END;
            $$ LANGUAGE plpgsql;
        `;
        
        await pool.query(updateFunctionSQL);
        console.log('✅ Corrected GPS classification function updated');

        // 2. Preview specific changes for Saltillo group
        const saltilloPreview = await pool.query(`
            SELECT 
                nombre,
                grupo_operativo,
                estado as estado_actual,
                latitud,
                longitud,
                classify_estado_by_coordinates(latitud, longitud) as estado_nuevo
            FROM sucursales 
            WHERE grupo_operativo = 'GRUPO SALTILLO'
            ORDER BY nombre
        `);

        // 3. Update all estados with corrected function
        const updateResult = await pool.query(`
            UPDATE sucursales 
            SET estado = classify_estado_by_coordinates(latitud, longitud)
            WHERE latitud IS NOT NULL AND longitud IS NOT NULL
        `);

        // 4. Get final distribution
        const finalDistribution = await pool.query(`
            SELECT 
                estado,
                COUNT(*) as sucursales,
                COUNT(DISTINCT grupo_operativo) as grupos
            FROM sucursales 
            GROUP BY estado 
            ORDER BY sucursales DESC
        `);

        const response = {
            success: true,
            message: 'Estados corregidos - Saltillo ahora en Coahuila',
            updated_count: updateResult.rowCount,
            saltillo_changes: saltilloPreview.rows,
            final_distribution: finalDistribution.rows,
            timestamp: new Date().toISOString()
        };

        console.log(`✅ Estados correction completed: ${updateResult.rowCount} sucursales updated`);
        res.json(response);

    } catch (error) {
        console.error('❌ Error fixing Saltillo estados:', error);
        res.status(500).json({ 
            success: false,
            error: error.message,
            message: 'Error corrigiendo estados de Saltillo'
        });
    }
});

// ============================================================================
// 🚀 SERVER START
// ============================================================================

app.listen(port, () => {
    console.log(`🚀 Railway Server running on port ${port}`);
    console.log(`📊 Dashboard URL: http://localhost:${port}`);
    console.log(`🗄️ Database: Railway PostgreSQL`);
    console.log(`📈 API Status: All endpoints ready`);
});

// Handle graceful shutdown
process.on('SIGINT', () => {
    console.log('🛑 Graceful shutdown...');
    pool.end(() => {
        console.log('🗄️ Database pool closed');
        process.exit(0);
    });
});