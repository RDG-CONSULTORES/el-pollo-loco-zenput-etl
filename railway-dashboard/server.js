// 🚀 RAILWAY BACKEND API - EL POLLO LOCO DASHBOARD
// API optimizado para velocidad máxima con PostgreSQL Railway
// Roberto: Backend completo con toggle operativas/seguridad

const express = require('express');
const { Pool } = require('pg');
const path = require('path');
const compression = require('compression');
const helmet = require('helmet');
const cors = require('cors');

const app = express();
const port = process.env.PORT || 3000;

// ============================================================================
// 🗄️ POSTGRESQL RAILWAY CONNECTION
// ============================================================================

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false },
    // POOL OPTIMIZADO PARA RAILWAY
    max: 20,                    // Máximo 20 conexiones
    idleTimeoutMillis: 30000,   // 30 segundos timeout
    connectionTimeoutMillis: 2000, // 2 segundos para conectar
    query_timeout: 5000,        // 5 segundos max por query
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

// Security and performance middleware
app.use(helmet({
    contentSecurityPolicy: false, // Disable for development
}));
app.use(compression());
app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// Serve static files
app.use(express.static(path.join(__dirname)));

// Request logging middleware
app.use((req, res, next) => {
    console.log(`📡 ${req.method} ${req.path} - ${new Date().toISOString()}`);
    next();
});

// ============================================================================
// 🔧 OPERATIVAS ENDPOINTS
// ============================================================================

// KPIs Operativas
app.get('/api/operativas/kpis', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT * FROM kpis_operativas
        `);
        
        if (result.rows.length > 0) {
            res.json(result.rows[0]);
        } else {
            res.json({
                total_sucursales: 0,
                total_grupos: 0,
                total_supervisiones: 0,
                promedio_general: 0,
                supervisiones_excelentes: 0,
                supervisiones_buenas: 0,
                ultima_actualizacion: null
            });
        }
    } catch (err) {
        console.error('Error KPIs operativas:', err);
        res.status(500).json({ error: err.message });
    }
});

// Dashboard Principal Operativas
app.get('/api/operativas/dashboard', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT * FROM dashboard_operativas 
            ORDER BY promedio_calificacion DESC NULLS LAST
        `);
        res.json(result.rows);
    } catch (err) {
        console.error('Error dashboard operativas:', err);
        res.status(500).json({ error: err.message });
    }
});

// Mapa Operativas
app.get('/api/operativas/mapa', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT 
                sucursal_nombre as nombre,
                latitud,
                longitud,
                grupo_operativo,
                promedio_calificacion,
                total_supervisiones
            FROM dashboard_operativas
            WHERE latitud IS NOT NULL 
            AND longitud IS NOT NULL
            ORDER BY promedio_calificacion DESC NULLS LAST
        `);
        res.json(result.rows);
    } catch (err) {
        console.error('Error mapa operativas:', err);
        res.status(500).json({ error: err.message });
    }
});

// Áreas Operativas
app.get('/api/operativas/areas', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT 
                ac.area_nombre,
                ROUND(AVG(ac.calificacion), 1) as promedio_area,
                COUNT(ac.id) as total_evaluaciones
            FROM areas_calificaciones ac
            JOIN supervisiones s ON ac.supervision_id = s.id
            WHERE s.tipo_supervision = 'operativas'
            GROUP BY ac.area_nombre
            HAVING AVG(ac.calificacion) IS NOT NULL
            ORDER BY promedio_area DESC
            LIMIT 20
        `);
        res.json(result.rows);
    } catch (err) {
        console.error('Error áreas operativas:', err);
        res.status(500).json({ error: err.message });
    }
});

// Histórico Operativas
app.get('/api/operativas/historico', async (req, res) => {
    const { grupo, sucursal } = req.query;
    
    let whereClause = `WHERE s.tipo_supervision = 'operativas'`;
    let params = [];
    let paramCount = 0;
    
    if (grupo) {
        whereClause += ` AND suc.grupo_operativo = $${++paramCount}`;
        params.push(grupo);
    }
    
    if (sucursal) {
        whereClause += ` AND suc.nombre = $${++paramCount}`;
        params.push(sucursal);
    }
    
    try {
        const result = await pool.query(`
            SELECT 
                DATE_TRUNC('week', s.fecha_supervision) as semana,
                ROUND(AVG(s.calificacion_general), 1) as promedio_semanal,
                COUNT(s.id) as total_supervisiones
            FROM supervisiones s
            JOIN sucursales suc ON s.sucursal_id = suc.id
            ${whereClause}
            GROUP BY DATE_TRUNC('week', s.fecha_supervision)
            ORDER BY semana DESC
            LIMIT 12
        `, params);
        
        res.json(result.rows);
    } catch (err) {
        console.error('Error histórico operativas:', err);
        res.status(500).json({ error: err.message });
    }
});

// ============================================================================
// 🛡️ SEGURIDAD ENDPOINTS
// ============================================================================

// KPIs Seguridad
app.get('/api/seguridad/kpis', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT * FROM kpis_seguridad
        `);
        
        if (result.rows.length > 0) {
            res.json(result.rows[0]);
        } else {
            res.json({
                total_sucursales: 0,
                total_grupos: 0,
                total_supervisiones: 0,
                promedio_general: 0,
                supervisiones_excelentes: 0,
                supervisiones_buenas: 0,
                ultima_actualizacion: null
            });
        }
    } catch (err) {
        console.error('Error KPIs seguridad:', err);
        res.status(500).json({ error: err.message });
    }
});

// Dashboard Principal Seguridad
app.get('/api/seguridad/dashboard', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT * FROM dashboard_seguridad 
            ORDER BY promedio_calificacion DESC NULLS LAST
        `);
        res.json(result.rows);
    } catch (err) {
        console.error('Error dashboard seguridad:', err);
        res.status(500).json({ error: err.message });
    }
});

// Mapa Seguridad
app.get('/api/seguridad/mapa', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT 
                sucursal_nombre as nombre,
                latitud,
                longitud,
                grupo_operativo,
                promedio_calificacion,
                total_supervisiones
            FROM dashboard_seguridad
            WHERE latitud IS NOT NULL 
            AND longitud IS NOT NULL
            ORDER BY promedio_calificacion DESC NULLS LAST
        `);
        res.json(result.rows);
    } catch (err) {
        console.error('Error mapa seguridad:', err);
        res.status(500).json({ error: err.message });
    }
});

// Áreas Seguridad
app.get('/api/seguridad/areas', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT 
                ac.area_nombre,
                ROUND(AVG(ac.calificacion), 1) as promedio_area,
                COUNT(ac.id) as total_evaluaciones
            FROM areas_calificaciones ac
            JOIN supervisiones s ON ac.supervision_id = s.id
            WHERE s.tipo_supervision = 'seguridad'
            GROUP BY ac.area_nombre
            HAVING AVG(ac.calificacion) IS NOT NULL
            ORDER BY promedio_area DESC
            LIMIT 20
        `);
        res.json(result.rows);
    } catch (err) {
        console.error('Error áreas seguridad:', err);
        res.status(500).json({ error: err.message });
    }
});

// Histórico Seguridad
app.get('/api/seguridad/historico', async (req, res) => {
    const { grupo, sucursal } = req.query;
    
    let whereClause = `WHERE s.tipo_supervision = 'seguridad'`;
    let params = [];
    let paramCount = 0;
    
    if (grupo) {
        whereClause += ` AND suc.grupo_operativo = $${++paramCount}`;
        params.push(grupo);
    }
    
    if (sucursal) {
        whereClause += ` AND suc.nombre = $${++paramCount}`;
        params.push(sucursal);
    }
    
    try {
        const result = await pool.query(`
            SELECT 
                DATE_TRUNC('week', s.fecha_supervision) as semana,
                ROUND(AVG(s.calificacion_general), 1) as promedio_semanal,
                COUNT(s.id) as total_supervisiones
            FROM supervisiones s
            JOIN sucursales suc ON s.sucursal_id = suc.id
            ${whereClause}
            GROUP BY DATE_TRUNC('week', s.fecha_supervision)
            ORDER BY semana DESC
            LIMIT 12
        `, params);
        
        res.json(result.rows);
    } catch (err) {
        console.error('Error histórico seguridad:', err);
        res.status(500).json({ error: err.message });
    }
});

// ============================================================================
// 📊 DRILL-DOWN ENDPOINTS (Para ambos tipos)
// ============================================================================

// Drill-down por Grupo Operativo
app.get('/api/:tipo/grupo/:grupo', async (req, res) => {
    const { tipo, grupo } = req.params;
    
    if (!['operativas', 'seguridad'].includes(tipo)) {
        return res.status(400).json({ error: 'Tipo debe ser operativas o seguridad' });
    }
    
    try {
        const result = await pool.query(`
            SELECT 
                s.nombre as sucursal_nombre,
                s.numero as sucursal_numero,
                AVG(sup.calificacion_general) as promedio,
                COUNT(sup.id) as total_supervisiones,
                MAX(sup.fecha_supervision) as ultima_supervision
            FROM sucursales s
            LEFT JOIN supervisiones sup ON s.id = sup.sucursal_id 
            WHERE s.grupo_operativo = $1 
            AND (sup.tipo_supervision = $2 OR sup.tipo_supervision IS NULL)
            GROUP BY s.id, s.nombre, s.numero
            ORDER BY promedio DESC NULLS LAST
        `, [grupo, tipo]);
        
        res.json(result.rows);
    } catch (err) {
        console.error(`Error drill-down ${tipo} grupo ${grupo}:`, err);
        res.status(500).json({ error: err.message });
    }
});

// Drill-down por Sucursal
app.get('/api/:tipo/sucursal/:numero', async (req, res) => {
    const { tipo, numero } = req.params;
    
    if (!['operativas', 'seguridad'].includes(tipo)) {
        return res.status(400).json({ error: 'Tipo debe ser operativas o seguridad' });
    }
    
    try {
        const result = await pool.query(`
            SELECT 
                s.id,
                s.submission_id,
                s.fecha_supervision,
                s.calificacion_general,
                s.periodo_cas,
                s.usuario,
                suc.nombre as sucursal_nombre,
                suc.grupo_operativo
            FROM supervisiones s
            JOIN sucursales suc ON s.sucursal_id = suc.id
            WHERE suc.numero = $1 
            AND s.tipo_supervision = $2
            ORDER BY s.fecha_supervision DESC
            LIMIT 50
        `, [numero, tipo]);
        
        res.json(result.rows);
    } catch (err) {
        console.error(`Error drill-down ${tipo} sucursal ${numero}:`, err);
        res.status(500).json({ error: err.message });
    }
});

// ============================================================================
// 🔄 UTILITARIOS
// ============================================================================

// Health check
app.get('/health', async (req, res) => {
    try {
        const result = await pool.query('SELECT NOW() as timestamp');
        res.json({
            status: 'healthy',
            database: 'connected',
            timestamp: result.rows[0].timestamp
        });
    } catch (err) {
        res.status(500).json({
            status: 'unhealthy',
            database: 'disconnected',
            error: err.message
        });
    }
});

// Refresh vistas materializadas (manual)
app.post('/api/refresh', async (req, res) => {
    try {
        await pool.query('SELECT refresh_dashboard_views()');
        res.json({ 
            success: true, 
            message: 'Vistas materializadas actualizadas',
            timestamp: new Date()
        });
    } catch (err) {
        console.error('Error refresh manual:', err);
        res.status(500).json({ error: err.message });
    }
});

// Estadísticas generales
app.get('/api/stats', async (req, res) => {
    try {
        const queries = await Promise.all([
            pool.query('SELECT COUNT(*) as total FROM sucursales'),
            pool.query('SELECT COUNT(*) as total FROM supervisiones WHERE tipo_supervision = $1', ['operativas']),
            pool.query('SELECT COUNT(*) as total FROM supervisiones WHERE tipo_supervision = $1', ['seguridad']),
            pool.query('SELECT COUNT(*) as total FROM areas_calificaciones'),
            pool.query('SELECT COUNT(DISTINCT grupo_operativo) as total FROM sucursales')
        ]);

        res.json({
            sucursales: queries[0].rows[0].total,
            operativas: queries[1].rows[0].total,
            seguridad: queries[2].rows[0].total,
            areas_evaluadas: queries[3].rows[0].total,
            grupos_operativos: queries[4].rows[0].total,
            timestamp: new Date()
        });
    } catch (err) {
        console.error('Error stats:', err);
        res.status(500).json({ error: err.message });
    }
});

// ============================================================================
// 🌐 ROUTES & ERROR HANDLING
// ============================================================================

// Serve main app
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

// API root
app.get('/api', (req, res) => {
    res.json({
        message: '🚀 Railway API - El Pollo Loco Dashboard',
        version: '1.0.0',
        endpoints: {
            operativas: '/api/operativas/*',
            seguridad: '/api/seguridad/*',
            health: '/health',
            stats: '/api/stats',
            refresh: '/api/refresh'
        }
    });
});

// 404 handler
app.use('*', (req, res) => {
    res.status(404).json({
        error: 'Endpoint no encontrado',
        path: req.originalUrl,
        timestamp: new Date()
    });
});

// Global error handler
app.use((err, req, res, next) => {
    console.error('❌ Error global:', err);
    res.status(500).json({
        error: 'Error interno del servidor',
        timestamp: new Date()
    });
});

// ============================================================================
// 🚀 SERVER START
// ============================================================================

app.listen(port, () => {
    console.log('🚀 RAILWAY SERVER INICIADO');
    console.log('=' * 50);
    console.log(`🌐 URL: http://localhost:${port}`);
    console.log(`🗄️ PostgreSQL: ${process.env.DATABASE_URL ? 'Configurado' : 'No configurado'}`);
    console.log(`⏰ Tiempo: ${new Date().toISOString()}`);
    console.log('🎯 Roberto: Dashboard Railway listo!');
    console.log('=' * 50);
});

// Graceful shutdown
process.on('SIGTERM', () => {
    console.log('🛑 Cerrando servidor...');
    pool.end(() => {
        console.log('🗄️ Pool PostgreSQL cerrado');
        process.exit(0);
    });
});

process.on('SIGINT', () => {
    console.log('🛑 Cerrando servidor...');
    pool.end(() => {
        console.log('🗄️ Pool PostgreSQL cerrado');
        process.exit(0);
    });
});