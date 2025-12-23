// 🚀 RAILWAY BACKEND API FIXED - EL POLLO LOCO DASHBOARD
// API simplificado que funciona directamente con tablas básicas
// Roberto: Backend corregido sin vistas materializadas

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
    contentSecurityPolicy: false,
}));
app.use(compression());
app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// Serve static files
app.use(express.static(path.join(__dirname)));

// ============================================================================
// 📱 HOME PAGE ROUTE - SERVE iOS CLONE
// ============================================================================

// Serve the iOS clone as default
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index-clone-original.html'));
});

// Request logging middleware
app.use((req, res, next) => {
    console.log(`📡 ${req.method} ${req.path} - ${new Date().toISOString()}`);
    next();
});

// ============================================================================
// 🔧 OPERATIVAS ENDPOINTS - CON DRILL-DOWN COMPLETO
// ============================================================================

// KPIs Operativas
app.get('/api/operativas/kpis', async (req, res) => {
    try {
        console.log('📊 Calculando KPIs operativas...');
        
        const queries = await Promise.all([
            pool.query('SELECT COUNT(DISTINCT s.id) as total FROM sucursales s'),
            pool.query('SELECT COUNT(DISTINCT s.grupo_operativo) as total FROM sucursales s'),
            pool.query('SELECT COUNT(*) as total FROM supervisiones WHERE tipo_supervision = $1', ['operativas']),
            pool.query('SELECT ROUND(AVG(calificacion_general), 1) as promedio FROM supervisiones WHERE tipo_supervision = $1', ['operativas']),
            pool.query('SELECT COUNT(*) as total FROM supervisiones WHERE tipo_supervision = $1 AND calificacion_general >= 90', ['operativas']),
            pool.query('SELECT COUNT(*) as total FROM supervisiones WHERE tipo_supervision = $1 AND calificacion_general >= 80', ['operativas']),
            pool.query('SELECT MAX(fecha_supervision) as ultima FROM supervisiones WHERE tipo_supervision = $1', ['operativas'])
        ]);

        const kpis = {
            total_sucursales: parseInt(queries[0].rows[0]?.total) || 0,
            total_grupos: parseInt(queries[1].rows[0]?.total) || 0,
            total_supervisiones: parseInt(queries[2].rows[0]?.total) || 0,
            promedio_general: parseFloat(queries[3].rows[0]?.promedio) || 0,
            supervisiones_excelentes: parseInt(queries[4].rows[0]?.total) || 0,
            supervisiones_buenas: parseInt(queries[5].rows[0]?.total) || 0,
            ultima_actualizacion: queries[6].rows[0]?.ultima || null
        };

        console.log('✅ KPIs operativas calculados:', kpis);
        res.json(kpis);
        
    } catch (err) {
        console.error('❌ Error KPIs operativas:', err);
        res.status(500).json({ error: err.message });
    }
});

// Dashboard Principal Operativas
app.get('/api/operativas/dashboard', async (req, res) => {
    try {
        console.log('📊 Cargando dashboard operativas...');
        
        const result = await pool.query(`
            SELECT 
                s.grupo_operativo,
                s.tipo_sucursal,
                s.nombre as sucursal_nombre,
                s.numero as sucursal_numero,
                s.latitud,
                s.longitud,
                s.estado,
                s.ciudad,
                COUNT(sup.id) as total_supervisiones,
                ROUND(AVG(sup.calificacion_general), 1) as promedio_calificacion,
                MIN(sup.calificacion_general) as min_calificacion,
                MAX(sup.calificacion_general) as max_calificacion,
                MAX(sup.fecha_supervision) as ultima_supervision
            FROM sucursales s
            LEFT JOIN supervisiones sup ON s.id = sup.sucursal_id 
                AND sup.tipo_supervision = 'operativas'
            GROUP BY s.id, s.grupo_operativo, s.tipo_sucursal, s.nombre, s.numero, s.latitud, s.longitud, s.estado, s.ciudad
            ORDER BY promedio_calificacion DESC NULLS LAST
        `);
        
        // Convertir campos numéricos a números
        const processedRows = result.rows.map(row => ({
            ...row,
            promedio_calificacion: row.promedio_calificacion ? parseFloat(row.promedio_calificacion) : null,
            min_calificacion: row.min_calificacion ? parseFloat(row.min_calificacion) : null,
            max_calificacion: row.max_calificacion ? parseFloat(row.max_calificacion) : null,
            total_supervisiones: parseInt(row.total_supervisiones) || 0,
            latitud: row.latitud ? parseFloat(row.latitud) : null,
            longitud: row.longitud ? parseFloat(row.longitud) : null
        }));
        
        console.log(`✅ Dashboard operativas: ${processedRows.length} registros`);
        res.json(processedRows);
        
    } catch (err) {
        console.error('❌ Error dashboard operativas:', err);
        res.status(500).json({ error: err.message });
    }
});

// Detalle Grupo Operativo
app.get('/api/operativas/grupo/:grupo', async (req, res) => {
    try {
        const { grupo } = req.params;
        console.log(`📊 Cargando grupo operativo: ${grupo}`);
        
        const result = await pool.query(`
            SELECT 
                s.id as sucursal_id,
                s.nombre as sucursal_nombre,
                s.numero as sucursal_numero,
                s.tipo_sucursal,
                s.ciudad,
                s.estado,
                s.latitud,
                s.longitud,
                COUNT(sup.id) as total_supervisiones,
                ROUND(AVG(sup.calificacion_general), 1) as promedio_calificacion,
                MIN(sup.calificacion_general) as min_calificacion,
                MAX(sup.calificacion_general) as max_calificacion,
                MAX(sup.fecha_supervision) as ultima_supervision,
                MIN(sup.fecha_supervision) as primera_supervision
            FROM sucursales s
            LEFT JOIN supervisiones sup ON s.id = sup.sucursal_id 
                AND sup.tipo_supervision = 'operativas'
            WHERE s.grupo_operativo = $1
            GROUP BY s.id, s.nombre, s.numero, s.tipo_sucursal, s.ciudad, s.estado, s.latitud, s.longitud
            ORDER BY promedio_calificacion DESC NULLS LAST
        `, [grupo]);
        
        // Convertir campos numéricos a números
        const processedRows = result.rows.map(row => ({
            ...row,
            promedio_calificacion: row.promedio_calificacion ? parseFloat(row.promedio_calificacion) : null,
            min_calificacion: row.min_calificacion ? parseFloat(row.min_calificacion) : null,
            max_calificacion: row.max_calificacion ? parseFloat(row.max_calificacion) : null,
            total_supervisiones: parseInt(row.total_supervisiones) || 0,
            latitud: row.latitud ? parseFloat(row.latitud) : null,
            longitud: row.longitud ? parseFloat(row.longitud) : null
        }));
        
        console.log(`✅ Grupo ${grupo}: ${processedRows.length} sucursales`);
        res.json(processedRows);
        
    } catch (err) {
        console.error('❌ Error detalle grupo operativo:', err);
        res.status(500).json({ error: err.message });
    }
});

// Detalle Sucursal Completo
app.get('/api/operativas/sucursal/:id', async (req, res) => {
    try {
        const { id } = req.params;
        console.log(`📊 Cargando sucursal: ${id}`);
        
        // Información básica sucursal
        const sucursalQuery = await pool.query(`
            SELECT 
                s.*,
                COUNT(sup.id) as total_supervisiones,
                ROUND(AVG(sup.calificacion_general), 1) as promedio_calificacion,
                MIN(sup.calificacion_general) as min_calificacion,
                MAX(sup.calificacion_general) as max_calificacion,
                MAX(sup.fecha_supervision) as ultima_supervision,
                MIN(sup.fecha_supervision) as primera_supervision
            FROM sucursales s
            LEFT JOIN supervisiones sup ON s.id = sup.sucursal_id 
                AND sup.tipo_supervision = 'operativas'
            WHERE s.id = $1
            GROUP BY s.id
        `, [id]);
        
        if (sucursalQuery.rows.length === 0) {
            return res.status(404).json({ error: 'Sucursal no encontrada' });
        }
        
        // Supervisiones detalladas
        const supervisionesQuery = await pool.query(`
            SELECT 
                sup.id,
                sup.fecha_supervision,
                sup.calificacion_general,
                sup.usuario,
                sup.areas_evaluadas
            FROM supervisiones sup
            WHERE sup.sucursal_id = $1 AND sup.tipo_supervision = 'operativas'
            ORDER BY sup.fecha_supervision DESC
        `, [id]);
        
        // Áreas evaluadas
        const areasQuery = await pool.query(`
            SELECT 
                ac.area_nombre,
                ROUND(AVG(ac.calificacion), 1) as promedio_area,
                COUNT(ac.id) as total_evaluaciones,
                MIN(ac.calificacion) as min_calificacion,
                MAX(ac.calificacion) as max_calificacion
            FROM areas_calificaciones ac
            JOIN supervisiones sup ON ac.supervision_id = sup.id
            WHERE sup.sucursal_id = $1 AND sup.tipo_supervision = 'operativas'
            GROUP BY ac.area_nombre
            ORDER BY promedio_area DESC
        `, [id]);
        
        const response = {
            sucursal: sucursalQuery.rows[0],
            supervisiones: supervisionesQuery.rows,
            areas: areasQuery.rows
        };
        
        console.log(`✅ Sucursal ${id}: ${supervisionesQuery.rows.length} supervisiones, ${areasQuery.rows.length} áreas`);
        res.json(response);
        
    } catch (err) {
        console.error('❌ Error detalle sucursal:', err);
        res.status(500).json({ error: err.message });
    }
});

// Histórico Sucursal
app.get('/api/operativas/sucursal/:id/historico', async (req, res) => {
    try {
        const { id } = req.params;
        console.log(`📈 Cargando histórico sucursal: ${id}`);
        
        const result = await pool.query(`
            SELECT 
                DATE_TRUNC('week', sup.fecha_supervision) as semana,
                ROUND(AVG(sup.calificacion_general), 1) as promedio_semanal,
                COUNT(sup.id) as total_supervisiones,
                MIN(sup.calificacion_general) as min_semanal,
                MAX(sup.calificacion_general) as max_semanal
            FROM supervisiones sup
            WHERE sup.sucursal_id = $1 AND sup.tipo_supervision = 'operativas'
            GROUP BY DATE_TRUNC('week', sup.fecha_supervision)
            ORDER BY semana DESC
            LIMIT 24
        `, [id]);
        
        console.log(`✅ Histórico sucursal ${id}: ${result.rows.length} semanas`);
        res.json(result.rows);
        
    } catch (err) {
        console.error('❌ Error histórico sucursal:', err);
        res.status(500).json({ error: err.message });
    }
});

// ============================================================================
// 🛡️ SEGURIDAD ENDPOINTS - CON DRILL-DOWN COMPLETO
// ============================================================================

// KPIs Seguridad
app.get('/api/seguridad/kpis', async (req, res) => {
    try {
        console.log('📊 Calculando KPIs seguridad...');
        
        const queries = await Promise.all([
            pool.query('SELECT COUNT(DISTINCT s.id) as total FROM sucursales s'),
            pool.query('SELECT COUNT(DISTINCT s.grupo_operativo) as total FROM sucursales s'),
            pool.query('SELECT COUNT(*) as total FROM supervisiones WHERE tipo_supervision = $1', ['seguridad']),
            pool.query('SELECT ROUND(AVG(calificacion_general), 1) as promedio FROM supervisiones WHERE tipo_supervision = $1', ['seguridad']),
            pool.query('SELECT COUNT(*) as total FROM supervisiones WHERE tipo_supervision = $1 AND calificacion_general >= 90', ['seguridad']),
            pool.query('SELECT COUNT(*) as total FROM supervisiones WHERE tipo_supervision = $1 AND calificacion_general >= 80', ['seguridad']),
            pool.query('SELECT MAX(fecha_supervision) as ultima FROM supervisiones WHERE tipo_supervision = $1', ['seguridad'])
        ]);

        const kpis = {
            total_sucursales: parseInt(queries[0].rows[0]?.total) || 0,
            total_grupos: parseInt(queries[1].rows[0]?.total) || 0,
            total_supervisiones: parseInt(queries[2].rows[0]?.total) || 0,
            promedio_general: parseFloat(queries[3].rows[0]?.promedio) || 0,
            supervisiones_excelentes: parseInt(queries[4].rows[0]?.total) || 0,
            supervisiones_buenas: parseInt(queries[5].rows[0]?.total) || 0,
            ultima_actualizacion: queries[6].rows[0]?.ultima || null
        };

        console.log('✅ KPIs seguridad calculados:', kpis);
        res.json(kpis);
        
    } catch (err) {
        console.error('❌ Error KPIs seguridad:', err);
        res.status(500).json({ error: err.message });
    }
});

// Dashboard Principal Seguridad
app.get('/api/seguridad/dashboard', async (req, res) => {
    try {
        console.log('📊 Cargando dashboard seguridad...');
        
        const result = await pool.query(`
            SELECT 
                s.grupo_operativo,
                s.tipo_sucursal,
                s.nombre as sucursal_nombre,
                s.numero as sucursal_numero,
                s.latitud,
                s.longitud,
                s.estado,
                s.ciudad,
                COUNT(sup.id) as total_supervisiones,
                ROUND(AVG(sup.calificacion_general), 1) as promedio_calificacion,
                MIN(sup.calificacion_general) as min_calificacion,
                MAX(sup.calificacion_general) as max_calificacion,
                MAX(sup.fecha_supervision) as ultima_supervision
            FROM sucursales s
            LEFT JOIN supervisiones sup ON s.id = sup.sucursal_id 
                AND sup.tipo_supervision = 'seguridad'
            GROUP BY s.id, s.grupo_operativo, s.tipo_sucursal, s.nombre, s.numero, s.latitud, s.longitud, s.estado, s.ciudad
            ORDER BY promedio_calificacion DESC NULLS LAST
        `);
        
        // Convertir campos numéricos a números
        const processedRows = result.rows.map(row => ({
            ...row,
            promedio_calificacion: row.promedio_calificacion ? parseFloat(row.promedio_calificacion) : null,
            min_calificacion: row.min_calificacion ? parseFloat(row.min_calificacion) : null,
            max_calificacion: row.max_calificacion ? parseFloat(row.max_calificacion) : null,
            total_supervisiones: parseInt(row.total_supervisiones) || 0,
            latitud: row.latitud ? parseFloat(row.latitud) : null,
            longitud: row.longitud ? parseFloat(row.longitud) : null
        }));
        
        console.log(`✅ Dashboard seguridad: ${processedRows.length} registros`);
        res.json(processedRows);
        
    } catch (err) {
        console.error('❌ Error dashboard seguridad:', err);
        res.status(500).json({ error: err.message });
    }
});

// Detalle Grupo Operativo Seguridad
app.get('/api/seguridad/grupo/:grupo', async (req, res) => {
    try {
        const { grupo } = req.params;
        console.log(`📊 Cargando grupo seguridad: ${grupo}`);
        
        const result = await pool.query(`
            SELECT 
                s.id as sucursal_id,
                s.nombre as sucursal_nombre,
                s.numero as sucursal_numero,
                s.tipo_sucursal,
                s.ciudad,
                s.estado,
                s.latitud,
                s.longitud,
                COUNT(sup.id) as total_supervisiones,
                ROUND(AVG(sup.calificacion_general), 1) as promedio_calificacion,
                MIN(sup.calificacion_general) as min_calificacion,
                MAX(sup.calificacion_general) as max_calificacion,
                MAX(sup.fecha_supervision) as ultima_supervision,
                MIN(sup.fecha_supervision) as primera_supervision
            FROM sucursales s
            LEFT JOIN supervisiones sup ON s.id = sup.sucursal_id 
                AND sup.tipo_supervision = 'seguridad'
            WHERE s.grupo_operativo = $1
            GROUP BY s.id, s.nombre, s.numero, s.tipo_sucursal, s.ciudad, s.estado, s.latitud, s.longitud
            ORDER BY promedio_calificacion DESC NULLS LAST
        `, [grupo]);
        
        // Convertir campos numéricos a números
        const processedRows = result.rows.map(row => ({
            ...row,
            promedio_calificacion: row.promedio_calificacion ? parseFloat(row.promedio_calificacion) : null,
            min_calificacion: row.min_calificacion ? parseFloat(row.min_calificacion) : null,
            max_calificacion: row.max_calificacion ? parseFloat(row.max_calificacion) : null,
            total_supervisiones: parseInt(row.total_supervisiones) || 0,
            latitud: row.latitud ? parseFloat(row.latitud) : null,
            longitud: row.longitud ? parseFloat(row.longitud) : null
        }));
        
        console.log(`✅ Grupo seguridad ${grupo}: ${processedRows.length} sucursales`);
        res.json(processedRows);
        
    } catch (err) {
        console.error('❌ Error detalle grupo seguridad:', err);
        res.status(500).json({ error: err.message });
    }
});

// Detalle Sucursal Seguridad
app.get('/api/seguridad/sucursal/:id', async (req, res) => {
    try {
        const { id } = req.params;
        console.log(`📊 Cargando sucursal seguridad: ${id}`);
        
        // Información básica sucursal
        const sucursalQuery = await pool.query(`
            SELECT 
                s.*,
                COUNT(sup.id) as total_supervisiones,
                ROUND(AVG(sup.calificacion_general), 1) as promedio_calificacion,
                MIN(sup.calificacion_general) as min_calificacion,
                MAX(sup.calificacion_general) as max_calificacion,
                MAX(sup.fecha_supervision) as ultima_supervision,
                MIN(sup.fecha_supervision) as primera_supervision
            FROM sucursales s
            LEFT JOIN supervisiones sup ON s.id = sup.sucursal_id 
                AND sup.tipo_supervision = 'seguridad'
            WHERE s.id = $1
            GROUP BY s.id
        `, [id]);
        
        if (sucursalQuery.rows.length === 0) {
            return res.status(404).json({ error: 'Sucursal no encontrada' });
        }
        
        // Supervisiones detalladas
        const supervisionesQuery = await pool.query(`
            SELECT 
                sup.id,
                sup.fecha_supervision,
                sup.calificacion_general,
                sup.usuario,
                sup.areas_evaluadas
            FROM supervisiones sup
            WHERE sup.sucursal_id = $1 AND sup.tipo_supervision = 'seguridad'
            ORDER BY sup.fecha_supervision DESC
        `, [id]);
        
        // Áreas evaluadas
        const areasQuery = await pool.query(`
            SELECT 
                ac.area_nombre,
                ROUND(AVG(ac.calificacion), 1) as promedio_area,
                COUNT(ac.id) as total_evaluaciones,
                MIN(ac.calificacion) as min_calificacion,
                MAX(ac.calificacion) as max_calificacion
            FROM areas_calificaciones ac
            JOIN supervisiones sup ON ac.supervision_id = sup.id
            WHERE sup.sucursal_id = $1 AND sup.tipo_supervision = 'seguridad'
            GROUP BY ac.area_nombre
            ORDER BY promedio_area DESC
        `, [id]);
        
        const response = {
            sucursal: sucursalQuery.rows[0],
            supervisiones: supervisionesQuery.rows,
            areas: areasQuery.rows
        };
        
        console.log(`✅ Sucursal seguridad ${id}: ${supervisionesQuery.rows.length} supervisiones, ${areasQuery.rows.length} áreas`);
        res.json(response);
        
    } catch (err) {
        console.error('❌ Error detalle sucursal seguridad:', err);
        res.status(500).json({ error: err.message });
    }
});

// Histórico Sucursal Seguridad
app.get('/api/seguridad/sucursal/:id/historico', async (req, res) => {
    try {
        const { id } = req.params;
        console.log(`📈 Cargando histórico seguridad: ${id}`);
        
        const result = await pool.query(`
            SELECT 
                DATE_TRUNC('week', sup.fecha_supervision) as semana,
                ROUND(AVG(sup.calificacion_general), 1) as promedio_semanal,
                COUNT(sup.id) as total_supervisiones,
                MIN(sup.calificacion_general) as min_semanal,
                MAX(sup.calificacion_general) as max_semanal
            FROM supervisiones sup
            WHERE sup.sucursal_id = $1 AND sup.tipo_supervision = 'seguridad'
            GROUP BY DATE_TRUNC('week', sup.fecha_supervision)
            ORDER BY semana DESC
            LIMIT 24
        `, [id]);
        
        console.log(`✅ Histórico seguridad ${id}: ${result.rows.length} semanas`);
        res.json(result.rows);
        
    } catch (err) {
        console.error('❌ Error histórico seguridad:', err);
        res.status(500).json({ error: err.message });
    }
});

// ============================================================================
// 🗺️ MAPA ENDPOINTS
// ============================================================================

// Mapa Operativas
app.get('/api/operativas/mapa', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT 
                s.nombre as sucursal_nombre,
                s.latitud,
                s.longitud,
                s.grupo_operativo,
                ROUND(AVG(sup.calificacion_general), 1) as promedio_calificacion,
                COUNT(sup.id) as total_supervisiones
            FROM sucursales s
            LEFT JOIN supervisiones sup ON s.id = sup.sucursal_id 
                AND sup.tipo_supervision = 'operativas'
            WHERE s.latitud IS NOT NULL AND s.longitud IS NOT NULL
            GROUP BY s.id, s.nombre, s.latitud, s.longitud, s.grupo_operativo
            ORDER BY promedio_calificacion DESC NULLS LAST
        `);
        
        // Convertir campos numéricos a números
        const processedRows = result.rows.map(row => ({
            ...row,
            promedio_calificacion: row.promedio_calificacion ? parseFloat(row.promedio_calificacion) : null,
            total_supervisiones: parseInt(row.total_supervisiones) || 0,
            latitud: row.latitud ? parseFloat(row.latitud) : null,
            longitud: row.longitud ? parseFloat(row.longitud) : null
        }));
        
        res.json(processedRows);
    } catch (err) {
        console.error('Error mapa operativas:', err);
        res.status(500).json({ error: err.message });
    }
});

// Mapa Seguridad
app.get('/api/seguridad/mapa', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT 
                s.nombre as sucursal_nombre,
                s.latitud,
                s.longitud,
                s.grupo_operativo,
                ROUND(AVG(sup.calificacion_general), 1) as promedio_calificacion,
                COUNT(sup.id) as total_supervisiones
            FROM sucursales s
            LEFT JOIN supervisiones sup ON s.id = sup.sucursal_id 
                AND sup.tipo_supervision = 'seguridad'
            WHERE s.latitud IS NOT NULL AND s.longitud IS NOT NULL
            GROUP BY s.id, s.nombre, s.latitud, s.longitud, s.grupo_operativo
            ORDER BY promedio_calificacion DESC NULLS LAST
        `);
        
        // Convertir campos numéricos a números
        const processedRows = result.rows.map(row => ({
            ...row,
            promedio_calificacion: row.promedio_calificacion ? parseFloat(row.promedio_calificacion) : null,
            total_supervisiones: parseInt(row.total_supervisiones) || 0,
            latitud: row.latitud ? parseFloat(row.latitud) : null,
            longitud: row.longitud ? parseFloat(row.longitud) : null
        }));
        
        res.json(processedRows);
    } catch (err) {
        console.error('Error mapa seguridad:', err);
        res.status(500).json({ error: err.message });
    }
});

// ============================================================================
// 📊 AREAS ENDPOINTS
// ============================================================================

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
        
        // Convertir campos numéricos a números
        const processedRows = result.rows.map(row => ({
            ...row,
            promedio_area: parseFloat(row.promedio_area) || 0,
            total_evaluaciones: parseInt(row.total_evaluaciones) || 0
        }));
        
        res.json(processedRows);
    } catch (err) {
        console.error('Error áreas operativas:', err);
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
        
        // Convertir campos numéricos a números
        const processedRows = result.rows.map(row => ({
            ...row,
            promedio_area: parseFloat(row.promedio_area) || 0,
            total_evaluaciones: parseInt(row.total_evaluaciones) || 0
        }));
        
        res.json(processedRows);
    } catch (err) {
        console.error('Error áreas seguridad:', err);
        res.status(500).json({ error: err.message });
    }
});

// ============================================================================
// 📈 HISTÓRICO ENDPOINTS
// ============================================================================

// Histórico Operativas
app.get('/api/operativas/historico', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT 
                DATE_TRUNC('week', s.fecha_supervision) as semana,
                ROUND(AVG(s.calificacion_general), 1) as promedio_semanal,
                COUNT(s.id) as total_supervisiones
            FROM supervisiones s
            WHERE s.tipo_supervision = 'operativas'
            GROUP BY DATE_TRUNC('week', s.fecha_supervision)
            ORDER BY semana DESC
            LIMIT 12
        `);
        
        // Convertir campos numéricos a números
        const processedRows = result.rows.map(row => ({
            ...row,
            promedio_semanal: parseFloat(row.promedio_semanal) || 0,
            total_supervisiones: parseInt(row.total_supervisiones) || 0
        }));
        
        res.json(processedRows);
    } catch (err) {
        console.error('Error histórico operativas:', err);
        res.status(500).json({ error: err.message });
    }
});

// Histórico Seguridad
app.get('/api/seguridad/historico', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT 
                DATE_TRUNC('week', s.fecha_supervision) as semana,
                ROUND(AVG(s.calificacion_general), 1) as promedio_semanal,
                COUNT(s.id) as total_supervisiones
            FROM supervisiones s
            WHERE s.tipo_supervision = 'seguridad'
            GROUP BY DATE_TRUNC('week', s.fecha_supervision)
            ORDER BY semana DESC
            LIMIT 12
        `);
        
        // Convertir campos numéricos a números
        const processedRows = result.rows.map(row => ({
            ...row,
            promedio_semanal: parseFloat(row.promedio_semanal) || 0,
            total_supervisiones: parseInt(row.total_supervisiones) || 0
        }));
        
        res.json(processedRows);
    } catch (err) {
        console.error('Error histórico seguridad:', err);
        res.status(500).json({ error: err.message });
    }
});

// ============================================================================
// 🔄 UTILITARIOS
// ============================================================================

// Health check
app.get('/health', async (req, res) => {
    try {
        const result = await pool.query('SELECT NOW() as timestamp, COUNT(*) as sucursales FROM sucursales');
        res.json({
            status: 'healthy',
            database: 'connected',
            timestamp: result.rows[0].timestamp,
            sucursales: result.rows[0].sucursales
        });
    } catch (err) {
        res.status(500).json({
            status: 'unhealthy',
            database: 'disconnected',
            error: err.message
        });
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
            sucursales: parseInt(queries[0].rows[0].total) || 0,
            operativas: parseInt(queries[1].rows[0].total) || 0,
            seguridad: parseInt(queries[2].rows[0].total) || 0,
            areas_evaluadas: parseInt(queries[3].rows[0].total) || 0,
            grupos_operativos: parseInt(queries[4].rows[0].total) || 0,
            timestamp: new Date()
        });
    } catch (err) {
        console.error('Error stats:', err);
        res.status(500).json({ error: err.message });
    }
});

// ============================================================================
// 🔧 ENDPOINTS ORIGINALES COMPATIBLES CON DASHBOARD ROBERTO
// ============================================================================

// Endpoint /grupos compatible con dashboard original
app.get('/api/grupos', async (req, res) => {
    try {
        const currentType = req.query.type || 'operativas';
        console.log(`📊 Loading grupos for type: ${currentType}`);
        
        const result = await pool.query(`
            SELECT 
                s.grupo_operativo,
                COUNT(sup.id) as supervisiones,
                COUNT(DISTINCT s.id) as sucursales,
                ROUND(AVG(sup.calificacion_general), 2) as performance,
                MIN(sup.calificacion_general) as min_calificacion,
                MAX(sup.calificacion_general) as max_calificacion
            FROM sucursales s
            LEFT JOIN supervisiones sup ON s.id = sup.sucursal_id 
                AND sup.tipo_supervision = $1
            WHERE s.grupo_operativo IS NOT NULL
            GROUP BY s.grupo_operativo
            HAVING COUNT(sup.id) > 0
            ORDER BY performance DESC
        `, [currentType]);
        
        // Process and add ranking
        const processedGroups = result.rows.map((group, index) => ({
            name: group.grupo_operativo,
            performance: parseFloat(group.performance) || 0,
            supervisiones: parseInt(group.supervisiones) || 0,
            sucursales: parseInt(group.sucursales) || 0,
            rank: index + 1,
            trend: Math.random() > 0.5 ? Math.random() * 5 : -Math.random() * 5 // Simulated trend
        }));
        
        console.log(`✅ Grupos loaded: ${processedGroups.length} groups`);
        res.json(processedGroups);
        
    } catch (err) {
        console.error('❌ Error loading grupos:', err);
        res.status(500).json({ error: err.message });
    }
});

// Endpoint /sucursales-ranking compatible con dashboard original
app.get('/api/sucursales-ranking', async (req, res) => {
    try {
        const { grupo, estado, type = 'operativas' } = req.query;
        console.log(`📊 Loading sucursales ranking - Grupo: ${grupo}, Estado: ${estado}, Type: ${type}`);
        
        let whereConditions = ['sup.tipo_supervision = $1'];
        let params = [type];
        let paramIndex = 2;
        
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
        
        const whereClause = whereConditions.join(' AND ');
        
        const result = await pool.query(`
            SELECT 
                s.nombre as sucursal,
                s.grupo_operativo,
                s.estado,
                s.ciudad,
                s.latitud,
                s.longitud,
                COUNT(sup.id) as evaluaciones,
                ROUND(AVG(sup.calificacion_general), 2) as promedio
            FROM sucursales s
            LEFT JOIN supervisiones sup ON s.id = sup.sucursal_id
            WHERE ${whereClause}
            GROUP BY s.id, s.nombre, s.grupo_operativo, s.estado, s.ciudad, s.latitud, s.longitud
            HAVING COUNT(sup.id) > 0
            ORDER BY promedio DESC
            LIMIT 1000
        `, params);
        
        // Process data to match original format
        const processedSucursales = result.rows.map(suc => ({
            sucursal: suc.sucursal,
            location_name: suc.sucursal,
            grupo_operativo: suc.grupo_operativo,
            estado: suc.estado,
            ciudad: suc.ciudad,
            latitud: parseFloat(suc.latitud),
            longitud: parseFloat(suc.longitud),
            evaluaciones: parseInt(suc.evaluaciones) || 0,
            supervisiones: parseInt(suc.evaluaciones) || 0,
            promedio: parseFloat(suc.promedio) || 0
        }));
        
        console.log(`✅ Sucursales ranking: ${processedSucursales.length} sucursales`);
        res.json(processedSucursales);
        
    } catch (err) {
        console.error('❌ Error sucursales ranking:', err);
        res.status(500).json({ error: err.message });
    }
});

// Endpoint /analisis-critico compatible con dashboard original  
app.get('/api/analisis-critico', async (req, res) => {
    try {
        const { tipo, id, estado, grupo, type = 'operativas' } = req.query;
        console.log(`📊 Análisis crítico - Tipo: ${tipo}, ID: ${id}, Type: ${type}`);
        
        if (tipo === 'sucursal' && id) {
            // Análisis de sucursal específica
            const result = await pool.query(`
                SELECT 
                    s.*,
                    COUNT(sup.id) as total_supervisiones,
                    ROUND(AVG(sup.calificacion_general), 2) as promedio_calificacion,
                    MIN(sup.calificacion_general) as min_calificacion,
                    MAX(sup.calificacion_general) as max_calificacion,
                    MAX(sup.fecha_supervision) as ultima_supervision
                FROM sucursales s
                LEFT JOIN supervisiones sup ON s.id = sup.sucursal_id 
                    AND sup.tipo_supervision = $2
                WHERE s.nombre = $1
                GROUP BY s.id
            `, [id, type]);
            
            if (result.rows.length === 0) {
                return res.status(404).json({ error: 'Sucursal no encontrada' });
            }
            
            const sucursal = result.rows[0];
            
            // Obtener áreas de evaluación para esta sucursal
            const areasResult = await pool.query(`
                SELECT 
                    ac.area_nombre,
                    ROUND(AVG(ac.calificacion), 2) as promedio_area,
                    COUNT(ac.id) as evaluaciones
                FROM areas_calificaciones ac
                JOIN supervisiones sup ON ac.supervision_id = sup.id
                WHERE sup.sucursal_id = $1 AND sup.tipo_supervision = $2
                GROUP BY ac.area_nombre
                ORDER BY promedio_area DESC
            `, [sucursal.id, type]);
            
            const response = {
                sucursal: {
                    ...sucursal,
                    total_supervisiones: parseInt(sucursal.total_supervisiones) || 0,
                    promedio_calificacion: parseFloat(sucursal.promedio_calificacion) || 0,
                    min_calificacion: parseFloat(sucursal.min_calificacion) || 0,
                    max_calificacion: parseFloat(sucursal.max_calificacion) || 0,
                    latitud: parseFloat(sucursal.latitud),
                    longitud: parseFloat(sucursal.longitud)
                },
                areas: areasResult.rows.map(area => ({
                    ...area,
                    promedio_area: parseFloat(area.promedio_area) || 0,
                    evaluaciones: parseInt(area.evaluaciones) || 0
                })),
                tipo: 'sucursal',
                timestamp: new Date()
            };
            
            console.log(`✅ Análisis sucursal ${id}: ${areasResult.rows.length} áreas`);
            res.json(response);
        } else {
            res.status(400).json({ error: 'Tipo de análisis no válido o parámetros faltantes' });
        }
        
    } catch (err) {
        console.error('❌ Error análisis crítico:', err);
        res.status(500).json({ error: err.message });
    }
});

// Endpoint /kpis compatible con dashboard original
app.get('/api/kpis', async (req, res) => {
    try {
        const type = req.query.type || 'operativas';
        console.log(`📊 Loading KPIs for type: ${type}`);
        
        // Reutilizar el endpoint existente pero con formato original
        const kpisResponse = await fetch(`${req.protocol}://${req.get('host')}/api/${type}/kpis`);
        const kpis = await kpisResponse.json();
        
        // Convertir al formato original esperado
        const originalFormatKpis = {
            promedio_general: parseFloat(kpis.promedio_general) || 0,
            total_supervisiones: parseInt(kpis.total_supervisiones) || 0,
            total_sucursales: parseInt(kpis.total_sucursales) || 0,
            total_grupos: parseInt(kpis.total_grupos) || 0,
            supervisiones_excelentes: parseInt(kpis.supervisiones_excelentes) || 0,
            supervisiones_buenas: parseInt(kpis.supervisiones_buenas) || 0,
            ultima_actualizacion: kpis.ultima_actualizacion
        };
        
        console.log('✅ KPIs loaded:', originalFormatKpis);
        res.json(originalFormatKpis);
        
    } catch (err) {
        console.error('❌ Error loading KPIs:', err);
        res.status(500).json({ error: err.message });
    }
});

// ============================================================================
// 🌐 ROUTES & ERROR HANDLING
// ============================================================================

// Serve main app - Clone exacto del dashboard original Roberto
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index-clone-original.html'));
});

// Serve old dashboard for comparison
app.get('/old', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

// API root
app.get('/api', (req, res) => {
    res.json({
        message: '🚀 Railway API CLONE ORIGINAL - El Pollo Loco Dashboard',
        version: '1.3.0',
        endpoints: {
            original: '/api/grupos, /api/kpis, /api/sucursales-ranking, /api/analisis-critico',
            operativas: '/api/operativas/* (kpis, dashboard, grupo/:grupo, sucursal/:id, mapa, areas)',
            seguridad: '/api/seguridad/* (kpis, dashboard, grupo/:grupo, sucursal/:id, mapa, areas)',
            health: '/health',
            stats: '/api/stats',
            drilldown: 'grupos → sucursales → detalle sucursal'
        }
    });
});

// 404 handler
app.use('*', (req, res) => {
    res.status(404).json({
        error: 'Endpoint no encontrado',
        path: req.originalUrl
    });
});

// Global error handler
app.use((err, req, res, next) => {
    console.error('❌ Error global:', err);
    res.status(500).json({
        error: 'Error interno del servidor',
        message: err.message
    });
});

// ============================================================================
// 🚀 SERVER START
// ============================================================================

app.listen(port, () => {
    console.log('🚀 RAILWAY SERVER FIXED INICIADO');
    console.log('=' * 50);
    console.log(`🌐 URL: http://localhost:${port}`);
    console.log(`🗄️ PostgreSQL: ${process.env.DATABASE_URL ? 'Configurado' : 'No configurado'}`);
    console.log(`⏰ Tiempo: ${new Date().toISOString()}`);
    console.log('🔧 Roberto: Backend corregido - sin vistas materializadas');
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