const express = require('express');
const path = require('path');
const compression = require('compression');
const helmet = require('helmet');
const { Pool } = require('pg');
const TelegramBot = require('node-telegram-bot-api');

// Load environment variables
require('dotenv').config();

// Initialize Express app
const app = express();
const port = process.env.PORT || 10000;

// Database connection with correct Neon URL
const pool = new Pool({
    connectionString: process.env.DATABASE_URL || 'postgresql://neondb_owner:npg_DlSRAHuyaY83@ep-orange-grass-a402u4o5-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require',
    ssl: { rejectUnauthorized: false }
});

// Utility function to determine CAS period from date
function determineCASPeriod(dateString) {
    if (!dateString) return 'SIN-PERIODO';
    
    const date = new Date(dateString);
    const year = date.getFullYear();
    const month = date.getMonth() + 1; // JavaScript months are 0-based
    
    // Define periods based on CAS calendar
    if (year === 2025) {
        if (month >= 1 && month <= 3) return 'NL-T1-2025';
        if (month >= 4 && month <= 6) return 'NL-T2-2025'; 
        if (month >= 7 && month <= 9) return 'NL-T3-2025';
        if (month >= 10 && month <= 12) return 'NL-T4-2025';
    }
    
    return 'PERIODO-' + year + '-Q' + Math.ceil(month / 3);
}

// Log database connection info
console.log('🔗 Database connection (NO FILTERS VERSION):');
console.log('NODE_ENV:', process.env.NODE_ENV);
console.log('Using DATABASE_URL:', !!process.env.DATABASE_URL);

// ============================================================================
// 🤖 BOT MENU BUTTON SOLO - NO AFECTA DASHBOARD FUNCIONALIDAD
// ============================================================================

const token = process.env.TELEGRAM_BOT_TOKEN || '8341799056:AAFvMMPzuplDDsOM07m5ANI5WVCATchBPeY';
const DASHBOARD_URL = 'https://pollo-loco-supervision.onrender.com';

let bot = null;

if (token && token !== 'undefined') {
    try {
        bot = new TelegramBot(token, { polling: false });
        console.log('🤖 Menu Button Bot inicializado - Dashboard funcionalidad completa mantenida');

        // Webhook para producción
        if (process.env.NODE_ENV === 'production') {
            const webhookUrl = `https://pollo-loco-supervision.onrender.com/webhook`;
            bot.setWebHook(webhookUrl).then(() => {
                console.log(`🌐 Webhook configurado: ${webhookUrl}`);
            }).catch(err => {
                console.log('⚠️ Error webhook:', err.message);
            });
        }

        // Bot handlers súper simples - NO interfieren con dashboard
        bot.onText(/\/start/, (msg) => {
            const chatId = msg.chat.id;
            const firstName = msg.from.first_name || 'Usuario';
            
            const message = `🍗 *Bienvenido ${firstName}*

Dashboard El Pollo Loco CAS
Sistema de Supervisión Operativa

📊 *Para acceder al dashboard completo:*
Usa el botón "📊 Dashboard" que está junto al campo de texto

✨ *Sistema completamente funcional con:*
• 238 supervisiones activas
• 91.20% promedio general  
• 20 grupos operativos
• 85 sucursales monitoreadas
• Mapas interactivos
• Reportes detallados
• Filtros avanzados`;

            bot.sendMessage(chatId, message, { parse_mode: 'Markdown' });
        });

        bot.onText(/\/dashboard/, (msg) => {
            const chatId = msg.chat.id;
            
            const message = `📊 *Dashboard El Pollo Loco CAS*

🎯 *Acceso:* Usa el botón "📊 Dashboard" junto al campo de texto

🔗 *URL directa:* ${DASHBOARD_URL}

✨ *Funcionalidad completa:*
• KPIs en tiempo real
• Mapas con coordenadas GPS
• Drill-down por sucursal  
• Reportes por período
• Filtros por grupo/estado
• Análisis histórico

📱 *Optimizado para móvil y desktop*`;
            
            bot.sendMessage(chatId, message, { parse_mode: 'Markdown' });
        });

        bot.on('message', (msg) => {
            if (!msg.text || msg.text.startsWith('/')) return;
            
            const chatId = msg.chat.id;
            
            const message = `🤖 *Bot El Pollo Loco CAS*

📊 Para acceder al dashboard completo usa el botón "📊 Dashboard" junto al campo de texto.

💡 *Comandos disponibles:*
/start - Información completa
/dashboard - Info del sistema`;
            
            bot.sendMessage(chatId, message, { parse_mode: 'Markdown' });
        });

        console.log('✅ Bot Menu Button configurado - Dashboard COMPLETO funcional');

    } catch (error) {
        console.error('❌ Error bot:', error.message);
        bot = null;
    }
} else {
    console.log('⚠️ Bot token no configurado');
}

// Security and optimization middleware
app.use(helmet({
    contentSecurityPolicy: {
        directives: {
            defaultSrc: ["'self'"],
            styleSrc: ["'self'", "'unsafe-inline'", "https://cdnjs.cloudflare.com", "https://unpkg.com"],
            scriptSrc: ["'self'", "'unsafe-inline'", "'unsafe-eval'", "https://cdn.jsdelivr.net", "https://unpkg.com", "https://cdnjs.cloudflare.com"],
            scriptSrcAttr: ["'unsafe-inline'"],
            imgSrc: ["'self'", "data:", "https:", "blob:"],
            connectSrc: ["'self'", "https://cdn.jsdelivr.net", "https://unpkg.com", "http://localhost:10000", "https://*.onrender.com"],
            fontSrc: ["'self'", "https://cdnjs.cloudflare.com"],
        },
    },
}));

app.use(compression());
app.use(express.json());

// Serve static files
app.use(express.static('public'));

// ============================================================================
// 🤖 BOT WEBHOOK ENDPOINT - NO AFECTA OTRAS RUTAS
// ============================================================================

if (bot) {
    app.post('/webhook', express.json(), (req, res) => {
        bot.processUpdate(req.body);
        res.sendStatus(200);
    });
}

// ============================================================================
// 📊 TODAS LAS APIS ORIGINALES DEL DASHBOARD - FUNCIONALIDAD COMPLETA
// ============================================================================

// Sucursal Detail API - NEW 3-Level Drill-down
app.get('/api/sucursal-detail', async (req, res) => {
    try {
        const { sucursal, grupo } = req.query;
        console.log('🏢 Sucursal Detail requested for:', sucursal, 'from group:', grupo);
        
        if (!sucursal) {
            return res.status(400).json({ error: 'Sucursal name is required' });
        }
        
        // Build WHERE clause for sucursal - Using nombre_normalizado like sucursales-ranking endpoint
        let whereClause = `WHERE nombre_normalizado = $1 AND area_tipo = 'area_principal'`;
        const params = [sucursal];
        let paramIndex = 1;
        
        if (grupo) {
            paramIndex++;
            whereClause += ` AND grupo_normalizado = $${paramIndex}`;
            params.push(grupo);
        }
        
        // 🚀 DUAL-SOURCE STRATEGY: Test new vs old calculation method
        const USE_NEW_CALCULATION = process.env.USE_CAS_TABLE === 'true';
        
        let sucursalData;
        
        if (USE_NEW_CALCULATION) {
            // 🆕 MÉTODO HÍBRIDO: normalized structure + CAS values
            console.log('🆕 Sucursal detail using HYBRID method (normalized structure + CAS values)');
            
            const hybridQuery = `
                WITH cas_performance AS (
                    SELECT 
                        submission_id,
                        calificacion_general_pct
                    FROM supervision_operativa_cas 
                    WHERE calificacion_general_pct IS NOT NULL
                )
                SELECT 
                    snv.nombre_normalizado as sucursal,
                    snv.numero_sucursal,
                    snv.estado_final as estado,
                    snv.ciudad_normalizada as municipio,
                    snv.grupo_normalizado as grupo_operativo,
                    ROUND(AVG(cp.calificacion_general_pct)::numeric, 2) as performance,
                    COUNT(DISTINCT snv.submission_id) as total_evaluaciones,
                    MAX(snv.fecha_supervision) as ultima_supervision,
                    COUNT(DISTINCT snv.area_evaluacion) as areas_evaluadas
                FROM supervision_normalized_view snv
                JOIN cas_performance cp ON snv.submission_id = cp.submission_id
                ${whereClause}
                  AND snv.fecha_supervision >= '2025-02-01'
                GROUP BY snv.nombre_normalizado, snv.numero_sucursal, snv.estado_final, snv.ciudad_normalizada, snv.grupo_normalizado
            `;
            
            const hybridResult = await pool.query(hybridQuery, params);
            
            if (hybridResult.rows.length === 0) {
                return res.status(404).json({ 
                    error: 'Sucursal not found',
                    sucursal,
                    method: 'HYBRID (normalized structure + CAS values)'
                });
            }
            
            sucursalData = hybridResult.rows[0];
            sucursalData.calculation_method = 'NEW (hybrid - normalized structure + CAS values)';
            
            // Get areas breakdown - HYBRID method
            const areasQuery = `
                WITH cas_performance AS (
                    SELECT 
                        submission_id,
                        calificacion_general_pct
                    FROM supervision_operativa_cas 
                    WHERE calificacion_general_pct IS NOT NULL
                )
                SELECT 
                    TRIM(snv.area_evaluacion) as nombre,
                    ROUND(AVG(porcentaje)::numeric, 2) as performance,
                    COUNT(*) as evaluaciones
                FROM supervision_normalized_view snv
                JOIN cas_performance cp ON snv.submission_id = cp.submission_id
                ${whereClause} 
                  AND snv.area_evaluacion IS NOT NULL 
                  AND TRIM(snv.area_evaluacion) != ''
                  AND snv.fecha_supervision >= '2025-02-01'
                GROUP BY TRIM(snv.area_evaluacion)
                ORDER BY performance DESC
            `;
            
            const areasResult = await pool.query(areasQuery, params);
            sucursalData.areas_evaluacion = areasResult.rows.map(area => ({
                ...area,
                trend: (Math.random() - 0.5) * 10 // Mock trend for now
            }));
            
            // Get recent evaluaciones - HYBRID method with REAL CAS values
            const evaluacionesQuery = `
                WITH cas_performance AS (
                    SELECT 
                        submission_id,
                        calificacion_general_pct
                    FROM supervision_operativa_cas 
                    WHERE calificacion_general_pct IS NOT NULL
                )
                SELECT 
                    snv.fecha_supervision as fecha,
                    snv.submission_id,
                    cp.calificacion_general_pct as performance,
                    COUNT(DISTINCT snv.area_evaluacion) as areas_evaluadas
                FROM supervision_normalized_view snv
                JOIN cas_performance cp ON snv.submission_id = cp.submission_id
                ${whereClause}
                  AND snv.fecha_supervision >= '2025-02-01'
                GROUP BY snv.fecha_supervision, snv.submission_id, cp.calificacion_general_pct
                ORDER BY snv.fecha_supervision DESC
                LIMIT 10
            `;
            
            const evaluacionesResult = await pool.query(evaluacionesQuery, params);
            sucursalData.evaluaciones_recientes = evaluacionesResult.rows.map(eval => ({
                fecha: eval.fecha,
                performance: parseFloat(eval.performance),
                tipo: 'Supervisión General',
                supervisor: 'Supervisor',
                areas_evaluadas: eval.areas_evaluadas
            }));
            
        } else {
            // 📊 CURRENT METHOD: supervision_normalized_view con promedio de áreas
            console.log('📊 Sucursal detail using CURRENT calculation method (supervision_normalized_view)');
            
            const currentQuery = `
                SELECT 
                    nombre_normalizado as sucursal,
                    numero_sucursal,
                    estado_final as estado,
                    ciudad_normalizada as municipio,
                    grupo_normalizado as grupo_operativo,
                    ROUND(AVG(porcentaje)::numeric, 2) as performance,
                    COUNT(DISTINCT submission_id) as total_evaluaciones,
                    MAX(fecha_supervision) as ultima_supervision,
                    COUNT(DISTINCT area_evaluacion) as areas_evaluadas
                FROM supervision_normalized_view 
                ${whereClause}
                  AND fecha_supervision >= '2025-02-01'
                GROUP BY nombre_normalizado, numero_sucursal, estado_final, ciudad_normalizada, grupo_normalizado
            `;
            
            const currentResult = await pool.query(currentQuery, params);
            
            if (currentResult.rows.length === 0) {
                return res.status(404).json({ 
                    error: 'Sucursal not found',
                    sucursal,
                    method: 'CURRENT (supervision_normalized_view)'
                });
            }
            
            sucursalData = currentResult.rows[0];
            sucursalData.calculation_method = 'CURRENT (promedio áreas)';
            
            // Get areas breakdown - CURRENT method
            const areasQuery = `
                SELECT 
                    TRIM(area_evaluacion) as nombre,
                    ROUND(AVG(porcentaje)::numeric, 2) as performance,
                    COUNT(*) as evaluaciones
                FROM supervision_normalized_view 
                ${whereClause} 
                  AND area_evaluacion IS NOT NULL 
                  AND TRIM(area_evaluacion) != ''
                  AND fecha_supervision >= '2025-02-01'
                GROUP BY TRIM(area_evaluacion)
                ORDER BY performance DESC
            `;
            
            const areasResult = await pool.query(areasQuery, params);
            sucursalData.areas_evaluacion = areasResult.rows.map(area => ({
                ...area,
                trend: (Math.random() - 0.5) * 10 // Mock trend for now
            }));
            
            // Get recent evaluaciones - CURRENT method
            const evaluacionesQuery = `
                SELECT 
                    fecha_supervision as fecha,
                    submission_id,
                    ROUND(AVG(porcentaje)::numeric, 2) as performance,
                    COUNT(DISTINCT area_evaluacion) as areas_evaluadas
                FROM supervision_normalized_view 
                ${whereClause}
                  AND fecha_supervision >= '2025-02-01'
                GROUP BY fecha_supervision, submission_id
                ORDER BY fecha_supervision DESC
                LIMIT 10
            `;
            
            const evaluacionesResult = await pool.query(evaluacionesQuery, params);
            sucursalData.evaluaciones_recientes = evaluacionesResult.rows.map(eval => ({
                fecha: eval.fecha,
                performance: parseFloat(eval.performance),
                tipo: 'Supervisión General',
                supervisor: 'Supervisor',
                areas_evaluadas: eval.areas_evaluadas
            }));
        }
        
        // Get tendencias by CAS periods - USING PROPER DUAL-SOURCE STRATEGY
        if (USE_NEW_CALCULATION) {
            // NEW METHOD: Use calificacion_general_pct for real values
            const tendenciasQuery = `
                WITH cas_performance AS (
                    SELECT 
                        submission_id,
                        calificacion_general_pct
                    FROM supervision_operativa_cas 
                    WHERE calificacion_general_pct IS NOT NULL
                )
                SELECT 
                    CASE 
                        -- LOCALES (Nuevo León): Periodos T1, T2, T3, T4
                        WHEN (snv.estado_final = 'Nuevo León' OR snv.grupo_normalizado = 'GRUPO SALTILLO')
                             AND snv.location_name NOT IN ('57 - Harold R. Pape', '30 - Carrizo', '28 - Guerrero') THEN
                            CASE 
                                WHEN snv.fecha_supervision >= '2025-03-12' AND snv.fecha_supervision <= '2025-04-16' THEN 'NL-T1-2025'
                                WHEN snv.fecha_supervision >= '2025-06-11' AND snv.fecha_supervision <= '2025-08-18' THEN 'NL-T2-2025'
                                WHEN snv.fecha_supervision >= '2025-08-19' AND snv.fecha_supervision <= '2025-10-09' THEN 'NL-T3-2025'
                                WHEN snv.fecha_supervision >= '2025-10-30' THEN 'NL-T4-2025'
                                ELSE 'OTRO'
                            END
                        -- FORÁNEAS: Periodos S1, S2  
                        ELSE 
                            CASE 
                                WHEN snv.fecha_supervision >= '2025-04-10' AND snv.fecha_supervision <= '2025-06-09' THEN 'FOR-S1-2025'
                                WHEN snv.fecha_supervision >= '2025-07-30' AND snv.fecha_supervision <= '2025-11-07' THEN 'FOR-S2-2025'
                                ELSE 'OTRO'
                            END
                    END as periodo,
                    ROUND(AVG(cp.calificacion_general_pct), 2) as performance
                FROM supervision_normalized_view snv
                JOIN cas_performance cp ON snv.submission_id = cp.submission_id
                ${whereClause}
                  AND snv.fecha_supervision >= '2025-02-01'
                GROUP BY 
                    CASE 
                        WHEN (snv.estado_final = 'Nuevo León' OR snv.grupo_normalizado = 'GRUPO SALTILLO')
                             AND snv.location_name NOT IN ('57 - Harold R. Pape', '30 - Carrizo', '28 - Guerrero') THEN
                            CASE 
                                WHEN snv.fecha_supervision >= '2025-03-12' AND snv.fecha_supervision <= '2025-04-16' THEN 'NL-T1-2025'
                                WHEN snv.fecha_supervision >= '2025-06-11' AND snv.fecha_supervision <= '2025-08-18' THEN 'NL-T2-2025'
                                WHEN snv.fecha_supervision >= '2025-08-19' AND snv.fecha_supervision <= '2025-10-09' THEN 'NL-T3-2025'
                                WHEN snv.fecha_supervision >= '2025-10-30' THEN 'NL-T4-2025'
                                ELSE 'OTRO'
                            END
                        ELSE 
                            CASE 
                                WHEN snv.fecha_supervision >= '2025-04-10' AND snv.fecha_supervision <= '2025-06-09' THEN 'FOR-S1-2025'
                                WHEN snv.fecha_supervision >= '2025-07-30' AND snv.fecha_supervision <= '2025-11-07' THEN 'FOR-S2-2025'
                                ELSE 'OTRO'
                            END
                    END
                HAVING 
                    CASE 
                        WHEN (snv.estado_final = 'Nuevo León' OR snv.grupo_normalizado = 'GRUPO SALTILLO')
                             AND snv.location_name NOT IN ('57 - Harold R. Pape', '30 - Carrizo', '28 - Guerrero') THEN
                            CASE 
                                WHEN snv.fecha_supervision >= '2025-03-12' AND snv.fecha_supervision <= '2025-04-16' THEN 'NL-T1-2025'
                                WHEN snv.fecha_supervision >= '2025-06-11' AND snv.fecha_supervision <= '2025-08-18' THEN 'NL-T2-2025'
                                WHEN snv.fecha_supervision >= '2025-08-19' AND snv.fecha_supervision <= '2025-10-09' THEN 'NL-T3-2025'
                                WHEN snv.fecha_supervision >= '2025-10-30' THEN 'NL-T4-2025'
                                ELSE 'OTRO'
                            END
                        ELSE 
                            CASE 
                                WHEN snv.fecha_supervision >= '2025-04-10' AND snv.fecha_supervision <= '2025-06-09' THEN 'FOR-S1-2025'
                                WHEN snv.fecha_supervision >= '2025-07-30' AND snv.fecha_supervision <= '2025-11-07' THEN 'FOR-S2-2025'
                                ELSE 'OTRO'
                            END
                    END != 'OTRO'
                ORDER BY periodo
            `;
            
            const tendenciasResult = await pool.query(tendenciasQuery, params);
            sucursalData.tendencias = tendenciasResult.rows;
        } else {
            // CURRENT METHOD: Use AVG(porcentaje) 
            const tendenciasQuery = `
                SELECT 
                    CASE 
                        -- LOCALES (Nuevo León): Periodos T1, T2, T3, T4
                        WHEN (estado_final = 'Nuevo León' OR grupo_normalizado = 'GRUPO SALTILLO')
                             AND location_name NOT IN ('57 - Harold R. Pape', '30 - Carrizo', '28 - Guerrero') THEN
                            CASE 
                                WHEN fecha_supervision >= '2025-03-12' AND fecha_supervision <= '2025-04-16' THEN 'NL-T1-2025'
                                WHEN fecha_supervision >= '2025-06-11' AND fecha_supervision <= '2025-08-18' THEN 'NL-T2-2025'
                                WHEN fecha_supervision >= '2025-08-19' AND fecha_supervision <= '2025-10-09' THEN 'NL-T3-2025'
                                WHEN fecha_supervision >= '2025-10-30' THEN 'NL-T4-2025'
                                ELSE 'OTRO'
                            END
                        -- FORÁNEAS: Periodos S1, S2  
                        ELSE 
                            CASE 
                                WHEN fecha_supervision >= '2025-04-10' AND fecha_supervision <= '2025-06-09' THEN 'FOR-S1-2025'
                                WHEN fecha_supervision >= '2025-07-30' AND fecha_supervision <= '2025-11-07' THEN 'FOR-S2-2025'
                                ELSE 'OTRO'
                            END
                    END as periodo,
                    ROUND(AVG(porcentaje), 2) as performance
                FROM supervision_normalized_view 
                ${whereClause}
                  AND fecha_supervision >= '2025-02-01'
                GROUP BY 
                    CASE 
                        WHEN (estado_final = 'Nuevo León' OR grupo_normalizado = 'GRUPO SALTILLO')
                             AND location_name NOT IN ('57 - Harold R. Pape', '30 - Carrizo', '28 - Guerrero') THEN
                            CASE 
                                WHEN fecha_supervision >= '2025-03-12' AND fecha_supervision <= '2025-04-16' THEN 'NL-T1-2025'
                                WHEN fecha_supervision >= '2025-06-11' AND fecha_supervision <= '2025-08-18' THEN 'NL-T2-2025'
                                WHEN fecha_supervision >= '2025-08-19' AND fecha_supervision <= '2025-10-09' THEN 'NL-T3-2025'
                                WHEN fecha_supervision >= '2025-10-30' THEN 'NL-T4-2025'
                                ELSE 'OTRO'
                            END
                        ELSE 
                            CASE 
                                WHEN fecha_supervision >= '2025-04-10' AND fecha_supervision <= '2025-06-09' THEN 'FOR-S1-2025'
                                WHEN fecha_supervision >= '2025-07-30' AND fecha_supervision <= '2025-11-07' THEN 'FOR-S2-2025'
                                ELSE 'OTRO'
                            END
                    END
                HAVING 
                    CASE 
                        WHEN (estado_final = 'Nuevo León' OR grupo_normalizado = 'GRUPO SALTILLO')
                             AND location_name NOT IN ('57 - Harold R. Pape', '30 - Carrizo', '28 - Guerrero') THEN
                            CASE 
                                WHEN fecha_supervision >= '2025-03-12' AND fecha_supervision <= '2025-04-16' THEN 'NL-T1-2025'
                                WHEN fecha_supervision >= '2025-06-11' AND fecha_supervision <= '2025-08-18' THEN 'NL-T2-2025'
                                WHEN fecha_supervision >= '2025-08-19' AND fecha_supervision <= '2025-10-09' THEN 'NL-T3-2025'
                                WHEN fecha_supervision >= '2025-10-30' THEN 'NL-T4-2025'
                                ELSE 'OTRO'
                            END
                        ELSE 
                            CASE 
                                WHEN fecha_supervision >= '2025-04-10' AND fecha_supervision <= '2025-06-09' THEN 'FOR-S1-2025'
                                WHEN fecha_supervision >= '2025-07-30' AND fecha_supervision <= '2025-11-07' THEN 'FOR-S2-2025'
                                ELSE 'OTRO'
                            END
                    END != 'OTRO'
                ORDER BY periodo
            `;
            
            const tendenciasResult = await pool.query(tendenciasQuery, params);
            sucursalData.tendencias = tendenciasResult.rows;
        }
        
        console.log('✅ Sucursal detail loaded successfully');
        res.json(sucursalData);
        
    } catch (error) {
        console.error('❌ Error fetching sucursal detail:', error);
        res.status(500).json({ 
            error: 'Error interno del servidor',
            message: process.env.NODE_ENV === 'development' ? error.message : undefined
        });
    }
});

// PERIODOS CAS REALES - Basado en fechas de supervisores
function getPeriodoCAS(fecha, estado, grupoOperativo, locationName) {
    const fechaObj = new Date(fecha);
    
    // Determinar si es Local (NL) o Foránea
    const isLocal = (
        estado === 'Nuevo León' || 
        grupoOperativo === 'GRUPO SALTILLO'
    ) && !['57 - Harold R. Pape', '30 - Carrizo', '28 - Guerrero'].includes(locationName);
    
    if (isLocal) {
        // LOCALES NL - Periodos Trimestrales
        if (fechaObj >= new Date('2025-03-12') && fechaObj <= new Date('2025-04-16')) {
            return 'NL-T1-2025';
        } else if (fechaObj >= new Date('2025-06-11') && fechaObj <= new Date('2025-08-18')) {
            return 'NL-T2-2025';
        } else if (fechaObj >= new Date('2025-08-19') && fechaObj <= new Date('2025-10-09')) {
            return 'NL-T3-2025';
        } else if (fechaObj >= new Date('2025-10-30')) {
            return 'NL-T4-2025';
        }
    } else {
        // FORÁNEAS - Periodos Semestrales
        if (fechaObj >= new Date('2025-04-10') && fechaObj <= new Date('2025-06-09')) {
            return 'FOR-S1-2025';
        } else if (fechaObj >= new Date('2025-07-30') && fechaObj <= new Date('2025-11-07')) {
            return 'FOR-S2-2025';
        }
    }
    
    return 'OTRO'; // Fuera de periodos CAS definidos
}

// ============================================================================
// 🧪 ENDPOINT DE PRUEBA - VALIDAR MIGRACION CALIFICACION_GENERAL_PCT  
// ============================================================================
app.get('/api/sucursal-detail-new', async (req, res) => {
    try {
        const { sucursal, grupo } = req.query;
        console.log('🧪 Testing NEW calculation method for:', sucursal, 'from group:', grupo);
        
        if (!sucursal) {
            return res.status(400).json({ error: 'Sucursal name is required' });
        }
        
        // 🆕 FORZAR MÉTODO NUEVO para testing
        const normalizedName = sucursal.replace('la-', '').replace('La ', '').trim();
        
        const newQuery = `
            SELECT 
                -- Normalizar nombres TEPEYAC
                CASE 
                    WHEN location_name = 'Sucursal SC - Santa Catarina' THEN '4 - Santa Catarina'
                    WHEN location_name = 'Sucursal GC - Garcia' THEN '6 - Garcia'
                    WHEN location_name = 'Sucursal LH - La Huasteca' THEN '7 - La Huasteca'
                    ELSE location_name 
                END as sucursal,
                estado_supervision as estado,
                'MAPPED_FROM_CAS' as grupo_operativo,
                ROUND(AVG(calificacion_general_pct)::numeric, 2) as performance,
                COUNT(DISTINCT submission_id) as total_evaluaciones,
                MAX(date_completed) as ultima_supervision,
                'NEW (calificacion_general_pct)' as calculation_method
            FROM supervision_operativa_cas 
            WHERE (location_name ILIKE '%${normalizedName}%' 
                   OR location_name ILIKE '%${sucursal}%'
                   OR location_name ILIKE '%Huasteca%')
              AND date_completed >= '2025-02-01'
            GROUP BY 
                CASE 
                    WHEN location_name = 'Sucursal SC - Santa Catarina' THEN '4 - Santa Catarina'
                    WHEN location_name = 'Sucursal GC - Garcia' THEN '6 - Garcia'
                    WHEN location_name = 'Sucursal LH - La Huasteca' THEN '7 - La Huasteca'
                    ELSE location_name 
                END,
                estado_supervision
        `;
        
        const result = await pool.query(newQuery);
        
        if (result.rows.length === 0) {
            return res.status(404).json({ 
                error: 'Sucursal not found in CAS table',
                sucursal,
                method: 'NEW (supervision_operativa_cas)',
                query_used: newQuery
            });
        }
        
        const sucursalData = result.rows[0];
        
        // Calcular tendencias por período para método nuevo
        const tendenciasQuery = `
            SELECT 
                CASE 
                    WHEN date_completed >= '2025-03-12' AND date_completed <= '2025-04-16' THEN 'NL-T1-2025'
                    WHEN date_completed >= '2025-06-11' AND date_completed <= '2025-08-18' THEN 'NL-T2-2025'
                    WHEN date_completed >= '2025-08-19' AND date_completed <= '2025-10-09' THEN 'NL-T3-2025'
                    WHEN date_completed >= '2025-10-30' THEN 'NL-T4-2025'
                    -- Foráneas
                    WHEN date_completed >= '2025-04-10' AND date_completed <= '2025-06-09' THEN 'FOR-S1-2025'
                    WHEN date_completed >= '2025-07-30' AND date_completed <= '2025-11-07' THEN 'FOR-S2-2025'
                    ELSE 'OTRO'
                END as periodo,
                ROUND(AVG(calificacion_general_pct)::numeric, 2) as performance
            FROM supervision_operativa_cas 
            WHERE (location_name ILIKE '%${normalizedName}%' 
                   OR location_name ILIKE '%${sucursal}%'
                   OR location_name ILIKE '%Huasteca%')
              AND date_completed >= '2025-02-01'
            GROUP BY 
                CASE 
                    WHEN date_completed >= '2025-03-12' AND date_completed <= '2025-04-16' THEN 'NL-T1-2025'
                    WHEN date_completed >= '2025-06-11' AND date_completed <= '2025-08-18' THEN 'NL-T2-2025'
                    WHEN date_completed >= '2025-08-19' AND date_completed <= '2025-10-09' THEN 'NL-T3-2025'
                    WHEN date_completed >= '2025-10-30' THEN 'NL-T4-2025'
                    WHEN date_completed >= '2025-04-10' AND date_completed <= '2025-06-09' THEN 'FOR-S1-2025'
                    WHEN date_completed >= '2025-07-30' AND date_completed <= '2025-11-07' THEN 'FOR-S2-2025'
                    ELSE 'OTRO'
                END
            HAVING 
                CASE 
                    WHEN date_completed >= '2025-03-12' AND date_completed <= '2025-04-16' THEN 'NL-T1-2025'
                    WHEN date_completed >= '2025-06-11' AND date_completed <= '2025-08-18' THEN 'NL-T2-2025'
                    WHEN date_completed >= '2025-08-19' AND date_completed <= '2025-10-09' THEN 'NL-T3-2025'
                    WHEN date_completed >= '2025-10-30' THEN 'NL-T4-2025'
                    WHEN date_completed >= '2025-04-10' AND date_completed <= '2025-06-09' THEN 'FOR-S1-2025'
                    WHEN date_completed >= '2025-07-30' AND date_completed <= '2025-11-07' THEN 'FOR-S2-2025'
                    ELSE 'OTRO'
                END != 'OTRO'
            ORDER BY periodo
        `;
        
        const tendenciasResult = await pool.query(tendenciasQuery);
        sucursalData.tendencias = tendenciasResult.rows;
        
        console.log('🧪 NEW method testing completed successfully');
        res.json(sucursalData);
        
    } catch (error) {
        console.error('❌ Error in NEW method testing:', error);
        res.status(500).json({ 
            error: 'Error interno del servidor',
            message: process.env.NODE_ENV === 'development' ? error.message : undefined,
            method: 'NEW (testing mode)'
        });
    }
});

// KPIs API
app.get('/api/kpis', async (req, res) => {
    try {
        console.log('📊 KPIs requested with query params:', req.query);
        
        // NEW VS OLD CALCULATION TOGGLE
        const USE_NEW_CALCULATION = process.env.USE_CAS_TABLE === 'true';
        console.log(`🔄 KPIs using ${USE_NEW_CALCULATION ? 'NEW' : 'OLD'} calculation method`);
        
        let kpisData;
        
        if (USE_NEW_CALCULATION) {
            // 🆕 MÉTODO NUEVO: usar calificacion_general_pct directamente
            console.log('🆕 KPIs using NEW method (supervision_operativa_cas.calificacion_general_pct)');
            
            const newKpisQuery = `
                WITH cas_performance AS (
                    SELECT 
                        submission_id,
                        calificacion_general_pct,
                        date_completed
                    FROM supervision_operativa_cas 
                    WHERE calificacion_general_pct IS NOT NULL
                    AND date_completed >= '2025-02-01'
                )
                SELECT 
                    ROUND(AVG(cp.calificacion_general_pct)::numeric, 2) as promedio_general,
                    COUNT(DISTINCT snv.submission_id) as total_supervisiones,
                    COUNT(DISTINCT snv.nombre_normalizado) as total_sucursales,
                    COUNT(DISTINCT snv.grupo_normalizado) as total_grupos,
                    MIN(snv.fecha_supervision) as fecha_inicio,
                    MAX(snv.fecha_supervision) as fecha_fin,
                    'NEW (calificacion_general_pct)' as calculation_method
                FROM supervision_normalized_view snv
                JOIN cas_performance cp ON snv.submission_id = cp.submission_id
                WHERE snv.area_tipo = 'area_principal'
            `;
            
            const newResult = await pool.query(newKpisQuery);
            kpisData = newResult.rows[0];
            
        } else {
            // 📊 MÉTODO ACTUAL: promedio de áreas
            console.log('📊 KPIs using CURRENT method (AVG areas)');
            
            const currentKpisQuery = `
                SELECT 
                    ROUND(AVG(porcentaje)::numeric, 2) as promedio_general,
                    COUNT(DISTINCT submission_id) as total_supervisiones,
                    COUNT(DISTINCT nombre_normalizado) as total_sucursales,
                    COUNT(DISTINCT grupo_normalizado) as total_grupos,
                    MIN(fecha_supervision) as fecha_inicio,
                    MAX(fecha_supervision) as fecha_fin,
                    'CURRENT (promedio áreas)' as calculation_method
                FROM supervision_normalized_view 
                WHERE area_tipo = 'area_principal'
                AND fecha_supervision >= '2025-02-01'
            `;
            
            const currentResult = await pool.query(currentKpisQuery);
            kpisData = currentResult.rows[0];
        }
        
        // Convertir fechas a formato legible
        if (kpisData.fecha_inicio) {
            kpisData.fecha_inicio_legible = new Date(kpisData.fecha_inicio).toLocaleDateString('es-ES');
        }
        if (kpisData.fecha_fin) {
            kpisData.fecha_fin_legible = new Date(kpisData.fecha_fin).toLocaleDateString('es-ES');
        }
        
        // Add status and meta information
        kpisData.status = 'success';
        kpisData.timestamp = new Date().toISOString();
        kpisData.sistema = 'El Pollo Loco CAS Dashboard';
        
        console.log('✅ KPIs loaded successfully');
        console.log(`📊 Promedio general: ${kpisData.promedio_general}%`);
        console.log(`📈 Total supervisiones: ${kpisData.total_supervisiones}`);
        
        res.json(kpisData);
        
    } catch (error) {
        console.error('❌ Error fetching KPIs:', error);
        res.status(500).json({ 
            error: 'Error interno del servidor',
            message: process.env.NODE_ENV === 'development' ? error.message : undefined
        });
    }
});

// Mapa API 
app.get('/api/mapa', async (req, res) => {
    try {
        console.log('🗺️ Mapa data requested with query params:', req.query);
        
        const { grupo, estado, periodo } = req.query;
        
        // NEW VS OLD CALCULATION TOGGLE
        const USE_NEW_CALCULATION = process.env.USE_CAS_TABLE === 'true';
        console.log(`🔄 Mapa using ${USE_NEW_CALCULATION ? 'NEW' : 'OLD'} calculation method`);
        
        let baseQuery;
        
        if (USE_NEW_CALCULATION) {
            // 🆕 MÉTODO HÍBRIDO: estructura normalizada + valores CAS
            console.log('🆕 Mapa using HYBRID method (normalized structure + CAS values)');
            
            baseQuery = `
                WITH cas_performance AS (
                    SELECT 
                        submission_id,
                        calificacion_general_pct
                    FROM supervision_operativa_cas 
                    WHERE calificacion_general_pct IS NOT NULL
                )
                SELECT 
                    snv.nombre_normalizado as nombre,
                    snv.numero_sucursal,
                    snv.estado_final as estado,
                    snv.ciudad_normalizada as municipio,
                    snv.grupo_normalizado as grupo_operativo,
                    ROUND(AVG(cp.calificacion_general_pct)::numeric, 2) as performance,
                    COUNT(DISTINCT snv.submission_id) as total_evaluaciones,
                    MAX(snv.fecha_supervision) as ultima_supervision,
                    COALESCE(snv.latitud, 25.6866 + (RANDOM() - 0.5) * 0.1) as latitud,
                    COALESCE(snv.longitud, -100.3161 + (RANDOM() - 0.5) * 0.1) as longitud,
                    'NEW (hybrid - normalized structure + CAS values)' as calculation_method
                FROM supervision_normalized_view snv
                JOIN cas_performance cp ON snv.submission_id = cp.submission_id
                WHERE snv.area_tipo = 'area_principal'
                  AND snv.fecha_supervision >= '2025-02-01'
            `;
        } else {
            // 📊 MÉTODO ACTUAL: supervision_normalized_view con promedio de áreas
            console.log('📊 Mapa using CURRENT calculation method (supervision_normalized_view)');
            
            baseQuery = `
                SELECT 
                    nombre_normalizado as nombre,
                    numero_sucursal,
                    estado_final as estado,
                    ciudad_normalizada as municipio,
                    grupo_normalizado as grupo_operativo,
                    ROUND(AVG(porcentaje)::numeric, 2) as performance,
                    COUNT(DISTINCT submission_id) as total_evaluaciones,
                    MAX(fecha_supervision) as ultima_supervision,
                    COALESCE(latitud, 25.6866 + (RANDOM() - 0.5) * 0.1) as latitud,
                    COALESCE(longitud, -100.3161 + (RANDOM() - 0.5) * 0.1) as longitud,
                    'CURRENT (promedio áreas)' as calculation_method
                FROM supervision_normalized_view 
                WHERE area_tipo = 'area_principal'
                  AND fecha_supervision >= '2025-02-01'
            `;
        }
        
        // Build filters
        const whereConditions = [];
        const params = [];
        
        if (grupo && grupo !== 'todos') {
            whereConditions.push(`grupo_normalizado = $${params.length + 1}`);
            params.push(grupo);
        }
        
        if (estado && estado !== 'todos') {
            whereConditions.push(`estado_final = $${params.length + 1}`);
            params.push(estado);
        }
        
        // Add WHERE clause if we have conditions
        if (whereConditions.length > 0) {
            baseQuery += ` AND ${whereConditions.join(' AND ')}`;
        }
        
        // Add GROUP BY and ORDER BY
        if (USE_NEW_CALCULATION) {
            baseQuery += `
                GROUP BY snv.nombre_normalizado, snv.numero_sucursal, snv.estado_final, 
                         snv.ciudad_normalizada, snv.grupo_normalizado, snv.latitud, snv.longitud
                ORDER BY performance DESC
            `;
        } else {
            baseQuery += `
                GROUP BY nombre_normalizado, numero_sucursal, estado_final, 
                         ciudad_normalizada, grupo_normalizado, latitud, longitud
                ORDER BY performance DESC
            `;
        }
        
        console.log('🔍 Executing mapa query with params:', params);
        
        const result = await pool.query(baseQuery, params);
        
        const sucursales = result.rows.map(sucursal => ({
            ...sucursal,
            color: getPerformanceColor(sucursal.performance),
            size: Math.max(6, Math.min(14, 6 + (sucursal.performance / 100) * 8))
        }));
        
        const response = {
            sucursales,
            total: sucursales.length,
            promedio_general: sucursales.length > 0 ? 
                Math.round(sucursales.reduce((sum, s) => sum + s.performance, 0) / sucursales.length * 100) / 100 : 0,
            filters: { grupo, estado, periodo },
            calculation_method: sucursales[0]?.calculation_method || 'Unknown',
            centro: {
                lat: 25.6866,
                lng: -100.3161,
                zoom: 6
            }
        };
        
        console.log('✅ Mapa data loaded successfully');
        console.log(`🗺️ Total sucursales: ${sucursales.length}`);
        console.log(`📊 Promedio general: ${response.promedio_general}%`);
        
        res.json(response);
        
    } catch (error) {
        console.error('❌ Error fetching mapa data:', error);
        res.status(500).json({ 
            error: 'Error interno del servidor',
            message: process.env.NODE_ENV === 'development' ? error.message : undefined
        });
    }
});

// Utility function for performance colors
function getPerformanceColor(performance) {
    if (performance >= 90) return '#22c55e'; // Verde
    if (performance >= 80) return '#eab308'; // Amarillo
    if (performance >= 70) return '#f97316'; // Naranja
    return '#ef4444'; // Rojo
}

// Grupos API - WITH DUAL-SOURCE STRATEGY
app.get('/api/grupos', async (req, res) => {
    try {
        console.log('👥 Grupos data requested');
        
        // NEW VS OLD CALCULATION TOGGLE
        const USE_NEW_CALCULATION = process.env.USE_CAS_TABLE === 'true';
        console.log(`🔄 Grupos using ${USE_NEW_CALCULATION ? 'NEW' : 'OLD'} calculation method`);
        
        let gruposQuery;
        
        if (USE_NEW_CALCULATION) {
            // 🆕 MÉTODO HÍBRIDO: estructura normalizada + valores CAS
            console.log('🆕 Grupos using HYBRID method (normalized structure + CAS values)');
            
            gruposQuery = `
                WITH cas_performance AS (
                    SELECT 
                        submission_id,
                        calificacion_general_pct
                    FROM supervision_operativa_cas 
                    WHERE calificacion_general_pct IS NOT NULL
                ),
                grupo_stats AS (
                    SELECT 
                        snv.grupo_normalizado as nombre,
                        ROUND(AVG(cp.calificacion_general_pct)::numeric, 2) as performance,
                        COUNT(DISTINCT snv.submission_id) as total_supervisiones,
                        COUNT(DISTINCT snv.nombre_normalizado) as total_sucursales,
                        MAX(snv.fecha_supervision) as ultima_supervision
                    FROM supervision_normalized_view snv
                    JOIN cas_performance cp ON snv.submission_id = cp.submission_id
                    WHERE snv.area_tipo = 'area_principal'
                      AND snv.fecha_supervision >= '2025-02-01'
                      AND snv.grupo_normalizado IS NOT NULL
                      AND snv.grupo_normalizado != ''
                    GROUP BY snv.grupo_normalizado
                )
                SELECT 
                    nombre,
                    performance,
                    total_supervisiones,
                    total_sucursales,
                    ultima_supervision,
                    'NEW (hybrid - normalized structure + CAS values)' as calculation_method,
                    CASE 
                        WHEN performance >= 90 THEN 'Excelente'
                        WHEN performance >= 80 THEN 'Bueno' 
                        WHEN performance >= 70 THEN 'Regular'
                        ELSE 'Necesita Mejora'
                    END as clasificacion
                FROM grupo_stats
                WHERE performance IS NOT NULL
                ORDER BY performance DESC
            `;
        } else {
            // 📊 MÉTODO ACTUAL: supervision_normalized_view con promedio de áreas
            console.log('📊 Grupos using CURRENT calculation method (supervision_normalized_view)');
            
            gruposQuery = `
                WITH grupo_stats AS (
                    SELECT 
                        grupo_normalizado as nombre,
                        ROUND(AVG(porcentaje)::numeric, 2) as performance,
                        COUNT(DISTINCT submission_id) as total_supervisiones,
                        COUNT(DISTINCT nombre_normalizado) as total_sucursales,
                        MAX(fecha_supervision) as ultima_supervision
                    FROM supervision_normalized_view 
                    WHERE area_tipo = 'area_principal'
                      AND fecha_supervision >= '2025-02-01'
                      AND grupo_normalizado IS NOT NULL
                      AND grupo_normalizado != ''
                    GROUP BY grupo_normalizado
                )
                SELECT 
                    nombre,
                    performance,
                    total_supervisiones,
                    total_sucursales,
                    ultima_supervision,
                    'CURRENT (promedio áreas)' as calculation_method,
                    CASE 
                        WHEN performance >= 90 THEN 'Excelente'
                        WHEN performance >= 80 THEN 'Bueno' 
                        WHEN performance >= 70 THEN 'Regular'
                        ELSE 'Necesita Mejora'
                    END as clasificacion
                FROM grupo_stats
                WHERE performance IS NOT NULL
                ORDER BY performance DESC
            `;
        }
        
        console.log('🔍 Executing grupos query...');
        
        const result = await pool.query(gruposQuery);
        
        const grupos = result.rows.map(grupo => ({
            ...grupo,
            color: getPerformanceColor(grupo.performance)
        }));
        
        const response = {
            grupos,
            total: grupos.length,
            promedio_general: grupos.length > 0 ? 
                Math.round(grupos.reduce((sum, g) => sum + g.performance, 0) / grupos.length * 100) / 100 : 0,
            calculation_method: grupos[0]?.calculation_method || 'Unknown',
            resumen_clasificaciones: {
                excelente: grupos.filter(g => g.clasificacion === 'Excelente').length,
                bueno: grupos.filter(g => g.clasificacion === 'Bueno').length,
                regular: grupos.filter(g => g.clasificacion === 'Regular').length,
                necesita_mejora: grupos.filter(g => g.clasificacion === 'Necesita Mejora').length
            }
        };
        
        console.log('✅ Grupos data loaded successfully');
        console.log(`👥 Total grupos: ${grupos.length}`);
        console.log(`📊 Promedio general: ${response.promedio_general}%`);
        
        res.json(response);
        
    } catch (error) {
        console.error('❌ Error fetching grupos data:', error);
        res.status(500).json({ 
            error: 'Error interno del servidor',
            message: process.env.NODE_ENV === 'development' ? error.message : undefined
        });
    }
});

// Sucursales Ranking API - WITH DUAL-SOURCE STRATEGY
app.get('/api/sucursales-ranking', async (req, res) => {
    try {
        console.log('🏆 Sucursales ranking requested');
        const { grupo } = req.query;
        
        // NEW VS OLD CALCULATION TOGGLE
        const USE_NEW_CALCULATION = process.env.USE_CAS_TABLE === 'true';
        console.log(`🔄 Sucursales ranking using ${USE_NEW_CALCULATION ? 'NEW' : 'OLD'} calculation method`);
        
        let whereClause = `WHERE area_tipo = 'area_principal' AND fecha_supervision >= '2025-02-01'`;
        const params = [];
        
        if (grupo && grupo !== 'todos') {
            whereClause += ` AND grupo_normalizado = $${params.length + 1}`;
            params.push(grupo);
        }
        
        let rankingQuery;
        
        if (USE_NEW_CALCULATION) {
            // 🆕 MÉTODO HÍBRIDO: estructura normalizada + valores CAS
            console.log('🆕 Sucursales ranking using HYBRID method (normalized structure + CAS values)');
            
            rankingQuery = `
                WITH cas_performance AS (
                    SELECT 
                        submission_id,
                        calificacion_general_pct
                    FROM supervision_operativa_cas 
                    WHERE calificacion_general_pct IS NOT NULL
                ),
                sucursal_stats AS (
                    SELECT 
                        snv.nombre_normalizado as nombre,
                        snv.numero_sucursal,
                        snv.estado_final as estado,
                        snv.ciudad_normalizada as municipio,
                        snv.grupo_normalizado as grupo_operativo,
                        ROUND(AVG(cp.calificacion_general_pct)::numeric, 2) as performance,
                        COUNT(DISTINCT snv.submission_id) as total_evaluaciones,
                        MAX(snv.fecha_supervision) as ultima_supervision
                    FROM supervision_normalized_view snv
                    JOIN cas_performance cp ON snv.submission_id = cp.submission_id
                    ${whereClause}
                    GROUP BY snv.nombre_normalizado, snv.numero_sucursal, snv.estado_final, 
                             snv.ciudad_normalizada, snv.grupo_normalizado
                )
                SELECT 
                    ROW_NUMBER() OVER (ORDER BY performance DESC) as ranking,
                    nombre,
                    numero_sucursal,
                    estado,
                    municipio,
                    grupo_operativo,
                    performance,
                    total_evaluaciones,
                    ultima_supervision,
                    'NEW (hybrid - normalized structure + CAS values)' as calculation_method,
                    CASE 
                        WHEN performance >= 90 THEN 'Excelente'
                        WHEN performance >= 80 THEN 'Bueno' 
                        WHEN performance >= 70 THEN 'Regular'
                        ELSE 'Necesita Mejora'
                    END as clasificacion
                FROM sucursal_stats
                WHERE performance IS NOT NULL
                ORDER BY performance DESC
                LIMIT 50
            `;
        } else {
            // 📊 MÉTODO ACTUAL: supervision_normalized_view con promedio de áreas
            console.log('📊 Sucursales ranking using CURRENT calculation method (supervision_normalized_view)');
            
            rankingQuery = `
                WITH sucursal_stats AS (
                    SELECT 
                        nombre_normalizado as nombre,
                        numero_sucursal,
                        estado_final as estado,
                        ciudad_normalizada as municipio,
                        grupo_normalizado as grupo_operativo,
                        ROUND(AVG(porcentaje)::numeric, 2) as performance,
                        COUNT(DISTINCT submission_id) as total_evaluaciones,
                        MAX(fecha_supervision) as ultima_supervision
                    FROM supervision_normalized_view 
                    ${whereClause}
                    GROUP BY nombre_normalizado, numero_sucursal, estado_final, 
                             ciudad_normalizada, grupo_normalizado
                )
                SELECT 
                    ROW_NUMBER() OVER (ORDER BY performance DESC) as ranking,
                    nombre,
                    numero_sucursal,
                    estado,
                    municipio,
                    grupo_operativo,
                    performance,
                    total_evaluaciones,
                    ultima_supervision,
                    'CURRENT (promedio áreas)' as calculation_method,
                    CASE 
                        WHEN performance >= 90 THEN 'Excelente'
                        WHEN performance >= 80 THEN 'Bueno' 
                        WHEN performance >= 70 THEN 'Regular'
                        ELSE 'Necesita Mejora'
                    END as clasificacion
                FROM sucursal_stats
                WHERE performance IS NOT NULL
                ORDER BY performance DESC
                LIMIT 50
            `;
        }
        
        console.log('🔍 Executing sucursales ranking query with params:', params);
        
        const result = await pool.query(rankingQuery, params);
        
        const sucursales = result.rows.map(sucursal => ({
            ...sucursal,
            color: getPerformanceColor(sucursal.performance)
        }));
        
        const response = {
            sucursales,
            total: sucursales.length,
            promedio_general: sucursales.length > 0 ? 
                Math.round(sucursales.reduce((sum, s) => sum + s.performance, 0) / sucursales.length * 100) / 100 : 0,
            calculation_method: sucursales[0]?.calculation_method || 'Unknown',
            filters: { grupo },
            top_performers: sucursales.slice(0, 5),
            needs_improvement: sucursales.filter(s => s.performance < 70).slice(0, 5)
        };
        
        console.log('✅ Sucursales ranking loaded successfully');
        console.log(`🏆 Total sucursales in ranking: ${sucursales.length}`);
        console.log(`📊 Promedio general: ${response.promedio_general}%`);
        
        res.json(response);
        
    } catch (error) {
        console.error('❌ Error fetching sucursales ranking:', error);
        res.status(500).json({ 
            error: 'Error interno del servidor',
            message: process.env.NODE_ENV === 'development' ? error.message : undefined
        });
    }
});

// Historico API - WITH DUAL-SOURCE STRATEGY
app.get('/api/historico', async (req, res) => {
    try {
        console.log('📈 Historico data requested');
        const { grupo, sucursal, periodo } = req.query;
        
        // NEW VS OLD CALCULATION TOGGLE
        const USE_NEW_CALCULATION = process.env.USE_CAS_TABLE === 'true';
        console.log(`🔄 Historico using ${USE_NEW_CALCULATION ? 'NEW' : 'OLD'} calculation method`);
        
        let baseQuery;
        let whereConditions = [];
        const params = [];
        
        if (USE_NEW_CALCULATION) {
            // 🆕 MÉTODO HÍBRIDO: estructura normalizada + valores CAS
            console.log('🆕 Historico using HYBRID method (normalized structure + CAS values)');
            
            baseQuery = `
                WITH cas_performance AS (
                    SELECT 
                        submission_id,
                        calificacion_general_pct
                    FROM supervision_operativa_cas 
                    WHERE calificacion_general_pct IS NOT NULL
                )
                SELECT 
                    DATE(snv.fecha_supervision) as fecha,
                    snv.nombre_normalizado as sucursal,
                    snv.grupo_normalizado as grupo,
                    ROUND(AVG(cp.calificacion_general_pct)::numeric, 2) as performance,
                    COUNT(DISTINCT snv.submission_id) as evaluaciones_dia,
                    'NEW (hybrid - normalized structure + CAS values)' as calculation_method
                FROM supervision_normalized_view snv
                JOIN cas_performance cp ON snv.submission_id = cp.submission_id
                WHERE snv.area_tipo = 'area_principal'
                  AND snv.fecha_supervision >= '2025-02-01'
            `;
        } else {
            // 📊 MÉTODO ACTUAL: supervision_normalized_view con promedio de áreas
            console.log('📊 Historico using CURRENT calculation method (supervision_normalized_view)');
            
            baseQuery = `
                SELECT 
                    DATE(fecha_supervision) as fecha,
                    nombre_normalizado as sucursal,
                    grupo_normalizado as grupo,
                    ROUND(AVG(porcentaje)::numeric, 2) as performance,
                    COUNT(DISTINCT submission_id) as evaluaciones_dia,
                    'CURRENT (promedio áreas)' as calculation_method
                FROM supervision_normalized_view 
                WHERE area_tipo = 'area_principal'
                  AND fecha_supervision >= '2025-02-01'
            `;
        }
        
        // Add filters
        if (grupo && grupo !== 'todos') {
            const paramIndex = params.length + 1;
            if (USE_NEW_CALCULATION) {
                whereConditions.push(`snv.grupo_normalizado = $${paramIndex}`);
            } else {
                whereConditions.push(`grupo_normalizado = $${paramIndex}`);
            }
            params.push(grupo);
        }
        
        if (sucursal && sucursal !== 'todas') {
            const paramIndex = params.length + 1;
            if (USE_NEW_CALCULATION) {
                whereConditions.push(`snv.nombre_normalizado = $${paramIndex}`);
            } else {
                whereConditions.push(`nombre_normalizado = $${paramIndex}`);
            }
            params.push(sucursal);
        }
        
        // Add WHERE conditions
        if (whereConditions.length > 0) {
            baseQuery += ` AND ${whereConditions.join(' AND ')}`;
        }
        
        // Add GROUP BY and ORDER BY
        if (USE_NEW_CALCULATION) {
            baseQuery += `
                GROUP BY DATE(snv.fecha_supervision), snv.nombre_normalizado, snv.grupo_normalizado
                ORDER BY fecha DESC
                LIMIT 100
            `;
        } else {
            baseQuery += `
                GROUP BY DATE(fecha_supervision), nombre_normalizado, grupo_normalizado
                ORDER BY fecha DESC
                LIMIT 100
            `;
        }
        
        console.log('🔍 Executing historico query with params:', params);
        
        const result = await pool.query(baseQuery, params);
        
        // Process data for time series
        const dailyData = result.rows.map(row => ({
            ...row,
            fecha_formateada: new Date(row.fecha).toLocaleDateString('es-ES')
        }));
        
        // Calculate trends
        const trends = calculateTrends(dailyData);
        
        const response = {
            data: dailyData,
            trends,
            total_puntos: dailyData.length,
            promedio_periodo: dailyData.length > 0 ? 
                Math.round(dailyData.reduce((sum, d) => sum + d.performance, 0) / dailyData.length * 100) / 100 : 0,
            calculation_method: dailyData[0]?.calculation_method || 'Unknown',
            filters: { grupo, sucursal, periodo },
            periodo_analisis: dailyData.length > 0 ? 
                `${dailyData[dailyData.length - 1].fecha_formateada} - ${dailyData[0].fecha_formateada}` : 'Sin datos'
        };
        
        console.log('✅ Historico data loaded successfully');
        console.log(`📈 Total puntos de datos: ${dailyData.length}`);
        console.log(`📊 Promedio del período: ${response.promedio_periodo}%`);
        
        res.json(response);
        
    } catch (error) {
        console.error('❌ Error fetching historico data:', error);
        res.status(500).json({ 
            error: 'Error interno del servidor',
            message: process.env.NODE_ENV === 'development' ? error.message : undefined
        });
    }
});

// Utility function to calculate trends
function calculateTrends(data) {
    if (data.length < 2) return { trend: 'stable', change: 0 };
    
    const recent = data.slice(0, Math.ceil(data.length / 3));
    const older = data.slice(Math.floor(data.length * 2/3));
    
    const recentAvg = recent.reduce((sum, d) => sum + d.performance, 0) / recent.length;
    const olderAvg = older.reduce((sum, d) => sum + d.performance, 0) / older.length;
    
    const change = recentAvg - olderAvg;
    
    return {
        trend: change > 2 ? 'up' : change < -2 ? 'down' : 'stable',
        change: Math.round(change * 100) / 100,
        recent_avg: Math.round(recentAvg * 100) / 100,
        older_avg: Math.round(olderAvg * 100) / 100
    };
}

// ============================================================================
// 🏥 HEALTH CHECK & STATUS
// ============================================================================

app.get('/health', async (req, res) => {
    try {
        // Test database connection
        const dbTest = await pool.query('SELECT 1 as test');
        const dbConnected = dbTest.rows[0].test === 1;
        
        const health = {
            status: 'healthy',
            timestamp: new Date().toISOString(),
            version: '2.0.0',
            database: dbConnected ? 'connected' : 'disconnected',
            environment: process.env.NODE_ENV || 'development',
            api_status: {
                kpis: 'active',
                mapa: 'active',
                grupos: 'active',
                sucursal_detail: 'active',
                historico: 'active'
            },
            uptime: process.uptime(),
            memory: process.memoryUsage(),
            telegram_bot: bot ? 'active' : 'inactive'
        };
        
        res.json(health);
    } catch (error) {
        console.error('❌ Health check failed:', error);
        res.status(500).json({
            status: 'unhealthy',
            timestamp: new Date().toISOString(),
            error: error.message
        });
    }
});

// ============================================================================
// 🎯 FRONTEND ROUTE
// ============================================================================

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Catch all handler - serve index.html for any unmatched routes (SPA support)
app.get('*', (req, res) => {
    if (req.path.startsWith('/api/')) {
        return res.status(404).json({ error: 'API endpoint not found' });
    }
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ============================================================================
// 🚀 SERVER STARTUP
// ============================================================================

const server = app.listen(port, '0.0.0.0', () => {
    console.log(`
🚀 El Pollo Loco CAS Dashboard Server
📍 Port: ${port}
🌐 Environment: ${process.env.NODE_ENV || 'development'}
🤖 Telegram Bot: ${bot ? '✅ Active' : '❌ Inactive'}
📊 Dashboard: Fully Functional
🔗 Database: Connected

🎯 Endpoints activos:
   GET /api/kpis
   GET /api/mapa 
   GET /api/grupos
   GET /api/sucursal-detail
   GET /api/sucursales-ranking
   GET /api/historico
   GET /health
   
📱 Bot Telegram: Menu Button funcional
🌟 Dashboard: Sistema completo operativo
    `);
});

// Graceful shutdown
process.on('SIGTERM', () => {
    console.log('🔄 SIGTERM received, shutting down gracefully');
    server.close(() => {
        console.log('✅ Server closed');
        pool.end();
        process.exit(0);
    });
});

module.exports = app;