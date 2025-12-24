#!/usr/bin/env node

/**
 * 🗺️ NORMALIZACIÓN DE ESTADOS - EL POLLO LOCO MÉXICO
 * Roberto: Todos los estados aparecen como "Nuevo León", necesitamos normalizar
 */

const { Pool } = require('pg');
require('dotenv').config();

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

// Función para clasificar estados por coordenadas GPS
async function createEstadoClassifierFunction() {
    const functionSQL = `
        CREATE OR REPLACE FUNCTION classify_estado_by_coordinates(latitud DECIMAL, longitud DECIMAL) 
        RETURNS VARCHAR AS $$
        BEGIN
            -- Verificar que las coordenadas sean válidas
            IF latitud IS NULL OR longitud IS NULL THEN 
                RETURN 'Desconocido';
            END IF;
            
            -- NUEVO LEÓN: Centro: 25.6866, -100.3161 (Monterrey)
            -- Rango aproximado: Lat 23.5-27.8, Lng -99.0-101.5  
            IF latitud BETWEEN 23.5 AND 27.8 AND longitud BETWEEN -101.5 AND -99.0 THEN
                RETURN 'Nuevo León';
                
            -- COAHUILA: Centro: 25.4232, -101.0 
            -- Rango aproximado: Lat 24.0-29.9, Lng -102.9-100.1
            ELSIF latitud BETWEEN 24.0 AND 29.9 AND longitud BETWEEN -102.9 AND -100.1 THEN
                RETURN 'Coahuila';
                
            -- TAMAULIPAS: Centro frontera: 25.9, -97.5
            -- Rango aproximado: Lat 22.2-27.7, Lng -99.5-97.1
            ELSIF latitud BETWEEN 22.2 AND 27.7 AND longitud BETWEEN -99.5 AND -97.1 THEN
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
                
            -- CHIHUAHUA: Centro: 28.6, -106.1
            -- Rango aproximado: Lat 25.5-31.8, Lng -109.1-103.3
            ELSIF latitud BETWEEN 25.5 AND 31.8 AND longitud BETWEEN -109.1 AND -103.3 THEN
                RETURN 'Chihuahua';
                
            -- SONORA: Centro: 29.1, -110.9
            -- Rango aproximado: Lat 26.0-32.5, Lng -115.0-108.4
            ELSIF latitud BETWEEN 26.0 AND 32.5 AND longitud BETWEEN -115.0 AND -108.4 THEN
                RETURN 'Sonora';
                
            -- BAJA CALIFORNIA: Centro Tijuana: 32.5, -117.0
            -- Rango aproximado: Lat 32.0-32.7, Lng -117.5-116.0
            ELSIF latitud BETWEEN 32.0 AND 32.7 AND longitud BETWEEN -117.5 AND -116.0 THEN
                RETURN 'Baja California';
                
            -- Default para coordenadas fuera de rango conocido
            ELSE 
                RETURN 'Otro Estado';
            END IF;
        END;
        $$ LANGUAGE plpgsql;
    `;

    try {
        await pool.query(functionSQL);
        console.log('✅ Función classify_estado_by_coordinates() creada');
        return true;
    } catch (error) {
        console.error('❌ Error creando función:', error.message);
        return false;
    }
}

// Verificar distribución actual
async function checkCurrentDistribution() {
    try {
        const result = await pool.query(`
            SELECT 
                estado,
                grupo_operativo,
                COUNT(*) as sucursales
            FROM sucursales 
            WHERE grupo_operativo IS NOT NULL
            GROUP BY estado, grupo_operativo 
            ORDER BY estado, sucursales DESC
        `);

        console.log('\n📊 DISTRIBUCIÓN ACTUAL:');
        console.log('='.repeat(60));
        result.rows.forEach(row => {
            console.log(`${row.estado.padEnd(20)} | ${row.grupo_operativo.padEnd(30)} | ${row.sucursales} sucursales`);
        });

        return result.rows;
    } catch (error) {
        console.error('❌ Error checking distribution:', error.message);
        return [];
    }
}

// Previsualizar nuevos estados basado en coordenadas
async function previewNewStates() {
    try {
        const result = await pool.query(`
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
        `);

        console.log('\n🔍 PREVISUALIZACIÓN CAMBIOS POR COORDENADAS GPS:');
        console.log('='.repeat(100));
        let cambios = 0;
        result.rows.forEach(row => {
            const cambio = row.estado_actual !== row.estado_nuevo ? '🔄 CAMBIO' : '✅ SIN CAMBIO';
            if (row.estado_actual !== row.estado_nuevo) cambios++;
            console.log(`${row.nombre.padEnd(25)} | ${row.grupo_operativo.padEnd(25)} | ${row.estado_actual.padEnd(12)} → ${row.estado_nuevo.padEnd(12)} | ${row.latitud.toFixed(4)}, ${row.longitud.toFixed(4)} | ${cambio}`);
        });

        console.log(`\n📊 RESUMEN: ${cambios} sucursales cambiarán de estado`);

        // Resumen por estado nuevo
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

        console.log('\n📋 DISTRIBUCIÓN POR ESTADO (NUEVA):');
        summary.rows.forEach(row => {
            console.log(`${row.estado_nuevo.padEnd(15)} | ${row.sucursales} sucursales | ${row.grupos} grupos`);
        });

        return result.rows;
    } catch (error) {
        console.error('❌ Error previewing changes:', error.message);
        return [];
    }
}

// Actualizar estados basado en coordenadas GPS
async function updateEstados() {
    try {
        const result = await pool.query(`
            UPDATE sucursales 
            SET estado = classify_estado_by_coordinates(latitud, longitud)
            WHERE latitud IS NOT NULL AND longitud IS NOT NULL
            RETURNING id, nombre, grupo_operativo, estado, latitud, longitud
        `);

        console.log(`\n✅ ACTUALIZACIÓN COMPLETADA: ${result.rowCount} sucursales actualizadas por coordenadas GPS`);
        return result.rowCount;
    } catch (error) {
        console.error('❌ Error updating estados:', error.message);
        return 0;
    }
}

// Verificar resultado final
async function checkFinalDistribution() {
    try {
        const result = await pool.query(`
            SELECT 
                estado,
                COUNT(DISTINCT grupo_operativo) as grupos,
                COUNT(*) as sucursales,
                ARRAY_AGG(DISTINCT grupo_operativo ORDER BY grupo_operativo) as grupos_list
            FROM sucursales 
            WHERE grupo_operativo IS NOT NULL
            GROUP BY estado 
            ORDER BY sucursales DESC
        `);

        console.log('\n🎯 DISTRIBUCIÓN FINAL:');
        console.log('='.repeat(60));
        result.rows.forEach(row => {
            console.log(`${row.estado.padEnd(15)} | ${row.grupos} grupos | ${row.sucursales} sucursales`);
            console.log(`   Grupos: ${row.grupos_list.join(', ')}`);
            console.log('');
        });

        return result.rows;
    } catch (error) {
        console.error('❌ Error checking final distribution:', error.message);
        return [];
    }
}

// Verificar clasificación territorial
async function checkTerritorialClassification() {
    try {
        const result = await pool.query(`
            SELECT 
                estado,
                CASE 
                    WHEN estado = 'Nuevo León' THEN 'LOCAL'
                    WHEN grupo_operativo = 'GRUPO SALTILLO' THEN 'LOCAL'  
                    ELSE 'FORANEA'
                END as tipo_territorial,
                COUNT(DISTINCT grupo_operativo) as grupos,
                COUNT(*) as sucursales
            FROM sucursales 
            WHERE grupo_operativo IS NOT NULL
            GROUP BY estado, 
                CASE 
                    WHEN estado = 'Nuevo León' THEN 'LOCAL'
                    WHEN grupo_operativo = 'GRUPO SALTILLO' THEN 'LOCAL'  
                    ELSE 'FORANEA'
                END
            ORDER BY tipo_territorial, sucursales DESC
        `);

        console.log('\n🏢 CLASIFICACIÓN TERRITORIAL:');
        console.log('='.repeat(60));
        result.rows.forEach(row => {
            console.log(`${row.tipo_territorial.padEnd(10)} | ${row.estado.padEnd(15)} | ${row.grupos} grupos | ${row.sucursales} sucursales`);
        });

        return result.rows;
    } catch (error) {
        console.error('❌ Error checking territorial classification:', error.message);
        return [];
    }
}

// Función principal
async function main() {
    console.log('🗺️  NORMALIZACIÓN DE ESTADOS POR COORDENADAS GPS - EL POLLO LOCO MÉXICO');
    console.log('='.repeat(80));
    console.log('Roberto: Usando coordenadas GPS para determinar estados reales');
    console.log('='.repeat(80));

    try {
        // 1. Crear función clasificadora
        console.log('\n1️⃣ Creando función clasificadora...');
        const functionCreated = await createEstadoClassifierFunction();
        if (!functionCreated) {
            console.log('❌ No se pudo crear la función. Abortando.');
            return;
        }

        // 2. Verificar distribución actual
        console.log('\n2️⃣ Verificando distribución actual...');
        await checkCurrentDistribution();

        // 3. Previsualizar cambios
        console.log('\n3️⃣ Previsualizando cambios...');
        const changes = await previewNewStates();

        // 4. Confirmar cambios (automático para Roberto)
        console.log('\n4️⃣ Aplicando cambios...');
        const updatedCount = await updateEstados();

        if (updatedCount > 0) {
            // 5. Verificar resultado
            console.log('\n5️⃣ Verificando resultado...');
            await checkFinalDistribution();

            // 6. Verificar clasificación territorial
            console.log('\n6️⃣ Verificando clasificación territorial...');
            await checkTerritorialClassification();

            console.log('\n🎉 NORMALIZACIÓN COMPLETADA EXITOSAMENTE');
            console.log('='.repeat(60));
            console.log(`✅ ${updatedCount} sucursales actualizadas`);
            console.log('✅ Estados normalizados por grupos operativos');
            console.log('✅ Clasificación territorial actualizada');
            console.log('✅ Filtros por estado ahora funcionarán correctamente');
        }

    } catch (error) {
        console.error('❌ Error en normalización:', error.message);
    } finally {
        await pool.end();
    }
}

if (require.main === module) {
    main();
}

module.exports = { 
    createEstadoClassifierFunction, 
    checkCurrentDistribution, 
    updateEstados,
    checkFinalDistribution 
};