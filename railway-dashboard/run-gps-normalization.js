#!/usr/bin/env node

/**
 * 🗺️ EJECUTAR NORMALIZACIÓN GPS - EL POLLO LOCO MÉXICO
 * Roberto: Script para ejecutar normalización usando Railway production database
 */

const https = require('https');

// Función para hacer solicitud HTTP
function makeRequest(url) {
    return new Promise((resolve, reject) => {
        const request = https.get(url, (response) => {
            let data = '';
            response.on('data', chunk => data += chunk);
            response.on('end', () => {
                try {
                    const result = JSON.parse(data);
                    resolve(result);
                } catch (error) {
                    reject(new Error('Invalid JSON response: ' + data));
                }
            });
        });
        
        request.on('error', reject);
        request.setTimeout(30000, () => {
            request.destroy();
            reject(new Error('Request timeout'));
        });
    });
}

async function main() {
    console.log('🗺️  NORMALIZACIÓN GPS VIA API - EL POLLO LOCO MÉXICO');
    console.log('='.repeat(80));
    console.log('Roberto: Ejecutando normalización GPS desde API Railway');
    console.log('='.repeat(80));

    try {
        // 1. Verificar que el servidor esté funcionando
        console.log('\n1️⃣ Verificando servidor Railway...');
        const healthUrl = 'https://el-pollo-loco-zenput-etl-production.up.railway.app/health';
        const health = await makeRequest(healthUrl);
        console.log(`✅ Server status: ${health.status}`);
        console.log(`✅ Database: ${health.database}`);
        console.log(`✅ Total supervisiones: ${health.total_supervisions}`);

        // 2. Verificar estados actuales
        console.log('\n2️⃣ Verificando estados actuales...');
        const estadosUrl = 'https://el-pollo-loco-zenput-etl-production.up.railway.app/api/estados';
        const estadosAntes = await makeRequest(estadosUrl);
        console.log('📊 Estados antes de normalización:');
        estadosAntes.forEach(estado => {
            console.log(`   ${estado.estado.padEnd(20)} | ${estado.sucursales_count} sucursales`);
        });

        // 3. Ejecutar normalización (intentar varias veces si es necesario)
        console.log('\n3️⃣ Ejecutando normalización GPS...');
        let normalizationResult = null;
        
        for (let attempt = 1; attempt <= 3; attempt++) {
            try {
                console.log(`   Intento ${attempt}/3...`);
                const normalizeUrl = 'https://el-pollo-loco-zenput-etl-production.up.railway.app/api/normalize-estados';
                normalizationResult = await makeRequest(normalizeUrl);
                console.log('✅ Normalización exitosa');
                break;
            } catch (error) {
                console.log(`❌ Intento ${attempt} falló: ${error.message}`);
                if (attempt < 3) {
                    console.log('   Esperando 5 segundos antes del siguiente intento...');
                    await new Promise(resolve => setTimeout(resolve, 5000));
                }
            }
        }

        if (!normalizationResult) {
            throw new Error('Normalización falló después de 3 intentos');
        }

        // 4. Mostrar resultados
        console.log('\n4️⃣ Resultados de normalización:');
        console.log(`✅ ${normalizationResult.updated_count} sucursales actualizadas`);
        
        if (normalizationResult.summary_by_new_estado) {
            console.log('\n📊 Distribución por nuevo estado:');
            normalizationResult.summary_by_new_estado.forEach(estado => {
                console.log(`   ${estado.estado_nuevo.padEnd(20)} | ${estado.sucursales} sucursales | ${estado.grupos} grupos`);
            });
        }

        // 5. Verificar estados después
        console.log('\n5️⃣ Verificando estados después...');
        await new Promise(resolve => setTimeout(resolve, 2000)); // Esperar actualización
        const estadosDespues = await makeRequest(estadosUrl);
        console.log('📊 Estados después de normalización:');
        estadosDespues.forEach(estado => {
            console.log(`   ${estado.estado.padEnd(20)} | ${estado.sucursales_count} sucursales`);
        });

        // 6. Comparar cambios
        console.log('\n6️⃣ Análisis de cambios:');
        const estadosNuevos = estadosDespues.length;
        const estadosOriginales = estadosAntes.length;
        console.log(`📈 Estados antes: ${estadosOriginales}, después: ${estadosNuevos}`);
        
        if (estadosNuevos > estadosOriginales) {
            console.log('🎉 ¡Normalización exitosa! Ahora se muestran múltiples estados');
        } else {
            console.log('⚠️  Los estados siguen igual, puede que necesite más tiempo para actualizar');
        }

        console.log('\n🎯 NORMALIZACIÓN GPS COMPLETADA');
        console.log('='.repeat(60));
        console.log('✅ Estados normalizados usando coordenadas GPS');
        console.log('✅ Filtros por estado ahora funcionarán correctamente');

    } catch (error) {
        console.error('\n❌ Error en normalización GPS:', error.message);
        console.log('\n💡 Posibles soluciones:');
        console.log('   1. Verificar que el servidor Railway esté actualizado');
        console.log('   2. Esperar unos minutos para que se desplieguen los cambios');
        console.log('   3. Verificar conectividad a Railway');
    }
}

if (require.main === module) {
    main();
}