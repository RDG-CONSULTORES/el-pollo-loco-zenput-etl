#!/usr/bin/env node

/**
 * 🗺️ ANÁLISIS Y CORRECCIÓN GPS - EL POLLO LOCO MÉXICO
 * Roberto: Vamos a revisar las coordenadas reales y normalizar correctamente
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
    console.log('🗺️  ANÁLISIS GPS COORDENADAS - EL POLLO LOCO MÉXICO');
    console.log('='.repeat(80));
    console.log('Roberto: Revisando coordenadas reales para normalizar correctamente');
    console.log('='.repeat(80));

    try {
        // 1. Obtener todas las sucursales con coordenadas
        console.log('\n1️⃣ Obteniendo todas las sucursales...');
        const mapaUrl = 'https://el-pollo-loco-zenput-etl-production.up.railway.app/api/mapa';
        const sucursales = await makeRequest(mapaUrl);
        
        console.log(`✅ ${sucursales.length} sucursales obtenidas`);

        // 2. Agrupar por grupos operativos para análisis
        console.log('\n2️⃣ Analizando por grupos operativos...');
        const porGrupo = {};
        sucursales.forEach(suc => {
            if (!porGrupo[suc.grupo_operativo]) {
                porGrupo[suc.grupo_operativo] = [];
            }
            porGrupo[suc.grupo_operativo].push(suc);
        });

        // 3. Mostrar coordenadas por grupo
        console.log('\n3️⃣ Coordenadas por grupo operativo:');
        console.log('='.repeat(100));
        
        Object.keys(porGrupo).sort().forEach(grupo => {
            const sucursales = porGrupo[grupo];
            const estadoActual = sucursales[0].estado;
            
            console.log(`\n📍 ${grupo} (${sucursales.length} sucursales) - Estado actual: ${estadoActual}`);
            
            sucursales.forEach(suc => {
                console.log(`   ${suc.sucursal_nombre.padEnd(25)} | Lat: ${suc.latitud.toFixed(6).padStart(10)} | Lng: ${suc.longitud.toFixed(6).padStart(12)} | ${suc.estado}`);
            });

            // Calcular centro del grupo
            const lats = sucursales.map(s => s.latitud);
            const lngs = sucursales.map(s => s.longitud);
            const centroLat = lats.reduce((a, b) => a + b, 0) / lats.length;
            const centroLng = lngs.reduce((a, b) => a + b, 0) / lngs.length;
            const minLat = Math.min(...lats);
            const maxLat = Math.max(...lats);
            const minLng = Math.min(...lngs);
            const maxLng = Math.max(...lngs);
            
            console.log(`   📊 Centro: ${centroLat.toFixed(4)}, ${centroLng.toFixed(4)}`);
            console.log(`   📏 Rango: Lat ${minLat.toFixed(4)}-${maxLat.toFixed(4)}, Lng ${minLng.toFixed(4)}-${maxLng.toFixed(4)}`);
        });

        // 4. Análisis específico de grupos problemáticos
        console.log('\n4️⃣ Análisis específico de GRUPO SALTILLO:');
        console.log('='.repeat(60));
        
        if (porGrupo['GRUPO SALTILLO']) {
            const saltillo = porGrupo['GRUPO SALTILLO'];
            console.log(`📍 GRUPO SALTILLO tiene ${saltillo.length} sucursales:`);
            saltillo.forEach(suc => {
                console.log(`   ${suc.sucursal_nombre.padEnd(25)} | ${suc.latitud.toFixed(6)}, ${suc.longitud.toFixed(6)} | Estado: ${suc.estado}`);
            });

            // Verificar si están en rango correcto para Coahuila
            console.log('\n🔍 Verificación para Coahuila:');
            console.log('   Las coordenadas de Saltillo deberían estar en Coahuila');
            console.log('   Saltillo real: ~25.4232, -101.0');
            console.log('   Rango actual Coahuila: Lat 24.0-29.9, Lng -102.9--100.1');
            
            saltillo.forEach(suc => {
                const enCoahuila = (suc.latitud >= 24.0 && suc.latitud <= 29.9) && 
                                (suc.longitud >= -102.9 && suc.longitud <= -100.1);
                console.log(`   ${suc.sucursal_nombre}: ${enCoahuila ? '✅ SÍ' : '❌ NO'} está en rango Coahuila`);
            });
        }

        // 5. Sugerir rangos GPS corregidos
        console.log('\n5️⃣ Sugerencias de rangos GPS corregidos:');
        console.log('='.repeat(60));
        
        console.log('🔧 NUEVO LEÓN (Monterrey y área metropolitana):');
        console.log('   Rango sugerido: Lat 25.5-25.9, Lng -100.5--100.1');
        console.log('   Incluye: Monterrey, Guadalupe, San Nicolás, Escobedo');

        console.log('\n🔧 COAHUILA (Saltillo y área):');
        console.log('   Rango sugerido: Lat 25.2-25.7, Lng -101.2--100.8');
        console.log('   Incluye: Saltillo, Ramos Arizpe, área metropolitana');

        console.log('\n🔧 TAMAULIPAS (Frontera norte):');
        console.log('   Rango sugerido: Lat 22.2-27.7, Lng -99.5--97.1');
        console.log('   Incluye: Matamoros, Reynosa, Nuevo Laredo, Tampico');

        console.log('\n💡 RECOMENDACIÓN ROBERTO:');
        console.log('   1. Primero ajustar rangos GPS para Saltillo → Coahuila');
        console.log('   2. Verificar Tamaulipas manualmente por nombres de ciudades');
        console.log('   3. Re-ejecutar normalización con rangos corregidos');
        console.log('   4. Validar resultado manualmente');

        console.log('\n📋 ¿Procedemos con la corrección Roberto?');

    } catch (error) {
        console.error('\n❌ Error analizando coordenadas:', error.message);
    }
}

if (require.main === module) {
    main();
}