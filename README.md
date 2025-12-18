# 🍗 EL POLLO LOCO MÉXICO - ZENPUT ETL SYSTEM

Sistema ETL para extracción automatizada de datos de formularios Zenput.

## 📋 OBJETIVO PRINCIPAL

Extraer **submissions diarias** de 5 formularios críticos de las 86 sucursales para análisis operativo.

## 🎯 FUNCIONALIDADES

### Core ETL (Diario)
- **877138:** Supervisión Operativa EPL CAS
- **877139:** Control Operativo de Seguridad EPL CAS  
- **877140:** Apertura EPL CAS
- **877141:** Entrega de Turno EPL CAS
- **877142:** Cierre EPL CAS

### Auto-detección (Semanal)
- Nuevas sucursales (87, 88, 89...)
- Nuevos formularios (877143, 877144...)
- Alertas de sucursales inactivas

## 🏗️ ESTRUCTURA DEL PROYECTO

```
el-pollo-loco-zenput-etl/
├── src/                    # Código fuente principal
│   ├── etl_core.py        # ETL principal 
│   ├── zenput_api.py      # Cliente API Zenput
│   ├── database.py        # Conexión PostgreSQL
│   └── alerts.py          # Sistema alertas WhatsApp
├── data/                  # Datos base
│   ├── 86_sucursales.csv  # Maestro de sucursales
│   └── forms_config.json  # Configuración formularios
├── config/                # Configuraciones
│   ├── settings.py        # Variables entorno
│   └── forms_mapping.py   # Mapeo de campos
├── scripts/               # Scripts utilidad
│   ├── daily_etl.py       # ETL diario
│   └── weekly_check.py    # Verificaciones semanales
├── docs/                  # Documentación
└── tests/                 # Pruebas
```

## 📊 DATOS CONFIRMADOS

### ✅ 86 Sucursales (Completas)
- **Rango:** 1-86 sin faltantes
- **Miguel de la Madrid:** ID 2261286, Guadalupe, NL
- **Coordenadas:** Validadas y sincronizadas

### ✅ 20 Grupos Operativos  
- Estructura jerárquica completa
- Directores identificados
- Asignación de sucursales confirmada

### ✅ 5 Formularios Activos
- Todos los Form IDs funcionando
- 20 submissions por formulario disponibles
- Estructura de campos identificada

## 🔄 FRECUENCIAS RECOMENDADAS

- **ETL Principal:** Diario 6:00 AM
- **Auto-detección:** Semanal domingo
- **Alertas:** Tiempo real
- **Backup:** Diario
- **Reportes:** Lunes ejecutivo

## 🚀 PRÓXIMOS PASOS

1. Mover archivos existentes a estructura organizada
2. Implementar ETL core enfocado en submissions
3. Configurar Railway PostgreSQL
4. Configurar alertas WhatsApp
5. Desplegar sistema productivo

## 📞 CONTACTOS

- **Director Operaciones:** Eduardo Martínez (emartinez@epl.mx)
- **Desarrollador:** Roberto Dávila (robertodavilag@gmail.com)

---

**Última actualización:** 17 Diciembre 2025  
**Estado:** ✅ 86 sucursales encontradas, listo para implementación