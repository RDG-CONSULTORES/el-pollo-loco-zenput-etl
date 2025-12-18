# 🚀 GUÍA DE DESPLIEGUE - EL POLLO LOCO ZENPUT ETL

## 📋 RESUMEN DEL SISTEMA

### ✅ LO QUE YA FUNCIONA (100%)
- **API Zenput:** ✅ 86 sucursales + 5 formularios funcionando
- **Extracción diaria:** ✅ 100 submissions por día (20 por formulario)
- **Auto-detección:** ✅ Nuevas sucursales y alertas
- **Estructura organizada:** ✅ Proyecto completamente estructurado

### 🔧 LO QUE FALTA CONFIGURAR
- **PostgreSQL Railway:** Base de datos productiva
- **WhatsApp Alertas:** Configuración Twilio + teléfonos directores
- **Cron Jobs:** Automatización diaria/semanal
- **Monitoreo:** Dashboard básico

---

## 🎯 FUNCIONALIDAD PRINCIPAL

### ETL Core (Lo importante)
El sistema está **ENFOCADO** en extraer **submissions diarias** de los 5 formularios:

```python
# ✅ FUNCIONANDO HOY
daily_data = client.get_daily_submissions()
# Resultado: 100 submissions/día (20 por formulario)
```

### Auto-detección Ligera (No pesada)
- **Semanal:** Verificar nuevas sucursales (87, 88, 89...)  
- **Alertas:** Sucursales que no reportan >3 días
- **NO hacer:** Cambios de directores (manual)

---

## 📁 ESTRUCTURA FINAL DEL PROYECTO

```
el-pollo-loco-zenput-etl/
├── README.md                           # ✅ Documentación principal
├── src/
│   ├── zenput_api.py                   # ✅ Cliente API (100% funcional)
│   ├── database.py                     # 🔧 Pendiente: PostgreSQL
│   └── alerts.py                       # 🔧 Pendiente: WhatsApp
├── data/
│   ├── 86_sucursales_master.csv        # ✅ Maestro actualizado
│   └── zenput_api_complete_data.json   # ✅ Datos completos API
├── config/
│   └── settings.py                     # ✅ Configuraciones
├── scripts/
│   ├── daily_etl.py                    # ✅ ETL diario (funcional)
│   └── weekly_check.py                 # ✅ Verificaciones semanales
├── docs/
│   └── DEPLOYMENT_GUIDE.md             # ✅ Esta guía
└── tests/                              # 🔧 Pendiente
```

---

## 🚀 PASOS PARA DESPLIEGUE PRODUCTIVO

### Paso 1: Configurar Railway PostgreSQL (1 día)

```bash
# 1. Crear proyecto Railway
# 2. Agregar PostgreSQL
# 3. Obtener credenciales
# 4. Configurar variables de entorno:

export RAILWAY_DB_HOST="xxx.railway.app"
export RAILWAY_DB_PORT="5432"
export RAILWAY_DB_NAME="railway"
export RAILWAY_DB_USER="postgres"  
export RAILWAY_DB_PASSWORD="xxx"
```

### Paso 2: Configurar WhatsApp Alertas (1 día)

```bash
# 1. Configurar cuenta Twilio
# 2. Obtener credenciales WhatsApp
# 3. Configurar teléfonos de 21 directores

export TWILIO_SID="xxx"
export TWILIO_TOKEN="xxx"
export TWILIO_WHATSAPP="+14155238886"
```

### Paso 3: Automatización con Cron (30 min)

```bash
# Editar crontab
crontab -e

# ETL diario 6:00 AM
0 6 * * * cd /path/to/el-pollo-loco-zenput-etl && python3 scripts/daily_etl.py

# Verificación semanal domingos 8:00 AM  
0 8 * * 0 cd /path/to/el-pollo-loco-zenput-etl && python3 scripts/weekly_check.py
```

### Paso 4: Testing Final (2 horas)

```bash
# Probar ETL completo
cd el-pollo-loco-zenput-etl
python3 scripts/daily_etl.py

# Probar verificación semanal
python3 scripts/weekly_check.py

# Validar que todo funcione
python3 src/zenput_api.py
```

---

## 📊 DATOS CONFIRMADOS Y VALIDADOS

### ✅ 86 Sucursales (100% Completas)
- **Rango:** 1-86 sin faltantes
- **Miguel de la Madrid:** ✅ ID 2261286, Guadalupe, NL
- **Coordenadas:** ✅ Sincronizadas API vs CSV

### ✅ 5 Formularios (100% Funcionales)
- **877138:** Supervisión Operativa ✅ 20 submissions/día
- **877139:** Control Seguridad ✅ 20 submissions/día
- **877140:** Apertura ✅ 20 submissions/día  
- **877141:** Entrega Turno ✅ 20 submissions/día
- **877142:** Cierre ✅ 20 submissions/día

### ✅ 20 Grupos Operativos (Jerarquía Completa)
- **El Pollo Loco México** → 20 grupos
  - Miguel de la Madrid bajo **Lourdes Azuara**

---

## 🔄 FRECUENCIAS OPERATIVAS

| Proceso | Frecuencia | Horario | Crítico |
|---------|------------|---------|---------|
| **ETL Submissions** | Diario | 6:00 AM | ✅ Sí |
| **Verificación Estructura** | Semanal | Dom 8:00 AM | ⚠️ Media |
| **Alertas Inactivas** | Tiempo real | Continuo | ✅ Sí |
| **Backup Datos** | Diario | 11:59 PM | ✅ Sí |
| **Reporte Ejecutivo** | Lunes | 9:00 AM | ⚠️ Media |

---

## 🎯 MÉTRICAS DE ÉXITO

### KPIs Operativos
- **Submissions diarias:** >80 (de 100 máximas)
- **Sucursales activas:** >81 (de 86 total)  
- **Formularios reportando:** 5/5
- **Uptime ETL:** >99%

### Alertas Automáticas
- **Crítica:** <50 submissions totales en día
- **Media:** >5 sucursales sin reportar
- **Info:** Nueva sucursal detectada

---

## 📞 CONTACTOS Y SOPORTE

### Contactos Clave
- **Director Operaciones:** Eduardo Martínez (emartinez@epl.mx)
- **Desarrollador:** Roberto Dávila (robertodavilag@gmail.com)

### Escalación de Problemas
1. **Crítico:** ETL no funciona → Llamar Roberto inmediatamente  
2. **Medio:** Pocas submissions → Email a Eduardo/Roberto
3. **Info:** Nueva sucursal → Email semanal a equipo

---

## 🛠️ TROUBLESHOOTING

### Problemas Comunes

**ETL no extrae datos:**
```bash
# 1. Verificar conexión API
cd el-pollo-loco-zenput-etl
python3 -c "from src.zenput_api import create_zenput_client; print(create_zenput_client().validate_api_connection())"

# 2. Si falla, verificar token API
```

**Pocas submissions:**
- Normal: Fines de semana o feriados
- Revisar: Si es >3 días consecutivos

**Nueva sucursal detectada:**
- Actualizar settings.py TOTAL_LOCATIONS
- Actualizar CSV maestro
- Notificar a equipo

---

## ✅ CHECKLIST DESPLIEGUE

### Pre-requisitos
- [ ] Servidor con Python 3.8+
- [ ] Acceso a internet para API Zenput
- [ ] Credenciales Railway PostgreSQL
- [ ] Credenciales Twilio WhatsApp

### Configuración
- [ ] Variables de entorno configuradas
- [ ] Cron jobs programados  
- [ ] Tests ejecutados exitosamente
- [ ] Monitoreo básico configurado

### Validación
- [ ] ETL diario ejecuta sin errores
- [ ] 100 submissions extraídas por día
- [ ] Alertas funcionando
- [ ] Datos guardándose correctamente

---

**🎉 Una vez completado esto, tendrás un sistema ETL 100% funcional extrayendo datos diarios de las 86 sucursales automáticamente.**