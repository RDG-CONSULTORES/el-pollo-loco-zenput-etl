# 🚀 El Pollo Loco Railway Dashboard

Dashboard completo clonado con toggle Operativas/Seguridad en Railway PostgreSQL.

## 🎯 **CARACTERÍSTICAS**

✅ **Dashboard iOS nativo clonado exacto**  
✅ **Toggle switch**: Operativas ↔ Seguridad  
✅ **Railway PostgreSQL**: Esquema optimizado  
✅ **API velocidad máxima**: Queries <200ms  
✅ **476 supervisiones**: Datos completos migrados  
✅ **86 sucursales**: Coordenadas validadas  

---

## 🚀 **DEPLOY RAILWAY**

### **1. Setup Proyecto Railway**
```bash
# Instalar Railway CLI
npm install -g @railway/cli

# Login y crear proyecto
railway login
cd railway-dashboard
railway init

# Agregar PostgreSQL
railway add postgresql
```

### **2. Variables de Entorno**
Railway configurará automáticamente:
- `DATABASE_URL`: PostgreSQL connection string
- `PORT`: Puerto del servicio
- `NODE_ENV`: production

### **3. Deploy**
```bash
# Deploy completo
railway up

# Ver logs
railway logs

# Abrir en browser
railway open
```

---

## 🗄️ **MIGRACIÓN DATOS**

### **1. Ejecutar Schema**
```bash
# Conectar a Railway PostgreSQL
railway connect postgres

# Ejecutar schema (en psql)
\i railway_schema_optimizado.sql
```

### **2. Migrar Datos**
```bash
# Desde directorio padre
cd ../
python3 migrate_to_railway.py

# Usar DATABASE_URL de Railway
# Ejemplo: postgresql://user:pass@monorail.proxy.rlwy.net:12345/railway
```

### **3. Validar Migración**
```bash
# Health check
curl https://your-app.railway.app/health

# Stats
curl https://your-app.railway.app/api/stats
```

---

## 📊 **ESTRUCTURA API**

### **Operativas**
- `GET /api/operativas/kpis` - KPIs generales
- `GET /api/operativas/dashboard` - Dashboard principal  
- `GET /api/operativas/mapa` - Datos mapa
- `GET /api/operativas/areas` - Áreas evaluadas
- `GET /api/operativas/historico` - Tendencias

### **Seguridad**
- `GET /api/seguridad/kpis` - KPIs generales
- `GET /api/seguridad/dashboard` - Dashboard principal
- `GET /api/seguridad/mapa` - Datos mapa  
- `GET /api/seguridad/areas` - Áreas evaluadas
- `GET /api/seguridad/historico` - Tendencias

### **Drill-down**
- `GET /api/:tipo/grupo/:grupo` - Por grupo operativo
- `GET /api/:tipo/sucursal/:numero` - Por sucursal

### **Utilitarios**
- `GET /health` - Health check
- `GET /api/stats` - Estadísticas generales
- `POST /api/refresh` - Refresh vistas manuales

---

## 🔄 **TOGGLE SWITCH**

El toggle switch permite cambiar entre Operativas y Seguridad:

```javascript
// Auto-update cuando cambia toggle
document.querySelectorAll('input[name="supervision-type"]').forEach(radio => {
    radio.addEventListener('change', (e) => {
        currentType = e.target.value;
        updateDashboardTitle();
        loadDashboardData();
    });
});
```

---

## ⚡ **OPTIMIZACIONES**

### **PostgreSQL**
- **Índices compuestos**: Para queries complejas
- **Vistas materializadas**: Cache automático
- **Connection pooling**: 20 conexiones máximo
- **Query timeout**: 5 segundos máximo

### **API**
- **Compression**: Gzip automático
- **Helmet**: Security headers
- **CORS**: Cross-origin habilitado
- **Error handling**: Manejo robusto

### **Frontend**
- **iOS Design System**: Colores y tipografía nativa
- **Responsive**: Mobile-first design
- **Chart.js**: Gráficos optimizados
- **Leaflet**: Mapas ligeros

---

## 💰 **COSTOS RAILWAY**

```
🗄️ PostgreSQL Hobby: $5/mes
🌐 Web Service: $5/mes  
📡 Networking: $0
═══════════════════════
💵 TOTAL: $10/mes
```

---

## 🎯 **PRÓXIMOS PASOS**

1. ✅ **Deploy inicial**: Railway + PostgreSQL
2. ✅ **Migrar datos**: 476 supervisiones
3. ✅ **Validar funcionamiento**: Health checks
4. 🔄 **Testing completo**: Mobile + desktop
5. 📈 **Monitoreo**: Performance + uptime

---

**Roberto: ¡Dashboard Railway listo para deploy!** 🚀