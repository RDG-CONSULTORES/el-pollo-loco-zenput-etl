
# 🚀 INSTRUCCIONES RAILWAY POSTGRESQL - EL POLLO LOCO MÉXICO

## 1. CREAR PROYECTO RAILWAY

1. Ve a https://railway.app
2. Crea cuenta o inicia sesión
3. **New Project** → **Provision PostgreSQL**
4. Nombra el proyecto: `el-pollo-loco-zenput-etl`

## 2. OBTENER CREDENCIALES

Una vez creada la base de datos PostgreSQL:

1. Ve a tu proyecto en Railway
2. Click en la base de datos PostgreSQL
3. Ve a **Connect** tab
4. Copia las credenciales:

```
HOST: xxxxx.railway.app
PORT: 5432  
DATABASE: railway
USER: postgres
PASSWORD: [tu-password-aquí]
```

## 3. CONFIGURAR ESQUEMA

### Opción A: Usando Railway GUI
1. En Railway, ve a **Data** tab
2. Copia y pega el contenido del archivo `railway_schema_final_[timestamp].sql`
3. Click **Execute**

### Opción B: Usando psql (Terminal)
```bash
psql postgresql://postgres:[password]@[host]:5432/railway -f railway_schema_final_[timestamp].sql
```

## 4. CARGAR DATOS INICIALES

1. Edita el archivo `railway_load_data_[timestamp].py`
2. Actualiza las credenciales Railway:
   ```python
   railway_config = {
       'host': 'TU_HOST_RAILWAY.railway.app',  # Cambiar
       'port': 5432,
       'database': 'railway',
       'user': 'postgres',
       'password': 'TU_PASSWORD_RAILWAY'  # Cambiar
   }
   ```
3. Ejecuta el script:
   ```bash
   python3 railway_load_data_[timestamp].py
   ```

## 5. VERIFICAR INSTALACIÓN

Después de ejecutar, deberías tener:

### Datos Cargados:
- ✅ **20 grupos operativos** (TEPEYAC, EXPO, OGAS, etc.)
- ✅ **86 sucursales** con GPS coordinates
- ✅ **6 períodos 2025** (T1-T4, S1-S2)
- ✅ **43 áreas supervisión** (31 operativa + 12 seguridad)
- ✅ **20 usuarios Zenput** con teams asignados

### Consultas de Verificación:
```sql
-- Verificar grupos operativos
SELECT nombre, total_sucursales, director_email FROM grupos_operativos ORDER BY total_sucursales DESC;

-- Verificar sucursales por estado  
SELECT estado, COUNT(*) FROM sucursales_master GROUP BY estado ORDER BY COUNT(*) DESC;

-- Verificar períodos 2025
SELECT codigo, descripcion, fecha_inicio, fecha_fin FROM periodos_supervision ORDER BY fecha_inicio;

-- Verificar áreas de supervisión
SELECT form_id, COUNT(*) as areas FROM supervision_areas GROUP BY form_id;

-- Verificar usuarios con teams
SELECT email, default_team_name, JSON_ARRAY_LENGTH(teams) as num_teams FROM usuarios_zenput;
```

## 6. PRÓXIMOS PASOS

Una vez verificada la instalación:

1. **✅ Base Railway lista** 
2. **📊 Implementar ETL supervisiones** - Extraer 238+ supervisiones existentes
3. **📈 Dashboard básico** - Vistas por grupo operativo
4. **🔄 ETL automático** - Programar extracción diaria/semanal
5. **📱 Alertas WhatsApp** - Sistema de notificaciones Twilio

## 7. TROUBLESHOOTING

### Error de conexión:
- Verificar credenciales Railway
- Asegurar que Railway PostgreSQL esté corriendo
- Verificar firewall/VPN

### Error de permisos:
- El usuario `postgres` debe tener permisos completos
- Railway maneja permisos automáticamente

### Datos faltantes:
- Verificar que archivos JSON/CSV estén en directorio `data/`
- Re-ejecutar scripts de extracción si es necesario

## 8. ESTRUCTURA FINAL

```
Grupos Operativos (20)
    └── Sucursales (86) 
        └── Supervisiones (877138 + 877139)
            └── Áreas (43 total)
                └── Respuestas detalladas
                
Usuarios Zenput (20)
    └── Teams asignados
    └── Sucursales owned_locations
```

## 🎯 RESULTADO ESPERADO

Dashboard operativo con:
- **Vista por grupo operativo** - Performance por director
- **Vista por sucursal** - Histórico supervisiones  
- **Vista por período** - T1-T4, S1-S2 comparativos
- **Vista por área** - Identificar áreas problemáticas
- **Alertas automáticas** - WhatsApp para supervisiones pendientes

---

**Roberto**: Una vez tengas Railway funcionando, continúamos con la implementación del ETL completo para extraer las 238+ supervisiones existentes.
