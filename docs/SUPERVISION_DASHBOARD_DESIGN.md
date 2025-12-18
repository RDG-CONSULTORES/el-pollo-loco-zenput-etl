# 📊 DISEÑO DE DASHBOARD - SUPERVISIONES EPL MÉXICO

Basado en análisis de contenido real de formularios 877138 y 877139.

---

## 🔍 ANÁLISIS DE CONTENIDO REAL

### ✅ DATOS CONFIRMADOS

**Form 877138 - Supervisión Operativa EPL CAS:**
- **559 campos** por submission
- **Estructura:** Secciones organizadas por áreas operativas
- **Tipos de datos:** Texto, SI/NO, Fechas, Imágenes, Fórmulas
- **Ejemplo:** "AREA DE MARINADO", "MESA DE TRABAJO", "CALIDAD"

**Form 877139 - Control Operativo de Seguridad EPL CAS:**
- **165 campos** por submission  
- **Campos clave identificados:**
  - `PUNTOS MAX`: 45 (máximo puntaje)
  - `PUNTOS TOTALES OBTENIDOS`: 39 (puntaje obtenido)
  - `CALIFICACION PORCENTAJE %`: 86.67% (porcentaje final)
  - `SUCURSAL`: "Lienzo Charro (Saltillo)"

### 📊 METADATOS DISPONIBLES

**Información de Contexto:**
- **Supervisor:** `created_by.display_name` - "Jorge Reynosa"
- **Rol:** `user_role.name` - "Gerente de Distrito"  
- **Sucursal:** `location.name` - "53 - Lienzo Charro"
- **Coordenadas:** `lat/lon` - 25.4551, -101.0086
- **Fechas:** `date_created_local`, `date_completed_local`
- **Tiempo:** `time_to_complete` - tiempo total de supervisión

---

## 🎯 COMPONENTES DE DASHBOARD

### 🔥 PRIORIDAD ALTA - KPIs Críticos

#### 1. **INDICADORES DE SEGURIDAD** (Form 877139)
```yaml
Componente: KPI_CARDS_SECURITY
Datos:
  - Calificación Promedio: promedio(CALIFICACION PORCENTAJE %)
  - Sucursales <80%: count(calificacion < 80)
  - Supervisiones del Mes: count(submissions)
  - Tendencia: comparación mes anterior
Alerta:
  - Crítica: <70%
  - Media: 70-79%
  - Buena: 80-89% 
  - Excelente: >90%
```

#### 2. **RANKING DE SUCURSALES** (Ambos Forms)
```yaml
Componente: BRANCH_RANKING
Datos:
  - Sucursal + Calificación Seguridad
  - Frecuencia de Supervisiones
  - Estado (Verde/Amarillo/Rojo)
  - Última Supervisión
Filtros:
  - Por región/distrito
  - Por rango de fechas
  - Por supervisor
```

### 📊 PRIORIDAD MEDIA - Análisis Operativo

#### 3. **ACTIVIDAD DE SUPERVISIONES**
```yaml
Componente: SUPERVISION_ACTIVITY_CHART
Datos:
  - Timeline de supervisiones (ambos formularios)
  - Frecuencia por día/semana
  - Distribución por sucursal
  - Comparación Operativa vs Seguridad
Visualización:
  - Gráfico de líneas temporal
  - Heatmap por sucursal
```

#### 4. **ANÁLISIS POR SUPERVISOR**
```yaml
Componente: SUPERVISOR_DASHBOARD
Datos:
  - created_by.display_name
  - user_role.name
  - Sucursales supervisadas
  - Tiempo promedio: time_to_complete
  - Calificaciones otorgadas
```

### 📋 PRIORIDAD BAJA - Detalles Operativos

#### 5. **MAPA DE SUPERVISIONES**
```yaml
Componente: SUPERVISION_MAP
Datos:
  - Coordenadas: smetadata.lat/lon
  - Sucursales con/sin supervisión
  - Calificaciones por ubicación
  - Rutas de supervisores
```

#### 6. **ANÁLISIS DE TIEMPO**
```yaml
Componente: TIME_ANALYSIS
Datos:
  - time_to_complete por formulario
  - Horarios de supervisión
  - Eficiencia por supervisor
  - Duración vs calidad
```

---

## 🏗️ ESTRUCTURA DE EXTRACCIÓN ETL

### 📋 CAMPOS A EXTRAER

**Tabla: supervision_submissions**
```sql
CREATE TABLE supervision_submissions (
    id VARCHAR(50) PRIMARY KEY,
    form_id VARCHAR(10),
    form_name VARCHAR(100),
    sucursal_id VARCHAR(10),
    sucursal_name VARCHAR(100),
    supervisor_name VARCHAR(100),
    supervisor_role VARCHAR(50),
    fecha_supervision TIMESTAMP,
    fecha_completada TIMESTAMP,
    tiempo_supervision INTEGER,  -- en segundos
    coordenadas_lat DECIMAL(10,8),
    coordenadas_lon DECIMAL(11,8),
    
    -- Campos específicos Form 877139 (Seguridad)
    puntos_max INTEGER,
    puntos_obtenidos INTEGER,
    calificacion_porcentaje DECIMAL(5,2),
    
    -- Metadatos
    plataforma VARCHAR(20),
    zona_horaria VARCHAR(50),
    distancia_sucursal DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT NOW()
);
```

**Tabla: supervision_answers** (Para análisis detallado)
```sql
CREATE TABLE supervision_answers (
    id SERIAL PRIMARY KEY,
    submission_id VARCHAR(50),
    field_id INTEGER,
    field_title TEXT,
    field_type VARCHAR(20),
    field_value TEXT,
    is_answered BOOLEAN,
    section_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (submission_id) REFERENCES supervision_submissions(id)
);
```

### 🔧 PROCESO ETL ESPECÍFICO

**1. Extracción Diaria (6:00 AM)**
```python
def extract_supervision_data():
    # Solo cuando hay supervisiones (no diario como otros forms)
    for form_id in ['877138', '877139']:
        submissions = client.get_submissions_for_form(form_id, days_back=1)
        for submission in submissions:
            # Extraer KPIs principales
            extract_main_metrics(submission)
            # Extraer answers detalladas (opcional)
            extract_detailed_answers(submission) 
```

**2. Campos Clave para Dashboard**
```yaml
KPIs_Criticos:
  - calificacion_porcentaje (Form 877139)
  - puntos_obtenidos/puntos_max (Form 877139) 
  - sucursal_name + supervisor_name
  - fecha_supervision + tiempo_supervision

Metadatos_Contexto:
  - coordenadas para mapa
  - plataforma (iOS/Android) para análisis UX
  - distancia_sucursal para validación GPS
```

---

## 🎨 MOCKUP DE DASHBOARD

### Layout Principal (Pantalla Completa)

```
┌─────────────────────────────────────────────────────────────┐
│ 🏢 EL POLLO LOCO MÉXICO - SUPERVISIONES   📅 Hoy: 17 Dic   │
├─────────────────────────────────────────────────────────────┤
│ 🔥 KPIs CRÍTICOS                                           │
│ ┌──────────┬──────────┬──────────┬──────────┐               │
│ │🛡️ Seg.   │📊 Prom.  │⚠️ <80%   │📍 Total │               │
│ │  86.7%   │  88.2%   │   12     │   45    │               │
│ └──────────┴──────────┴──────────┴──────────┘               │
├─────────────────────────────────────────────────────────────┤
│ 📊 RANKING SUCURSALES              │ 📈 ACTIVIDAD SEMANAL  │
│ ┌────────────────────────────────┐ │ ┌─────────────────────┐ │
│ │1. Cumbres Norte      94.5% 🟢 │ │ │      ▄▆█▄▃▆▇       │ │
│ │2. Centro Sur         91.2% 🟢 │ │ │   Form 877139       │ │
│ │3. Lienzo Charro      86.7% 🟡 │ │ │   Form 877138       │ │
│ │4. Miguel de la M.    82.1% 🟡 │ │ └─────────────────────┘ │
│ │5. Valle Verde        76.3% 🔴 │ │                         │
│ └────────────────────────────────┘ │                         │
├─────────────────────────────────────┼─────────────────────────┤
│ 👨‍💼 SUPERVISORES ACTIVOS           │ 🗺️ MAPA SUPERVISIONES │
│ • Israel Garcia - 3 supervisiones  │ ┌─────────────────────┐ │
│ • Jorge Reynosa - 2 supervisiones  │ │  📍53  📍12  📍67  │ │
│ • María López   - 4 supervisiones  │ │    📍85    📍34    │ │
│                                     │ │       📍22         │ │
│                                     │ └─────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

---

## 🚀 IMPLEMENTACIÓN RECOMENDADA

### Fase 1 - Dashboard Básico (1 semana)
- ✅ KPIs críticos de seguridad
- ✅ Ranking de sucursales
- ✅ Actividad de supervisiones
- ✅ ETL específico para supervisiones

### Fase 2 - Dashboard Completo (2 semanas)  
- ✅ Análisis por supervisor
- ✅ Mapa de supervisiones
- ✅ Análisis temporal
- ✅ Alertas automáticas

### Fase 3 - Optimizaciones (1 semana)
- ✅ Filtros avanzados
- ✅ Exportación de reportes
- ✅ Notificaciones WhatsApp
- ✅ Dashboard móvil

---

## 💡 PRÓXIMOS PASOS

1. **✅ Validar campos identificados** con Roberto
2. **🔧 Implementar ETL específico** para supervisiones
3. **🎨 Crear prototipo** de dashboard con datos reales
4. **📱 Configurar alertas** para calificaciones <80%
5. **🚀 Desplegar** en Railway con PostgreSQL

---

**🎯 RESULTADO ESPERADO:** Dashboard en tiempo real que permita monitorear la calidad operativa y de seguridad de las 86 sucursales, con alertas automáticas para supervisiones de baja calificación y análisis completo por supervisor y región.