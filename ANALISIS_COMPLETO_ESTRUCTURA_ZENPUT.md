# ANÁLISIS COMPLETO Y SISTEMÁTICO DE LA ESTRUCTURA ZENPUT API - EL POLLO LOCO

**Fecha:** 2025-12-18  
**Objetivo:** Análisis completo de la estructura real de Zenput API para diseñar el ETL correcto definitivo

## 1. ESTRUCTURA REAL DE SUBMISSIONS 

### 1.1 Formulario Operativo (877138)

**Estructura principal identificada:**
```json
{
  "id": "6939c9725a75a5e3f89b929e",
  "activity": {
    "id": 864759,
    "name": "SUPERVISION OPERATIVA CAS 1.1 REV 250125"
  },
  "location": {
    "id": 2247052,
    "name": "53 - Lienzo Charro",
    "external_key": "53",
    "city": "Saltillo",
    "state": "CO",
    "lat": 25.3952928,
    "lon": -100.9895515,
    "tags": [
      {"id": 56930, "name": "All Locations"},
      {"id": 59266, "name": "EPL CAS"}
    ]
  },
  "user_teams": [
    {"id": 114836},
    {"id": 115095}
  ],
  "location_teams": [
    {"id": 115108},
    {"id": 115132},
    {"id": 114836},
    {"id": 115095}
  ],
  "answers": [
    {
      "field_id": 12786112,
      "field_type": "section|yesno|image|datetime|formula",
      "title": "Campo específico",
      "value": "Valor capturado",
      "is_answered": true,
      "visible": true
    }
  ]
}
```

**Campos críticos extraídos:**
- **31 áreas operativas** identificadas
- **186 campos de evaluación** totales  
- **Fotos obligatorias** por área
- **Sistema de puntuación** basado en compliance
- **Comentarios obligatorios** en no cumplimientos

### 1.2 Formulario Seguridad (877139)

**Estructura principal identificada:**
```json
{
  "id": "6939ca30f0f64132fa23ea6c",
  "activity": {
    "id": 864760,
    "name": "CONTROL OPERATIVO DE SEGURIDAD CAS 1.1 REV. 25012025"
  },
  "location": {
    "id": 2247052,
    "name": "53 - Lienzo Charro"
  },
  "answers": [
    {
      "field_id": 12786662,
      "title": "CONTROL OPERATIVO DE SEGURIDAD",
      "field_type": "section"
    },
    {
      "field_id": 12786663,
      "title": "PUNTOS MAX",
      "field_type": "formula",
      "value": 45
    },
    {
      "field_id": 12786664,
      "title": "PUNTOS TOTALES OBTENIDOS", 
      "field_type": "formula",
      "value": 39
    },
    {
      "field_id": 12786665,
      "title": "CALIFICACION PORCENTAJE %",
      "field_type": "formula", 
      "value": 86.67
    }
  ]
}
```

**Campos críticos de seguridad:**
- **45 puntos máximos** de evaluación
- **Sistema automático de cálculo** de porcentaje
- **Menos campos** que operativa pero más críticos
- **Evaluación binaria** principalmente

## 2. ESTRUCTURA REAL DE LOCATIONS

**Total identificado:** 86 sucursales activas

### 2.1 Estructura de Location
```json
{
  "id": 2247000,
  "name": "1 - Pino Suarez", 
  "external_key": "1",
  "address": "Av. Pino Suarez #500 sur Col. Centro",
  "city": "Monterrey",
  "state": "NL",
  "zipcode": "64000",
  "lat": 25.6689279,
  "lon": -100.3203777,
  "email": "pinosuarez@epl.mx",
  "phone": "812-191-1171",
  "time_zone": "America/Monterrey",
  "tags": [
    {"id": 56930, "name": "All Locations"},
    {"id": 59266, "name": "EPL CAS"}
  ]
}
```

### 2.2 Mapeo Locations → Sucursales Master
**Correlación identificada en 86_sucursales_master.csv:**

| Campo Zenput | Campo Master | Tipo | Uso ETL |
|--------------|--------------|------|---------|
| `id` | `Location_Code` | int | Primary Key |
| `external_key` | `Numero_Sucursal` | int | Business Key |
| `name` | `Nombre_Sucursal` | str | Display |
| `city` | `Ciudad` | str | Geography |
| `state` | `Estado` | str | Geography |
| `lat/lon` | `Latitude/Longitude` | float | GIS |

**PROBLEMA CRÍTICO IDENTIFICADO:** Roberto tiene la estructura correcta en su CSV, pero falta correlacionar los **Grupos Operativos**.

## 3. ESTRUCTURA REAL DE TEAMS

### 3.1 Teams encontrados en Submissions
```json
"user_teams": [{"id": 114836}, {"id": 115095}],
"location_teams": [{"id": 115108}, {"id": 115132}, {"id": 114836}, {"id": 115095}]
```

### 3.2 Teams correlacionados con Directores
**Estructura identificada en correlacion_completa_directores_teams.json:**

```json
{
  "directores_operaciones": [
    {
      "id": 356987,
      "nombre": "Anibal Hernandez",
      "email": "anibal@elpolloloco.mx",
      "rol": "Director de Operaciones",
      "teams": [{"id": 115117, "name": "Carlos Gamez"}],
      "default_team": {"id": 115117, "name": "Carlos Gamez"}
    }
  ]
}
```

**PROBLEMA IDENTIFICADO:** Los teams en submissions no coinciden exactamente con los teams de directores. Hay teams intermedios (gerentes de distrito).

## 4. RELACIONES REALES MAPEADAS

### 4.1 Jerarquía Real Identificada
```
LOCATIONS (86) 
    ↓ (location_teams)
TEAMS SUCURSALES (115108, 115132, etc.)
    ↓ (supervised_by)  
TEAMS GERENTES DISTRITO (114836, 115095, etc.)
    ↓ (reports_to)
TEAMS DIRECTORES OPERACIÓN (115117, etc.)
    ↓ (belongs_to)
GRUPOS OPERATIVOS (20 grupos - CRR, TEPEYAC, OGAS, etc.)
```

### 4.2 Estructura de Grupos Operativos
**Identificados en estructura_definitiva_epl_20251217_182113.json:**

| Grupo | Sucursales | Estados | Director Asignado |
|-------|------------|---------|-------------------|
| CRR | 3 | Tamaulipas | [Identificar] |
| TEPEYAC | 7 | Nuevo León | [Identificar] |
| OGAS | 8 | Nuevo León | [Identificar] |
| EPL SO | 1 | Nuevo León | [Identificar] |
| EFM | 7 | Nuevo León | [Identificar] |
| ... | ... | ... | ... |

**Total:** 20 grupos operativos, 86 sucursales

## 5. GAPS Y PROBLEMAS CRÍTICOS IDENTIFICADOS

### 5.1 Problemas en Estructura Actual

❌ **Gap 1 - Relación Teams → Grupos Operativos**
- Los teams de Zenput NO están mapeados directamente a grupos operativos
- Roberto tiene los grupos operativos correctos pero sin teams IDs
- Necesitamos correlacionar `team_id` → `grupo_operativo`

❌ **Gap 2 - Jerarquía Completa de Teams** 
- Hay 3 niveles de teams: sucursal, distrito, director
- Las submissions solo muestran teams de sucursal y distrito
- Falta mapear teams de distrito → teams de director → grupos operativos

❌ **Gap 3 - Períodos de Evaluación**
- Las submissions no tienen información de período estándar
- Cada submission tiene timestamp pero no período de reporte
- Necesitamos estandarizar períodos (semanal/mensual)

❌ **Gap 4 - Usuarios y Roles**
- 20 usuarios identificados con roles diversos
- No está clara la asignación usuario → team → sucursales  
- Faltan permisos y responsabilidades

❌ **Gap 5 - Campos de Evaluación Dinámicos**
- Los field_ids pueden cambiar entre versiones de formularios
- Necesitamos mapeo estable de `field_id` → `concepto_evaluacion`
- Los títulos de campos pueden cambiar

### 5.2 Datos Faltantes Críticos

🔍 **Faltante 1:** Mapeo directo `team_id` → `grupo_operativo`  
🔍 **Faltante 2:** Jerarquía completa de reporting  
🔍 **Faltante 3:** Tabla maestra de campos de evaluación  
🔍 **Faltante 4:** Períodos estándares de reporte  
🔍 **Faltante 5:** Usuarios responsables por grupo operativo

## 6. DIAGNÓSTICO COMPARATIVO

### 6.1 Estructura Roberto vs Estructura Zenput Real

✅ **CORRECTOS en CSV Roberto:**
- 86 sucursales completas
- Nombres y códigos correctos  
- Coordenadas geográficas
- Grupos operativos bien definidos
- Estados y ciudades correctas

❌ **FALTANTES en CSV Roberto:**
- Location_Code → Team_ID mapping
- Teams de distrito y directores
- Usuarios responsables
- Períodos de evaluación

✅ **CORRECTOS en Zenput API:**
- Submissions completas con datos reales
- Teams IDs en cada submission
- Usuario que realizó evaluación
- Timestamp exacto de evaluación

❌ **FALTANTES en Zenput API:**
- Grupos operativos no están expuestos
- Jerarquía de teams incompleta
- Períodos estándares indefinidos

## 7. DISEÑO ETL CORRECTO FINAL

### 7.1 Arquitectura ETL Corregida

```
┌─ ZENPUT API ─┐    ┌─ MASTER DATA ─┐    ┌─ TARGET DB ─┐
│               │    │               │    │             │
│ Submissions   │    │ 86_sucursales │    │ Fact_       │
│ 877138/877139 │ →  │ _master.csv   │ →  │ Supervisiones│
│               │    │               │    │             │
│ Locations     │    │ Grupos_Ops    │    │ Dim_        │
│ API           │ →  │ Mapping       │ →  │ Sucursales  │
│               │    │               │    │             │
│ Teams/Users   │    │ Teams_Jerarq  │    │ Dim_Teams   │
│ API           │ →  │ Mapping       │ →  │ Dim_Users   │
└───────────────┘    └───────────────┘    └─────────────┘
```

### 7.2 Pasos ETL Definitivos

**PASO 1: MASTER DATA CORRELATION**
1. Cargar 86_sucursales_master.csv como base
2. Correlacionar con Zenput locations por `external_key`
3. Extraer teams de submissions recientes
4. Mapear teams → grupos operativos manualmente (una vez)

**PASO 2: INCREMENTAL SUBMISSIONS EXTRACTION** 
1. Extraer submissions desde última fecha
2. Enriquecer con master data (grupos operativos)
3. Normalizar campos de evaluación
4. Calcular KPIs y métricas

**PASO 3: DIMENSIONAL MODELING**
1. `dim_sucursales`: Master + coordenadas + grupos
2. `dim_tiempo`: Períodos estándares (semanal/mensual)
3. `dim_areas_evaluacion`: 31 áreas operativas + seguridad
4. `fact_supervisiones`: Submissions normalizadas

**PASO 4: QUALITY ASSURANCE**
1. Validar 86 sucursales presentes
2. Verificar periods sin gaps
3. Confirmar KPIs por grupo operativo
4. Alertas de submissions faltantes

## 8. PRÓXIMOS PASOS INMEDIATOS

### 8.1 Acciones Críticas (Roberto)

🎯 **Prioridad 1:** Crear tabla maestra `teams_to_grupos_mapping`
```sql
CREATE TABLE teams_grupos_mapping (
  team_id INT PRIMARY KEY,
  team_name VARCHAR(100),
  team_type ENUM('sucursal', 'distrito', 'director'),
  grupo_operativo VARCHAR(50),
  director_responsable VARCHAR(100),
  created_at TIMESTAMP
);
```

🎯 **Prioridad 2:** Extraer 20-30 submissions recientes y mapear teams manualmente:
```python
# Para cada submission reciente
submission_teams = extract_teams_from_recent_submissions()
# Correlacionar con grupos conocidos
map_teams_to_grupos_operativos(submission_teams)
```

🎯 **Prioridad 3:** Definir períodos estándares de reporte
- Semanal: Lunes a Domingo
- Mensual: 1 al último día del mes
- Período de gracia: 3 días para completar

### 8.2 Validaciones Necesarias

✅ **Validación 1:** Confirmar que 86 sucursales tienen Location_Code en Zenput  
✅ **Validación 2:** Verificar que cada grupo operativo tiene submissions regulares  
✅ **Validación 3:** Asegurar que hay directores asignados por grupo  

## CONCLUSIÓN

Roberto tiene **CORRECTA la estructura base** en su CSV. El problema NO es la estructura sino la **falta de correlación Teams → Grupos Operativos**. 

La API de Zenput está funcionando correctamente pero falta el **mapping intermedio** para convertir los `team_ids` de las submissions en los `grupos_operativos` que Roberto ya tiene bien definidos.

**SOLUCIÓN:** Crear el mapping manualmente una vez, y después el ETL funcionará perfectamente con la estructura que Roberto ya tiene.

**NO hay que reestructurar todo** ✅  
**Solo hay que completar el mapping** ✅