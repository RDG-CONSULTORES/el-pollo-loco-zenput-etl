# 🚂 GUÍA PASO A PASO - RAILWAY DEPLOYMENT

**OBJETIVO**: Configurar Railway PostgreSQL + Dashboard completo en 3 horas

---

## 🎯 PLAN DE EJECUCIÓN COMPLETO

### ⏰ **TIEMPO ESTIMADO: 3 HORAS**
- **Hora 1**: Railway PostgreSQL + Base de datos ✅
- **Hora 2**: ETL histórico 2025 + Validación ✅  
- **Hora 3**: Dashboard básico funcionando ✅

---

## 🚀 PASO 1: RAILWAY POSTGRESQL (45 minutos)

### **1.1 Crear Cuenta y Proyecto Railway**
```bash
# 1. Ir a https://railway.app
# 2. Sign up with GitHub (recomendado)
# 3. New Project → Deploy PostgreSQL
# 4. Nombre: "epl-supervisiones-dashboard"
```

### **1.2 Configurar PostgreSQL**
```bash
# Después de crear PostgreSQL en Railway:
# 1. Click en PostgreSQL service
# 2. Variables tab → Copy connection details

# Exportar variables (Railway te da estos valores):
export RAILWAY_DB_HOST="xxxx.railway.app"
export RAILWAY_DB_PORT="5432"  
export RAILWAY_DB_NAME="railway"
export RAILWAY_DB_USER="postgres"
export RAILWAY_DB_PASSWORD="xxxx-xxxx-xxxx"

# Test conexión:
psql postgresql://$RAILWAY_DB_USER:$RAILWAY_DB_PASSWORD@$RAILWAY_DB_HOST:$RAILWAY_DB_PORT/$RAILWAY_DB_NAME
```

### **1.3 Crear Estructura de Base de Datos**
```bash
# En tu terminal local:
cd el-pollo-loco-zenput-etl

# 1. Crear esquema principal:
psql postgresql://$RAILWAY_DB_USER:$RAILWAY_DB_PASSWORD@$RAILWAY_DB_HOST:$RAILWAY_DB_PORT/$RAILWAY_DB_NAME \
  -f sql/database_schema_20251217_151951.sql

# 2. Insertar sucursales maestras:
psql postgresql://$RAILWAY_DB_USER:$RAILWAY_DB_PASSWORD@$RAILWAY_DB_HOST:$RAILWAY_DB_PORT/$RAILWAY_DB_NAME \
  -f sql/insert_sucursales_master_20251217_151955.sql
```

### **1.4 Configurar Períodos 2025**
```sql
-- Ejecutar esto en Railway PostgreSQL console o por psql:

-- Insertar períodos reales 2025
INSERT INTO periodos_supervision (periodo_codigo, año, tipo_sucursal, fecha_inicio, fecha_fin, fecha_limite_supervision, formularios_requeridos, descripcion) VALUES
-- TRIMESTRALES (Locales)
('T1', 2025, 'LOCAL', '2025-03-12', '2025-04-16', '2025-04-20', ARRAY['877138', '877139'], '1er Trimestre 2025 - Expo a Centrito'),
('T2', 2025, 'LOCAL', '2025-06-11', '2025-08-18', '2025-08-25', ARRAY['877138', '877139'], '2do Trimestre 2025 - Apodaca Centro a Echeverría'),  
('T3', 2025, 'LOCAL', '2025-08-19', '2025-10-29', '2025-11-05', ARRAY['877138', '877139'], '3er Trimestre 2025'),
('T4', 2025, 'LOCAL', '2025-10-30', '2025-12-31', '2026-01-10', ARRAY['877138', '877139'], '4to Trimestre 2025 - Valle Soleado en adelante'),

-- SEMESTRALES (Foráneas)  
('S1', 2025, 'FORANEA', '2025-04-10', '2025-06-09', '2025-06-15', ARRAY['877138', '877139'], '1er Semestre 2025 - Harold R. Pape a Guasave'),
('S2', 2025, 'FORANEA', '2025-07-30', '2025-11-07', '2025-11-15', ARRAY['877138', '877139'], '2do Semestre 2025 - PAPE Monclova a Lázaro Cárdenas');
```

---

## 📊 PASO 2: ETL HISTÓRICO COMPLETO (45 minutos)

### **2.1 Configurar Variables de Entorno Locales**
```bash
# Crear archivo .env en el proyecto:
echo "RAILWAY_DB_HOST=$RAILWAY_DB_HOST" > .env
echo "RAILWAY_DB_PORT=$RAILWAY_DB_PORT" >> .env
echo "RAILWAY_DB_NAME=$RAILWAY_DB_NAME" >> .env
echo "RAILWAY_DB_USER=$RAILWAY_DB_USER" >> .env
echo "RAILWAY_DB_PASSWORD=$RAILWAY_DB_PASSWORD" >> .env
echo "ZENPUT_API_TOKEN=cb908e0d4e0f5501c635325c611db314" >> .env

# Cargar variables:
source .env
```

### **2.2 Ejecutar ETL Histórico Completo**
```bash
# Cargar datos históricos completos (más de 7 días):
python3 scripts/complete_supervision_etl.py

# Resultado esperado:
# ✅ Total supervisiones procesadas: 80-120
# ✅ Guardadas exitosamente: 80-120  
# ✅ 20 sucursales normalizadas automáticamente
# ✅ KPIs calculados por 11 áreas (Seguridad) + 31 áreas (Operaciones)
```

### **2.3 Verificar Datos en Railway**
```sql
-- Conectar a Railway console y verificar:

-- 1. Sucursales cargadas:
SELECT COUNT(*) FROM sucursales_master;  -- Debe dar 20

-- 2. Supervisiones cargadas:
SELECT form_name, COUNT(*) 
FROM supervisiones 
GROUP BY form_name;
-- Debe mostrar ambos formularios con datos

-- 3. Áreas con KPIs:
SELECT area_codigo, COUNT(*), AVG(conformidad_porcentaje) 
FROM supervision_areas 
GROUP BY area_codigo
ORDER BY AVG(conformidad_porcentaje) DESC;
-- Debe mostrar 42 áreas (11 Seguridad + 31 Operaciones)
```

---

## 📱 PASO 3: DASHBOARD BÁSICO (90 minutos)

### **3.1 Crear Nuevo Servicio de Dashboard en Railway**
```bash
# En Railway dashboard:
# 1. Add service → GitHub Repo
# 2. Conectar a tu repo del dashboard
# 3. Configurar variables de entorno automáticamente
```

### **3.2 Crear Dashboard Simple con Streamlit**
```bash
# Crear nuevo archivo para dashboard:
touch dashboard/app.py
touch dashboard/requirements.txt
```

```python
# dashboard/requirements.txt:
streamlit==1.29.0
psycopg2-binary==2.9.7
plotly==5.17.0
pandas==2.1.3
```

```python
# dashboard/app.py:
import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import os

# Configuración de página
st.set_page_config(
    page_title="El Pollo Loco - Supervisiones",
    page_icon="🏢",
    layout="wide"
)

@st.cache_data
def get_db_connection():
    """Conectar a Railway PostgreSQL"""
    return psycopg2.connect(
        host=os.getenv('RAILWAY_DB_HOST'),
        port=os.getenv('RAILWAY_DB_PORT'), 
        database=os.getenv('RAILWAY_DB_NAME'),
        user=os.getenv('RAILWAY_DB_USER'),
        password=os.getenv('RAILWAY_DB_PASSWORD')
    )

@st.cache_data(ttl=300)  # Cache 5 minutos
def load_dashboard_data():
    """Cargar datos para dashboard"""
    conn = get_db_connection()
    
    # KPIs generales
    kpis_query = """
    SELECT 
        COUNT(*) as total_supervisiones,
        COUNT(DISTINCT sucursal_numero) as sucursales_supervisadas,
        AVG(calificacion_general) as calificacion_promedio,
        COUNT(CASE WHEN calificacion_general < 80 THEN 1 END) as sucursales_criticas
    FROM supervisiones 
    WHERE calificacion_general IS NOT NULL
    """
    
    # Ranking sucursales  
    ranking_query = """
    SELECT 
        sm.nombre_actual as sucursal,
        sm.tipo_sucursal,
        AVG(s.calificacion_general) as calificacion_promedio,
        COUNT(s.id) as total_supervisiones,
        MAX(s.fecha_supervision) as ultima_supervision
    FROM sucursales_master sm
    LEFT JOIN supervisiones s ON sm.sucursal_numero = s.sucursal_numero
    WHERE s.calificacion_general IS NOT NULL
    GROUP BY sm.sucursal_numero, sm.nombre_actual, sm.tipo_sucursal
    ORDER BY calificacion_promedio DESC
    """
    
    # Áreas críticas
    areas_query = """
    SELECT 
        area_codigo,
        area_nombre,
        AVG(conformidad_porcentaje) as conformidad_promedio,
        COUNT(*) as total_supervisiones
    FROM supervision_areas
    WHERE conformidad_porcentaje IS NOT NULL  
    GROUP BY area_codigo, area_nombre
    ORDER BY conformidad_promedio ASC
    """
    
    kpis = pd.read_sql(kpis_query, conn)
    ranking = pd.read_sql(ranking_query, conn)  
    areas = pd.read_sql(areas_query, conn)
    
    conn.close()
    return kpis, ranking, areas

def main():
    """Dashboard principal"""
    
    st.title("🏢 El Pollo Loco México - Dashboard Supervisiones")
    st.markdown("---")
    
    # Cargar datos
    try:
        kpis, ranking, areas = load_dashboard_data()
        
        # KPIs principales
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Total Supervisiones", 
                f"{int(kpis['total_supervisiones'].iloc[0]):,}",
                delta="Histórico 2025"
            )
        
        with col2:
            st.metric(
                "Sucursales Supervisadas",
                f"{int(kpis['sucursales_supervisadas'].iloc[0])}/86",
                delta=f"{(int(kpis['sucursales_supervisadas'].iloc[0])/86*100):.0f}% completado"
            )
            
        with col3:
            calificacion = kpis['calificacion_promedio'].iloc[0]
            st.metric(
                "Calificación Promedio",
                f"{calificacion:.1f}%",
                delta="Excelente" if calificacion >= 90 else "Bueno" if calificacion >= 80 else "Mejorar"
            )
            
        with col4:
            criticas = int(kpis['sucursales_criticas'].iloc[0])
            st.metric(
                "Sucursales Críticas (<80%)",
                f"{criticas}",
                delta="🚨 Atención" if criticas > 0 else "✅ Sin problemas"
            )
        
        st.markdown("---")
        
        # Dos columnas principales
        col1, col2 = st.columns([1, 1])
        
        with col1:
            st.subheader("🏆 Ranking de Sucursales")
            
            # Colorear por performance
            def color_performance(val):
                if val >= 90:
                    return 'background-color: #90EE90'  # Verde claro
                elif val >= 80:
                    return 'background-color: #FFFFE0'  # Amarillo claro  
                else:
                    return 'background-color: #FFB6C1'  # Rojo claro
            
            ranking_display = ranking[['sucursal', 'tipo_sucursal', 'calificacion_promedio', 'total_supervisiones']].copy()
            ranking_display['calificacion_promedio'] = ranking_display['calificacion_promedio'].round(1)
            
            st.dataframe(
                ranking_display.style.applymap(
                    color_performance, 
                    subset=['calificacion_promedio']
                ),
                use_container_width=True,
                height=400
            )
        
        with col2:
            st.subheader("🚨 Áreas Críticas")
            
            # Mostrar áreas con menor conformidad
            areas_criticas = areas.head(10).copy()
            areas_criticas['conformidad_promedio'] = areas_criticas['conformidad_promedio'].round(1)
            
            fig = px.bar(
                areas_criticas,
                x='conformidad_promedio',
                y='area_codigo', 
                orientation='h',
                color='conformidad_promedio',
                color_continuous_scale='RdYlGn',
                title="Top 10 Áreas que Requieren Atención"
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        # Filtros y análisis adicional
        st.markdown("---")
        st.subheader("📊 Análisis por Período")
        
        # Selector de período
        periodo_select = st.selectbox(
            "Seleccionar Período 2025:",
            ["Todos", "T1 (Mar-Abr)", "T2 (Jun-Ago)", "T3 (Ago-Oct)", "T4 (Oct-Dic)", "S1 (Abr-Jun)", "S2 (Jul-Nov)"]
        )
        
        st.info(f"📊 Mostrando datos para: {periodo_select}")
        
    except Exception as e:
        st.error(f"❌ Error cargando datos: {e}")
        st.info("💡 Verificar conexión a Railway PostgreSQL")

if __name__ == "__main__":
    main()
```

### **3.3 Deploy Dashboard en Railway**
```bash
# 1. Commit dashboard files:
git add dashboard/
git commit -m "Add basic Streamlit dashboard"
git push

# 2. En Railway:
# - Add service → GitHub repo  
# - Select dashboard folder
# - Add environment variables automáticamente
# - Deploy

# 3. Configurar start command:
# Start Command: streamlit run dashboard/app.py --server.port $PORT
```

---

## ✅ PASO 4: VALIDACIÓN FINAL (15 minutos)

### **4.1 Verificar Dashboard Funcionando**
```bash
# 1. URL de Railway dashboard funcionando
# 2. KPIs mostrando datos reales
# 3. Ranking de sucursales correcto
# 4. Áreas críticas identificadas
```

### **4.2 Verificar ETL Automático** 
```bash
# Test ETL con datos nuevos:
python3 scripts/complete_supervision_etl.py

# Verificar que:
# ✅ Solo procesa supervisiones nuevas  
# ✅ No duplica datos existentes
# ✅ Dashboard se actualiza automáticamente
```

### **4.3 Configurar Cron para ETL Diario**
```bash
# En el servidor que ejecutará ETL:
crontab -e

# Agregar línea:
0 6 * * * cd /path/to/el-pollo-loco-zenput-etl && source .env && python3 scripts/complete_supervision_etl.py >> /var/log/epl-etl.log 2>&1
```

---

## 🎯 RESULTADO ESPERADO (3 HORAS)

### ✅ **AL FINAL TENDRÁS:**
1. **Railway PostgreSQL** con estructura completa funcionando
2. **80-120 supervisiones** históricas 2025 cargadas
3. **Dashboard web público** mostrando KPIs en tiempo real
4. **ETL automático diario** procesando supervisiones nuevas
5. **Base sólida** para expandir análisis Operaciones (877138)

### 📊 **DASHBOARD FUNCIONANDO CON:**
- KPIs generales de las supervisiones
- Ranking de 20 sucursales por performance  
- Top 10 áreas críticas que requieren atención
- Filtros por períodos T1-T4 y S1-S2
- Actualización automática cada 5 minutos

### 🚀 **LISTOS PARA:**
- Análisis detallado Supervisión Operativa (877138)
- Sistema de alertas WhatsApp
- Dashboard avanzado con drill-down por grupos operativos
- Preparación para normalización 2026

---

**🎯 Roberto: Siguiendo esta guía paso a paso en 3 horas tendrás el sistema completo funcionando. ¿Empezamos con el Paso 1 (Railway PostgreSQL)?**