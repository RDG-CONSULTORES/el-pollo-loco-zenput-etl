-- 🗺️ NORMALIZACIÓN DE ESTADOS REALES - EL POLLO LOCO MÉXICO
-- Roberto: Todos aparecen como "Nuevo León", necesitamos mapear por grupos operativos

-- ============================================================================
-- 📊 FUNCIÓN PARA CLASIFICAR ESTADOS POR GRUPO OPERATIVO 
-- ============================================================================

CREATE OR REPLACE FUNCTION classify_estado_by_group(grupo_operativo VARCHAR) 
RETURNS VARCHAR AS $$
BEGIN
    -- Grupos de Nuevo León (locales)
    IF grupo_operativo IN (
        'TEPEYAC', 'OGAS', 'EFM', 'EPL SO', 'PLOG NUEVO LEON', 
        'GRUPO CENTRITO', 'GRUPO SABINAS HIDALGO', 'TEC', 'EXPO'
    ) THEN 
        RETURN 'Nuevo León';
        
    -- Grupos de Coahuila
    ELSIF grupo_operativo IN (
        'GRUPO SALTILLO', 'GRUPO PIEDRAS NEGRAS'
    ) THEN 
        RETURN 'Coahuila';
        
    -- Grupos de Tamaulipas
    ELSIF grupo_operativo IN (
        'OCHTER TAMPICO', 'GRUPO MATAMOROS', 'GRUPO NUEVO LAREDO (RUELAS)', 
        'GRUPO RIO BRAVO'
    ) THEN 
        RETURN 'Tamaulipas';
        
    -- Grupos de Torreón/Laguna (Durango/Coahuila)
    ELSIF grupo_operativo IN (
        'PLOG LAGUNA'
    ) THEN 
        RETURN 'Durango';
        
    -- Grupos de Querétaro
    ELSIF grupo_operativo IN (
        'PLOG QUERETARO'
    ) THEN 
        RETURN 'Querétaro';
        
    -- Grupos de Michoacán  
    ELSIF grupo_operativo IN (
        'GRUPO CANTERA ROSA (MORELIA)'
    ) THEN 
        RETURN 'Michoacán';
        
    -- Grupos genéricos (inferir por región)
    ELSIF grupo_operativo IN ('RAP') THEN 
        RETURN 'Tamaulipas'; -- RAP típicamente en frontera
        
    ELSIF grupo_operativo IN ('CRR') THEN 
        RETURN 'Coahuila'; -- CRR típicamente en Coahuila
        
    -- Default: mantener como estaba
    ELSE 
        RETURN 'Nuevo León'; 
    END IF;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 🔄 ACTUALIZAR TABLA SUCURSALES CON ESTADOS CORRECTOS
-- ============================================================================

-- Primero ver cuántas sucursales hay por grupo
SELECT 
    grupo_operativo,
    COUNT(*) as sucursales,
    classify_estado_by_group(grupo_operativo) as estado_correcto
FROM sucursales 
GROUP BY grupo_operativo 
ORDER BY sucursales DESC;

-- Actualizar estados basándose en grupos operativos
UPDATE sucursales 
SET estado = classify_estado_by_group(grupo_operativo)
WHERE grupo_operativo IS NOT NULL;

-- ============================================================================
-- 📊 VERIFICAR RESULTADO DE LA NORMALIZACIÓN
-- ============================================================================

-- Ver distribución por estado después de la normalización
SELECT 
    estado,
    COUNT(DISTINCT grupo_operativo) as grupos,
    COUNT(*) as sucursales,
    ARRAY_AGG(DISTINCT grupo_operativo ORDER BY grupo_operativo) as grupos_list
FROM sucursales 
GROUP BY estado 
ORDER BY sucursales DESC;

-- Ver sucursales por estado y grupo
SELECT 
    estado,
    grupo_operativo,
    COUNT(*) as sucursales,
    STRING_AGG(nombre, ', ') as sucursales_list
FROM sucursales 
GROUP BY estado, grupo_operativo 
ORDER BY estado, sucursales DESC;

-- ============================================================================
-- 🔍 VERIFICAR CLASIFICACIÓN TERRITORIAL DESPUÉS
-- ============================================================================

SELECT 
    estado,
    grupo_operativo,
    COUNT(*) as sucursales,
    CASE 
        WHEN estado = 'Nuevo León' THEN 'LOCAL'
        WHEN grupo_operativo = 'GRUPO SALTILLO' THEN 'LOCAL'  
        ELSE 'FORANEA'
    END as tipo_territorial
FROM sucursales 
GROUP BY estado, grupo_operativo
ORDER BY tipo_territorial, estado, sucursales DESC;