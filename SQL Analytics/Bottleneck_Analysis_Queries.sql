-- Bottleneck Analysis: Equipment Constraint Identification
-- ArcelorMittal Vanderbijlpark Works - Hot Rolling Plant Temper Line
-- Analysis Period: April-August 2024

-- =====================================================
-- 1. Equipment Time Share Analysis
-- =====================================================

-- Calculate total operation time per equipment
SELECT 
    e.equipment_id,
    e.equipment_name,
    e.section,
    e.is_bottleneck_candidate,
    COUNT(DISTINCT oc.coil_id) AS coils_processed,
    SUM(oc.operation_duration_sec) / 3600.0 AS total_operation_hours,
    AVG(oc.operation_duration_sec) / 60.0 AS avg_operation_min,
    MIN(oc.operation_duration_sec) / 60.0 AS min_operation_min,
    MAX(oc.operation_duration_sec) / 60.0 AS max_operation_min
FROM 
    fact_coil_operation_cycle oc
INNER JOIN 
    dim_equipment e ON oc.equipment_id = e.equipment_id
WHERE 
    e.is_active = 1
GROUP BY 
    e.equipment_id, e.equipment_name, e.section, e.is_bottleneck_candidate
ORDER BY 
    total_operation_hours DESC;

-- =====================================================
-- 2. Bottleneck Share (% of Total Line Time)
-- =====================================================

WITH equipment_time AS (
    SELECT 
        e.equipment_id,
        e.equipment_name,
        e.section,
        e.is_bottleneck_candidate,
        SUM(oc.operation_duration_sec) AS total_op_sec
    FROM 
        fact_coil_operation_cycle oc
    INNER JOIN 
        dim_equipment e ON oc.equipment_id = e.equipment_id
    WHERE 
        e.is_active = 1
    GROUP BY 
        e.equipment_id, e.equipment_name, e.section, e.is_bottleneck_candidate
),
line_total AS (
    SELECT SUM(total_op_sec) AS total_line_sec
    FROM equipment_time
)
SELECT 
    et.equipment_name,
    et.section,
    et.is_bottleneck_candidate,
    et.total_op_sec / 3600.0 AS total_hours,
    (et.total_op_sec * 100.0 / lt.total_line_sec) AS share_of_line_pct,
    SUM(et.total_op_sec * 100.0 / lt.total_line_sec) OVER (
        ORDER BY et.total_op_sec DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_share_pct
FROM 
    equipment_time et
CROSS JOIN 
    line_total lt
ORDER BY 
    et.total_op_sec DESC;

-- =====================================================
-- 3. Equipment Utilization (RUN/IDLE/FAULT Breakdown)
-- =====================================================

WITH event_summary AS (
    SELECT 
        equipment_name,
        event_type,
        SUM(event_duration_sec) / 3600.0 AS total_hours
    FROM 
        fact_equipment_event_log
    GROUP BY 
        equipment_name, event_type
)
SELECT 
    equipment_name,
    MAX(CASE WHEN event_type = 'RUN' THEN total_hours ELSE 0 END) AS run_hours,
    MAX(CASE WHEN event_type = 'IDLE' THEN total_hours ELSE 0 END) AS idle_hours,
    MAX(CASE WHEN event_type = 'FAULT' THEN total_hours ELSE 0 END) AS fault_hours,
    SUM(total_hours) AS total_hours,
    MAX(CASE WHEN event_type = 'RUN' THEN total_hours ELSE 0 END) * 100.0 / 
        NULLIF(SUM(total_hours), 0) AS run_pct,
    MAX(CASE WHEN event_type = 'IDLE' THEN total_hours ELSE 0 END) * 100.0 / 
        NULLIF(SUM(total_hours), 0) AS idle_pct,
    MAX(CASE WHEN event_type = 'FAULT' THEN total_hours ELSE 0 END) * 100.0 / 
        NULLIF(SUM(total_hours), 0) AS fault_pct
FROM 
    event_summary
GROUP BY 
    equipment_name
ORDER BY 
    fault_hours DESC;

-- =====================================================
-- 4. Bottleneck Severity by Shift
-- =====================================================

SELECT 
    e.equipment_name,
    e.is_bottleneck_candidate,
    oc.shift_code,
    COUNT(DISTINCT oc.coil_id) AS coils_processed,
    SUM(oc.operation_duration_sec) / 3600.0 AS total_hours,
    AVG(oc.operation_duration_sec) / 60.0 AS avg_operation_min
FROM 
    fact_coil_operation_cycle oc
INNER JOIN 
    dim_equipment e ON oc.equipment_id = e.equipment_id
WHERE 
    e.is_bottleneck_candidate = 1
    AND e.is_active = 1
GROUP BY 
    e.equipment_name, e.is_bottleneck_candidate, oc.shift_code
ORDER BY 
    e.equipment_name, oc.shift_code;

-- =====================================================
-- 5. Product Mix Impact on Bottleneck Equipment
-- =====================================================

WITH product_classification AS (
    SELECT 
        coil_id,
        parent_coil_id,
        type_code,
        is_prime,
        is_scrap,
        thickness_mm,
        width_mm,
        CASE 
            WHEN thickness_mm <= 2.0 AND width_mm <= 1300 THEN 'Thin & Narrow (Fast)'
            WHEN thickness_mm > 3.0 AND width_mm > 1400 THEN 'Thick & Wide (Slow)'
            ELSE 'Standard Mix'
        END AS product_band
    FROM 
        fact_production_coil
)
SELECT 
    e.equipment_name,
    pc.product_band,
    pc.is_prime,
    COUNT(DISTINCT oc.coil_id) AS coils_processed,
    AVG(oc.operation_duration_sec) / 60.0 AS avg_operation_min,
    SUM(oc.operation_duration_sec) / 3600.0 AS total_hours
FROM 
    fact_coil_operation_cycle oc
INNER JOIN 
    dim_equipment e ON oc.equipment_id = e.equipment_id
INNER JOIN 
    product_classification pc ON oc.coil_id = pc.coil_id
WHERE 
    e.is_bottleneck_candidate = 1
    AND e.is_active = 1
GROUP BY 
    e.equipment_name, pc.product_band, pc.is_prime
ORDER BY 
    e.equipment_name, pc.product_band, pc.is_prime;

-- =====================================================
-- 6. Top 10 Longest Equipment Operations (Outlier Detection)
-- =====================================================

SELECT TOP 10
    e.equipment_name,
    oc.coil_id,
    pc.parent_coil_id,
    pc.type_code,
    pc.is_prime,
    pc.thickness_mm,
    pc.width_mm,
    oc.operation_duration_sec / 60.0 AS operation_min,
    oc.operation_start_ts,
    oc.operation_end_ts,
    oc.shift_code
FROM 
    fact_coil_operation_cycle oc
INNER JOIN 
    dim_equipment e ON oc.equipment_id = e.equipment_id
INNER JOIN 
    fact_production_coil pc ON oc.coil_id = pc.coil_id
WHERE 
    e.is_bottleneck_candidate = 1
ORDER BY 
    oc.operation_duration_sec DESC;

-- =====================================================
-- 7. Equipment Idle Time Analysis (Waiting Between Coils)
-- =====================================================

SELECT 
    equipment_name,
    COUNT(*) AS idle_event_count,
    SUM(event_duration_sec) / 3600.0 AS total_idle_hours,
    AVG(event_duration_sec) / 60.0 AS avg_idle_min,
    MAX(event_duration_sec) / 60.0 AS max_idle_min
FROM 
    fact_equipment_event_log
WHERE 
    event_type = 'IDLE'
GROUP BY 
    equipment_name
ORDER BY 
    total_idle_hours DESC;

-- =====================================================
-- 8. Maintenance Downtime Impact on Bottleneck Equipment
-- =====================================================

SELECT 
    e.equipment_name,
    e.is_bottleneck_candidate,
    COUNT(*) AS fault_event_count,
    SUM(me.duration_hours) AS total_downtime_hours,
    AVG(me.duration_hours) AS avg_downtime_hours,
    MAX(me.duration_hours) AS max_downtime_hours,
    SUM(me.duration_hours) * 100.0 / 
        (SELECT SUM(duration_hours) FROM fact_maintenance_event) AS share_of_total_downtime_pct
FROM 
    fact_maintenance_event me
INNER JOIN 
    dim_equipment e ON me.equipment_name = e.equipment_name
WHERE 
    e.is_active = 1
GROUP BY 
    e.equipment_name, e.is_bottleneck_candidate
ORDER BY 
    total_downtime_hours DESC;

-- =====================================================
-- 9. Bottleneck Equipment Daily Performance
-- =====================================================

SELECT 
    e.equipment_name,
    oc.production_date,
    COUNT(DISTINCT oc.coil_id) AS coils_processed,
    SUM(oc.operation_duration_sec) / 3600.0 AS total_operation_hours,
    AVG(oc.operation_duration_sec) / 60.0 AS avg_operation_min
FROM 
    fact_coil_operation_cycle oc
INNER JOIN 
    dim_equipment e ON oc.equipment_id = e.equipment_id
WHERE 
    e.is_bottleneck_candidate = 1
    AND e.is_active = 1
GROUP BY 
    e.equipment_name, oc.production_date
ORDER BY 
    e.equipment_name, oc.production_date;

-- =====================================================
-- 10. Bottleneck Summary Report
-- =====================================================

WITH bottleneck_stats AS (
    SELECT 
        e.equipment_name,
        e.section,
        COUNT(DISTINCT oc.coil_id) AS total_coils,
        SUM(oc.operation_duration_sec) / 3600.0 AS total_hours,
        AVG(oc.operation_duration_sec) / 60.0 AS avg_operation_min
    FROM 
        fact_coil_operation_cycle oc
    INNER JOIN 
        dim_equipment e ON oc.equipment_id = e.equipment_id
    WHERE 
        e.is_bottleneck_candidate = 1
        AND e.is_active = 1
    GROUP BY 
        e.equipment_name, e.section
),
line_total AS (
    SELECT SUM(operation_duration_sec) / 3600.0 AS total_line_hours
    FROM fact_coil_operation_cycle
)
SELECT 
    bs.equipment_name,
    bs.section,
    bs.total_coils,
    bs.total_hours,
    bs.avg_operation_min,
    (bs.total_hours * 100.0 / lt.total_line_hours) AS share_of_line_pct,
    CASE 
        WHEN (bs.total_hours * 100.0 / lt.total_line_hours) > 15 THEN 'Critical'
        WHEN (bs.total_hours * 100.0 / lt.total_line_hours) > 10 THEN 'High'
        ELSE 'Moderate'
    END AS severity
FROM 
    bottleneck_stats bs
CROSS JOIN 
    line_total lt
ORDER BY 
    bs.total_hours DESC;
