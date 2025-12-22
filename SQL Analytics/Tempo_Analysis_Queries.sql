-- Tempo Analysis: Production Rate Optimization and Gap Analysis
-- ArcelorMittal Vanderbijlpark Works - Hot Rolling Plant Temper Line
-- Analysis Period: April-August 2024

-- =====================================================
-- 1. Daily Tempo Trend (Pieces per Hour)
-- =====================================================

-- Calculate daily production tempo with 7-day moving average
WITH daily_production AS (
    SELECT 
        production_date,
        COUNT(*) AS pieces_produced,
        MIN(completion_ts) AS first_completion,
        MAX(completion_ts) AS last_completion,
        DATEDIFF(HOUR, MIN(completion_ts), MAX(completion_ts)) AS production_hours
    FROM 
        fact_production_coil
    GROUP BY 
        production_date
),
daily_tempo AS (
    SELECT 
        production_date,
        pieces_produced,
        production_hours,
        CASE 
            WHEN production_hours > 0 THEN CAST(pieces_produced AS FLOAT) / production_hours
            ELSE 0
        END AS tempo_pieces_per_hour
    FROM 
        daily_production
)
SELECT 
    production_date,
    pieces_produced,
    production_hours,
    tempo_pieces_per_hour,
    AVG(tempo_pieces_per_hour) OVER (
        ORDER BY production_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS tempo_7day_ma
FROM 
    daily_tempo
ORDER BY 
    production_date;

-- =====================================================
-- 2. Inter-Coil Gap Distribution
-- =====================================================

-- Analyze gaps between consecutive coil completions
SELECT 
    CASE 
        WHEN gap_from_prev_completion_min < 2 THEN '0-2 min (Continuous)'
        WHEN gap_from_prev_completion_min < 5 THEN '2-5 min (Normal)'
        WHEN gap_from_prev_completion_min < 10 THEN '5-10 min (Gap)'
        WHEN gap_from_prev_completion_min < 20 THEN '10-20 min (Delay)'
        WHEN gap_from_prev_completion_min < 60 THEN '20-60 min (Extended)'
        ELSE '60+ min (Major)'
    END AS gap_category,
    COUNT(*) AS occurrence_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS DECIMAL(5,2)) AS pct_of_total,
    AVG(gap_from_prev_completion_min) AS avg_gap_min,
    MIN(gap_from_prev_completion_min) AS min_gap_min,
    MAX(gap_from_prev_completion_min) AS max_gap_min
FROM 
    fact_production_coil
WHERE 
    gap_from_prev_completion_min IS NOT NULL
GROUP BY 
    CASE 
        WHEN gap_from_prev_completion_min < 2 THEN '0-2 min (Continuous)'
        WHEN gap_from_prev_completion_min < 5 THEN '2-5 min (Normal)'
        WHEN gap_from_prev_completion_min < 10 THEN '5-10 min (Gap)'
        WHEN gap_from_prev_completion_min < 20 THEN '10-20 min (Delay)'
        WHEN gap_from_prev_completion_min < 60 THEN '20-60 min (Extended)'
        ELSE '60+ min (Major)'
    END
ORDER BY 
    MIN(gap_from_prev_completion_min);

-- =====================================================
-- 3. Tempo by Shift (Crew Performance Comparison)
-- =====================================================

SELECT 
    shift_code,
    COUNT(*) AS pieces_produced,
    AVG(total_cycle_time_min) AS avg_cycle_time_min,
    60.0 / AVG(total_cycle_time_min) AS implied_tempo_pcs_per_hour,
    AVG(gap_from_prev_completion_min) AS avg_gap_min,
    MIN(gap_from_prev_completion_min) AS min_gap_min,
    MAX(gap_from_prev_completion_min) AS max_gap_min,
    STDEV(gap_from_prev_completion_min) AS gap_std_dev
FROM 
    fact_production_coil
WHERE 
    shift_code IS NOT NULL
    AND gap_from_prev_completion_min IS NOT NULL
GROUP BY 
    shift_code
ORDER BY 
    implied_tempo_pcs_per_hour DESC;

-- =====================================================
-- 4. Shift Handover Loss Quantification
-- =====================================================

-- Identify gaps occurring at shift change times (06:00 and 18:00)
WITH shift_change_gaps AS (
    SELECT 
        coil_id,
        completion_ts,
        DATEPART(HOUR, completion_ts) AS completion_hour,
        gap_from_prev_completion_min,
        shift_code,
        CASE 
            WHEN DATEPART(HOUR, completion_ts) BETWEEN 6 AND 7 THEN 'Morning Handover (06:00-07:00)'
            WHEN DATEPART(HOUR, completion_ts) BETWEEN 18 AND 19 THEN 'Evening Handover (18:00-19:00)'
            ELSE 'Normal Operation'
        END AS period_type
    FROM 
        fact_production_coil
    WHERE 
        gap_from_prev_completion_min IS NOT NULL
)
SELECT 
    period_type,
    COUNT(*) AS occurrence_count,
    AVG(gap_from_prev_completion_min) AS avg_gap_min,
    MIN(gap_from_prev_completion_min) AS min_gap_min,
    MAX(gap_from_prev_completion_min) AS max_gap_min,
    AVG(gap_from_prev_completion_min) - (
        SELECT AVG(gap_from_prev_completion_min) 
        FROM fact_production_coil 
        WHERE gap_from_prev_completion_min IS NOT NULL
    ) AS gap_vs_baseline_min
FROM 
    shift_change_gaps
GROUP BY 
    period_type
ORDER BY 
    avg_gap_min DESC;

-- =====================================================
-- 5. Product Mix Impact on Tempo
-- =====================================================

WITH product_classification AS (
    SELECT 
        coil_id,
        type_code,
        is_prime,
        thickness_mm,
        width_mm,
        total_cycle_time_min,
        gap_from_prev_completion_min,
        CASE 
            WHEN thickness_mm <= 2.0 AND width_mm <= 1300 THEN 'Thin & Narrow (Fast)'
            WHEN thickness_mm > 3.0 AND width_mm > 1400 THEN 'Thick & Wide (Slow)'
            ELSE 'Standard Mix'
        END AS product_band
    FROM 
        fact_production_coil
)
SELECT 
    product_band,
    is_prime,
    COUNT(*) AS pieces_produced,
    AVG(total_cycle_time_min) AS avg_cycle_time_min,
    60.0 / AVG(total_cycle_time_min) AS implied_tempo_pcs_per_hour,
    AVG(gap_from_prev_completion_min) AS avg_gap_min,
    AVG(thickness_mm) AS avg_thickness_mm,
    AVG(width_mm) AS avg_width_mm
FROM 
    product_classification
GROUP BY 
    product_band, is_prime
ORDER BY 
    avg_cycle_time_min;

-- =====================================================
-- 6. Parent Coil Transition Efficiency
-- =====================================================

-- Analyze gaps between parent coils (CID transitions)
SELECT 
    CASE 
        WHEN gap_from_prev_parent_min < 1 THEN '0-1 min (Immediate)'
        WHEN gap_from_prev_parent_min < 2 THEN '1-2 min (Quick)'
        WHEN gap_from_prev_parent_min < 5 THEN '2-5 min (Normal)'
        WHEN gap_from_prev_parent_min < 10 THEN '5-10 min (Delayed)'
        WHEN gap_from_prev_parent_min < 20 THEN '10-20 min (Extended)'
        ELSE '20+ min (Major)'
    END AS parent_gap_category,
    COUNT(DISTINCT parent_coil_id) AS parent_coil_count,
    AVG(gap_from_prev_parent_min) AS avg_gap_min,
    MIN(gap_from_prev_parent_min) AS min_gap_min,
    MAX(gap_from_prev_parent_min) AS max_gap_min
FROM 
    fact_production_coil
WHERE 
    gap_from_prev_parent_min IS NOT NULL
GROUP BY 
    CASE 
        WHEN gap_from_prev_parent_min < 1 THEN '0-1 min (Immediate)'
        WHEN gap_from_prev_parent_min < 2 THEN '1-2 min (Quick)'
        WHEN gap_from_prev_parent_min < 5 THEN '2-5 min (Normal)'
        WHEN gap_from_prev_parent_min < 10 THEN '5-10 min (Delayed)'
        WHEN gap_from_prev_parent_min < 20 THEN '10-20 min (Extended)'
        ELSE '20+ min (Major)'
    END
ORDER BY 
    MIN(gap_from_prev_parent_min);

-- =====================================================
-- 7. Hourly Tempo Pattern (24-Hour View)
-- =====================================================

-- Production tempo by hour of day
SELECT 
    DATEPART(HOUR, completion_ts) AS hour_of_day,
    COUNT(*) AS pieces_produced,
    AVG(gap_from_prev_completion_min) AS avg_gap_min,
    60.0 / AVG(total_cycle_time_min) AS implied_tempo_pcs_per_hour,
    CASE 
        WHEN DATEPART(HOUR, completion_ts) BETWEEN 6 AND 17 THEN 'Day Shift'
        ELSE 'Night Shift'
    END AS shift_period
FROM 
    fact_production_coil
WHERE 
    gap_from_prev_completion_min IS NOT NULL
    AND total_cycle_time_min IS NOT NULL
GROUP BY 
    DATEPART(HOUR, completion_ts)
ORDER BY 
    hour_of_day;

-- =====================================================
-- 8. Weekly Tempo Trend (Day of Week Analysis)
-- =====================================================

SELECT 
    DATENAME(WEEKDAY, production_date) AS day_of_week,
    DATEPART(WEEKDAY, production_date) AS day_number,
    COUNT(DISTINCT production_date) AS date_count,
    SUM(CASE WHEN is_prime = 1 THEN 1 ELSE 0 END) AS prime_pieces,
    COUNT(*) AS total_pieces,
    AVG(total_cycle_time_min) AS avg_cycle_time_min,
    60.0 / AVG(total_cycle_time_min) AS implied_tempo_pcs_per_hour,
    AVG(gap_from_prev_completion_min) AS avg_gap_min
FROM 
    fact_production_coil
WHERE 
    total_cycle_time_min IS NOT NULL
GROUP BY 
    DATENAME(WEEKDAY, production_date),
    DATEPART(WEEKDAY, production_date)
ORDER BY 
    day_number;

-- =====================================================
-- 9. Prime vs Scrap Tempo Comparison
-- =====================================================

SELECT 
    is_prime,
    type_code,
    COUNT(*) AS pieces_produced,
    AVG(total_cycle_time_min) AS avg_cycle_time_min,
    60.0 / AVG(total_cycle_time_min) AS implied_tempo_pcs_per_hour,
    AVG(gap_from_prev_completion_min) AS avg_gap_min,
    STDEV(gap_from_prev_completion_min) AS gap_std_dev
FROM 
    fact_production_coil
WHERE 
    type_code IS NOT NULL
    AND total_cycle_time_min IS NOT NULL
GROUP BY 
    is_prime, type_code
ORDER BY 
    is_prime DESC, avg_cycle_time_min;

-- =====================================================
-- 10. Tempo Improvement Opportunities Summary
-- =====================================================

WITH tempo_metrics AS (
    SELECT 
        production_date,
        shift_code,
        COUNT(*) AS pieces_produced,
        AVG(total_cycle_time_min) AS avg_cycle_time_min,
        60.0 / AVG(total_cycle_time_min) AS tempo_pcs_per_hour,
        AVG(gap_from_prev_completion_min) AS avg_gap_min
    FROM 
        fact_production_coil
    WHERE 
        total_cycle_time_min IS NOT NULL
        AND gap_from_prev_completion_min IS NOT NULL
    GROUP BY 
        production_date, shift_code
),
tempo_percentiles AS (
    SELECT 
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY tempo_pcs_per_hour) OVER () AS p25_tempo,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY tempo_pcs_per_hour) OVER () AS p50_tempo,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY tempo_pcs_per_hour) OVER () AS p75_tempo,
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY tempo_pcs_per_hour) OVER () AS p90_tempo
    FROM 
        tempo_metrics
)
SELECT DISTINCT
    'Tempo Performance' AS metric_category,
    p25_tempo AS tempo_p25,
    p50_tempo AS tempo_median,
    p75_tempo AS tempo_p75,
    p90_tempo AS tempo_p90,
    p90_tempo - p25_tempo AS tempo_range,
    (p90_tempo - p50_tempo) * 12 AS potential_additional_pieces_per_shift,
    CAST((p90_tempo - p50_tempo) / p50_tempo * 100 AS DECIMAL(5,2)) AS potential_improvement_pct
FROM 
    tempo_percentiles;

-- =====================================================
-- 11. Monthly Tempo Comparison
-- =====================================================

SELECT 
    YEAR(production_date) AS year,
    MONTH(production_date) AS month,
    DATENAME(MONTH, production_date) AS month_name,
    COUNT(*) AS pieces_produced,
    COUNT(DISTINCT parent_coil_id) AS parent_coils_processed,
    AVG(total_cycle_time_min) AS avg_cycle_time_min,
    60.0 / AVG(total_cycle_time_min) AS implied_tempo_pcs_per_hour,
    AVG(gap_from_prev_completion_min) AS avg_gap_min,
    SUM(CASE WHEN is_prime = 1 THEN 1 ELSE 0 END) AS prime_pieces,
    CAST(SUM(CASE WHEN is_prime = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS prime_rate_pct
FROM 
    fact_production_coil
WHERE 
    total_cycle_time_min IS NOT NULL
GROUP BY 
    YEAR(production_date),
    MONTH(production_date),
    DATENAME(MONTH, production_date)
ORDER BY 
    year, month;

-- =====================================================
-- 12. Gap Outlier Detection
-- =====================================================

-- Identify unusually large gaps that impact tempo
WITH gap_stats AS (
    SELECT 
        AVG(gap_from_prev_completion_min) AS mean_gap,
        STDEV(gap_from_prev_completion_min) AS std_gap
    FROM 
        fact_production_coil
    WHERE 
        gap_from_prev_completion_min IS NOT NULL
)
SELECT 
    pc.coil_id,
    pc.parent_coil_id,
    pc.completion_ts,
    pc.shift_code,
    pc.type_code,
    pc.is_prime,
    pc.gap_from_prev_completion_min,
    gs.mean_gap,
    (pc.gap_from_prev_completion_min - gs.mean_gap) / gs.std_gap AS z_score,
    CASE 
        WHEN (pc.gap_from_prev_completion_min - gs.mean_gap) / gs.std_gap > 3 THEN 'Extreme Outlier'
        WHEN (pc.gap_from_prev_completion_min - gs.mean_gap) / gs.std_gap > 2 THEN 'Significant Outlier'
        ELSE 'Normal'
    END AS outlier_category
FROM 
    fact_production_coil pc
CROSS JOIN 
    gap_stats gs
WHERE 
    pc.gap_from_prev_completion_min IS NOT NULL
    AND (pc.gap_from_prev_completion_min - gs.mean_gap) / gs.std_gap > 2
ORDER BY 
    z_score DESC;
