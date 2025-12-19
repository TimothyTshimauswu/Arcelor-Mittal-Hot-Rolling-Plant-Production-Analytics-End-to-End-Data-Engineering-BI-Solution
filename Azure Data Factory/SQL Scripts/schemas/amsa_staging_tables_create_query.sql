-- =============================================
-- STAGING TABLES FOR HOT ROLLING PLANT ANALYTICS
-- =============================================
-- Purpose: Match CSV file schemas exactly (all NVARCHAR)
-- Then transform and load into production tables


-- =============================================
-- DROP STAGING TABLES IF EXIST (for clean deployment)
-- =============================================
IF OBJECT_ID('stg_dim_equipment', 'U') IS NOT NULL DROP TABLE stg_dim_equipment;
IF OBJECT_ID('stg_dim_date_crew_schedule', 'U') IS NOT NULL DROP TABLE stg_dim_date_crew_schedule;
IF OBJECT_ID('stg_fact_production_coil', 'U') IS NOT NULL DROP TABLE stg_fact_production_coil;
IF OBJECT_ID('stg_fact_maintenance_event', 'U') IS NOT NULL DROP TABLE stg_fact_maintenance_event;
IF OBJECT_ID('stg_fact_coil_operation_cycle', 'U') IS NOT NULL DROP TABLE stg_fact_coil_operation_cycle;
IF OBJECT_ID('stg_fact_equipment_event_log', 'U') IS NOT NULL DROP TABLE stg_fact_equipment_event_log;
GO

-- =============================================
-- STAGING TABLE 1: dim_equipment
-- =============================================
-- CSV Schema: All columns are String type
CREATE TABLE stg_dim_equipment (
    equipment_id NVARCHAR(50),
    equipment_name NVARCHAR(200),
    process_order NVARCHAR(50),
    section NVARCHAR(50),
    equipment_type NVARCHAR(100),
    is_bottleneck_candidate NVARCHAR(10),
    is_active NVARCHAR(10),
    loaded_date DATETIME2 DEFAULT GETDATE()
);
GO

-- =============================================
-- STAGING TABLE 2: dim_date_crew_schedule
-- =============================================
-- CSV Schema: production_date, day_crew, night_crew (all String)
CREATE TABLE stg_dim_date_crew_schedule (
    production_date NVARCHAR(50),
    day_crew NVARCHAR(10),
    night_crew NVARCHAR(10),
    loaded_date DATETIME2 DEFAULT GETDATE()
);
GO

-- =============================================
-- STAGING TABLE 3: fact_production_coil
-- =============================================
-- CSV Schema: 21 columns, all String type
CREATE TABLE stg_fact_production_coil (
    coil_id NVARCHAR(100),
    parent_coil_id NVARCHAR(100),
    production_date NVARCHAR(50),
    completion_ts NVARCHAR(50),
    shift_code NVARCHAR(10),
    thickness_mm NVARCHAR(50),
    width_mm NVARCHAR(50),
    mass_out_tons NVARCHAR(50),
    Hours NVARCHAR(50),
    Grade NVARCHAR(100),
    NextProcess NVARCHAR(100),
    type_code NVARCHAR(50),
    is_prime NVARCHAR(10),
    is_scrap NVARCHAR(10),
    gap_from_prev_completion_min NVARCHAR(50),
    gap_from_prev_parent_min NVARCHAR(50),
    Cast NVARCHAR(100),
    Slab NVARCHAR(100),
    start_datetime NVARCHAR(50),
    end_datetime NVARCHAR(50),
    total_cycle_time_min NVARCHAR(50),
    loaded_date DATETIME2 DEFAULT GETDATE()
);
GO

-- =============================================
-- STAGING TABLE 4: fact_maintenance_event
-- =============================================
-- CSV Schema: 15 columns, all String type
CREATE TABLE stg_fact_maintenance_event (
    start_datetime NVARCHAR(50),
    duration_hours NVARCHAR(50),
    duration_min NVARCHAR(50),
    equipment_name NVARCHAR(200),
    Crew NVARCHAR(100),
    Shifts NVARCHAR(100),
    Category NVARCHAR(200),
    [Delay Type] NVARCHAR(200),
    Area NVARCHAR(100),
    [Sub Area] NVARCHAR(100),
    Hierachy NVARCHAR(200),
    Decription NVARCHAR(1000),
    [Day] NVARCHAR(50),
    Reasponsible NVARCHAR(200),  -- Note: Typo in source
    Responsible NVARCHAR(200),
    loaded_date DATETIME2 DEFAULT GETDATE()
);
GO

-- =============================================
-- STAGING TABLE 5: fact_coil_operation_cycle
-- =============================================
-- CSV Schema: 14 columns, all String type
CREATE TABLE stg_fact_coil_operation_cycle (
    coil_id NVARCHAR(100),
    parent_coil_id NVARCHAR(100),
    equipment_id NVARCHAR(50),
    equipment_name NVARCHAR(200),
    production_date NVARCHAR(50),
    shift_code NVARCHAR(10),
    operation_start_ts NVARCHAR(50),
    operation_end_ts NVARCHAR(50),
    operation_duration_sec NVARCHAR(50),
    queue_time_sec NVARCHAR(50),
    is_bottleneck_step NVARCHAR(10),
    type_code NVARCHAR(50),
    is_prime NVARCHAR(10),
    is_scrap NVARCHAR(10),
    loaded_date DATETIME2 DEFAULT GETDATE()
);
GO

-- =============================================
-- STAGING TABLE 6: fact_equipment_event_log
-- =============================================
-- CSV Schema: 13 columns, all String type
CREATE TABLE stg_fact_equipment_event_log (
    equipment_id NVARCHAR(50),
    equipment_name NVARCHAR(200),
    event_type NVARCHAR(50),
    event_start_ts NVARCHAR(50),
    event_end_ts NVARCHAR(50),
    event_duration_sec NVARCHAR(50),
    coil_id NVARCHAR(100),
    parent_coil_id NVARCHAR(100),
    shift_code NVARCHAR(10),
    type_code NVARCHAR(50),
    is_prime NVARCHAR(10),
    is_scrap NVARCHAR(10),
    event_date NVARCHAR(50),
    loaded_date DATETIME2 DEFAULT GETDATE()
);
GO

-- =============================================
-- VERIFY STAGING TABLES CREATED
-- =============================================
SELECT 
    TABLE_NAME,
    'Staging' AS table_type
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_NAME LIKE 'stg_%'
ORDER BY TABLE_NAME;
GO

PRINT 'All staging tables created successfully!';
GO


   
   SELECT TABLE_NAME 
   FROM INFORMATION_SCHEMA.TABLES 
   WHERE TABLE_NAME LIKE 'stg_%'
   ORDER BY TABLE_NAME;
   
   -- Should show 6 staging tables:
   -- stg_dim_date_crew_schedule
   -- stg_dim_equipment
   -- stg_fact_coil_operation_cycle
   -- stg_fact_equipment_event_log
   -- stg_fact_maintenance_event
   -- stg_fact_production_coil