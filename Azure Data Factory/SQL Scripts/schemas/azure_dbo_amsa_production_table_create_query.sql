-- =============================================
-- Create dim_equipment
-- =============================================
CREATE TABLE dim_equipment (
    equipment_id INT PRIMARY KEY,
    equipment_name NVARCHAR(100) NOT NULL,
    section NVARCHAR(20),
    process_order INT,
    is_bottleneck_candidate BIT,
    is_active BIT,
    created_date DATETIME2 DEFAULT GETDATE()
);

-- =============================================
-- Create dim_date_crew_schedule
-- =============================================
CREATE TABLE dim_date_crew_schedule (
    production_date DATE PRIMARY KEY,
    day_crew CHAR(1),
    night_crew CHAR(1),
    week_number INT,
    month_number INT,
    month_name NVARCHAR(20),
    year_number INT,
    created_date DATETIME2 DEFAULT GETDATE()
);


-- =============================================
-- Create fact_production_coil
-- =============================================
CREATE TABLE fact_production_coil (
    coil_id NVARCHAR(50) PRIMARY KEY,
    parent_coil_id NVARCHAR(50),
    completion_ts DATETIME2,
    start_datetime DATETIME2,
    end_datetime DATETIME2,
    production_date DATE,
    shift_code CHAR(1),
    type_code NVARCHAR(10),
    is_prime BIT,
    is_scrap BIT,
    thickness_mm DECIMAL(5,2),
    width_mm DECIMAL(6,2),
    Grade NVARCHAR(50),
    mass_out_tons DECIMAL(8,3),
    total_cycle_time_min DECIMAL(8,2),
    gap_from_prev_completion_min DECIMAL(8,2),
    gap_from_prev_parent_min DECIMAL(8,2),
    created_date DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (production_date) REFERENCES dim_date_crew_schedule(production_date)
);

-- =============================================
-- Create fact_maintenance_event
-- =============================================
CREATE TABLE fact_maintenance_event (
    event_id INT IDENTITY(1,1) PRIMARY KEY,
    equipment_name NVARCHAR(100),
    start_datetime DATETIME2,
    duration_hours DECIMAL(8,2),
    duration_min DECIMAL(8,2),
    Category NVARCHAR(100),
    [Delay Type] NVARCHAR(100),
    Area NVARCHAR(100),
    Crew NVARCHAR(50),
    Shifts NVARCHAR(50),
    Hierachy NVARCHAR(100),
    Decription NVARCHAR(500),
    created_date DATETIME2 DEFAULT GETDATE()
);

-- =============================================
-- Create fact_coil_operation_cycle
-- =============================================
CREATE TABLE fact_coil_operation_cycle (
    operation_id INT IDENTITY(1,1) PRIMARY KEY,
    coil_id NVARCHAR(50),
    parent_coil_id NVARCHAR(50),
    equipment_id INT,
    equipment_name NVARCHAR(100),
    operation_start_ts DATETIME2,
    operation_end_ts DATETIME2,
    operation_duration_sec DECIMAL(10,2),
    shift_code CHAR(1),
    type_code NVARCHAR(10),
    is_prime BIT,
    is_scrap BIT,
    created_date DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (equipment_id) REFERENCES dim_equipment(equipment_id),
    FOREIGN KEY (coil_id) REFERENCES fact_production_coil(coil_id)
);

-- =============================================
-- Create fact_equipment_event_log
-- =============================================
CREATE TABLE fact_equipment_event_log (
    log_id INT IDENTITY(1,1) PRIMARY KEY,
    equipment_id INT,
    equipment_name NVARCHAR(100),
    event_type NVARCHAR(20), -- RUN, IDLE, FAULT
    event_start_ts DATETIME2,
    event_end_ts DATETIME2,
    event_duration_sec DECIMAL(10,2),
    event_date DATE,
    coil_id NVARCHAR(50),
    parent_coil_id NVARCHAR(50),
    shift_code CHAR(1),
    type_code NVARCHAR(10),
    is_prime BIT,
    is_scrap BIT,
    created_date DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (equipment_id) REFERENCES dim_equipment(equipment_id)
);


-- =============================================
-- Create Indexes
-- =============================================

-- Indexes on fact_production_coil
CREATE INDEX IX_fact_production_coil_date ON fact_production_coil(production_date);
CREATE INDEX IX_fact_production_coil_shift ON fact_production_coil(shift_code);
CREATE INDEX IX_fact_production_coil_type ON fact_production_coil(type_code);
CREATE INDEX IX_fact_production_coil_parent ON fact_production_coil(parent_coil_id);

-- Indexes on fact_coil_operation_cycle
CREATE INDEX IX_fact_coil_operation_equipment ON fact_coil_operation_cycle(equipment_id);
CREATE INDEX IX_fact_coil_operation_coil ON fact_coil_operation_cycle(coil_id);
CREATE INDEX IX_fact_coil_operation_parent ON fact_coil_operation_cycle(parent_coil_id);

-- Indexes on fact_equipment_event_log
CREATE INDEX IX_fact_equipment_event_equipment ON fact_equipment_event_log(equipment_id);
CREATE INDEX IX_fact_equipment_event_type ON fact_equipment_event_log(event_type);
CREATE INDEX IX_fact_equipment_event_date ON fact_equipment_event_log(event_date);

-- Indexes on fact_maintenance_event
CREATE INDEX IX_fact_maintenance_equipment ON fact_maintenance_event(equipment_name);
CREATE INDEX IX_fact_maintenance_datetime ON fact_maintenance_event(start_datetime);


-- Check all tables
SELECT TABLE_NAME 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME;

-- Should see:
-- dim_date_crew_schedule
-- dim_equipment
-- fact_coil_operation_cycle
-- fact_equipment_event_log
-- fact_maintenance_event
-- fact_production_coil
