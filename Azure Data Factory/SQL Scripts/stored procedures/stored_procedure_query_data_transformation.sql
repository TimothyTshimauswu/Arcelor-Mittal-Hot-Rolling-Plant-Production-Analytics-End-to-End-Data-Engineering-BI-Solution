-- =============================================
-- TRANSFORMATION & LOAD STORED PROCEDURES
-- =============================================
-- Purpose: Transform data from staging tables (NVARCHAR)
--          to production tables (proper data types)
-- =============================================



-- =============================================
-- STORED PROCEDURE 1: Load dim_equipment
-- =============================================
CREATE OR ALTER PROCEDURE usp_Load_dim_equipment
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Clear production table
        TRUNCATE TABLE dim_equipment;
        
        -- Transform and load
        INSERT INTO dim_equipment (
            equipment_id,
            equipment_name,
            section,
            process_order,
            is_bottleneck_candidate,
            is_active
        )
        SELECT 
            CAST(equipment_id AS INT) AS equipment_id,
            LTRIM(RTRIM(equipment_name)) AS equipment_name,
            LTRIM(RTRIM(section)) AS section,
            CAST(process_order AS INT) AS process_order,
            CASE 
                WHEN LOWER(LTRIM(RTRIM(is_bottleneck_candidate))) IN ('true', '1', 'yes') THEN 1
                ELSE 0 
            END AS is_bottleneck_candidate,
            CASE 
                WHEN LOWER(LTRIM(RTRIM(is_active))) IN ('true', '1', 'yes') THEN 1
                ELSE 0 
            END AS is_active
        FROM stg_dim_equipment
        WHERE equipment_id IS NOT NULL 
          AND equipment_id <> '';
        
        COMMIT TRANSACTION;
        
        PRINT 'dim_equipment loaded successfully. Rows: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO

-- =============================================
-- STORED PROCEDURE 2: Load dim_date_crew_schedule
-- =============================================
CREATE OR ALTER PROCEDURE usp_Load_dim_date_crew_schedule
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Clear production table
        TRUNCATE TABLE dim_date_crew_schedule;
        
        -- Transform and load
        INSERT INTO dim_date_crew_schedule (
            production_date,
            day_crew,
            night_crew,
            week_number,
            month_number,
            month_name,
            year_number
        )
        SELECT 
            CAST(production_date AS DATE) AS production_date,
            LTRIM(RTRIM(day_crew)) AS day_crew,
            LTRIM(RTRIM(night_crew)) AS night_crew,
            DATEPART(WEEK, CAST(production_date AS DATE)) AS week_number,
            MONTH(CAST(production_date AS DATE)) AS month_number,
            DATENAME(MONTH, CAST(production_date AS DATE)) AS month_name,
            YEAR(CAST(production_date AS DATE)) AS year_number
        FROM stg_dim_date_crew_schedule
        WHERE production_date IS NOT NULL 
          AND production_date <> ''
          AND TRY_CAST(production_date AS DATE) IS NOT NULL;
        
        COMMIT TRANSACTION;
        
        PRINT 'dim_date_crew_schedule loaded successfully. Rows: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO

-- =============================================
-- STORED PROCEDURE 3: Load fact_production_coil
-- =============================================
CREATE OR ALTER PROCEDURE usp_Load_fact_production_coil
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Clear production table (disable FK constraints temporarily)
        ALTER TABLE fact_coil_operation_cycle NOCHECK CONSTRAINT ALL;
        TRUNCATE TABLE fact_production_coil;
        
        -- Transform and load
        INSERT INTO fact_production_coil (
            coil_id,
            parent_coil_id,
            completion_ts,
            start_datetime,
            end_datetime,
            production_date,
            shift_code,
            type_code,
            is_prime,
            is_scrap,
            thickness_mm,
            width_mm,
            Grade,
            mass_out_tons,
            total_cycle_time_min,
            gap_from_prev_completion_min,
            gap_from_prev_parent_min
        )
        SELECT 
            LTRIM(RTRIM(coil_id)) AS coil_id,
            LTRIM(RTRIM(parent_coil_id)) AS parent_coil_id,
            TRY_CAST(completion_ts AS DATETIME2) AS completion_ts,
            TRY_CAST(start_datetime AS DATETIME2) AS start_datetime,
            TRY_CAST(end_datetime AS DATETIME2) AS end_datetime,
            TRY_CAST(production_date AS DATE) AS production_date,
            LTRIM(RTRIM(shift_code)) AS shift_code,
            LTRIM(RTRIM(type_code)) AS type_code,
            CASE 
                WHEN LOWER(LTRIM(RTRIM(is_prime))) IN ('true', '1', 'yes') THEN 1
                ELSE 0 
            END AS is_prime,
            CASE 
                WHEN LOWER(LTRIM(RTRIM(is_scrap))) IN ('true', '1', 'yes') THEN 1
                ELSE 0 
            END AS is_scrap,
            TRY_CAST(thickness_mm AS DECIMAL(5,2)) AS thickness_mm,
            TRY_CAST(width_mm AS DECIMAL(6,2)) AS width_mm,
            LTRIM(RTRIM(Grade)) AS Grade,
            TRY_CAST(mass_out_tons AS DECIMAL(8,3)) AS mass_out_tons,
            TRY_CAST(total_cycle_time_min AS DECIMAL(8,2)) AS total_cycle_time_min,
            TRY_CAST(gap_from_prev_completion_min AS DECIMAL(8,2)) AS gap_from_prev_completion_min,
            TRY_CAST(gap_from_prev_parent_min AS DECIMAL(8,2)) AS gap_from_prev_parent_min
        FROM stg_fact_production_coil
        WHERE coil_id IS NOT NULL 
          AND coil_id <> ''
          AND TRY_CAST(completion_ts AS DATETIME2) IS NOT NULL;
        
        -- Re-enable FK constraints
        ALTER TABLE fact_coil_operation_cycle CHECK CONSTRAINT ALL;
        
        COMMIT TRANSACTION;
        
        PRINT 'fact_production_coil loaded successfully. Rows: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        
        -- Re-enable constraints on error
        ALTER TABLE fact_coil_operation_cycle CHECK CONSTRAINT ALL;
        
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO

-- =============================================
-- STORED PROCEDURE 4: Load fact_maintenance_event
-- =============================================
CREATE OR ALTER PROCEDURE usp_Load_fact_maintenance_event
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Clear production table
        DELETE FROM fact_maintenance_event;
        
        -- Transform and load
        INSERT INTO fact_maintenance_event (
            equipment_name,
            start_datetime,
            duration_hours,
            duration_min,
            Category,
            [Delay Type],
            Area,
            Crew,
            Shifts,
            Hierachy,
            Decription
        )
        SELECT 
            LTRIM(RTRIM(equipment_name)) AS equipment_name,
            TRY_CAST(start_datetime AS DATETIME2) AS start_datetime,
            TRY_CAST(duration_hours AS DECIMAL(8,2)) AS duration_hours,
            TRY_CAST(duration_min AS DECIMAL(8,2)) AS duration_min,
            LTRIM(RTRIM(Category)) AS Category,
            LTRIM(RTRIM([Delay Type])) AS [Delay Type],
            LTRIM(RTRIM(Area)) AS Area,
            LTRIM(RTRIM(Crew)) AS Crew,
            LTRIM(RTRIM(Shifts)) AS Shifts,
            LTRIM(RTRIM(Hierachy)) AS Hierachy,
            LTRIM(RTRIM(Decription)) AS Decription
        FROM stg_fact_maintenance_event
        WHERE equipment_name IS NOT NULL 
          AND equipment_name <> ''
          AND TRY_CAST(start_datetime AS DATETIME2) IS NOT NULL;
        
        COMMIT TRANSACTION;
        
        PRINT 'fact_maintenance_event loaded successfully. Rows: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO

-- =============================================
-- STORED PROCEDURE 5: Load fact_coil_operation_cycle
-- =============================================
CREATE OR ALTER PROCEDURE usp_Load_fact_coil_operation_cycle
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Clear production table
        DELETE FROM fact_coil_operation_cycle;
        
        -- Transform and load
        INSERT INTO fact_coil_operation_cycle (
            coil_id,
            parent_coil_id,
            equipment_id,
            equipment_name,
            operation_start_ts,
            operation_end_ts,
            operation_duration_sec,
            shift_code,
            type_code,
            is_prime,
            is_scrap
        )
        SELECT 
            LTRIM(RTRIM(s.coil_id)) AS coil_id,
            LTRIM(RTRIM(s.parent_coil_id)) AS parent_coil_id,
            TRY_CAST(s.equipment_id AS INT) AS equipment_id,
            LTRIM(RTRIM(s.equipment_name)) AS equipment_name,
            TRY_CAST(s.operation_start_ts AS DATETIME2) AS operation_start_ts,
            TRY_CAST(s.operation_end_ts AS DATETIME2) AS operation_end_ts,
            TRY_CAST(s.operation_duration_sec AS DECIMAL(10,2)) AS operation_duration_sec,
            LTRIM(RTRIM(s.shift_code)) AS shift_code,
            LTRIM(RTRIM(s.type_code)) AS type_code,
            CASE 
                WHEN LOWER(LTRIM(RTRIM(s.is_prime))) IN ('true', '1', 'yes') THEN 1
                ELSE 0 
            END AS is_prime,
            CASE 
                WHEN LOWER(LTRIM(RTRIM(s.is_scrap))) IN ('true', '1', 'yes') THEN 1
                ELSE 0 
            END AS is_scrap
        FROM stg_fact_coil_operation_cycle s
        WHERE s.coil_id IS NOT NULL 
          AND s.coil_id <> ''
          AND TRY_CAST(s.equipment_id AS INT) IS NOT NULL
          -- Ensure FK to equipment exists
          AND EXISTS (
              SELECT 1 FROM dim_equipment e 
              WHERE e.equipment_id = TRY_CAST(s.equipment_id AS INT)
          )
          -- Ensure FK to production coil exists
          AND EXISTS (
              SELECT 1 FROM fact_production_coil p 
              WHERE p.coil_id = LTRIM(RTRIM(s.coil_id))
          );
        
        COMMIT TRANSACTION;
        
        PRINT 'fact_coil_operation_cycle loaded successfully. Rows: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO

-- =============================================
-- STORED PROCEDURE 6: Load fact_equipment_event_log
-- =============================================
CREATE OR ALTER PROCEDURE usp_Load_fact_equipment_event_log
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Clear production table
        DELETE FROM fact_equipment_event_log;
        
        -- Transform and load
        INSERT INTO fact_equipment_event_log (
            equipment_id,
            equipment_name,
            event_type,
            event_start_ts,
            event_end_ts,
            event_duration_sec,
            event_date,
            coil_id,
            parent_coil_id,
            shift_code,
            type_code,
            is_prime,
            is_scrap
        )
        SELECT 
            TRY_CAST(s.equipment_id AS INT) AS equipment_id,
            LTRIM(RTRIM(s.equipment_name)) AS equipment_name,
            LTRIM(RTRIM(s.event_type)) AS event_type,
            TRY_CAST(s.event_start_ts AS DATETIME2) AS event_start_ts,
            TRY_CAST(s.event_end_ts AS DATETIME2) AS event_end_ts,
            TRY_CAST(s.event_duration_sec AS DECIMAL(10,2)) AS event_duration_sec,
            TRY_CAST(s.event_date AS DATE) AS event_date,
            LTRIM(RTRIM(s.coil_id)) AS coil_id,
            LTRIM(RTRIM(s.parent_coil_id)) AS parent_coil_id,
            LTRIM(RTRIM(s.shift_code)) AS shift_code,
            LTRIM(RTRIM(s.type_code)) AS type_code,
            CASE 
                WHEN LOWER(LTRIM(RTRIM(s.is_prime))) IN ('true', '1', 'yes') THEN 1
                ELSE 0 
            END AS is_prime,
            CASE 
                WHEN LOWER(LTRIM(RTRIM(s.is_scrap))) IN ('true', '1', 'yes') THEN 1
                ELSE 0 
            END AS is_scrap
        FROM stg_fact_equipment_event_log s
        WHERE s.equipment_id IS NOT NULL 
          AND s.equipment_id <> ''
          AND TRY_CAST(s.equipment_id AS INT) IS NOT NULL
          -- Ensure FK to equipment exists
          AND EXISTS (
              SELECT 1 FROM dim_equipment e 
              WHERE e.equipment_id = TRY_CAST(s.equipment_id AS INT)
          );
        
        COMMIT TRANSACTION;
        
        PRINT 'fact_equipment_event_log loaded successfully. Rows: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO

-- =============================================
-- MASTER STORED PROCEDURE: Execute All Loads in Order
-- =============================================
CREATE OR ALTER PROCEDURE usp_Master_Load_All_Tables
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @StartTime DATETIME2 = GETDATE();
    DECLARE @ErrorOccurred BIT = 0;
    
    PRINT '========================================';
    PRINT 'STARTING MASTER DATA LOAD';
    PRINT 'Start Time: ' + CONVERT(VARCHAR(30), @StartTime, 120);
    PRINT '========================================';
    PRINT '';
    
    -- Load Dimension Tables First
    BEGIN TRY
        PRINT '1. Loading dim_equipment...';
        EXEC usp_Load_dim_equipment;
        PRINT 'SUCCESS';
        PRINT '';
    END TRY
    BEGIN CATCH
        PRINT 'ERROR in dim_equipment: ' + ERROR_MESSAGE();
        SET @ErrorOccurred = 1;
    END CATCH
    
    BEGIN TRY
        PRINT '2. Loading dim_date_crew_schedule...';
        EXEC usp_Load_dim_date_crew_schedule;
        PRINT 'SUCCESS';
        PRINT '';
    END TRY
    BEGIN CATCH
        PRINT 'ERROR in dim_date_crew_schedule: ' + ERROR_MESSAGE();
        SET @ErrorOccurred = 1;
    END CATCH
    
    -- Load Fact Tables (in dependency order)
    IF @ErrorOccurred = 0
    BEGIN
        BEGIN TRY
            PRINT '3. Loading fact_production_coil...';
            EXEC usp_Load_fact_production_coil;
            PRINT 'SUCCESS';
            PRINT '';
        END TRY
        BEGIN CATCH
            PRINT 'ERROR in fact_production_coil: ' + ERROR_MESSAGE();
            SET @ErrorOccurred = 1;
        END CATCH
        
        BEGIN TRY
            PRINT '4. Loading fact_maintenance_event...';
            EXEC usp_Load_fact_maintenance_event;
            PRINT 'SUCCESS';
            PRINT '';
        END TRY
        BEGIN CATCH
            PRINT 'ERROR in fact_maintenance_event: ' + ERROR_MESSAGE();
            SET @ErrorOccurred = 1;
        END CATCH
        
        BEGIN TRY
            PRINT '5. Loading fact_coil_operation_cycle...';
            EXEC usp_Load_fact_coil_operation_cycle;
            PRINT 'SUCCESS';
            PRINT '';
        END TRY
        BEGIN CATCH
            PRINT 'ERROR in fact_coil_operation_cycle: ' + ERROR_MESSAGE();
            SET @ErrorOccurred = 1;
        END CATCH
        
        BEGIN TRY
            PRINT '6. Loading fact_equipment_event_log...';
            EXEC usp_Load_fact_equipment_event_log;
            PRINT 'SUCCESS';
            PRINT '';
        END TRY
        BEGIN CATCH
            PRINT 'ERROR in fact_equipment_event_log: ' + ERROR_MESSAGE();
            SET @ErrorOccurred = 1;
        END CATCH
    END
    
    DECLARE @EndTime DATETIME2 = GETDATE();
    DECLARE @Duration INT = DATEDIFF(SECOND, @StartTime, @EndTime);
    
    PRINT '========================================';
    PRINT 'MASTER DATA LOAD COMPLETE';
    PRINT 'End Time: ' + CONVERT(VARCHAR(30), @EndTime, 120);
    PRINT 'Duration: ' + CAST(@Duration AS VARCHAR(10)) + ' seconds';
    
    IF @ErrorOccurred = 1
        PRINT 'STATUS: COMPLETED WITH ERRORS';
    ELSE
        PRINT 'STATUS: ALL TABLES LOADED SUCCESSFULLY';
    
    PRINT '========================================';
END;
GO

-- =============================================
-- VERIFY STORED PROCEDURES CREATED
-- =============================================
SELECT 
    name AS stored_procedure_name,
    create_date,
    modify_date
FROM sys.procedures
WHERE name LIKE 'usp_%'
ORDER BY name;
GO

PRINT 'All stored procedures created successfully!';
GO

SELECT 
       name AS stored_procedure_name,
       create_date,
       modify_date
   FROM sys.procedures
   WHERE name LIKE 'usp_%'
   ORDER BY name;
   
   -- Should show 7 procedures:
   -- usp_Load_dim_date_crew_schedule
   -- usp_Load_dim_equipment
   -- usp_Load_fact_coil_operation_cycle
   -- usp_Load_fact_equipment_event_log
   -- usp_Load_fact_maintenance_event
   -- usp_Load_fact_production_coil
   -- usp_Master_Load_All_Tables