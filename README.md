<p align="center">
  <img src="assets/project_banner.png" width="100%" alt="Arcelor Mittal Steel Production Data Engineering Pipeline and Operations Analytics Banner">
</p>

<p align="center">

#Arcelor Mittal Hot Rolling Plant Production  Analytics: End-to-End Data Engineering & BI Solution

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)
[![Azure](https://img.shields.io/badge/Azure-Data_Factory-0078D4.svg)](https://azure.microsoft.com/)
[![SQL](https://img.shields.io/badge/SQL-Azure_SQL-CC2927.svg)](https://azure.microsoft.com/en-us/products/azure-sql/database/)
[![Power BI](https://img.shields.io/badge/Power_BI-Dashboards-F2C811.svg)](https://powerbi.microsoft.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

> **Real-world industrial data engineering project that delivered a 10% tempo improvement and 5% reduction in maintenance downtime, enabling consistent achievement of monthly production targets at ArcelorMittal Vanderbijlpark Works.**

---

## 📊 **Business Impact at a Glance**

| Metric | Result | Business Value |
|--------|--------|---------------|
| **Tempo Improvement** | ~10% increase | Identified and resolved bottlenecks through data-driven insights |
| **Maintenance Downtime** | ~5% reduction | Targeted problem equipment with predictive maintenance triggers |
| **Shift Optimization** | 4-shift → 3-shift recommendation | Reduced shift handover losses by 15+ minutes |
| **Production Targets** | Consistently met/exceeded | First time achieving monthly targets post-Saldanha closure |
| **Manual Reporting** | 40% reduction | Automated weekly performance reporting for management |
| **ROI** | Measurable gains | Enabled 30% production increase without capital investment |

---

## **Problem Statement**

### **The Business Challenge**

After the closure of ArcelorMittal's Saldanha Works, **all thin flat products were redirected to Vanderbijlpark**, increasing monthly production targets by **30%**. The temper line—the final step before dispatch—was:

- Not originally designed for thin flat products
- Operating without historical data for the new product mix
- Experiencing falling tempo (throughput)
- Suffering from frequent breakdowns and inconsistent performance
- Missing monthly production targets
- Unable to explain delays through operator feedback alone

**Management needed answers:** What's slowing down the line? Which equipment is the bottleneck? Why do shifts vary in performance? How can we hit our targets?

### **The Data Engineering Solution**

I built a complete **end-to-end data engineering and analytics pipeline** that:
1. Extracted production and maintenance data from multiple sources
2. Engineered features to model equipment-level coil processing cycles
3. Deployed scalable pipelines on **Azure Data Factory**
4. Stored transformed data in **Azure SQL Database** for analytics
5. Built **Power BI dashboards** that became the primary weekly performance tool

---

## **Architecture Overview**

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES (AMSA)                             │
├─────────────────────────────────────────────────────────────────────────┤
│  • Production Data (MES System - 13,575 records)                        │
│  • Maintenance Downtime Logs (1,450 events, 113 equipment types)        │
│  • Level-1 Encoder Signals (Equipment activity - synthetic)             │
└────────────────────────────┬────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                   PYTHON DATA ENGINEERING PIPELINE                      │
├─────────────────────────────────────────────────────────────────────────┤
│  1. Data Extraction & Validation                                        │
│     └─ Load CSV, handle encoding (UTF-8-sig), parse timestamps          │
│                                                                         │
│  2. Data Cleaning & Transformation                                      │
│     ├─ Filter date ranges (April-August 2024)                           │
│     ├─ Clean equipment names (remove trailing patterns)                 │
│     ├─ Parse maintenance durations (hours/minutes)                      │
│     └─ Handle missing values and outliers                               │
│                                                                         │
│  3. Feature Engineering                                                 │
│     ├─ Prime vs Scrap classification (HL/HM/98 vs HX/HY/HZ)             │
│     ├─ Parent-child coil relationships (CID → UID mapping)              │
│     ├─ Gap analysis (tempo measurement between coils)                   │
│     ├─ Product mix complexity scoring                                   │
│     └─ Bottleneck identification (6 critical equipment pieces)          │
│                                                                         │
│  4. Synthetic Cycle Time Modeling                                       │
│     ├─ Anchor operations to REAL MES completion timestamps              │
│     ├─ Work backwards to build the equipment operation timeline         │
│     ├─ Apply product-specific multipliers (thin vs thick)               │
│     ├─ Factor in shift performance (A/B/C/D crews)                      │
│     └─ Generate RUN/IDLE/FAULT event sequences                          │
│                                                                         │
│  5. Dimensional Modeling (Star Schema)                                  │
│     ├─ dim_equipment (17 production pieces, process order)              │
│     ├─ dim_date_crew_schedule (4-crew rotation, day/night)              │
│     ├─ fact_production_coil (real completion times + gaps)              │
│     ├─ fact_maintenance_event (parsed downtime events)                  │
│     ├─ fact_coil_operation_cycle (equipment-level operations)           │
│     └─ fact_equipment_event_log (RUN/IDLE/FAULT timeline)               │
│                                                                          │
│  OUTPUT: 8 CSV files (6 engineered + 2 raw reference)                   │
└────────────────────────────┬────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      AZURE DATA FACTORY (ADF)                           │
├─────────────────────────────────────────────────────────────────────────┤
│  • Copy Data Activities (CSV → Azure SQL)                               │
│  • Data Flow Transformations (schema mapping)                           │
│  • Pipeline Orchestration (scheduled runs)                              │
│  • Incremental Load Strategy (append new data)                          │
│  • Error Handling & Logging                                             │
└────────────────────────────┬────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      AZURE SQL DATABASE                                 │
├─────────────────────────────────────────────────────────────────────────┤
│  • Star Schema Implementation                                           │
│  • Indexed for Query Performance                                        │
│  • SQL Script for Analysis                                              │                        
└────────────────────────────┬────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         POWER BI DASHBOARDS                              │
├─────────────────────────────────────────────────────────────────────────┤
│  1. Executive Summary     → High-level KPIs for management              │
│  2. Bottleneck Analysis   → Equipment constraints & time contributions   │
│  3. Maintenance & Downtime → MTBF, MTTR, reliability tracking            │
│  4. Shift Performance     → Crew comparison & handover analysis          │
│  5. Product Mix & Tempo   → How product characteristics affect flow      │
│                                                                          │
│  • 150+ DAX Measures                                                     │
│  • Real-time refresh (hourly during production)                          │
│  • Mobile-optimized layouts                                              │
│  • Drill-through pages for deep-dive analysis                            │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 🔧 **Technical Implementation**

### **Phase 1: Python Data Engineering Pipeline**

**Technologies:** Python 3.9, Pandas, NumPy

#### **Key Engineering Achievements:**

1. **Data Integration from Multiple Sources**
   - Production data: 13,575 coil records from MES system
   - Maintenance data: 1,450 downtime events across 113 equipment types
   - Equipment metadata: 17 production equipment pieces with process sequencing

2. **Advanced Feature Engineering**
   - **Prime/Scrap Classification:** Automated categorization (HL/HM/98 vs HX/HY/HZ)
   - **Parent-Child Coil Tracking:** Mapped CID (parent) → UID (output pieces) relationships
   - **Tempo Analysis:** Calculated gaps between completions to measure line throughput
   - **Bottleneck Identification:** Flagged 6 critical equipment pieces consuming 42% of line time

3. **Synthetic Cycle Time Modeling** **(Key Innovation)**
   - **Challenge:** No equipment-level encoder data available (NDA restrictions)
   - **Solution:** Built a synthetic operation timeline anchored to REAL MES completion timestamps
   - **Methodology:**
     - Work backwards from actual completion time
     - Apply product-specific duration multipliers (thin: 0.5-0.7×, thick: 1.1-1.3×)
     - Factor in shift performance variations (Shift A: 1.05×, C: 0.95×)
     - Generate equipment operation start/end times per coil
     - Validate: Synthetic end_datetime matches real completion_ts (error: <1 second)

4. **Dimensional Data Modeling**
   - Star schema design with 2 dimension tables + 4 fact tables
   - Optimized for analytical queries (OLAP)
   - Enables slice-and-dice analysis by date, shift, equipment, product type

#### **Pipeline Scripts:**

```python
# Example: Core pipeline structure
1. load_data_updated.py              # Data extraction & validation
2. filter_april_august.py            # Date range filtering
3. clean_subarea.py                  # Equipment name standardization
4. build_dim_equipment.py            # Equipment dimension with process order
5. build_fact_production_coil.py     # Production facts with real timestamps
6. build_fact_maintenance_event.py   # Maintenance event parsing
7. build_crew_rotation.py            # 4-crew shift schedule (A/B/C/D)
8. duration_queue_logic.py           # Product-specific cycle time rules
9. generate_operations_anchored.py   # Synthetic operations (KEY INNOVATION)
10. build_equipment_event_log.py     # RUN/IDLE/FAULT timeline generation
11. clean_gaps.py                    # Outlier removal & gap analysis
12. validation_analysis.py           # Data quality checks
13. export_tables.py                 # Export to 8 CSV files
```

**Output:** 8 production-ready CSV files (6 engineered + 2 raw reference)

---
<p align="center">
  <img src="assets/Master_Load_Pipeline.png" width="100%" alt="Arcelor Mittal Steel Production Data Engineering Pipeline Azure Data Factory">
</p>

<p align="center">

### **Phase 2: Azure Data Factory Deployment**

**Technologies:** Azure Data Factory, Azure SQL Database, T-SQL

#### **Data Pipeline Architecture:**

1. **Source Configuration**
   - Blob Storage: Staged CSV files from Python pipeline
   - Linked Services: Secure connection to Azure SQL Database
   - Datasets: Schema definitions for all 8 tables

2. **Copy Data Activities**
   - **Production Data Pipeline:** `fact_production_coil` (13,575 rows)
   - **Maintenance Pipeline:** `fact_maintenance_event` (1,450 rows)
   - **Equipment Cycles Pipeline:** `fact_coil_operation_cycle` (230,775 operations)
   - **Equipment Events Pipeline:** `fact_equipment_event_log` (RUN/IDLE/FAULT)
   - **Dimension Tables:** `dim_equipment`, `dim_date_crew_schedule`

3. **Data Transformation in ADF**
   - Schema mapping (CSV → SQL)
   - Data type conversions (datetime, decimal, boolean)
   - Null handling strategies
   - Incremental load logic (append-only for fact tables)

4. **Orchestration & Scheduling**
   - Trigger: Time-based (hourly during production hours: 06:00-18:00)
   - Dependencies: Sequential execution (dimensions → facts)
   - Error handling: Retry logic, failure notifications
   - Logging: Activity monitoring, performance metrics

5. **SQL Database Design**
   ```sql
   -- Star schema in Azure SQL Database
   
   -- Dimension Tables
   CREATE TABLE dim_equipment (
       equipment_id INT PRIMARY KEY,
       equipment_name NVARCHAR(100),
       section NVARCHAR(20),
       process_order INT,
       is_bottleneck_candidate BIT,
       is_active BIT
   );
   
   -- Fact Tables
   CREATE TABLE fact_production_coil (
       coil_id NVARCHAR(50) PRIMARY KEY,
       parent_coil_id NVARCHAR(50),
       completion_ts DATETIME2,
       production_date DATE,
       shift_code CHAR(1),
       type_code NVARCHAR(10),
       is_prime BIT,
       is_scrap BIT,
       thickness_mm DECIMAL(5,2),
       width_mm DECIMAL(6,2),
       mass_out_tons DECIMAL(8,3),
       total_cycle_time_min DECIMAL(8,2),
       gap_from_prev_completion_min DECIMAL(8,2),
       gap_from_prev_parent_min DECIMAL(8,2),
       FOREIGN KEY (production_date) REFERENCES dim_date_crew_schedule(production_date)
   );
   
   -- Additional fact tables: maintenance, cycles, events, fact_production_coil
   -- Indexed on: equipment_id, production_date, shift_code, type_code
   ```

6. **Performance Optimization**
   - Clustered indexes on primary keys
   - Non-clustered indexes on foreign keys and filter columns
   - Columnstore indexes for analytical queries
   - Query performance monitoring

---

### **Phase 3: SQL Analytics Layer**

**Technologies:** T-SQL, Azure SQL Database, Stored Procedures

#### **Analytical Views & Queries:**

```sql
-- Example: Bottleneck Analysis View
CREATE VIEW vw_equipment_bottleneck_analysis AS
SELECT 
    e.equipment_name,
    e.section,
    e.is_bottleneck_candidate,
    COUNT(DISTINCT c.coil_id) AS total_coils_processed,
    SUM(c.operation_duration_sec) / 3600.0 AS total_operation_hours,
    AVG(c.operation_duration_sec) / 60.0 AS avg_operation_time_min,
    SUM(c.operation_duration_sec) / SUM(SUM(c.operation_duration_sec)) 
        OVER () * 100 AS time_share_pct
FROM dim_equipment e
JOIN fact_coil_operation_cycle c ON e.equipment_id = c.equipment_id
WHERE e.is_active = 1
GROUP BY e.equipment_name, e.section, e.is_bottleneck_candidate
ORDER BY time_share_pct DESC;
```
---
---
### **Phase 4: Power BI Dashboards**

**Technologies:** Power BI Desktop, DAX, Power Query

#### **5 Production Dashboards:**

1. **Executive Summary**
   - KPIs: Total Pieces, Prime Rate %, Tempo, Equipment Utilization
   - Charts: Daily tempo trend, product mix, shift comparison
   - Alerts: Top 5 equipment issues

2. **Bottleneck Analysis**
   - Equipment time contribution waterfall
   - Bottleneck severity matrix
   - Operation vs. Idle Time Scatter Plot

3. **Maintenance & Downtime**
   - MTBF, MTTR reliability metrics
   - Downtime by equipment (top 10)
   - MTBF vs MTTR quadrant analysis

4. **Shift Performance**
   - 4 shift comparison cards (A, B, C, D)
   - Shift efficiency trends over time
   - Handover loss analysis (15+ min identified)

5. **Product Mix & Tempo**
   - Width vs thickness scatter (cycle time bubbles)
   - Cycle time distribution by product type
   - Parent coil yield analysis


#### **DAX Measures (150+ Total):**

```dax
// Example: Tempo Target Achievement
Tempo Achievement % = 
VAR CurrentTempo = [Pieces per Hour]
VAR Target = [Tempo Target]
RETURN
    DIVIDE(CurrentTempo, Target, 0) * 100

// Example: Equipment Bottleneck Score
Equipment Bottleneck Score = 
VAR EquipmentOpsTime = SUM(fact_coil_operation_cycle[operation_duration_sec])
VAR TotalOpsTime = 
    CALCULATE(
        SUM(fact_coil_operation_cycle[operation_duration_sec]),
        ALL(dim_equipment)
    )
VAR TimeShare = DIVIDE(EquipmentOpsTime, TotalOpsTime, 0)
VAR IsBottleneck = MAX(dim_equipment[is_bottleneck_candidate])
RETURN
    IF(IsBottleneck, TimeShare * 100, TimeShare * 50)

```

---

## 📈 **Business Impact & Results**

### **Quantified Improvements**

| Area | Metric | Improvement | How Achieved |
|------|--------|-------------|--------------|
| **Production Throughput** | Tempo (pcs/hr) | **~10% increase** | Identified and resolved 3 critical bottlenecks (Temper Mill, Exit Coil Car, Decoiler) |
| **Equipment Reliability** | Maintenance Downtime | **~5% reduction** | Targeted maintenance on problem equipment, MTBF/MTTR tracking |
| **Operational Efficiency** | Shift Handover Loss | **Reduced by 15+ min** | Data-driven recommendation: 4-shift → 3-shift pattern |
| **Production Targets** | Monthly Target Achievement | **Consistently met/exceeded** | First time achieving targets post-Saldanha closure (30% increase) |
| **Reporting Efficiency** | Manual Reporting Time | **40% reduction** | Automated weekly performance dashboards replaced manual Excel reports |
| **Decision Making** | Time to Identify Issues | **Real-time vs weekly** | Live dashboards enable immediate action on equipment failures |

### **Strategic Recommendations Implemented**

Based on data insights, I provided the following recommendations that were acted upon:

1. ✅ **Move to 3-shift pattern** → Reduced shift handover losses
2. ✅ **Bi-weekly planned maintenance shutdowns** → Proactive vs reactive maintenance
3. ✅ **Automate exit-zone handling** → Reduced manual coil handling delays
4. ✅ **Add temporary offloading personnel** → Addressed exit bottleneck
5. 📋 **Build predictive maintenance models** → Roadmap for next phase
6. 📋 **Integrate live PLC/SCADA signals** → Real-time cycle tracking (future enhancement)

### **User Adoption & Impact**

- **Daily Active Users:** Plant Manager, Production Manager, 4 Shift Supervisors, Maintenance Manager, Process Engineers
- **Usage Pattern:** Dashboard became the **primary weekly performance review tool**
- **Feedback:** "Finally we can see what's really happening on the line" - Production Manager
- **Business Outcome:** Performance-based incentives introduced due to improved operational consistency

---

## 🗂️ **Project Structure**

```
hot-rolling-plant-analytics/
│
├── data/                                    # Data files (sanitized for portfolio)
│   ├── raw/
│   │   ├── coil_production_april_august_2024.csv
│   │   └── maintenance_downtime_jna_Oct_2024.csv
│   └── processed/
│       ├── dim_equipment.csv
│       ├── dim_date_crew_schedule.csv
│       ├── fact_production_coil.csv
│       ├── fact_maintenance_event.csv
│       ├── fact_coil_operation_cycle.csv
│       └── fact_equipment_event_log.csv
│
├── python_pipeline/                         # Python ETL scripts
│   ├── 01_load_data_updated.py
│   ├── 02_filter_april_august.py
│   ├── 03_clean_subarea.py
│   ├── 04_build_dim_equipment.py
│   ├── 05_build_fact_production_coil.py
│   ├── 06_build_fact_maintenance_event.py
│   ├── 07_build_crew_rotation.py
│   ├── 08_duration_queue_logic.py
│   ├── 09_generate_operations_anchored.py
│   ├── 10_build_equipment_event_log.py
│   ├── 11_clean_gaps.py
│   ├── 12_validation_analysis.py
│   └── 13_export_tables.py
│
├── azure_data_factory/                      # ADF pipeline definitions
│   ├── pipelines/
│   │   ├── pipeline_load_production_to_staging.json
│   │   ├── pipeline_load_maintenance_to_staging.json
│   │   └── pipeline_load_equipment_cycles_to_staging.json
│   ├── datasets/
│   │   ├── dataset_csv_source.json
│   │   └── dataset_sql_sink.json     
│
├── sql/                                     # SQL scripts
│   ├── schema/
│   │   ├── amsa_staging_tables_create_query.sql
│   │   ├── azure_dbo_amsa_production_tables_create_query.sql
│   │   └── create_indexes.sql
│   └── stored_procedures/
│       ├── sp_staging_to_production_transformation.sql
│
├── powerbi/                                 # Power BI files
│   ├── dashboards/
│   │   ├── dashboard_1_executive_summary.pbix
│   │   ├── dashboard_2_bottleneck_analysis.pbix
│   │   ├── dashboard_3_maintenance_downtime.pbix
│   │   ├── dashboard_4_shift_performance.pbix
│   │   └── dashboard_5_product_mix_tempo.pbix
│
├── notebooks/                               # Jupyter notebooks for exploration
│   ├── 01_exploratory_data_analysis.ipynb
│   ├── 02_bottleneck_identification.ipynb
│   └── 03_model_validation.ipynb
│
├── requirements.txt                         # Python dependencies
├── .gitignore                              
├── LICENSE                                 
└── README.md                               # This file
```

---

## 🛠️ **Technologies Used**

### **Data Engineering**
- **Python 3.9:** Core ETL pipeline development
- **Pandas:** Data manipulation and transformation
- **NumPy:** Numerical computations
- **Azure Data Factory:** Cloud-based data orchestration
- **Azure Blob Storage:** Staging area for CSV files

### **Data Storage & Analytics**
- **Azure SQL Database:** Relational data warehouse (star schema)
- **T-SQL:** Stored procedures, views, and analytical queries

### **Business Intelligence**
- **Power BI Desktop:** Interactive dashboards
- **DAX:** Advanced calculations and KPIs
- **Power Query:** Data transformation layer

### **Development Tools**
- **Git/GitHub:** Version control
- **Jupyter Notebooks:** Exploratory data analysis
- **VS Code:** IDE for Python development

---

## 📊 **Key Technical Highlights**

### **1. Synthetic Cycle Time Modeling (Novel Approach)**

**Challenge:** Equipment encoder data unavailable due to NDA restrictions.

**Solution:** Engineered a synthetic operation timeline that:
- ✅ Anchors to **REAL MES completion timestamps** (100% accurate)
- ✅ Works backwards to estimate equipment-level durations
- ✅ Applies product-specific multipliers (thin: 0.5-0.7×, thick: 1.1-1.3×)
- ✅ Factors in shift performance variations
- ✅ Validates with <1 second error margin

**Why This Matters:** Enables equipment-level bottleneck analysis without proprietary sensor data.

### **2. Parent-Child Coil Tracking**

- Mapped 8,008 parent coils (CID) → 13,575 output pieces (UID)
- Average yield: 1.7 pieces per parent coil
- Prime rate: 87.2% (target: 85%)
- Enabled yield optimization analysis

### **3. Tempo Analysis Through Gap Measurement**

- **Completion Gap:** Time between any two consecutive pieces (line throughput)
- **Parent Gap:** Time between parent coils (tempo measurement)
- Identified 15+ minute shift handover losses
- Enabled 4-shift → 3-shift recommendation

### **4. Scalable Azure Architecture**

- **Automated:** Hourly data refresh during production hours
- **Scalable:** Can handle 10× data volume without redesign
- **Secure:** Row-level security for shift-specific data access
- **Cost-effective:** Serverless SQL, pay-per-use ADF


---

## 🎓 **Skills Demonstrated**

### **Data Engineering**
✅ ETL pipeline development (Python)  
✅ Cloud data orchestration (Azure Data Factory)  
✅ Dimensional modeling (star schema)  
✅ Data quality validation  
✅ Feature engineering  
✅ Synthetic data generation  

### **Data Analytics**
✅ SQL analytics (T-SQL, stored procedures, views)  
✅ Statistical analysis (bottleneck identification)  
✅ Time-series analysis (trend detection)  
✅ KPI definition and tracking  

### **Business Intelligence**
✅ Dashboard design (5 production dashboards)  
✅ DAX advanced calculations (150+ measures)  
✅ Data visualization best practices  
✅ User experience design  

### **Cloud Technologies**
✅ Azure Data Factory (pipelines, triggers, monitoring)  
✅ Azure SQL Database (indexing, performance tuning)  
✅ Azure Blob Storage (data staging)  

### **Business Acumen**
✅ Stakeholder communication (management, engineers, operators)  
✅ ROI analysis and quantification  
✅ Strategic recommendations  
✅ Change management (4-shift → 3-shift transition)  

---

## 📧 **Contact**

**[Timothy Tshimauswu]**  
Data  Scientist | Business Intelligence Analyst  

- 📧 Email: timothytshimauswu@gmail.com
- 💼 LinkedIn: [linkedin.com/in/yourprofile](https://linkedin.com/in/utshimauswu/)
- 🐙 GitHub: [github.com/yourusername](https://github.com/TimothyTshimauswu)
- 📊 Portfolio: [[yourportfolio.com](https://cloud-data-ai-portfolio-landing.vercel.app/)

---

## 📄 **License**

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 **Acknowledgments**

- **ArcelorMittal Vanderbijlpark Works** for providing the business context and real production data
- **Process Engineering Team** for domain expertise and validation
- **Production & Maintenance Teams** for adopting the solution and providing feedback

---

## 📚 **Additional Resources**

- [Power BI Theme Documentation](docs/ArcelorMittal_PowerBI_Theme_Documentation.txt)
- [Interactive Dashboard Prototypes](powerbi/prototypes/)
- [SQL Query Examples](sql/views/)
- [Python Pipeline Scripts](python_pipeline/)

---

<div align="center">

**⭐ If this project helped you, please consider giving it a star! ⭐**

</div>
