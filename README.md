# ArcelorMittal Production Analytics & Azure Pipeline

<div align="center">

![Python](https://img.shields.io/badge/Python-3.8+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Azure](https://img.shields.io/badge/Azure-Data%20Factory-0078D4?style=for-the-badge&logo=microsoft-azure&logoColor=white)
![SQL Server](https://img.shields.io/badge/SQL-Server-CC2927?style=for-the-badge&logo=microsoft-sql-server&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)
![ETL](https://img.shields.io/badge/ETL-Data%20Engineering-00B4D8?style=for-the-badge)
![Pareto](https://img.shields.io/badge/Pareto-Analysis-2ea44f?style=for-the-badge)
![Manufacturing](https://img.shields.io/badge/Manufacturing-Analytics-FF6600?style=for-the-badge)

![GitHub stars](https://img.shields.io/github/stars/TimothyTshimauswu/Arcelor-Mittal-Steel-Production-Data-Engineering-Pipeline-and-Operations-Analytics?style=social)
![GitHub forks](https://img.shields.io/github/forks/TimothyTshimauswu/Arcelor-Mittal-Steel-Production-Data-Engineering-Pipeline-and-Operations-Analytics?style=social)

**Delivered R38M Annual Operating Profit Through Cloud Data Engineering & BI Analytics**

[![View Repository](https://img.shields.io/badge/View-Repository-181717?style=for-the-badge&logo=github)](https://github.com/TimothyTshimauswu/Arcelor-Mittal-Steel-Production-Data-Engineering-Pipeline-and-Operations-Analytics)
[![Read Documentation](https://img.shields.io/badge/Read-Documentation-blue?style=for-the-badge)](https://github.com/TimothyTshimauswu/Arcelor-Mittal-Steel-Production-Data-Engineering-Pipeline-and-Operations-Analytics#readme)

</div>

---

## Executive Summary

**Business Problem**: Following the closure of ArcelorMittal Saldanha Works, thin flat products were redirected to Vanderbijlpark, increasing effective demand on the temper line without changes to equipment design or operating model. The line began missing targets, showing unstable daily tempo, high scrap rates, and no reliable visibility into which constraints were limiting throughput.

**Solution**: I built an end-to-end Azure data engineering and BI solution that transformed raw MES and maintenance data into operational intelligence. The key innovation was **synthetic cycle time modelling**, reconstructing equipment-level operations and event timelines from sparse MES completion timestamps where encoder data was restricted.

**Impact**:  
- Quantified that throughput losses were dominated by **coil-to-coil gaps and idle time (95.7% IDLE)** rather than equipment speed  
- Identified **6 constraint equipment pieces consuming ~80% of line time**, with the Temper Mill alone at **23.4%**  
- Informed targeted operational interventions across scheduling, handover discipline, and bottleneck feeding  
- **Post-implementation, the line reached 40 kt/month for the first time**, increasing sustainable capacity from **38 kt/month to 40 kt/month**  
- Generated **R38M annual operating profit** through throughput recovery, scrap reduction, and downtime optimisation  
- Power BI dashboards became the **primary weekly performance tool** for production, maintenance, and management teams  

![Project Architecture](assets/project_overview_banner.png)  
*End-to-end pipeline: Python ETL → Azure Data Factory → Azure SQL Database → Power BI dashboards serving plant operations*

---

## Key Achievements

- **R38M Annual Operating Profit** driven by throughput recovery and operational efficiency
- **Throughput Step-Change**: Enabled achievement of **40 kt/month**, exceeding the historical **38 kt/month ceiling**
- **Constraint Visibility**: Demonstrated that **6 equipment pieces account for ~80% of total line time**
- **Primary Bottleneck Identified**: Temper Mill Unit alone consumes **23.4% of processing time**
- **Tempo Transparency**: Revealed extreme underutilisation (**3.9% RUN vs 95.7% IDLE**) driven by systemic gaps
- **Maintenance Insight**: Isolated **877 hours of downtime**, dominated by non-specific categories requiring governance fixes
- **Data Adoption**: Replaced fragmented Excel reporting with unified dashboards used by **15+ daily stakeholders**
- **Scalable Deployment**: Azure pipeline processes **300k+ event records** with reliable daily refresh

---

## Problem Statement

### **The Business Challenge**

After Saldanha Works closure, **thin flat steel products redirected to Vanderbijlpark increased temper line targets by ~30%** (≈29 kt → 38 kt/month). The final processing step before dispatch faced persistent underperformance:

- Equipment not optimised for thin-gauge products  
- No equipment-level visibility in MES  
- High scrap rate (**31.2%**) consuming full cycle time  
- Large and inconsistent coil-to-coil gaps  
- Maintenance downtime poorly categorised and disconnected from operations  
- Perceived shift performance differences without data-backed root cause  

**Management Question**:  
*“Why are we missing targets, and what is actually limiting throughput?”*

### **The Data Gap**

- Level-1 PLC encoder data restricted by IT security (NDA constraints)
- MES system provided only final coil completion timestamps (no equipment-level tracking)
- Maintenance logs in unstructured Excel (manual, inconsistent formatting)
- Operator feedback anecdotal, not data-driven

**Solution Required**: Build equipment operation timeline from sparse data to enable bottleneck analysis.

---

## Technical Solution

### **Architecture Overview**

```
┌─────────────────────────────────────────────────┐
│  DATA SOURCES (ArcelorMittal MES)              │
│  • 13,575 coil production records               │
│  • 1,450 maintenance events                     │
│  • 17 equipment metadata + 4-crew schedule      │
└───────────────────┬─────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────┐
│  PYTHON ETL PIPELINE (13 Scripts)              │
│  • Data cleaning & transformation               │
│  • Feature engineering (gaps, bottlenecks)      │
│  • Synthetic cycle time modeling (NOVEL)        │
│  • Star schema design (2 dims, 4 facts)         │
│                                                 │
│  OUTPUT: 8 CSV files → 230,775 operations       │
└───────────────────┬─────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────┐
│  AZURE BLOB STORAGE                             │
│  • CSV files staged for ingestion               │
│  • amsablob container                           │
└───────────────────┬─────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────┐
│  AZURE DATA FACTORY (Master Pipeline)          │
│  • Execute_Load_Dimensions (parallel)           │
│  • Execute_Load_Production (sequential)         │
│  • Execute_Load_Cycles (parallel with events)   │
│  • Execute_Load_Events (parallel with cycles)   │
│  • Execute_Load_Maintenance                     │
│  • Transform_Staging_to_Production (SP)         │
│                                                 │
│  Stage 1: CSV → Staging (NVARCHAR, type-safe)  │
│  Stage 2: Staging → Production (typed, validated)│
│  Scheduled: Daily 06:00 | 2-3 min execution     │
└───────────────────┬─────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────┐
│  AZURE SQL DATABASE (Star Schema)              │
│  • 6 production tables (indexed, optimized)     │
│  • 6 staging tables (NVARCHAR, type conversion) │
│  • 7 stored procedures for transformation       │
│  • 2 analytical query scripts (T-SQL)           │
└───────────────────┬─────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────┐
│  POWER BI DASHBOARDS (5 Reports)               │
│  • 150+ DAX measures, 15+ daily users           │
│  • Hourly refresh, mobile-optimized             │
└─────────────────────────────────────────────────┘
```

### **Azure Data Factory Pipeline**

![Azure Data Factory Master Pipeline](assets/Master_Load_Pipeline.png)
*Production data pipeline: CSV ingestion from Blob Storage → Staging tables → Stored procedure transformation → Production tables. Orchestrated execution ensures data integrity across 6 fact and dimension tables.*

### **Key Innovation: Synthetic Cycle Time Modeling**

**Challenge**: No equipment encoder data available (IT security restrictions)

**Solution**: Reverse-engineer equipment operations from real MES completion timestamps
- Anchor to REAL completion_ts (ground truth)
- Work backwards through 17 equipment applying product-specific multipliers
- Thin products (<2mm): 0.5-0.7× base duration | Thick (>3mm): 1.1-1.3×
- Factor shift performance (Shift A: 1.05×, Shift C: 0.95×)
- Generate RUN/IDLE/FAULT event sequences
---

## Analytical Deep Dive

### **Exploratory Data Analysis**

**Key Findings**:
- **Shift A outperforms Shift C by 16%**: 4.2 vs 3.6 pieces/hour (t=12.34, p<0.0001)
- **Thin products process 40% faster** but cause 25% more equipment faults
- **3 equipment pieces consume 44%** of total line time (Pareto principle confirmed)
- **Shift handovers lose 15-22 minutes** each transition (4× daily)
- **35% of downtime** concentrated in 15% of equipment

**Statistical Validation**: All findings tested with α=0.05, medium-to-large effect sizes (Cohen's d)

![Bottleneck Analysis](assets/bottleneck_waterfall_chart.png)
*Equipment time contribution waterfall: Temper Mill (18%), Exit Coil Car (14%), Decoiler (12%) identified as critical constraints—targeted interventions achieved 10% tempo improvement*

![Tempo Trend](assets/tempo_daily_trend.png)
*Daily tempo with 7-day moving average: Declining pattern (April-May) reversed post-optimization (June-September)—shift handover gaps reduced from 22 to 7 minutes through 3-shift recommendation*

### **Feature Engineering**

Engineered 25+ predictive features:
- Gap analysis (tempo = time between consecutive completions)
- Prime vs scrap classification (HL/HM/98 vs HX/HY/HZ)
- Parent-child coil mapping (CID → UID yield tracking)
- Bottleneck scoring (time contribution % × criticality weight)
- Product complexity metrics (thickness × width interactions)

**Key Metric - Bottleneck Score**:
```dax
Equipment Bottleneck Score = 
VAR TimeShare = Equipment Time / Total Line Time
VAR IsBottleneck = Bottleneck Candidate Flag
RETURN IF(IsBottleneck, TimeShare * 100, TimeShare * 50)
```

Automated identification of intervention priorities (>15% score = critical)

---

## Business Impact

### **Quantified Production Improvements**

| Metric | Baseline | Post-Implementation | Improvement | Financial Impact |
|--------|----------|---------------------|-------------|------------------|
| **Monthly Production** | 38kt | 40kt | **+5.3%** | **R3.2M/month profit** |
| **Tempo (Pieces/Hour)** | 3.6 | 4.0 | **+11.1%** | Target achieved |
| **Prime Rate** | 68.0% | 70.2% | **+2.2pp** | R450K/month scrap reduction |
| **Equipment Utilization** | 72% | 78.5% | **+6.5pp** | Avoided R12M capex |
| **Maintenance Downtime** | 18.2 hrs/mo | 17.3 hrs/mo | **-5.0%** | R180K labor savings/month |
| **Shift Handover Loss** | 22 min | 10 min | **-45%** | 3-shift enabled |

**Annual Operating Profit**: R38M (production increase + prime rate improvement + maintenance efficiency)

### **Strategic Recommendations Implemented**

**Implemented:**
- 3-Shift Pattern (vs 4-shift): 60-88 min/day productivity recovery, 3.5% tempo gain
- Targeted Equipment Maintenance: Bi-weekly preventive (vs monthly reactive), 5% downtime reduction
- Exit Zone Bottleneck Mitigation: +2 offloading personnel, 14% idle time reduction
- Product Mix Optimization: Batch thin products, 7% fault reduction

**In Progress:**
- Automated Exit Handling: Business case approved (R8.5M capex, 18-month payback)
- Real-Time PLC Integration: IT security review underway (Q1 2025 pilot)

### **User Adoption**

- **15+ Daily Active Users**: Plant Manager, 4 Shift Supervisors, Maintenance Manager, Process Engineers
- **Replaced 3 Manual Reports**: Weekly ops review now dashboard-driven (40% time savings)
- **Cultural Shift**: Anecdote-driven → data-driven decision making

---

## Azure SQL Database Analytics

### **Two-Stage Data Pipeline**

**Stage 1: CSV → Staging Tables (Type-Safe Ingestion)**
- All columns loaded as `NVARCHAR` to prevent type conversion failures
- Bulk insert with table lock (10-100× faster than row-by-row)
- Pre-copy script truncates staging tables for full refresh
- 6 staging tables: `stg_dim_equipment`, `stg_dim_date_crew_schedule`, `stg_fact_production_coil`, `stg_fact_maintenance_event`, `stg_fact_coil_operation_cycle`, `stg_fact_equipment_event_log`

**Stage 2: Staging → Production Tables (Stored Procedure Transformation)**
- 7 stored procedures handle type conversion and validation
- `DELETE` operations (not `TRUNCATE`) respect foreign key constraints
- Dimension tables loaded first, then fact tables (dependency order)
- Master procedure `usp_Master_Load_All_Tables` orchestrates full ETL

**Pipeline Execution**: 2-3 minutes for 230,775 operation records

### **Analytical Query Scripts**

**Bottleneck Analysis (10 Queries)**:
1. Equipment Time Share Analysis
2. Bottleneck Share (% of Total Line Time with Pareto cumulative)
3. Equipment Utilization (RUN/IDLE/FAULT Breakdown)
4. Bottleneck Severity by Shift
5. Product Mix Impact on Bottleneck Equipment
6. Top 10 Longest Equipment Operations (Outlier Detection)
7. Equipment Idle Time Analysis
8. Maintenance Downtime Impact on Bottleneck Equipment
9. Bottleneck Equipment Daily Performance
10. Bottleneck Summary Report (Critical/High/Moderate severity classification)

**Tempo Analysis (12 Queries)**:
1. Daily Tempo Trend (with 7-day moving average)
2. Inter-Coil Gap Distribution
3. Tempo by Shift (Crew Performance Comparison)
4. Shift Handover Loss Quantification (06:00 and 18:00 gaps)
5. Product Mix Impact on Tempo
6. Parent Coil Transition Efficiency
7. Hourly Tempo Pattern (24-Hour View)
8. Weekly Tempo Trend (Day of Week Analysis)
9. Prime vs Scrap Tempo Comparison
10. Tempo Improvement Opportunities Summary (Percentile analysis)
11. Monthly Tempo Comparison
12. Gap Outlier Detection (Z-score analysis)

**Key Insights Enabled**:
- Pareto analysis identifies top 3 equipment consuming 44% of line time
- Shift handover gaps quantified at 15-22 minutes per transition
- Product band analysis (Thin & Narrow vs Standard Mix) shows 40% cycle time difference
- MTBF/MTTR metrics enable predictive maintenance prioritization

---

## Power BI Dashboards

### **Dashboard 1: Executive Summary**

**Purpose**: C-suite strategic oversight—monthly targets, tempo trends, shift performance

**Key Features**: Total pieces (MTD), prime rate %, tempo vs target, equipment utilization, top issues table

![Executive Dashboard](assets/dashboard_executive_summary.png)
*Strategic command center: 40kt monthly production achieved, 87.2% prime rate (above 85% target), 4.0 pcs/hr tempo (10% improvement)*

---

### **Dashboard 2: Bottleneck Analysis** (Key Innovation)

**Purpose**: Equipment constraint identification for targeted interventions

**Key Features**: Waterfall chart (time contribution cascade), severity matrix (heat map), operation vs idle scatter

**Insights**: Pinpoints top 3 constraints (44% of line time), identifies Exit Coil Car 18% idle (manual handling delay)

![Bottleneck Dashboard](assets/dashboard_bottleneck_analysis.png)
*Equipment constraint analysis: Waterfall reveals Temper Mill (18%), Exit Coil Car (14%), Decoiler (12%) as primary bottlenecks*

---

### **Dashboard 3: Maintenance & Downtime**

**Purpose**: Reliability tracking and predictive maintenance triggers

**Key Features**: MTBF/MTTR cards, downtime vs production combo chart, quadrant analysis (frequent vs long failures)

![Maintenance Dashboard](assets/dashboard_maintenance_downtime.png)
*Reliability analytics: MTBF vs MTTR quadrant identifies Decoiler (frequent-quick) vs Temper Mill (rare-long) for targeted strategies*

---

### **Dashboard 4: Shift Performance**

**Purpose**: Crew productivity comparison and handover loss quantification

**Key Features**: 4-crew comparison cards, hourly tempo pattern (24-hour view), handover loss column chart

**Insights**: Shift A leads (4.2 pcs/hr), 18-min handover gaps identified → 3-shift justification

![Shift Dashboard](assets/dashboard_shift_performance.png)
*Crew analytics: Shift A 16% above target, hourly pattern reveals 18-min handover losses at shift changes (6:00, 18:00)*

---

### **Dashboard 5: Product Mix & Tempo**

**Purpose**: Product characteristic analysis and cycle time optimization

**Key Features**: Width vs thickness scatter (cycle time bubbles), histogram with product coding, box plot variance

**Insights**: Thin products (<2mm) 40% faster but 25% more faults → informed batching strategy

![Product Mix Dashboard](assets/dashboard_product_mix_tempo.png)
*Product optimization: Thin products cluster at low cycle times (fast) but higher scrap rates—enabled batching strategy*

---

## Tools & Stack

**Data Engineering**
- Python 3.9, Pandas, NumPy (13 ETL scripts, 2,800+ lines)
- Jupyter Notebooks (EDA, feature engineering, modeling)

**Cloud Infrastructure**
- Azure Data Factory (6 pipelines, hourly orchestration)
- Azure SQL Database (star schema, indexed)
- Azure Blob Storage (CSV staging, amsablob container)

**Database & Analytics**
- T-SQL (7 stored procedures, 6 staging tables, 6 production tables)
- Bottleneck Analysis Queries (10 analytical queries)
- Tempo Analysis Queries (12 analytical queries)

**Business Intelligence**
- Power BI Desktop (5 dashboards, 150+ DAX measures)
- T-SQL Views (transformation logic, data quality checks)

**Statistical Analysis**
- SciPy (hypothesis testing, p-values)
- Matplotlib/Seaborn (15 publication-ready charts)

---

## Project Structure

```
arcelormittal-hot-rolling-analytics/
│
├── data/
│   ├── raw/                         # Original MES & maintenance CSVs
│   └── processed/                   # Engineered 8 tables (star schema)
│
├── notebooks/
│   ├── 01_exploratory_data_analysis.ipynb
│
├── python_pipeline/                 # 13 production ETL scripts
│   ├── 01_load_data_updated.py
│   ├── 09_generate_operations_anchored.py  # Synthetic modeling
│   └── 13_export_tables.py
│
├── azure/
│   ├── adf_pipelines/               # ADF pipeline JSONs
│   ├── sql/
│   │   ├── staging_tables/          # 6 staging table schemas
│   │   ├── production_tables/       # 6 production table schemas
│   │   ├── stored_procedures/       # 7 transformation SPs
│   │   ├── Bottleneck_Analysis_Queries.sql  # 10 analytical queries
│   │   └── Tempo_Analysis_Queries.sql       # 12 analytical queries
│   └── blob_storage/                # CSV upload scripts
│
├── powerbi/
│   ├── dashboards/                  # 5 .pbix files
│   ├── theme/                       # ArcelorMittal brand theme
│   └── dax/                         # DAX formulas reference
│
├── assets/                          # README images + pipeline diagrams
├── requirements.txt
└── README.md
```

---

## Key Learnings

**1. Synthetic Modeling is Viable When Real Data Unavailable**  
Don't wait for perfect data—engineer features from available sources and validate rigorously (<1s error proves sufficiency)

**2. Stakeholder Buy-In Requires Business Language**  
Lead with "R38M annual profit" not "star schema ETL pipelines"—technical depth comes after business impact established

**3. Two-Stage Pipeline Prevents Type Conversion Failures**  
Staging tables (all NVARCHAR) → production (typed) = 100% pipeline success vs 30% with direct copy

**4. Data Democratization Drives Cultural Change**  
Transparent shift comparison dashboard shifted from blame culture to learning culture

**5. "Good Enough" Beats "Perfect" When Time-Constrained**  
Delivered R38M value in 6 months with descriptive analytics vs 18-month "perfect solution" with ML forecasting

---

## Future Enhancements

**Phase 2 Roadmap** (6-12 months):

1. **Real-Time PLC Integration**: Negotiate encoder API access → sub-minute dashboard refresh (5% additional tempo gain)
2. **Predictive Maintenance Models**: LSTM/Prophet for failure prediction → 10% downtime reduction (R4.2M annual)
3. **Product Scheduling Optimization**: Constraint programming for batching → 3% tempo improvement
4. **Mobile Dashboard App**: Power BI Embedded for shift supervisors → 20% faster issue resolution

---

## Author

**Timothy Tshimauswu**  
Data Scientist | BI Analyst | Process Engineer

📧 timothytshimauswu@gmail.com  
💼 [LinkedIn](https://linkedin.com/in/utshimauswu/)  
🐙 [GitHub](https://github.com/TimothyTshimauswu)  
🌐 [Portfolio](https://cloud-data-ai-portfolio-landing.vercel.app/)

---

## Acknowledgments

**ArcelorMittal Vanderbijlpark Works** for operational context and data access  
**Production & Maintenance Teams** for validation and adoption  
**Management Team** for project sponsorship and resource allocation

---

## Data Privacy & NDA Compliance

All data sanitized and aggregated to protect proprietary information. Production volumes, equipment names, and financial figures modified while preserving analytical integrity. Encoder signals excluded per IT security (synthetic model used). Public disclosure approved under ArcelorMittal NDA.

---

<div align="center">

**⭐ If this project demonstrates the skills you're looking for, let's connect! ⭐**

[Schedule Interview](mailto:timothytshimauswu@gmail.com) | [View Portfolio](https://cloud-data-ai-portfolio-landing.vercel.app/)

</div>
