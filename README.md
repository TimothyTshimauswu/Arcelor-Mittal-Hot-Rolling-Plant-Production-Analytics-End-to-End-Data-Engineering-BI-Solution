<p align="center">
  <img src="assets/project_banner.png" width="100%" alt="Arcelor Mittal Steel Production Data Engineering Pipeline and Operations Analytics Banner">
</p>

<h1 align="center">Arcelor Mittal Steel Production Data Engineering Pipeline and Operations Analytics</h1>

<p align="center">
  <strong>Python • Pandas • NumPy • SQL (MySQL) • ETL Pipelines • Feature Engineering • Time-Series Modelling • Industrial Analytics • Maintenance Analytics • Process Engineering • Power BI • Manufacturing KPIs</strong>
</p>

<p align="center">

A full-scale data engineering and analytics pipeline built to diagnose throughput losses, equipment bottlenecks, downtime patterns, and shift-related delays in a steel hot rolling temper line.  
The system reconstructs coil cycle behaviour, integrates production and maintenance data, and delivers BI insights that support higher throughput, reduced downtime, and better operational planning.
</p>
---

## Executive Summary

As I was working as a Process Engineer at Arcelor Mittal, this project started after the closure of ArcelorMittal’s Saldanha Works plant.  
All thin flat products previously produced at Saldanha were moved to Vanderbijlpark, increasing monthly production targets by about 30%. The temper line I supported was the final step before dispatch, yet it lacked historical data for these products and was not designed to handle them.

Management needed answers about falling tempo, shift-to-shift delays, frequent breakdowns, and inconsistent throughput. None of this could be explained by operator feedback alone. There was no equipment-level operational dataset to show what was slowing down the line.

To address this, I built a complete data engineering and analytics pipeline:  
I extracted equipment activity from Level-1 encoders, pulled production data from Azure Data Factory, ingested maintenance logs, merged the datasets, modelled the coil cycle using Python, and built a BI dashboard that revealed bottlenecks, delays, maintenance problems, and shift-related issues.  
The result became the primary weekly performance tool for production, maintenance, and executive teams.

---

## Business Problem

Redirecting thin flat products to a line not designed for them created instability across the process. Leadership needed a data-driven view of:

1. Why were monthly production targets not being met  
2. Why shift transitions caused large production losses  
3. Which equipment was the true bottleneck  
4. How mechanical failures and downtime were impacting tempo  
5. How the product mix affected the coil cycle time  
6. How scrap-versus-prime splits affected flow  
7. How to increase throughput without major capital investment  

The goal was to build a reliable operational intelligence system combining production, maintenance, and equipment activity.

---
## 📁 Project Structure

| Folder / File | Description |
|---------------|-------------|
| `data/` | Raw and processed datasets |
| `notebooks/` | EDA, feature engineering, model training |
| `sql/` | SQL analytics for Operational Analytics |
| `Power BI Production Dashboard/` | Power BI visuals and reporting |
| `assets/` | Images, banners, visuals |

---
## Technical Overview

I delivered the full solution across data engineering, modelling, SQL analytics, and BI reporting.

---

## 1. Data Engineering

I built the data foundation by integrating:

- Level-1 encoder signals  
- Azure-hosted production records  
- Maintenance breakdown logs  
- Crew rotation data  
- Coil geometry (width, thickness), grade, and product type  

### Key engineering steps

- Cleaned timestamps and removed invalid gaps  
- Classified scrap vs prime (HX, HY, HZ vs HL, HM, 98)  
- Built parent–child coil relationships  
- Mapped all equipment in the temper line  
- Defined process order and bottleneck flags  
- Reconstructed run–idle–fault timelines  
- Loaded all engineered tables into SQL  

### Outputs

- `fact_production_coil`  
- `fact_coil_operation_cycle`  
- `fact_equipment_event_log`  
- `fact_maintenance_event`  
- `dim_equipment`  

This reduced manual reporting and data collection time by 40 percent.

---

## 2. Coil Cycle Modelling

Because the line had no equipment-level time tracking for thin coils, I built a cycle-modelling engine using:

- Actual coil completion timestamps  
- Product mix behaviour  
- Bottleneck amplification logic  
- Shift performance factors  
- Line-flow constraints  

The model produced:

- Per-equipment operation durations  
- Realistic run Windows  
- Queue and idle behaviour  
- Complete end-to-end cycle times  

This clarified how thin coils moved through the line and why throughput dropped.

---

## 3. Operations and Maintenance Analytics

Using SQL and Power BI, I analysed:

### Production Analytics
- Cycle time per coil  
- Throughput per shift  
- Tempo vs product mix  
- Scrap vs prime flow  
- Parent–child coil behaviour  

### Bottleneck Analysis
- Run, idle, and fault percentages  
- Bottleneck severity per shift  
- Cumulative delay contribution  

### Maintenance Analytics
- Unplanned downtime per equipment  
- Fault alignment with coil delays  
- Maintenance frequency and severity  

### Shift Analysis
- Crew A–D performance  
- Losses during shift changes  
- Line readiness issues  

These insights revealed the true causes behind lost production hours.

---

## 4. BI Dashboards

I built an operational Power BI dashboard that included:

- Tempo trends  
- Bottleneck visualisation  
- Downtime heatmaps  
- Shift-level KPIs  
- Cycle-time drivers  
- Scrap vs prime behaviour  
- Recommendations panel  

This became the main weekly report used by plant leadership.

---

## Results and Business Impact

This pipeline delivered measurable gains:

1. **~10 percent tempo improvement** through bottleneck identification and workflow changes.  
2. **~5 percent reduction in maintenance downtime** by targeting problem equipment.  
3. **Reduced shift losses** after recommending a move from a 4-shift to 3-shift pattern.  
4. **Improved staffing decisions** by adding temporary offloading personnel.  
5. **Reduced evacuation delays** through automation recommendations.  
6. **Consistent achievement of monthly production targets**, with some months exceeding planned output.  
7. **Performance-based incentives introduced** due to improved operational consistency.  
8. **40 percent reduction in manual reporting**, enabled by the automated data pipeline.

The system became a central operational intelligence tool across the plant.

---

## How Teams Used This System

- Production managers monitored tempo and coil flow  
- Maintenance teams linked coil delays to equipment faults  
- Process engineers tracked bottlenecks and delays  
- Shift supervisors monitored performance trends  
- Executives used the dashboard for planning and resource allocation  

---

## Skills Demonstrated

Python  
Pandas  
NumPy  
SQL (MySQL)  
ETL and Data Pipelines  
Feature Engineering  
Time-Series Modelling  
Industrial Analytics  
Maintenance Analytics  
Process Engineering  
Power BI  
Manufacturing KPIs  

---

## Recommendations

1. Move to a 3-shift pattern to reduce handover losses  
2. Implement bi-weekly planned maintenance shutdowns  
3. Automate exit-zone handling to reduce cycle time  
4. Build predictive maintenance models for bottleneck equipment  
5. Integrate live PLC/SCADA signals for real-time cycle tracking  

---

## Next Steps

1. Build a real-time streaming version of the pipeline  
2. Add anomaly detection for equipment slowdowns  
3. Develop production and downtime forecasting  
4. Deploy a web-based operations monitoring tool  
5. Implement full OEE monitoring across the line  

