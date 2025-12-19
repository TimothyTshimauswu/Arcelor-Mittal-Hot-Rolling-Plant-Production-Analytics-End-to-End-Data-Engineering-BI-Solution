# ============================
# EXPORT FILES FOR BI ANALYSIS
# ============================

import os

print("="*80)
print("EXPORTING DATA TO CSV FILES")
print("="*80)

# Create output directory if it doesn't exist
output_dir = "output_tables"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
    print(f"\n✓ Created output directory: {output_dir}")
else:
    print(f"\n✓ Using existing output directory: {output_dir}")

# Define the tables to export
tables_to_export = {
    # Dimension tables
    "dim_equipment": dim_equipment,
    "dim_date_crew_schedule": pd.DataFrame([
        {"production_date": k, "day_crew": v[0], "night_crew": v[1]}
        for k, v in date_to_crews.items()
    ]),
    
    # Fact tables (with real completion times and anchored synthetic operations)
    "fact_production_coil": fact_production_coil,
    "fact_maintenance_event": fact_maintenance_event,
    "fact_coil_operation_cycle": fact_coil_operation_cycle,
    "fact_equipment_event_log": fact_equipment_event_log,
    
    # Optional: Raw/filtered source data for reference
    "raw_production_filtered": df_prod,  # Filtered production data (Apr-Aug)
    "raw_maintenance_filtered": df_maint,  # Filtered maintenance data (Apr-Aug)
}

print("\n" + "-"*80)
print("EXPORTING TABLES")
print("-"*80)

exported_files = []

for table_name, table_df in tables_to_export.items():
    filepath = os.path.join(output_dir, f"{table_name}.csv")
    
    # Export to CSV
    table_df.to_csv(filepath, index=False, encoding='utf-8')
    
    file_size_kb = os.path.getsize(filepath) / 1024
    
    print(f"\n✓ Exported: {table_name}")
    print(f"  - Filepath: {filepath}")
    print(f"  - Rows: {len(table_df):,}")
    print(f"  - Columns: {len(table_df.columns)}")
    print(f"  - File size: {file_size_kb:.2f} KB")
    
    exported_files.append(filepath)

print("\n" + "="*80)
print("EXPORT SUMMARY")
print("="*80)

print(f"\n✓ Successfully exported {len(exported_files)} CSV files")
print(f"✓ Output directory: {output_dir}/")

print("\nExported files:")
for i, filepath in enumerate(exported_files, 1):
    filename = os.path.basename(filepath)
    print(f"  {i}. {filename}")

# Print detailed info about key tables
print("\n" + "-"*80)
print("KEY TABLE DESCRIPTIONS")
print("-"*80)

print("\nfact_production_coil (with real completion times):")
print(f"  - Rows: {len(fact_production_coil):,}")
print(f"  - Unique output pieces (UID): {fact_production_coil['coil_id'].nunique():,}")
print(f"  - Unique parent coils (CID): {fact_production_coil['parent_coil_id'].nunique():,}")
print(f"  - Key features: Real MES completion timestamps, prime/scrap classification, gap analysis")
print(f"  - Columns: {', '.join(['coil_id', 'parent_coil_id', 'completion_ts', 'start_datetime', 'end_datetime', 'total_cycle_time_min', 'type_code', 'is_prime', 'is_scrap', 'gap_from_prev_completion_min', 'gap_from_prev_parent_min'])}")

print("\nfact_coil_operation_cycle (anchored to real completion):")
print(f"  - Rows: {len(fact_coil_operation_cycle):,}")
print(f"  - Description: Synthetic equipment operations anchored to real MES completion times")
print(f"  - Key features: Equipment-level detail, bottleneck flags, type/prime/scrap tracking")

print("\nfact_equipment_event_log (RUN/IDLE/FAULT):")
print(f"  - Rows: {len(fact_equipment_event_log):,}")
print(f"  - RUN events: {(fact_equipment_event_log['event_type'] == 'RUN').sum():,}")
print(f"  - IDLE events: {(fact_equipment_event_log['event_type'] == 'IDLE').sum():,}")
print(f"  - FAULT events: {(fact_equipment_event_log['event_type'] == 'FAULT').sum():,}")

print("\nfact_maintenance_event (processed):")
print(f"  - Rows: {len(fact_maintenance_event):,}")
print(f"  - Description: Cleaned, filtered (Apr-Aug), with parsed durations and equipment mapping")

print("\nraw_production_filtered:")
print(f"  - Rows: {len(df_prod):,}")
print(f"  - Description: Original production data filtered to Apr-Aug 2024")

print("\nraw_maintenance_filtered:")
print(f"  - Rows: {len(df_maint):,}")
print(f"  - Description: Original maintenance data filtered to Apr-Aug 2024")

# Print column info for all tables
print("\n" + "-"*80)
print("TABLE SCHEMAS (for reference)")
print("-"*80)

for table_name, table_df in tables_to_export.items():
    print(f"\n{table_name}:")
    print(f"  Columns ({len(table_df.columns)}): {', '.join(table_df.columns.tolist())}")

# Data model relationship summary
print("\n" + "-"*80)
print("DATA MODEL RELATIONSHIPS")
print("-"*80)
print("""
Key Relationships for BI Tools:

1. fact_production_coil
   - Primary key: coil_id (UID)
   - Foreign keys: parent_coil_id (CID), shift_code
   - Date key: production_date
   - Real timestamps: completion_ts (from MES)
   - Synthetic timestamps: start_datetime, end_datetime (anchored to completion_ts)

2. fact_coil_operation_cycle
   - Foreign keys: coil_id → fact_production_coil.coil_id
                  equipment_id → dim_equipment.equipment_id
                  parent_coil_id → for parent coil grouping
   - Contains: Equipment-level operations anchored to real completion times

3. fact_equipment_event_log
   - Foreign key: equipment_id → dim_equipment.equipment_id
   - Event types: RUN, IDLE, FAULT
   - Links coil_id when event_type = 'RUN'

4. fact_maintenance_event
   - Links to dim_equipment via equipment_name
   - Source of FAULT events in fact_equipment_event_log

5. dim_equipment
   - Primary key: equipment_id
   - Contains: process_order, section, bottleneck flags

6. dim_date_crew_schedule
   - Links to production_date
   - Contains: crew rotation schedule (day_crew, night_crew)

Suggested BI Visualizations:
- Throughput by product type (prime vs scrap)
- Parent coil yield analysis (CID → UID breakdown)
- Equipment utilization (RUN/IDLE/FAULT %)
- Bottleneck analysis (equipment with highest operation time %)
- Gap analysis (tempo between coils)
- Crew performance comparison (A/B/C/D shifts)
- Real vs synthetic timeline validation
""")

print("\n" + "="*80)
print("EXPORT COMPLETE!")
print("="*80)
print(f"\nYou can now use these CSV files in Power BI, Tableau, or other BI tools.")
print(f"All files are located in: {output_dir}/")
print(f"\nIMPORTANT: fact_production_coil contains REAL MES completion times")
print(f"           with synthetic start times anchored backwards from completion")