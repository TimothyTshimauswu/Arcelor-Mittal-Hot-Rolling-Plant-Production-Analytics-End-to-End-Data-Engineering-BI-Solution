# Export processed data tables for BI analysis

import os

print("Exporting data to CSV files\n")

# Create output directory
output_dir = "output_tables"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
    print(f"✓ Created directory: {output_dir}")
else:
    print(f"✓ Using directory: {output_dir}")

# Define tables for export
tables_to_export = {
    "dim_equipment": dim_equipment,
    "dim_date_crew_schedule": pd.DataFrame([
        {"production_date": k, "day_crew": v[0], "night_crew": v[1]}
        for k, v in date_to_crews.items()
    ]),
    "fact_production_coil": fact_production_coil,
    "fact_maintenance_event": fact_maintenance_event,
    "fact_coil_operation_cycle": fact_coil_operation_cycle,
    "fact_equipment_event_log": fact_equipment_event_log,
    "raw_production_filtered": df_prod,
    "raw_maintenance_filtered": df_maint,
}

print(f"\nExporting {len(tables_to_export)} tables:")

exported_files = []

for table_name, table_df in tables_to_export.items():
    filepath = os.path.join(output_dir, f"{table_name}.csv")
    
    table_df.to_csv(filepath, index=False, encoding='utf-8')
    
    file_size_kb = os.path.getsize(filepath) / 1024
    
    print(f"\n✓ {table_name}")
    print(f"  Rows: {len(table_df):,} | Columns: {len(table_df.columns)} | Size: {file_size_kb:.2f} KB")
    
    exported_files.append(filepath)

print(f"\n{'='*80}")
print("EXPORT SUMMARY")
print(f"{'='*80}")

print(f"\n✓ Exported {len(exported_files)} CSV files to: {output_dir}/")

print("\nKey table descriptions:")

print(f"\nfact_production_coil:")
print(f"  {len(fact_production_coil):,} records | {fact_production_coil['coil_id'].nunique():,} unique pieces")
print(f"  Real MES completion timestamps with synthetic start times anchored backwards")

print(f"\nfact_coil_operation_cycle:")
print(f"  {len(fact_coil_operation_cycle):,} records | Equipment-level operations")
print(f"  Synthetic timelines anchored to real completion times")

print(f"\nfact_equipment_event_log:")
print(f"  {len(fact_equipment_event_log):,} events")
print(f"  RUN: {(fact_equipment_event_log['event_type'] == 'RUN').sum():,}")
print(f"  IDLE: {(fact_equipment_event_log['event_type'] == 'IDLE').sum():,}")
print(f"  FAULT: {(fact_equipment_event_log['event_type'] == 'FAULT').sum():,}")

print(f"\nfact_maintenance_event:")
print(f"  {len(fact_maintenance_event):,} records | Cleaned and filtered to Apr-Aug 2024")

print("\nData model relationships:")
print("""
  fact_production_coil
    └─ coil_id (PK), parent_coil_id, completion_ts (real MES timestamp)
    
  fact_coil_operation_cycle
    └─ coil_id → fact_production_coil.coil_id
    └─ equipment_id → dim_equipment.equipment_id
    
  fact_equipment_event_log
    └─ equipment_id → dim_equipment.equipment_id
    └─ coil_id (when event_type = 'RUN')
    
  fact_maintenance_event
    └─ equipment_name → dim_equipment.equipment_name
    
  dim_equipment
    └─ equipment_id (PK), process_order, section, is_bottleneck_candidate
    
  dim_date_crew_schedule
    └─ production_date, day_crew, night_crew
""")

print(f"\n{'='*80}")
print("EXPORT COMPLETE")
print(f"{'='*80}")
print(f"\nFiles ready for Power BI, Tableau, or Azure SQL Database import")
print(f"Location: {output_dir}/")