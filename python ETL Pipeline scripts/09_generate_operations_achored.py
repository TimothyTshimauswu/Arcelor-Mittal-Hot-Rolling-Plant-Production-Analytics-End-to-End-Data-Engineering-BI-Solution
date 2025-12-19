# ============================
# 9. GENERATE fact_coil_operation_cycle ANCHORED TO REAL COMPLETION TIME
# ============================

print("="*80)
print("GENERATING SYNTHETIC OPERATIONS ANCHORED TO REAL COMPLETION TIMES")
print("="*80)

fact_coil_ops_rows = []

# Work on a sorted copy by completion time
fact_production_coil = fact_production_coil.sort_values("completion_ts").reset_index(drop=True)

# Add placeholders for synthetic start/end
fact_production_coil["start_datetime"] = pd.NaT
fact_production_coil["end_datetime"] = pd.NaT

print(f"\nProcessing {len(fact_production_coil):,} coil records...")
print("Anchoring synthetic operations to MES completion timestamps...\n")

processed_count = 0

for idx, coil in fact_production_coil.iterrows():
    coil_id = coil["coil_id"]
    d = coil["production_date"]
    thickness_mm = coil["thickness_mm"]
    width_mm = coil["width_mm"]
    completion_ts = coil["completion_ts"]
    
    # Skip if completion timestamp is invalid
    if pd.isna(completion_ts):
        continue

    # Day vs night crew based on completion time
    day_crew, night_crew = date_to_crews.get(d, ("A", "B"))  # Default to A/B if date not found
    if 6 <= completion_ts.hour < 18:
        shift_code = day_crew
    else:
        shift_code = night_crew

    # 9.1 Draw durations for all equipment for this coil
    durations = []
    for _, eq_row in line_equipment.iterrows():
        equip_name = eq_row["equipment_name"]
        is_bottle = bool(eq_row["is_bottleneck_candidate"])

        dur_sec = draw_duration_seconds(
            equip_name,
            is_bottle,
            shift_code,
            thickness_mm,
            width_mm,
        )
        durations.append(dur_sec)

    total_sec = float(np.sum(durations))

    # 9.2 Anchor the full operation window to real completion time:
    # coil_end = MES completion timestamp, coil_start = end - total synthetic duration
    coil_end_ts = completion_ts
    coil_start_ts = coil_end_ts - timedelta(seconds=total_sec)

    fact_production_coil.at[idx, "start_datetime"] = coil_start_ts
    fact_production_coil.at[idx, "end_datetime"] = coil_end_ts
    fact_production_coil.at[idx, "shift_code"] = shift_code

    # 9.3 Build per-equipment operation timeline within that window
    ts = coil_start_ts
    for (_, eq_row), dur_sec in zip(line_equipment.iterrows(), durations):
        equipment_id = eq_row["equipment_id"]
        equip_name = eq_row["equipment_name"]
        is_bottle = bool(eq_row["is_bottleneck_candidate"])

        op_start_ts = ts
        op_end_ts = ts + timedelta(seconds=dur_sec)

        fact_coil_ops_rows.append({
            "coil_id": coil_id,
            "parent_coil_id": coil["parent_coil_id"],
            "equipment_id": equipment_id,
            "equipment_name": equip_name,
            "production_date": d,
            "shift_code": shift_code,
            "operation_start_ts": op_start_ts,
            "operation_end_ts": op_end_ts,
            "operation_duration_sec": dur_sec,
            "queue_time_sec": 0.0,     # queue within coil not modelled; gaps are between coils
            "is_bottleneck_step": is_bottle,
            "type_code": coil["type_code"],
            "is_prime": coil["is_prime"],
            "is_scrap": coil["is_scrap"],
        })

        ts = op_end_ts  # next equipment starts immediately

    processed_count += 1
    
    # Progress indicator
    if (processed_count % 1000 == 0):
        print(f"  Processed {processed_count:,} / {len(fact_production_coil):,} coils...")

# Build fact_coil_operation_cycle DataFrame
fact_coil_operation_cycle = pd.DataFrame(fact_coil_ops_rows)

# Total cycle time per coil in minutes (synthetic, but aligned with MES completion)
fact_production_coil["total_cycle_time_min"] = (
    (fact_production_coil["end_datetime"] - fact_production_coil["start_datetime"])
    .dt.total_seconds() / 60.0
)

print(f"\n✓ Generated {len(fact_coil_operation_cycle):,} operation records")
print(f"✓ Processed {processed_count:,} coils with valid completion times")

print("\n" + "="*80)
print("FACT TABLE: fact_coil_operation_cycle (ANCHORED TO REAL COMPLETION)")
print("="*80)

print("\nOperation cycle statistics:")
print(f"  Total operations: {len(fact_coil_operation_cycle):,}")
print(f"  Unique coils: {fact_coil_operation_cycle['coil_id'].nunique():,}")
print(f"  Unique parent coils: {fact_coil_operation_cycle['parent_coil_id'].nunique():,}")
print(f"  Unique equipment: {fact_coil_operation_cycle['equipment_id'].nunique()}")
print(f"  Date range: {fact_coil_operation_cycle['production_date'].min()} to {fact_coil_operation_cycle['production_date'].max()}")

print("\nSample operations (first 20 records):")
print(fact_coil_operation_cycle[[
    'coil_id', 'parent_coil_id', 'equipment_name', 'operation_start_ts', 
    'operation_end_ts', 'operation_duration_sec', 'type_code', 'is_prime'
]].head(20))

print("\n" + "="*80)
print("UPDATED: fact_production_coil (WITH SYNTHETIC TIMES ANCHORED TO REAL)")
print("="*80)

print("\nCycle time summary (minutes):")
print(fact_production_coil["total_cycle_time_min"].describe())

print("\nSample production coil records:")
print(fact_production_coil[[
    'coil_id', 'parent_coil_id', 'production_date', 'shift_code', 
    'start_datetime', 'end_datetime', 'completion_ts', 'total_cycle_time_min',
    'type_code', 'is_prime', 'thickness_mm', 'width_mm'
]].head(20))

# Validation: Check alignment between synthetic end and real completion
time_diff = (fact_production_coil['end_datetime'] - fact_production_coil['completion_ts']).dt.total_seconds()
print("\n" + "-"*80)
print("VALIDATION: Synthetic end vs Real completion alignment")
print("-"*80)
print("Time difference (should be ~0 seconds):")
print(time_diff.describe())

if time_diff.abs().max() < 1:  # Less than 1 second difference
    print("\n✓ VALIDATION PASSED: Synthetic operations perfectly anchored to real completion times")
else:
    print(f"\n⚠ WARNING: Max time difference of {time_diff.abs().max():.2f} seconds detected")

# Analysis by product type
print("\n" + "-"*80)
print("CYCLE TIME ANALYSIS BY PRODUCT TYPE")
print("-"*80)

cycle_by_type = (
    fact_production_coil
    .groupby(['type_code', 'is_prime', 'is_scrap'])['total_cycle_time_min']
    .agg(['count', 'mean', 'std', 'min', 'max'])
    .round(2)
)
print("\nCycle times by product type:")
print(cycle_by_type)

print("\nPrime vs Scrap cycle times:")
prime_vs_scrap = fact_production_coil.groupby('is_prime')['total_cycle_time_min'].describe()
print(prime_vs_scrap)