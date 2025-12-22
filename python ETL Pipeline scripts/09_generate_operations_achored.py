# Generate synthetic equipment operations anchored to real MES completion times

fact_coil_ops_rows = []

# Sort by completion time for sequential processing
fact_production_coil = fact_production_coil.sort_values("completion_ts").reset_index(drop=True)

# Initialize synthetic timing columns
fact_production_coil["start_datetime"] = pd.NaT
fact_production_coil["end_datetime"] = pd.NaT

print(f"Processing {len(fact_production_coil):,} coil records...")
print("Anchoring synthetic operations to MES completion timestamps\n")

processed_count = 0

for idx, coil in fact_production_coil.iterrows():
    coil_id = coil["coil_id"]
    d = coil["production_date"]
    thickness_mm = coil["thickness_mm"]
    width_mm = coil["width_mm"]
    completion_ts = coil["completion_ts"]
    
    if pd.isna(completion_ts):
        continue

    # Assign shift based on completion hour
    day_crew, night_crew = date_to_crews.get(d, ("A", "B"))
    if 6 <= completion_ts.hour < 18:
        shift_code = day_crew
    else:
        shift_code = night_crew

    # Generate equipment-specific durations
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

    # Anchor operation window to real completion time
    coil_end_ts = completion_ts
    coil_start_ts = coil_end_ts - timedelta(seconds=total_sec)

    fact_production_coil.at[idx, "start_datetime"] = coil_start_ts
    fact_production_coil.at[idx, "end_datetime"] = coil_end_ts
    fact_production_coil.at[idx, "shift_code"] = shift_code

    # Build equipment operation timeline
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
            "queue_time_sec": 0.0,
            "is_bottleneck_step": is_bottle,
            "type_code": coil["type_code"],
            "is_prime": coil["is_prime"],
            "is_scrap": coil["is_scrap"],
        })

        ts = op_end_ts

    processed_count += 1
    
    if (processed_count % 1000 == 0):
        print(f"  Processed {processed_count:,} / {len(fact_production_coil):,} coils")

# Build operation cycle fact table
fact_coil_operation_cycle = pd.DataFrame(fact_coil_ops_rows)

# Calculate total cycle time per coil
fact_production_coil["total_cycle_time_min"] = (
    (fact_production_coil["end_datetime"] - fact_production_coil["start_datetime"])
    .dt.total_seconds() / 60.0
)

print(f"\nGenerated {len(fact_coil_operation_cycle):,} operation records")
print(f"Processed {processed_count:,} coils with valid completion times")

print("\nOperation cycle summary:")
print(f"  Total operations: {len(fact_coil_operation_cycle):,}")
print(f"  Unique coils: {fact_coil_operation_cycle['coil_id'].nunique():,}")
print(f"  Unique parent coils: {fact_coil_operation_cycle['parent_coil_id'].nunique():,}")
print(f"  Equipment tracked: {fact_coil_operation_cycle['equipment_id'].nunique()}")

print("\nCycle time distribution (minutes):")
cycle_stats = fact_production_coil["total_cycle_time_min"].describe()
print(f"  Mean: {cycle_stats['mean']:.2f} min")
print(f"  Median: {cycle_stats['50%']:.2f} min")
print(f"  Range: {cycle_stats['min']:.2f} - {cycle_stats['max']:.2f} min")

# Validate synthetic end vs real completion alignment
time_diff = (fact_production_coil['end_datetime'] - fact_production_coil['completion_ts']).dt.total_seconds()

print("\nValidation - Synthetic vs Real completion alignment:")
print(f"  Mean difference: {time_diff.mean():.4f} seconds")
print(f"  Max difference: {time_diff.abs().max():.4f} seconds")

if time_diff.abs().max() < 1:
    print("  ✓ VALIDATION PASSED: Synthetic operations anchored to real completion times")
else:
    print(f"  ⚠ WARNING: Max difference {time_diff.abs().max():.2f}s exceeds threshold")

# Cycle time analysis by product characteristics
print("\nCycle time by product type:")
cycle_by_type = (
    fact_production_coil
    .groupby('type_code')['total_cycle_time_min']
    .agg(['count', 'mean', 'min', 'max'])
    .round(2)
    .sort_values('mean', ascending=False)
    .head(10)
)
print(cycle_by_type)

print("\nPrime vs Scrap cycle time comparison:")
prime_vs_scrap = fact_production_coil.groupby('is_prime')['total_cycle_time_min'].agg(['count', 'mean', 'std']).round(2)
print(prime_vs_scrap)

print("\nSample operation records:")
print(fact_coil_operation_cycle[[
    'coil_id', 'equipment_name', 'operation_start_ts', 
    'operation_end_ts', 'operation_duration_sec', 'is_bottleneck_step'
]].head(15))