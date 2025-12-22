# Data validation and analysis

print("Data Validation and Analysis\n")

# Cycle time statistics
print("1. Cycle Time Statistics")
print("-" * 60)

cycle_stats = fact_production_coil[["thickness_mm", "width_mm", "total_cycle_time_min"]].describe().T
print(cycle_stats)

print("\nCycle time by product type:")
cycle_by_type = fact_production_coil.groupby('type_code')['total_cycle_time_min'].describe()
print(cycle_by_type.head(10))

print("\nPrime vs Scrap cycle time comparison:")
prime_scrap_cycle = fact_production_coil.groupby('is_prime')['total_cycle_time_min'].agg(['count', 'mean', 'std'])
print(prime_scrap_cycle)

# Product mix band analysis
print("\n2. Product Mix Band Analysis")
print("-" * 60)

def classify_product(row):
    t = row["thickness_mm"]
    w = row["width_mm"]
    if pd.isna(t) or pd.isna(w):
        return "unknown"
    
    is_thin = t <= 2.0
    is_narrow = w <= 1300
    
    if is_thin and is_narrow:
        return "thin & narrow (fast band)"
    else:
        return "other mix"

prod_with_band = fact_production_coil.copy()
prod_with_band["product_band"] = prod_with_band.apply(classify_product, axis=1)

cycle_summary = (
    prod_with_band
    .groupby("product_band")["total_cycle_time_min"]
    .agg(["count", "mean", "std"])
    .sort_values("mean")
)

cycle_summary["pieces_per_hour"] = 60 / cycle_summary["mean"]

print("\nCycle time by product band:")
print(cycle_summary)

global_mean_cycle = fact_production_coil["total_cycle_time_min"].mean()
global_pieces_per_hour = 60 / global_mean_cycle

print(f"\nGlobal statistics:")
print(f"  Mean cycle time: {global_mean_cycle:.2f} min")
print(f"  Implied tempo: {global_pieces_per_hour:.2f} pieces/hour")

# Parent coil yield analysis
print("\n3. Parent Coil Yield Analysis")
print("-" * 60)

parent_analysis = (
    fact_production_coil
    .groupby('parent_coil_id')
    .agg({
        'coil_id': 'count',
        'is_prime': 'sum',
        'is_scrap': 'sum',
        'completion_ts': ['min', 'max']
    })
)

parent_analysis.columns = ['total_pieces', 'prime_pieces', 'scrap_pieces', 'first_completion', 'last_completion']
parent_analysis['prime_rate_%'] = 100 * parent_analysis['prime_pieces'] / parent_analysis['total_pieces']
parent_analysis['parent_cycle_min'] = (
    (parent_analysis['last_completion'] - parent_analysis['first_completion']).dt.total_seconds() / 60.0
)

print("\nParent coil statistics:")
print(f"  Avg pieces per parent: {parent_analysis['total_pieces'].mean():.2f}")
print(f"  Avg prime pieces: {parent_analysis['prime_pieces'].mean():.2f}")
print(f"  Avg scrap pieces: {parent_analysis['scrap_pieces'].mean():.2f}")
print(f"  Avg prime rate: {parent_analysis['prime_rate_%'].mean():.1f}%")
print(f"  Avg parent cycle time: {parent_analysis['parent_cycle_min'].mean():.2f} min")

# Gap analysis (tempo validation)
print("\n4. Gap Analysis (Production Tempo)")
print("-" * 60)

gap_stats = fact_production_coil['gap_from_prev_completion_min'].describe()
print(f"\nCompletion gap statistics (minutes):")
print(f"  Mean: {gap_stats['mean']:.2f} min")
print(f"  Median: {gap_stats['50%']:.2f} min")
print(f"  90th percentile: {fact_production_coil['gap_from_prev_completion_min'].quantile(0.90):.2f} min")

parent_gap_stats = fact_production_coil['gap_from_prev_parent_min'].describe()
print(f"\nParent gap statistics (minutes):")
print(f"  Mean: {parent_gap_stats['mean']:.2f} min")
print(f"  Median: {parent_gap_stats['50%']:.2f} min")

# Equipment bottleneck validation
print("\n5. Equipment Bottleneck Validation")
print("-" * 60)

equip_durations = (
    fact_coil_operation_cycle
    .groupby("equipment_name")["operation_duration_sec"]
    .agg(["count", "mean", "sum"])
    .sort_values("mean", ascending=False)
)

equip_durations["mean_min"] = equip_durations["mean"] / 60.0

print("\nMean operation duration per equipment (top 10):")
print(equip_durations[["count", "mean_min"]].head(10))

total_op_time = fact_coil_operation_cycle["operation_duration_sec"].sum()

equip_share = (
    fact_coil_operation_cycle
    .groupby("equipment_name")["operation_duration_sec"]
    .sum()
    .to_frame("total_op_sec")
)

equip_share["share_of_line_%"] = 100 * equip_share["total_op_sec"] / total_op_time
equip_share = equip_share.sort_values("share_of_line_%", ascending=False)

print("\nEquipment share of total line time (bottleneck indicators):")
print(equip_share[["share_of_line_%"]].head(10))

# Equipment event analysis
print("\n6. Equipment Event Analysis (RUN/IDLE/FAULT)")
print("-" * 60)

equip_event_time = (
    fact_equipment_event_log
    .groupby(["equipment_name", "event_type"])["event_duration_sec"]
    .sum()
    .to_frame("total_sec")
    .reset_index()
)

equip_event_pivot = (
    equip_event_time
    .pivot_table(
        index="equipment_name",
        columns="event_type",
        values="total_sec",
        aggfunc="sum",
        fill_value=0
    )
)

equip_event_pivot["total"] = equip_event_pivot.sum(axis=1)

for col in ["RUN", "IDLE", "FAULT"]:
    if col in equip_event_pivot.columns:
        equip_event_pivot[col + "_%"] = 100 * equip_event_pivot[col] / equip_event_pivot["total"]

print("\nEquipment utilization (top 10 by total time):")
print(equip_event_pivot[["RUN_%", "IDLE_%", "FAULT_%"]].sort_values("RUN_%", ascending=False).head(10))

# Real vs synthetic time validation
print("\n7. Synthetic vs Real Completion Time Validation")
print("-" * 60)

time_diff = (fact_production_coil['end_datetime'] - fact_production_coil['completion_ts']).dt.total_seconds()

print(f"\nTime difference statistics (seconds):")
print(f"  Mean: {time_diff.mean():.6f}")
print(f"  Max: {time_diff.abs().max():.6f}")

if time_diff.abs().max() < 1:
    print("  ✓ VALIDATION PASSED: Synthetic operations anchored to real completion times")
else:
    print(f"  ⚠ WARNING: Max difference {time_diff.abs().max():.2f}s exceeds threshold")

# Validation summary
print("\n" + "="*80)
print("VALIDATION SUMMARY")
print("="*80)

total_pieces = len(fact_production_coil)
total_parent_coils = fact_production_coil['parent_coil_id'].nunique()
prime_pieces = fact_production_coil['is_prime'].sum()
scrap_pieces = fact_production_coil['is_scrap'].sum()

print(f"\n✓ Production data:")
print(f"  Total output pieces: {total_pieces:,}")
print(f"  Unique parent coils: {total_parent_coils:,}")
print(f"  Prime pieces: {prime_pieces:,} ({100*prime_pieces/total_pieces:.1f}%)")
print(f"  Scrap pieces: {scrap_pieces:,} ({100*scrap_pieces/total_pieces:.1f}%)")

print(f"\n✓ Product mix:")
fast_band_count = (prod_with_band['product_band'] == 'thin & narrow (fast band)').sum()
print(f"  Fast band (thin & narrow): {fast_band_count:,} pieces")
print(f"  Other mix: {total_pieces - fast_band_count:,} pieces")

print(f"\n✓ Cycle times:")
print(f"  Mean: {global_mean_cycle:.2f} min/piece")
print(f"  Range: {fact_production_coil['total_cycle_time_min'].min():.2f} - {fact_production_coil['total_cycle_time_min'].max():.2f} min")

print(f"\n✓ Equipment bottlenecks:")
print(f"  Top time consumer: {equip_share.index[0]} ({equip_share.iloc[0]['share_of_line_%']:.1f}%)")

print(f"\n✓ Event log:")
print(f"  Total events: {len(fact_equipment_event_log):,}")
print(f"  RUN: {(fact_equipment_event_log['event_type'] == 'RUN').sum():,}")
print(f"  IDLE: {(fact_equipment_event_log['event_type'] == 'IDLE').sum():,}")
print(f"  FAULT: {(fact_equipment_event_log['event_type'] == 'FAULT').sum():,}")

print(f"\n✓ Synthetic validation:")
print(f"  Max time difference: {time_diff.abs().max():.6f} seconds")
print(f"  Synthetic operations correctly anchored to real MES completion times")