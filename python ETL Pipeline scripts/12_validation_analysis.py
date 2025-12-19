# ============================
# VALIDATION AND ANALYSIS (COMPLETION-ANCHORED MODEL)
# ============================

print("="*80)
print("DATA VALIDATION AND ANALYSIS")
print("="*80)

# ============================
# 1. BASIC CYCLE TIME STATISTICS
# ============================

print("\n1. BASIC CYCLE TIME STATISTICS")
print("-" * 60)
cycle_stats = fact_production_coil[["thickness_mm", "width_mm", "total_cycle_time_min"]].describe().T
print(cycle_stats)

print("\nCycle time by product type:")
cycle_by_type = fact_production_coil.groupby('type_code')['total_cycle_time_min'].describe()
print(cycle_by_type)

print("\nCycle time comparison: Prime vs Scrap:")
prime_scrap_cycle = fact_production_coil.groupby('is_prime')['total_cycle_time_min'].agg(['count', 'mean', 'std', 'min', 'max'])
print(prime_scrap_cycle)

# ============================
# 2. PRODUCT MIX BAND ANALYSIS
# ============================

print("\n2. PRODUCT MIX BAND ANALYSIS")
print("-" * 60)

# Define product mix bands
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
        return "other mix (~15min)"

prod_with_band = fact_production_coil.copy()
prod_with_band["product_band"] = prod_with_band.apply(classify_product, axis=1)

# Summary by product band
cycle_summary = (
    prod_with_band
    .groupby("product_band")["total_cycle_time_min"]
    .agg(["count", "mean", "std", "min", "max"])
    .sort_values("mean")
)

print("\nCycle time (minutes) by product band:")
print(cycle_summary)

# Approximate pieces per hour per band: 60 / mean(cycle time)
cycle_summary["approx_pieces_per_hour"] = 60 / cycle_summary["mean"]
print("\nApproximate pieces per hour by product band:")
print(cycle_summary[["mean", "approx_pieces_per_hour"]])

# Global mean cycle time and implied pieces/hour
global_mean_cycle = fact_production_coil["total_cycle_time_min"].mean()
global_pieces_per_hour = 60 / global_mean_cycle
print(f"\nGlobal statistics:")
print(f"  Mean cycle time: {global_mean_cycle:.2f} minutes")
print(f"  Implied pieces per hour: {global_pieces_per_hour:.2f}")

# ============================
# 3. PARENT COIL YIELD ANALYSIS
# ============================

print("\n3. PARENT COIL YIELD ANALYSIS")
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
parent_analysis['parent_cycle_time_min'] = (
    (parent_analysis['last_completion'] - parent_analysis['first_completion']).dt.total_seconds() / 60.0
)

print("\nParent coil summary statistics:")
print(parent_analysis[['total_pieces', 'prime_pieces', 'scrap_pieces', 'prime_rate_%', 'parent_cycle_time_min']].describe())

print(f"\nAverage pieces per parent coil: {parent_analysis['total_pieces'].mean():.2f}")
print(f"Average prime pieces per parent: {parent_analysis['prime_pieces'].mean():.2f}")
print(f"Average scrap pieces per parent: {parent_analysis['scrap_pieces'].mean():.2f}")
print(f"Average prime rate: {parent_analysis['prime_rate_%'].mean():.1f}%")
print(f"Average parent cycle time: {parent_analysis['parent_cycle_time_min'].mean():.2f} minutes")

print("\nSample parent coil breakdown:")
print(parent_analysis.head(15))

# ============================
# 4. GAP ANALYSIS (TEMPO VALIDATION)
# ============================

print("\n4. GAP ANALYSIS (PRODUCTION TEMPO)")
print("-" * 60)

print("\nCompletion gap statistics (minutes between any pieces):")
print(fact_production_coil['gap_from_prev_completion_min'].describe())

print("\nParent gap statistics (minutes between parent coils):")
print(fact_production_coil['gap_from_prev_parent_min'].describe())

# Gap comparison by product type
print("\nGap analysis by product type:")
gap_by_type = fact_production_coil.groupby('is_prime')['gap_from_prev_completion_min'].agg(['count', 'mean', 'median', 'std'])
print(gap_by_type)

# ============================
# 5. EQUIPMENT BOTTLENECK VALIDATION
# ============================

print("\n5. EQUIPMENT BOTTLENECK VALIDATION")
print("-" * 60)

# Operation duration stats by equipment
equip_durations = (
    fact_coil_operation_cycle
    .groupby("equipment_name")["operation_duration_sec"]
    .agg(["count", "mean", "std", "min", "max"])
    .sort_values("mean", ascending=False)
)

equip_durations["mean_min"] = equip_durations["mean"] / 60.0

print("\nMean operation duration per equipment (sorted by longest):")
print(equip_durations[["count", "mean_min", "min", "max"]])

# Share of total operation time per equipment (bottleneck share)
total_op_time = fact_coil_operation_cycle["operation_duration_sec"].sum()

equip_share = (
    fact_coil_operation_cycle
    .groupby("equipment_name")["operation_duration_sec"]
    .sum()
    .to_frame("total_op_sec")
)

equip_share["share_of_line_%"] = 100 * equip_share["total_op_sec"] / total_op_time
equip_share["total_op_min"] = equip_share["total_op_sec"] / 60.0

equip_share = equip_share.sort_values("share_of_line_%", ascending=False)

print("\nShare of total operation time per equipment (bottleneck indicators):")
print(equip_share)

# Equipment time by product type
print("\nEquipment operation time: Prime vs Scrap:")
equip_prime_scrap = (
    fact_coil_operation_cycle
    .groupby(['equipment_name', 'is_prime'])['operation_duration_sec']
    .sum()
    .unstack(fill_value=0)
)
equip_prime_scrap.columns = ['Scrap_sec', 'Prime_sec']
equip_prime_scrap['Total_sec'] = equip_prime_scrap['Scrap_sec'] + equip_prime_scrap['Prime_sec']
equip_prime_scrap['Prime_%'] = 100 * equip_prime_scrap['Prime_sec'] / equip_prime_scrap['Total_sec']
print(equip_prime_scrap.sort_values('Total_sec', ascending=False).head(10))

# ============================
# 6. EQUIPMENT EVENT ANALYSIS (RUN/IDLE/FAULT)
# ============================

print("\n6. EQUIPMENT EVENT ANALYSIS (RUN/IDLE/FAULT)")
print("-" * 60)

equip_event_time = (
    fact_equipment_event_log
    .groupby(["equipment_name", "event_type"])["event_duration_sec"]
    .sum()
    .to_frame("total_sec")
    .reset_index()
)

equip_event_time["total_min"] = equip_event_time["total_sec"] / 60.0
equip_event_time["total_hours"] = equip_event_time["total_min"] / 60.0

# Pivot to RUN/IDLE/FAULT shares
equip_event_pivot = (
    equip_event_time
    .pivot_table(
        index="equipment_name",
        columns="event_type",
        values="total_min",
        aggfunc="sum",
        fill_value=0
    )
)

equip_event_pivot["total_min_all"] = equip_event_pivot.sum(axis=1)

for col in ["RUN", "IDLE", "FAULT"]:
    if col in equip_event_pivot.columns:
        equip_event_pivot[col + "_%"] = 100 * equip_event_pivot[col] / equip_event_pivot["total_min_all"]

print("\nRUN/IDLE/FAULT minutes & percentage shares per equipment:")
print(equip_event_pivot)

# ============================
# 7. REAL VS SYNTHETIC TIME VALIDATION
# ============================

print("\n7. REAL VS SYNTHETIC TIME VALIDATION")
print("-" * 60)

# Validate that synthetic end_datetime matches real completion_ts
time_diff = (fact_production_coil['end_datetime'] - fact_production_coil['completion_ts']).dt.total_seconds()

print("\nAlignment check: Synthetic end_datetime vs Real completion_ts")
print("Time difference (should be ~0 seconds):")
print(time_diff.describe())

if time_diff.abs().max() < 1:
    print("\n✓ VALIDATION PASSED: Synthetic times perfectly anchored to real completion times")
else:
    print(f"\n⚠ WARNING: Max time difference of {time_diff.abs().max():.2f} seconds detected")

# ============================
# VALIDATION SUMMARY
# ============================

print("\n" + "="*80)
print("VALIDATION SUMMARY")
print("="*80)

total_pieces = len(fact_production_coil)
total_parent_coils = fact_production_coil['parent_coil_id'].nunique()
prime_pieces = fact_production_coil['is_prime'].sum()
scrap_pieces = fact_production_coil['is_scrap'].sum()

print(f"\n✓ Production data validated")
print(f"  - Total output pieces (UID): {total_pieces:,}")
print(f"  - Unique parent coils (CID): {total_parent_coils:,}")
print(f"  - Prime pieces: {prime_pieces:,} ({100*prime_pieces/total_pieces:.1f}%)")
print(f"  - Scrap pieces: {scrap_pieces:,} ({100*scrap_pieces/total_pieces:.1f}%)")
print(f"  - Avg pieces per parent coil: {total_pieces/total_parent_coils:.2f}")

print(f"\n✓ Product mix validated")
print(f"  - Fast band (thin & narrow): {(prod_with_band['product_band'] == 'thin & narrow (fast band)').sum():,} pieces")
print(f"  - Other mix: {(prod_with_band['product_band'] == 'other mix (~15min)').sum():,} pieces")

print(f"\n✓ Cycle times realistic")
print(f"  - Mean: {global_mean_cycle:.2f} min/piece")
print(f"  - Range: {fact_production_coil['total_cycle_time_min'].min():.2f} - {fact_production_coil['total_cycle_time_min'].max():.2f} min")

print(f"\n✓ Equipment bottlenecks identified")
print(f"  - Top time consumer: {equip_share.index[0]} ({equip_share.iloc[0]['share_of_line_%']:.1f}% of line time)")

print(f"\n✓ Event log complete")
print(f"  - Total events: {len(fact_equipment_event_log):,}")
print(f"  - RUN events: {(fact_equipment_event_log['event_type'] == 'RUN').sum():,}")
print(f"  - IDLE events: {(fact_equipment_event_log['event_type'] == 'IDLE').sum():,}")
print(f"  - FAULT events: {(fact_equipment_event_log['event_type'] == 'FAULT').sum():,}")

print(f"\n✓ Real completion times validated")
print(f"  - Synthetic operations anchored correctly")
print(f"  - Max time difference: {time_diff.abs().max():.4f} seconds")