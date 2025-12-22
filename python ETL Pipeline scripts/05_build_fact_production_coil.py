# Build production fact table with real MES completion timestamps
prod = df_prod.copy()

# Extract thickness measurement (handle column name variations)
if "Thickess" in prod.columns and prod["Thickess"].notna().any():
    prod["thickness_mm"] = prod["Thickess"]
elif "Thick" in prod.columns:
    prod["thickness_mm"] = prod["Thick"]
else:
    raise ValueError("No thickness column found in production data")

# Map product dimensions and mass
prod["width_mm"] = prod["Width"]
prod["mass_out_tons"] = prod["Mass out tons"]

# Parse completion timestamps from MES system
prod["Production Date"] = pd.to_datetime(
    prod["Production Date"], 
    format='%m/%d/%y %H:%M', 
    errors='coerce'
)

prod["production_date"] = prod["Production Date"].dt.date
prod["coil_id"] = prod["UID"].astype(str)
prod["parent_coil_id"] = prod["CID"].astype(str)
prod["completion_ts"] = prod["Production Date"]

# Classify product types (prime vs scrap)
if "Type" in prod.columns:
    prod["type_code"] = prod["Type"].astype(str).str.strip().str.upper()
    prime_types = ["HL", "HM", "98", "71", "72", "74", "75", "76", "77", "70"]
    scrap_types = ["HX", "HY", "HZ", "HC", "HH", "HR"]
    prod["is_prime"] = prod["type_code"].isin(prime_types)
    prod["is_scrap"] = prod["type_code"].isin(scrap_types)
else:
    prod["type_code"] = None
    prod["is_prime"] = False
    prod["is_scrap"] = False

# Sort by completion time for tempo analysis
prod = prod.sort_values("completion_ts").reset_index(drop=True)

# Calculate inter-coil gaps (tempo measurement)
prod["prev_completion_ts"] = prod["completion_ts"].shift(1)
prod["gap_from_prev_completion_min"] = (
    (prod["completion_ts"] - prod["prev_completion_ts"]).dt.total_seconds() / 60.0
)

# Calculate parent coil transition times
parent_timing = (
    prod.groupby("parent_coil_id")["completion_ts"]
    .agg(['min', 'max'])
    .rename(columns={'min': 'parent_first_completion_ts', 'max': 'parent_last_completion_ts'})
    .reset_index()
    .sort_values("parent_first_completion_ts")
    .reset_index(drop=True)
)

parent_timing["prev_parent_last_completion_ts"] = parent_timing["parent_last_completion_ts"].shift(1)
parent_timing["gap_from_prev_parent_min"] = (
    (parent_timing["parent_first_completion_ts"] - parent_timing["prev_parent_last_completion_ts"])
    .dt.total_seconds() / 60.0
)

# Merge parent-level gaps back to coil records
prod = prod.merge(
    parent_timing[["parent_coil_id", "gap_from_prev_parent_min"]],
    on="parent_coil_id",
    how="left"
)

# Placeholder shift assignment (will be updated with crew rotation)
prod["shift_code"] = "A"

# Build production fact table
fact_production_coil = prod[[
    "coil_id",
    "parent_coil_id",
    "production_date",
    "completion_ts",
    "shift_code",
    "thickness_mm",
    "width_mm",
    "mass_out_tons",
    "Hours",
    "Grade",
    "NextProcess",
    "type_code",
    "is_prime",
    "is_scrap",
    "gap_from_prev_completion_min",
    "gap_from_prev_parent_min",
    "Cast",
    "Slab",
]].copy()

print(f"Production fact table created: {len(fact_production_coil):,} records")
print(f"  Unique output pieces (UID): {fact_production_coil['coil_id'].nunique():,}")
print(f"  Unique parent coils (CID): {fact_production_coil['parent_coil_id'].nunique():,}")
print(f"  Date range: {fact_production_coil['production_date'].min()} to {fact_production_coil['production_date'].max()}")

print("\nProduct type distribution:")
print(fact_production_coil['type_code'].value_counts().head(10))

print("\nPrime vs Scrap classification:")
print(f"  Prime: {fact_production_coil['is_prime'].sum():,} ({100*fact_production_coil['is_prime'].mean():.1f}%)")
print(f"  Scrap: {fact_production_coil['is_scrap'].sum():,} ({100*fact_production_coil['is_scrap'].mean():.1f}%)")

print("\nTempo analysis - Inter-coil gaps (minutes):")
gap_stats = fact_production_coil['gap_from_prev_completion_min'].describe()
print(f"  Mean: {gap_stats['mean']:.1f} min")
print(f"  Median: {gap_stats['50%']:.1f} min")
print(f"  90th percentile: {fact_production_coil['gap_from_prev_completion_min'].quantile(0.90):.1f} min")

print("\nParent coil transition gaps (minutes):")
parent_gap_stats = fact_production_coil['gap_from_prev_parent_min'].describe()
print(f"  Mean: {parent_gap_stats['mean']:.1f} min")
print(f"  Median: {parent_gap_stats['50%']:.1f} min")

# Parent coil yield analysis
parent_yield = (
    fact_production_coil
    .groupby('parent_coil_id')
    .agg({
        'coil_id': 'count',
        'is_prime': 'sum',
        'is_scrap': 'sum'
    })
    .rename(columns={'coil_id': 'total_pieces', 'is_prime': 'prime_pieces', 'is_scrap': 'scrap_pieces'})
)

parent_yield['prime_rate_%'] = 100 * parent_yield['prime_pieces'] / parent_yield['total_pieces']

print("\nParent coil yield metrics:")
print(f"  Avg pieces per parent: {parent_yield['total_pieces'].mean():.2f}")
print(f"  Avg prime pieces: {parent_yield['prime_pieces'].mean():.2f}")
print(f"  Avg scrap pieces: {parent_yield['scrap_pieces'].mean():.2f}")
print(f"  Avg prime rate: {parent_yield['prime_rate_%'].mean():.1f}%")

print("\nSample records with completion times and gaps:")
print(fact_production_coil[[
    'coil_id', 'parent_coil_id', 'completion_ts', 'type_code', 
    'is_prime', 'gap_from_prev_completion_min'
]].head(15))