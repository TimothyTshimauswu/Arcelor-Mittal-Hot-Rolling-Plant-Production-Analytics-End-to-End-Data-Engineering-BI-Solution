# ============================
# 11A. CLEAN GAPS (REMOVE OUTLIERS & FIX SCRAP/PARENT LOGIC)
# ============================

print("="*80)
print("CLEANING GAP DATA (REMOVING OUTLIERS)")
print("="*80)

df_clean = fact_production_coil.copy()

print("\nOriginal gap statistics:")
print(df_clean[["gap_from_prev_completion_min", "gap_from_prev_parent_min"]].describe())

# Count records before cleaning
total_records = len(df_clean)
original_completion_gaps = df_clean["gap_from_prev_completion_min"].notna().sum()
original_parent_gaps = df_clean["gap_from_prev_parent_min"].notna().sum()

print(f"\nRecords before cleaning:")
print(f"  Total records: {total_records:,}")
print(f"  Records with completion gaps: {original_completion_gaps:,}")
print(f"  Records with parent gaps: {original_parent_gaps:,}")

# ============================
# 1. Remove impossible negative gaps
# ============================
negative_completion = (df_clean["gap_from_prev_completion_min"] < 0).sum()
negative_parent = (df_clean["gap_from_prev_parent_min"] < 0).sum()

print(f"\nNegative gaps detected:")
print(f"  Completion gaps < 0: {negative_completion}")
print(f"  Parent gaps < 0: {negative_parent}")

df_clean.loc[df_clean["gap_from_prev_completion_min"] < 0, "gap_from_prev_completion_min"] = np.nan
df_clean.loc[df_clean["gap_from_prev_parent_min"] < 0, "gap_from_prev_parent_min"] = np.nan

# ============================
# 2. Cap completion gaps above reasonable threshold
# ============================
# Gaps > 6 hours (360 min) likely indicate extended downtime, shift changes, or data issues
# We'll cap these to avoid skewing tempo calculations

completion_outliers = (df_clean["gap_from_prev_completion_min"] > 360).sum()
print(f"\nCompletion gaps > 360 min (6 hours): {completion_outliers}")

df_clean.loc[df_clean["gap_from_prev_completion_min"] > 360, "gap_from_prev_completion_min"] = np.nan

# ============================
# 3. Parent coil gaps: reasonable threshold
# ============================
# Parent coil gaps should typically be small (coil pieces come out close together)
# Gaps > 30 min likely indicate the pieces are from different production runs

parent_outliers = (df_clean["gap_from_prev_parent_min"] > 30).sum()
print(f"\nParent gaps > 30 min: {parent_outliers}")

df_clean.loc[df_clean["gap_from_prev_parent_min"] > 30, "gap_from_prev_parent_min"] = np.nan

# ============================
# 4. Handle scrap pieces logic
# ============================
# Scrap pieces (HX, HY, HZ) from the same parent coil may have very short gaps
# This is expected behavior - they're trim pieces coming off quickly
# We keep these small gaps as they represent real production tempo

print("\n" + "-"*80)
print("Scrap piece gap analysis:")
print("-"*80)

scrap_gaps = df_clean[df_clean["is_scrap"] == True]["gap_from_prev_completion_min"]
prime_gaps = df_clean[df_clean["is_prime"] == True]["gap_from_prev_completion_min"]

print(f"\nScrap pieces (HX/HY/HZ) gaps:")
print(f"  Count: {scrap_gaps.notna().sum():,}")
print(f"  Mean: {scrap_gaps.mean():.2f} min")
print(f"  Median: {scrap_gaps.median():.2f} min")

print(f"\nPrime pieces (HL/HM/98/etc) gaps:")
print(f"  Count: {prime_gaps.notna().sum():,}")
print(f"  Mean: {prime_gaps.mean():.2f} min")
print(f"  Median: {prime_gaps.median():.2f} min")

# ============================
# SUMMARY
# ============================

print("\n" + "="*80)
print("CLEANED GAP SUMMARY")
print("="*80)

gap_summary_clean = df_clean[[
    "gap_from_prev_completion_min",
    "gap_from_prev_parent_min"
]].describe()

print("\nCleaned gap statistics:")
print(gap_summary_clean)

# Count records after cleaning
cleaned_completion_gaps = df_clean["gap_from_prev_completion_min"].notna().sum()
cleaned_parent_gaps = df_clean["gap_from_prev_parent_min"].notna().sum()

print(f"\nRecords after cleaning:")
print(f"  Total records: {len(df_clean):,}")
print(f"  Records with valid completion gaps: {cleaned_completion_gaps:,}")
print(f"  Records with valid parent gaps: {cleaned_parent_gaps:,}")

print(f"\nRemoved outliers:")
print(f"  Completion gaps removed: {original_completion_gaps - cleaned_completion_gaps:,}")
print(f"  Parent gaps removed: {original_parent_gaps - cleaned_parent_gaps:,}")

# Visualize gap distribution
print("\n" + "-"*80)
print("GAP DISTRIBUTION (after cleaning)")
print("-"*80)

print("\nCompletion gap distribution (minutes):")
completion_bins = [0, 1, 2, 5, 10, 20, 30, 60, 120, 360]
completion_dist = pd.cut(
    df_clean["gap_from_prev_completion_min"].dropna(),
    bins=completion_bins,
    include_lowest=True
).value_counts().sort_index()
print(completion_dist)

print("\nParent gap distribution (minutes):")
parent_bins = [0, 1, 2, 5, 10, 20, 30]
parent_dist = pd.cut(
    df_clean["gap_from_prev_parent_min"].dropna(),
    bins=parent_bins,
    include_lowest=True
).value_counts().sort_index()
print(parent_dist)

# Update the main fact table
fact_production_coil = df_clean

print("\n" + "="*80)
print("✓ GAP CLEANING COMPLETE")
print("="*80)
print("\nfact_production_coil has been updated with cleaned gap data")