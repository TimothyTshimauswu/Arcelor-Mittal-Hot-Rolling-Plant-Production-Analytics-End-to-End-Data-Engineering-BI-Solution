# Clean gap data by removing outliers and handling edge cases

df_clean = fact_production_coil.copy()

print("Cleaning gap data - removing outliers and data quality issues\n")

# Initial gap statistics
print("Original gap statistics:")
print(df_clean[["gap_from_prev_completion_min", "gap_from_prev_parent_min"]].describe())

total_records = len(df_clean)
original_completion_gaps = df_clean["gap_from_prev_completion_min"].notna().sum()
original_parent_gaps = df_clean["gap_from_prev_parent_min"].notna().sum()

print(f"\nRecords before cleaning:")
print(f"  Total: {total_records:,}")
print(f"  With completion gaps: {original_completion_gaps:,}")
print(f"  With parent gaps: {original_parent_gaps:,}")

# Remove negative gaps (data quality issues)
negative_completion = (df_clean["gap_from_prev_completion_min"] < 0).sum()
negative_parent = (df_clean["gap_from_prev_parent_min"] < 0).sum()

if negative_completion > 0 or negative_parent > 0:
    print(f"\nRemoving negative gaps:")
    print(f"  Completion gaps < 0: {negative_completion}")
    print(f"  Parent gaps < 0: {negative_parent}")
    
    df_clean.loc[df_clean["gap_from_prev_completion_min"] < 0, "gap_from_prev_completion_min"] = np.nan
    df_clean.loc[df_clean["gap_from_prev_parent_min"] < 0, "gap_from_prev_parent_min"] = np.nan

# Cap completion gaps above 6 hours (likely shift changes or extended downtime)
completion_outliers = (df_clean["gap_from_prev_completion_min"] > 360).sum()
if completion_outliers > 0:
    print(f"\nCapping completion gaps > 360 min (6 hours): {completion_outliers} records")
    df_clean.loc[df_clean["gap_from_prev_completion_min"] > 360, "gap_from_prev_completion_min"] = np.nan

# Cap parent coil gaps above 30 minutes (pieces from same parent should be close)
parent_outliers = (df_clean["gap_from_prev_parent_min"] > 30).sum()
if parent_outliers > 0:
    print(f"\nCapping parent gaps > 30 min: {parent_outliers} records")
    df_clean.loc[df_clean["gap_from_prev_parent_min"] > 30, "gap_from_prev_parent_min"] = np.nan

# Analyze scrap vs prime piece gaps
scrap_gaps = df_clean[df_clean["is_scrap"] == True]["gap_from_prev_completion_min"]
prime_gaps = df_clean[df_clean["is_prime"] == True]["gap_from_prev_completion_min"]

print(f"\nGap analysis by product classification:")
print(f"  Scrap pieces (HX/HY/HZ):")
print(f"    Count: {scrap_gaps.notna().sum():,}")
print(f"    Mean: {scrap_gaps.mean():.2f} min")
print(f"    Median: {scrap_gaps.median():.2f} min")
print(f"  Prime pieces (HL/HM/98):")
print(f"    Count: {prime_gaps.notna().sum():,}")
print(f"    Mean: {prime_gaps.mean():.2f} min")
print(f"    Median: {prime_gaps.median():.2f} min")

# Cleaned gap statistics
print("\nCleaned gap statistics:")
print(df_clean[["gap_from_prev_completion_min", "gap_from_prev_parent_min"]].describe())

cleaned_completion_gaps = df_clean["gap_from_prev_completion_min"].notna().sum()
cleaned_parent_gaps = df_clean["gap_from_prev_parent_min"].notna().sum()

print(f"\nOutliers removed:")
print(f"  Completion gaps: {original_completion_gaps - cleaned_completion_gaps:,}")
print(f"  Parent gaps: {original_parent_gaps - cleaned_parent_gaps:,}")
print(f"  Total records preserved: {len(df_clean):,}")

# Gap distribution analysis
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

# Update main fact table
fact_production_coil = df_clean

print("\n✓ Gap cleaning complete - fact_production_coil updated")