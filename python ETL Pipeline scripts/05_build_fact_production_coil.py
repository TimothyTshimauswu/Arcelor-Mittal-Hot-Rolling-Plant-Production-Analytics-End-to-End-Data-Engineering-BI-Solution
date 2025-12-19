import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta

# ============================
# 5. BUILD fact_production_coil WITH REAL COMPLETION TIME
# ============================

prod = df_prod.copy()

# Thickness: prefer 'Thickess' if present and not all NaN, else 'Thick'
if "Thickess" in prod.columns and prod["Thickess"].notna().any():
    prod["thickness_mm"] = prod["Thickess"]
elif "Thick" in prod.columns:
    prod["thickness_mm"] = prod["Thick"]
else:
    raise ValueError("No thickness column found in production data")

# Basic geometry and mass
prod["width_mm"] = prod["Width"]
prod["mass_out_tons"] = prod["Mass out tons"]

# Parse Production Date as datetime
prod["Production Date"] = pd.to_datetime(prod["Production Date"], format='%m/%d/%y %H:%M', errors='coerce')

# Date and IDs
prod["production_date"] = prod["Production Date"].dt.date
prod["coil_id"] = prod["UID"].astype(str)  # UID is the unique piece identifier
prod["parent_coil_id"] = prod["CID"].astype(str)  # CID is the parent coil

# Real completion timestamp from system (end of processing for this coil piece)
# The "Production Date" column already contains the completion timestamp
prod["completion_ts"] = prod["Production Date"]

# Type-based prime/scrap classification
if "Type" in prod.columns:
    prod["type_code"] = prod["Type"].astype(str).str.strip().str.upper()
    prime_types = ["HL", "HM", "98", "71", "72", "74", "75", "76", "77", "70"]  # Prime product types
    scrap_types = ["HX", "HY", "HZ", "HC", "HH", "HR"]  # Scrap types
    prod["is_prime"] = prod["type_code"].isin(prime_types)
    prod["is_scrap"] = prod["type_code"].isin(scrap_types)
else:
    prod["type_code"] = None
    prod["is_prime"] = False
    prod["is_scrap"] = False

# Sort by completion time to compute gaps between coils
prod = prod.sort_values("completion_ts").reset_index(drop=True)

# Real gap between completions (minutes) – plant-level tempo from MES system
prod["prev_completion_ts"] = prod["completion_ts"].shift(1)
prod["gap_from_prev_completion_min"] = (
    (prod["completion_ts"] - prod["prev_completion_ts"])
    .dt.total_seconds() / 60.0
)

# Compute gaps between parent coils (CID)
# Get first and last completion time for each parent coil
parent_timing = (
    prod.groupby("parent_coil_id")["completion_ts"]
    .agg(['min', 'max'])
    .rename(columns={'min': 'parent_first_completion_ts', 'max': 'parent_last_completion_ts'})
    .reset_index()
)

# Sort parent coils by their first completion time
parent_timing = parent_timing.sort_values("parent_first_completion_ts").reset_index(drop=True)

# Calculate gap from previous parent coil's last completion to this parent's first completion
parent_timing["prev_parent_last_completion_ts"] = parent_timing["parent_last_completion_ts"].shift(1)
parent_timing["gap_from_prev_parent_min"] = (
    (parent_timing["parent_first_completion_ts"] - parent_timing["prev_parent_last_completion_ts"])
    .dt.total_seconds() / 60.0
)

# Attach parent-level gaps back to rows
prod = prod.merge(
    parent_timing[["parent_coil_id", "gap_from_prev_parent_min"]],
    on="parent_coil_id",
    how="left"
)

# Placeholder shift_code for now (will be overwritten later using crew rotation)
prod["shift_code"] = "A"

# Base fact table
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

print("="*80)
print("FACT TABLE: fact_production_coil (WITH REAL COMPLETION TIMES)")
print("="*80)

print(f"\nTotal records: {len(fact_production_coil):,}")
print(f"Unique output pieces (UID/coil_id): {fact_production_coil['coil_id'].nunique():,}")
print(f"Unique parent coils (CID/parent_coil_id): {fact_production_coil['parent_coil_id'].nunique():,}")
print(f"Date range: {fact_production_coil['production_date'].min()} to {fact_production_coil['production_date'].max()}")

print("\nProduct type distribution:")
print(fact_production_coil['type_code'].value_counts().head(15))

print("\nPrime vs Scrap:")
print(f"  Prime pieces: {fact_production_coil['is_prime'].sum():,} ({100*fact_production_coil['is_prime'].mean():.1f}%)")
print(f"  Scrap pieces: {fact_production_coil['is_scrap'].sum():,} ({100*fact_production_coil['is_scrap'].mean():.1f}%)")

print("\nGap statistics (minutes between completions):")
print(fact_production_coil['gap_from_prev_completion_min'].describe())

print("\nParent coil gap statistics (minutes between parent coils):")
print(fact_production_coil['gap_from_prev_parent_min'].describe())

print("\nSample records (showing real completion times and gaps):")
print(fact_production_coil[[
    'coil_id', 'parent_coil_id', 'completion_ts', 'type_code', 
    'is_prime', 'is_scrap', 'gap_from_prev_completion_min'
]].head(20))

# Additional analysis: Parent coil yield
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

print("\n" + "-"*80)
print("PARENT COIL YIELD ANALYSIS")
print("-"*80)
print(f"Average pieces per parent coil: {parent_yield['total_pieces'].mean():.2f}")
print(f"Average prime pieces per parent: {parent_yield['prime_pieces'].mean():.2f}")
print(f"Average scrap pieces per parent: {parent_yield['scrap_pieces'].mean():.2f}")
print(f"Average prime rate: {parent_yield['prime_rate_%'].mean():.1f}%")

print("\nSample parent coil breakdown:")
print(parent_yield.head(10))