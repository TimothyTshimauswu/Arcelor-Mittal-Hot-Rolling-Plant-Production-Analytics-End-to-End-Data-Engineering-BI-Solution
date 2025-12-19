# ============================
# 6. BUILD fact_maintenance_event BASE
# ============================

maint = df_maint.copy()

# --- 6.1 Parse start datetime ---
maint["start_datetime"] = pd.to_datetime(maint["Start"], format='%m/%d/%y %H:%M', errors='coerce')

# --- 6.2 Duration hours ---
# Primary source: "Time (Hours)" column
if "Time (Hours)" in maint.columns:
    maint["duration_hours"] = pd.to_numeric(maint["Time (Hours)"], errors="coerce")
else:
    # Fallback: parse "Duration" column (format: HH:MM:SS)
    if "Duration" in maint.columns:
        maint["duration_timedelta"] = pd.to_timedelta(maint["Duration"], errors='coerce')
        maint["duration_hours"] = maint["duration_timedelta"].dt.total_seconds() / 3600
    else:
        raise ValueError("No valid duration column found in maintenance file.")

# Duration in minutes
maint["duration_min"] = maint["duration_hours"] * 60

# --- 6.3 Equipment name (from SubArea_Clean) ---
# Ensure the cleaned SubArea exists
if "SubArea_Clean" not in maint.columns:
    raise ValueError("SubArea_Clean column does not exist. Make sure cleaning step ran.")

maint["equipment_name"] = maint["SubArea_Clean"].astype(str).str.strip()

# --- 6.4 Build the fact table ---
fact_maintenance_event = maint[[
    "start_datetime",
    "duration_hours",
    "duration_min",
    "equipment_name",
    "Crew",
    "Shifts",
    "Category",
    "Delay Type",
    "Area",
    "Sub Area",
    "Hierachy",
    "Decription",
    "Day",
    "Reasponsible",
    "Responsible"
]].copy()

# Remove rows with missing start_datetime or duration
fact_maintenance_event = fact_maintenance_event.dropna(subset=["start_datetime", "duration_hours"])

print("="*80)
print("FACT TABLE: fact_maintenance_event")
print("="*80)
print(f"\nTotal maintenance events: {len(fact_maintenance_event):,}")
print(f"Date range: {fact_maintenance_event['start_datetime'].min()} to {fact_maintenance_event['start_datetime'].max()}")
print(f"Unique equipment: {fact_maintenance_event['equipment_name'].nunique()}")

print("\nDuration statistics (hours):")
print(fact_maintenance_event['duration_hours'].describe())

print("\nTop 10 equipment by event count:")
print(fact_maintenance_event['equipment_name'].value_counts().head(10))

print("\nTop delay types:")
print(fact_maintenance_event['Delay Type'].value_counts().head(10))

print("\nSample records:")
print(fact_maintenance_event.head(10))