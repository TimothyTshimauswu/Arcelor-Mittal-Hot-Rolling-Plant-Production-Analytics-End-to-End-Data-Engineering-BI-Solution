# Build maintenance event fact table
maint = df_maint.copy()

# Parse maintenance event timestamps
maint["start_datetime"] = pd.to_datetime(
    maint["Start"], 
    format='%m/%d/%y %H:%M', 
    errors='coerce'
)

# Extract duration (hours and minutes)
if "Time (Hours)" in maint.columns:
    maint["duration_hours"] = pd.to_numeric(maint["Time (Hours)"], errors="coerce")
else:
    if "Duration" in maint.columns:
        maint["duration_timedelta"] = pd.to_timedelta(maint["Duration"], errors='coerce')
        maint["duration_hours"] = maint["duration_timedelta"].dt.total_seconds() / 3600
    else:
        raise ValueError("No valid duration column found in maintenance data")

maint["duration_min"] = maint["duration_hours"] * 60

# Map cleaned equipment names
if "SubArea_Clean" not in maint.columns:
    raise ValueError("SubArea_Clean column missing - run cleaning step first")

maint["equipment_name"] = maint["SubArea_Clean"].astype(str).str.strip()

# Build maintenance fact table
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

# Remove incomplete records
fact_maintenance_event = fact_maintenance_event.dropna(
    subset=["start_datetime", "duration_hours"]
)

print(f"Maintenance fact table created: {len(fact_maintenance_event):,} events")
print(f"  Date range: {fact_maintenance_event['start_datetime'].min()} to {fact_maintenance_event['start_datetime'].max()}")
print(f"  Unique equipment: {fact_maintenance_event['equipment_name'].nunique()}")

print("\nDowntime duration statistics (hours):")
duration_stats = fact_maintenance_event['duration_hours'].describe()
print(f"  Mean: {duration_stats['mean']:.2f} hrs")
print(f"  Median: {duration_stats['50%']:.2f} hrs")
print(f"  Total downtime: {fact_maintenance_event['duration_hours'].sum():.1f} hrs")

print("\nTop 10 equipment by maintenance frequency:")
print(fact_maintenance_event['equipment_name'].value_counts().head(10))

print("\nMaintenance event categories:")
print(fact_maintenance_event['Category'].value_counts().head(10))

print("\nDelay type distribution:")
print(fact_maintenance_event['Delay Type'].value_counts().head(10))

print("\nSample maintenance events:")
print(fact_maintenance_event[[
    'start_datetime', 'equipment_name', 'duration_hours', 
    'Category', 'Delay Type'
]].head(10))