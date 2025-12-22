# Build equipment event log (RUN/IDLE/FAULT timeline)

event_rows = []

# Generate RUN and IDLE events from synthetic coil operations
print("Generating RUN and IDLE events from coil operations...")

run_df = fact_coil_operation_cycle.copy()
run_df["event_start_ts"] = run_df["operation_start_ts"]
run_df["event_end_ts"] = run_df["operation_end_ts"]

run_count = 0
idle_count = 0

for eq_id, g in run_df.groupby("equipment_id"):
    g = g.sort_values("event_start_ts").reset_index(drop=True)
    equip_name = g["equipment_name"].iloc[0]

    # RUN events (one per coil at this equipment)
    for _, r in g.iterrows():
        event_rows.append({
            "equipment_id": eq_id,
            "equipment_name": equip_name,
            "event_type": "RUN",
            "event_start_ts": r["event_start_ts"],
            "event_end_ts": r["event_end_ts"],
            "event_duration_sec": (r["event_end_ts"] - r["event_start_ts"]).total_seconds(),
            "coil_id": r["coil_id"],
            "parent_coil_id": r["parent_coil_id"],
            "shift_code": r["shift_code"],
            "type_code": r.get("type_code"),
            "is_prime": r.get("is_prime", False),
            "is_scrap": r.get("is_scrap", False),
        })
        run_count += 1

    # IDLE events (gaps between RUN events)
    min_idle_sec = 30

    for i in range(len(g) - 1):
        end_curr = g.loc[i, "event_end_ts"]
        start_next = g.loc[i + 1, "event_start_ts"]

        gap = (start_next - end_curr).total_seconds()
        if gap > min_idle_sec:
            event_rows.append({
                "equipment_id": eq_id,
                "equipment_name": equip_name,
                "event_type": "IDLE",
                "event_start_ts": end_curr,
                "event_end_ts": start_next,
                "event_duration_sec": gap,
                "coil_id": None,
                "parent_coil_id": None,
                "shift_code": None,
                "type_code": None,
                "is_prime": False,
                "is_scrap": False,
            })
            idle_count += 1

print(f"  Generated {run_count:,} RUN events")
print(f"  Generated {idle_count:,} IDLE events (>30s gaps)")

# Generate FAULT events from maintenance data
print("\nGenerating FAULT events from maintenance data...")

faults = fact_maintenance_event.merge(
    dim_equipment[["equipment_id", "equipment_name"]],
    on="equipment_name",
    how="left"
)

faults["fault_start_ts"] = faults["start_datetime"]
faults["fault_end_ts"] = faults["start_datetime"] + pd.to_timedelta(
    faults["duration_min"], unit="m"
)

fault_count = 0
fault_skipped = 0

for _, row in faults.iterrows():
    eq_id = row["equipment_id"]
    equip_name = row["equipment_name"]

    if pd.isna(eq_id):
        fault_skipped += 1
        continue

    event_rows.append({
        "equipment_id": eq_id,
        "equipment_name": equip_name,
        "event_type": "FAULT",
        "event_start_ts": row["fault_start_ts"],
        "event_end_ts": row["fault_end_ts"],
        "event_duration_sec": (row["fault_end_ts"] - row["fault_start_ts"]).total_seconds(),
        "coil_id": None,
        "parent_coil_id": None,
        "shift_code": row["Shifts"] if "Shifts" in faults.columns else None,
        "type_code": None,
        "is_prime": False,
        "is_scrap": False,
    })
    fault_count += 1

print(f"  Generated {fault_count:,} FAULT events")
print(f"  Skipped {fault_skipped:,} faults (equipment not in line)")

# Build equipment event log fact table
fact_equipment_event_log = pd.DataFrame(event_rows)
fact_equipment_event_log["event_date"] = fact_equipment_event_log["event_start_ts"].dt.date

print(f"\nEquipment event log created: {len(fact_equipment_event_log):,} events")
print(f"  RUN: {(fact_equipment_event_log['event_type'] == 'RUN').sum():,}")
print(f"  IDLE: {(fact_equipment_event_log['event_type'] == 'IDLE').sum():,}")
print(f"  FAULT: {(fact_equipment_event_log['event_type'] == 'FAULT').sum():,}")

print("\nEvent duration statistics by type (minutes):")
event_stats = fact_equipment_event_log.groupby('event_type')['event_duration_sec'].describe()
event_stats = event_stats / 60  # Convert to minutes
print(event_stats[['mean', '50%', 'max']].round(2))

# Analyze RUN events by product classification
run_events = fact_equipment_event_log[fact_equipment_event_log['event_type'] == 'RUN']
print(f"\nRUN event product distribution:")
print(f"  Prime pieces: {run_events['is_prime'].sum():,} ({100*run_events['is_prime'].mean():.1f}%)")
print(f"  Scrap pieces: {run_events['is_scrap'].sum():,} ({100*run_events['is_scrap'].mean():.1f}%)")

# Operation time by product type
if 'type_code' in run_events.columns:
    type_stats = (
        run_events.groupby('type_code')['event_duration_sec']
        .agg(['count', 'mean', 'sum'])
        .sort_values('count', ascending=False)
    )
    type_stats['mean_min'] = (type_stats['mean'] / 60.0).round(2)
    type_stats['total_hours'] = (type_stats['sum'] / 3600.0).round(1)
    
    print("\nTop 10 product types by operation time:")
    print(type_stats[['count', 'mean_min', 'total_hours']].head(10))

print("\nSample event records:")
print(fact_equipment_event_log[[
    'equipment_name', 'event_type', 'event_start_ts', 'event_end_ts', 
    'coil_id', 'type_code', 'is_prime'
]].head(15))