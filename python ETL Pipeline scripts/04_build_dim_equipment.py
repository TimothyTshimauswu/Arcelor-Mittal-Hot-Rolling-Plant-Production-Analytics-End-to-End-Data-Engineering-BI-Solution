# ============================
# 4. BUILD dim_equipment FROM SubArea_Clean (NO CRANE)
# ============================

equip_series = (
    df_maint["SubArea_Clean"]
    .dropna()
    .astype(str)
    .str.strip()
)

unique_equipment = sorted(equip_series.unique())

# Exclude crane and non-production equipment
line_equipment_names = [
    e for e in unique_equipment
    if not any(exclude in e.upper() for exclude in [
        "CRANE",
        "CCTV",           # Monitoring equipment
        "COMPUTER ROOM",  # IT infrastructure
        "GENERAL",        # Generic category
        "OPERATION",      # Generic operations category
        "SHUTDOWN",       # Not equipment
        "SERVICES"        # Generic services category
    ])
]

dim_equipment = pd.DataFrame({
    "equipment_name": line_equipment_names
})

dim_equipment["equipment_id"] = range(1, len(dim_equipment) + 1)
dim_equipment["process_order"] = np.nan          # will fill later
dim_equipment["section"] = None                  # 'ENTRY','CENTRE','EXIT'
dim_equipment["equipment_type"] = None           # 'Shear','Coil Car', etc.
dim_equipment["is_bottleneck_candidate"] = False
dim_equipment["is_active"] = True

# Reorder columns
dim_equipment = dim_equipment[[
    "equipment_id", "equipment_name", "process_order",
    "section", "equipment_type", "is_bottleneck_candidate", "is_active"
]]

print("="*80)
print("DIMENSION TABLE: dim_equipment")
print("="*80)
print(f"\nTotal equipment records: {len(dim_equipment)}")
print(f"\nExcluded from dimension table:")
excluded = [e for e in unique_equipment if e not in line_equipment_names]
for eq in excluded:
    print(f"  - {eq}")

print(f"\nIncluded equipment (first 15):")
print(dim_equipment.head(15))

print(f"\nEquipment summary:")
print(f"  Total unique equipment in raw data: {len(unique_equipment)}")
print(f"  Excluded (support/non-production): {len(excluded)}")
print(f"  Included in dim_equipment: {len(dim_equipment)}")

# ============================
# 4.1 FILL PROCESS ORDER & BOTTLENECK FLAGS
# ============================

# Define the production line process order (entry to exit)
process_order_map = {
    "Entry Coil Car": 1,
    "Coil Prep Sattion": 2,
    "Decoiler": 3,
    "Entry Guide Table": 4,
    "Entry SnubberHold Down & Pressure Rolls": 5,
    "Entry & Exit Feed Table": 6,
    "Pinch Roll & Bending Unit": 7,
    "Flattener, Pinch & Deflator Rolls": 8,
    "Temper Mill Unit": 9,
    "Crop Shear": 10,
    "Recoiler": 11,
    "First Conveyor": 12,
    "Second Conveyor": 13,
    "Scale M65 (conveyor)": 14,
    "Delivery Conveyor": 15,
    "Exit Coil Car": 16,
    "Strapping Machine": 17,
}

# Mark critical bottleneck equipment based on typical hot rolling plant operations
bottleneck_map = {
    "Temper Mill Unit": True,        # Main processing equipment
    "Decoiler": True,                # Entry bottleneck
    "Recoiler": True,                # Exit bottleneck
    "Exit Coil Car": True,           # Exit logistics
    "Crop Shear": True,              # Critical cutting operation
    "Scale M65 (conveyor)": True,    # Quality control checkpoint
}

# Mark non-line / utility equipment as inactive for the coil path
non_line = [
    "CCTV Camera",
    "Central Hyd System",
    "Common Equipment",
    "Computer Room",
    "Cranes",
    "Electrical Basement",
    "Farval Systems",
    "General",
    "Lube System",
    "Main Mill Hyd System",
    "Operation",
    "Operations",
    "Pulpits",
    "Roll Shop",
    "Services",
    "Shutdown",
]

# Apply mappings
dim_equipment["process_order"] = dim_equipment["equipment_name"].map(process_order_map)
dim_equipment["is_bottleneck_candidate"] = (
    dim_equipment["equipment_name"].map(bottleneck_map).fillna(False)
)

# Set section based on process order
def assign_section(row):
    if pd.isna(row["process_order"]):
        return None
    order = row["process_order"]
    if order <= 6:
        return "ENTRY"
    elif order <= 11:
        return "CENTRE"
    else:
        return "EXIT"

dim_equipment["section"] = dim_equipment.apply(assign_section, axis=1)

# Assign equipment types
def assign_equipment_type(name):
    name_upper = name.upper()
    if "COIL CAR" in name_upper:
        return "Coil Car"
    elif "DECOILER" in name_upper:
        return "Decoiler"
    elif "RECOILER" in name_upper:
        return "Recoiler"
    elif "SHEAR" in name_upper:
        return "Shear"
    elif "MILL" in name_upper:
        return "Mill"
    elif "CONVEYOR" in name_upper or "SCALE" in name_upper:
        return "Conveyor/Transfer"
    elif "STRAPPING" in name_upper:
        return "Strapping"
    elif "FLATTENER" in name_upper or "PINCH" in name_upper or "ROLL" in name_upper:
        return "Roll Equipment"
    elif "GUIDE" in name_upper or "TABLE" in name_upper or "FEED" in name_upper:
        return "Guide/Support"
    else:
        return "Other"

dim_equipment["equipment_type"] = dim_equipment["equipment_name"].apply(assign_equipment_type)

# Default all to active, then switch non-line to inactive
dim_equipment["is_active"] = True
dim_equipment.loc[
    dim_equipment["equipment_name"].isin(non_line),
    "is_active"
] = False

# Keep only active, ordered equipment for the coil path
line_equipment = (
    dim_equipment
    .query("is_active == True")
    .dropna(subset=["process_order"])
    .sort_values("process_order")
    .reset_index(drop=True)
)

print("="*80)
print("LINE EQUIPMENT IN PROCESS ORDER")
print("="*80)
print(line_equipment[["equipment_id", "equipment_name", "process_order",
                       "section", "equipment_type", "is_bottleneck_candidate"]])

print("\n" + "="*80)
print("SUMMARY STATISTICS")
print("="*80)
print(f"Total equipment in dim_equipment: {len(dim_equipment)}")
print(f"Active line equipment: {len(line_equipment)}")
print(f"Bottleneck candidates: {line_equipment['is_bottleneck_candidate'].sum()}")
print(f"\nEquipment by section:")
print(line_equipment['section'].value_counts().sort_index())
print(f"\nEquipment by type:")
print(line_equipment['equipment_type'].value_counts())