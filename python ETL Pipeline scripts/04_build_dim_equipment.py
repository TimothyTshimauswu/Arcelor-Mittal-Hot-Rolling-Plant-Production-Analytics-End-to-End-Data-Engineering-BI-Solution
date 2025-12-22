# Build equipment dimension table from cleaned maintenance data
equip_series = (
    df_maint["SubArea_Clean"]
    .dropna()
    .astype(str)
    .str.strip()
)

unique_equipment = sorted(equip_series.unique())

# Exclude non-production and support equipment
exclude_keywords = [
    "CRANE", "CCTV", "COMPUTER ROOM", "GENERAL", 
    "OPERATION", "SHUTDOWN", "SERVICES"
]

line_equipment_names = [
    e for e in unique_equipment
    if not any(keyword in e.upper() for keyword in exclude_keywords)
]

# Initialize dimension table
dim_equipment = pd.DataFrame({
    "equipment_name": line_equipment_names
})

dim_equipment["equipment_id"] = range(1, len(dim_equipment) + 1)
dim_equipment["process_order"] = np.nan
dim_equipment["section"] = None
dim_equipment["equipment_type"] = None
dim_equipment["is_bottleneck_candidate"] = False
dim_equipment["is_active"] = True

dim_equipment = dim_equipment[[
    "equipment_id", "equipment_name", "process_order",
    "section", "equipment_type", "is_bottleneck_candidate", "is_active"
]]

print(f"Equipment dimension table created: {len(dim_equipment)} records")
print(f"  Production equipment: {len(line_equipment_names)}")
print(f"  Support equipment excluded: {len(unique_equipment) - len(line_equipment_names)}")

print("\nFirst 10 equipment records:")
print(dim_equipment.head(10))

# Define production line sequence (entry to exit)
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

# Identify known bottleneck equipment
bottleneck_map = {
    "Temper Mill Unit": True,
    "Decoiler": True,
    "Recoiler": True,
    "Exit Coil Car": True,
    "Crop Shear": True,
    "Scale M65 (conveyor)": True,
}

# Apply process order and bottleneck flags
dim_equipment["process_order"] = dim_equipment["equipment_name"].map(process_order_map)
dim_equipment["is_bottleneck_candidate"] = (
    dim_equipment["equipment_name"].map(bottleneck_map).fillna(False)
)

# Assign section based on process position
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

# Classify equipment types
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

# Filter to active line equipment with defined process order
line_equipment = (
    dim_equipment
    .query("is_active == True")
    .dropna(subset=["process_order"])
    .sort_values("process_order")
    .reset_index(drop=True)
)

print("\nProduction line equipment in process sequence:")
print(line_equipment[["equipment_id", "equipment_name", "process_order",
                      "section", "equipment_type", "is_bottleneck_candidate"]])

print(f"\nDimension table summary:")
print(f"  Total equipment records: {len(dim_equipment)}")
print(f"  Active line equipment: {len(line_equipment)}")
print(f"  Bottleneck candidates: {line_equipment['is_bottleneck_candidate'].sum()}")

print(f"\nEquipment distribution by section:")
print(line_equipment['section'].value_counts().sort_index())

print(f"\nEquipment distribution by type:")
print(line_equipment['equipment_type'].value_counts())