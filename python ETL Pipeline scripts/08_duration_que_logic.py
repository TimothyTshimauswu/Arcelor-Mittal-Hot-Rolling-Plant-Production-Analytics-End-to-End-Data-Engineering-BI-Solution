# Configure equipment operation durations and product-specific multipliers

# Base operation durations (seconds) before adjustments
DURATION_RANGES = {
    "Entry Coil Car": (40, 80),
    "Coil Prep Sattion": (30, 60),
    "Decoiler": (60, 120),
    "Entry Guide Table": (10, 20),
    "Entry SnubberHold Down & Pressure Rolls": (20, 40),
    "Entry & Exit Feed Table": (20, 40),
    "Pinch Roll & Bending Unit": (30, 60),
    "Flattener, Pinch & Deflator Rolls": (40, 90),
    "Temper Mill Unit": (120, 240),
    "Crop Shear": (20, 40),
    "Recoiler": (60, 120),
    "First Conveyor": (10, 20),
    "Second Conveyor": (10, 20),
    "Scale M65 (conveyor)": (40, 90),
    "Delivery Conveyor": (20, 40),
    "Exit Coil Car": (60, 120),
    "Strapping Machine": (40, 80),
}

DEFAULT_RANGE = (20, 40)

def get_duration_range(equipment_name: str):
    """Retrieve base duration range for equipment"""
    return DURATION_RANGES.get(equipment_name, DEFAULT_RANGE)

# Shift performance multipliers
SHIFT_MULTIPLIER = {
    "A": 1.05,
    "B": 1.00,
    "C": 0.95,
    "D": 1.00,
}

def product_mix_factor(thickness_mm: float, width_mm: float) -> float:
    """
    Calculate speed multiplier based on product dimensions:
    - Thin (≤2.0mm) & narrow (≤1300mm): 0.5-0.7× (faster processing)
    - Thick (>3.0mm) & wide (>1400mm): 1.1-1.3× (slower processing)
    - Other combinations: 0.9-1.1× (baseline)
    """
    if pd.isna(thickness_mm) or pd.isna(width_mm):
        return 1.0

    t = float(thickness_mm)
    w = float(width_mm)

    is_thin = t <= 2.0
    is_narrow = w <= 1300
    is_thick = t > 3.0
    is_wide = w > 1400

    if is_thin and is_narrow:
        return np.random.uniform(0.5, 0.7)
    elif is_thick and is_wide:
        return np.random.uniform(1.1, 1.3)
    else:
        return np.random.uniform(0.9, 1.1)

def draw_duration_seconds(equipment_name: str,
                          is_bottleneck: bool,
                          shift_code: str,
                          thickness_mm: float,
                          width_mm: float) -> float:
    """
    Generate operation duration (seconds) with adjustments for:
    - Equipment base range
    - Product mix (thickness × width)
    - Shift performance
    - Bottleneck behavior
    """
    low, high = get_duration_range(equipment_name)
    base = np.random.uniform(low, high)

    mix_factor = product_mix_factor(thickness_mm, width_mm)
    shift_factor = SHIFT_MULTIPLIER.get(shift_code, 1.0)
    bottleneck_factor = 1.10 if is_bottleneck else 1.0

    return base * mix_factor * shift_factor * bottleneck_factor

def draw_queue_seconds(is_bottleneck: bool) -> float:
    """
    Generate queue time (seconds) before equipment operation.
    Bottleneck equipment experiences longer queues.
    """
    if is_bottleneck:
        return np.random.uniform(20, 60)
    return np.random.uniform(0, 20)

print(f"Duration configuration loaded: {len(DURATION_RANGES)} equipment defined")

print("\nEquipment operation ranges (minutes):")
for eq, (low, high) in sorted(DURATION_RANGES.items(), key=lambda x: x[1][0]):
    low_min, high_min = low/60, high/60
    print(f"  {eq:45} {low_min:.1f}–{high_min:.1f} min")

print(f"\nShift performance multipliers: {SHIFT_MULTIPLIER}")

print("\nProduct mix speed factors:")
print("  Thin (≤2.0mm) & Narrow (≤1300mm): 0.5–0.7× baseline (faster)")
print("  Thick (>3.0mm) & Wide (>1400mm):  1.1–1.3× baseline (slower)")
print("  Other combinations:                0.9–1.1× baseline")