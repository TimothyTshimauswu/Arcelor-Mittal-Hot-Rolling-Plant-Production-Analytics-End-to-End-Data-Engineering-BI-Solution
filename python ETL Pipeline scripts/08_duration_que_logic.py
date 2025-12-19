# ============================
# 8. REALISTIC DURATION & QUEUE LOGIC (WIDTH + THICKNESS AWARE)
# ============================

# Base operation duration ranges in SECONDS per coil, before product/shift/bottleneck adjustment.
# These are tuned so a "normal" coil ends up around 15 minutes for the full line.

DURATION_RANGES = {
    "Entry Coil Car": (40, 80),                        # 0.7–1.3 min
    "Coil Prep Sattion": (30, 60),                     # 0.5–1.0 min
    "Decoiler": (60, 120),                             # 1–2 min (bottleneck)
    "Entry Guide Table": (10, 20),                     # 0.2–0.3 min
    "Entry SnubberHold Down & Pressure Rolls": (20, 40), # 0.3–0.7 min
    "Entry & Exit Feed Table": (20, 40),               # 0.3–0.7 min
    "Pinch Roll & Bending Unit": (30, 60),             # 0.5–1.0 min
    "Flattener, Pinch & Deflator Rolls": (40, 90),     # 0.7–1.5 min
    "Temper Mill Unit": (120, 240),                    # 2–4 min (primary bottleneck)
    "Crop Shear": (20, 40),                            # 0.3–0.7 min (bottleneck)
    "Recoiler": (60, 120),                             # 1–2 min (bottleneck)
    "First Conveyor": (10, 20),                        # 0.2–0.3 min
    "Second Conveyor": (10, 20),                       # 0.2–0.3 min
    "Scale M65 (conveyor)": (40, 90),                  # 0.7–1.5 min (bottleneck)
    "Delivery Conveyor": (20, 40),                     # 0.3–0.7 min
    "Exit Coil Car": (60, 120),                        # 1–2 min (bottleneck)
    "Strapping Machine": (40, 80),                     # 0.7–1.3 min
}

DEFAULT_RANGE = (20, 40)  # fallback for anything else


def get_duration_range(equipment_name: str):
    """Get the base duration range for a piece of equipment"""
    if equipment_name in DURATION_RANGES:
        return DURATION_RANGES[equipment_name]
    return DEFAULT_RANGE


# Shift multipliers to capture crew differences
SHIFT_MULTIPLIER = {
    "A": 1.05,   # slightly slower
    "B": 1.00,   # baseline
    "C": 0.95,   # slightly faster
    "D": 1.00,   # baseline
}


def product_mix_factor(thickness_mm: float, width_mm: float) -> float:
    """
    Product mix speed rule:
      - Narrow + thin coils run FASTER.
      - Wide + thick coils run SLOWER (around the baseline ~15 min/coil).

    We'll treat:
      - "thin" as thickness <= 2.0 mm
      - "narrow" as width <= 1300 mm

    Factor ranges:
      - thin & narrow        -> 0.5–0.7  (much faster than baseline)
      - thick OR wide        -> 0.9–1.1  (around 15 min baseline)
      - thick AND wide       -> 1.1–1.3  (slower than baseline)
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
        # Fast-running coils
        return np.random.uniform(0.5, 0.7)
    elif is_thick and is_wide:
        # Slow-running coils
        return np.random.uniform(1.1, 1.3)
    else:
        # General product mix around 15 min baseline
        return np.random.uniform(0.9, 1.1)


def draw_duration_seconds(equipment_name: str,
                          is_bottleneck: bool,
                          shift_code: str,
                          thickness_mm: float,
                          width_mm: float) -> float:
    """
    Draw a random operation duration (seconds) for a given equipment,
    adjusted for:
      - base equipment range
      - product mix (thickness + width)
      - shift performance
      - bottleneck behaviour
    """
    low, high = get_duration_range(equipment_name)
    base = np.random.uniform(low, high)

    mix_factor = product_mix_factor(thickness_mm, width_mm)
    shift_factor = SHIFT_MULTIPLIER.get(shift_code, 1.0)
    bottleneck_factor = 1.10 if is_bottleneck else 1.0

    return base * mix_factor * shift_factor * bottleneck_factor


def draw_queue_seconds(is_bottleneck: bool) -> float:
    """
    Queue time (seconds) before an equipment step.
    Bottlenecks experience slightly longer queues.
    """
    if is_bottleneck:
        return np.random.uniform(20, 60)    # 0.3–1.0 min
    return np.random.uniform(0, 20)         # 0–0.3 min


print("="*80)
print("DURATION & QUEUE LOGIC CONFIGURATION")
print("="*80)
print(f"\nConfigured equipment: {len(DURATION_RANGES)}")
print("\nEquipment duration ranges (seconds):")
for eq, (low, high) in sorted(DURATION_RANGES.items(), key=lambda x: x[1][0]):
    low_min, high_min = low/60, high/60
    print(f"  {eq:45} {low:3.0f}–{high:3.0f}s ({low_min:.1f}–{high_min:.1f} min)")

print(f"\nShift multipliers: {SHIFT_MULTIPLIER}")
print("\nProduct mix factors:")
print("  - Thin (≤2.0mm) & Narrow (≤1300mm): 0.5–0.7× (faster)")
print("  - Thick (>3.0mm) & Wide (>1400mm):  1.1–1.3× (slower)")
print("  - Other combinations:                0.9–1.1× (baseline)")