# ============================
# 7. BUILD 4-CREW 12-HOUR ROTATION
# ============================

unique_dates = sorted(fact_production_coil["production_date"].unique())
crew_codes = ["A", "B", "C", "D"]

# For each date: (day_crew, night_crew)
date_to_crews = {}
for idx, d in enumerate(unique_dates):
    day_crew = crew_codes[idx % 4]
    night_crew = crew_codes[(idx + 1) % 4]
    date_to_crews[d] = (day_crew, night_crew)

print("="*80)
print("4-CREW 12-HOUR ROTATION SCHEDULE")
print("="*80)
print(f"\nTotal production days: {len(unique_dates)}")
print(f"Crew rotation pattern: {crew_codes}")

print("\nFirst 10 days schedule:")
for i, d in enumerate(unique_dates[:10]):
    day_c, night_c = date_to_crews[d]
    print(f"  {d}: Day={day_c}, Night={night_c}")

print("\nLast 5 days schedule:")
for d in unique_dates[-5:]:
    day_c, night_c = date_to_crews[d]
    print(f"  {d}: Day={day_c}, Night={night_c}")