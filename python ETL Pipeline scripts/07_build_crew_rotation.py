# Build 4-crew 12-hour rotation schedule
unique_dates = sorted(fact_production_coil["production_date"].unique())
crew_codes = ["A", "B", "C", "D"]

# Assign day and night crews for each production date
date_to_crews = {}
for idx, d in enumerate(unique_dates):
    day_crew = crew_codes[idx % 4]
    night_crew = crew_codes[(idx + 1) % 4]
    date_to_crews[d] = (day_crew, night_crew)

print(f"Crew rotation schedule created: {len(unique_dates)} production days")
print(f"Rotation pattern: {' → '.join(crew_codes)} (4-crew, 12-hour shifts)")

print("\nFirst 10 days:")
for i, d in enumerate(unique_dates[:10]):
    day_c, night_c = date_to_crews[d]
    print(f"  {d}: Day={day_c}, Night={night_c}")

print("\nLast 5 days:")
for d in unique_dates[-5:]:
    day_c, night_c = date_to_crews[d]
    print(f"  {d}: Day={day_c}, Night={night_c}")