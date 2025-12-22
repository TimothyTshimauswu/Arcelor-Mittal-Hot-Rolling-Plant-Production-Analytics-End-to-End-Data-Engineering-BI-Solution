# Parse datetime columns for filtering
df_prod_raw["Production Date"] = pd.to_datetime(
    df_prod_raw["Production Date"],
    format="%m/%d/%y %H:%M",
    errors="coerce"
)

df_maint_raw["Start"] = pd.to_datetime(
    df_maint_raw["Start"],
    format="%m/%d/%y %H:%M",
    errors="coerce"
)

# Filter to May–September 2024 production window
df_prod = df_prod_raw[
    (df_prod_raw["Production Date"] >= "2024-05-01") &
    (df_prod_raw["Production Date"] <= "2024-09-30")
].copy()

df_maint = df_maint_raw[
    (df_maint_raw["Start"] >= "2024-05-01") &
    (df_maint_raw["Start"] <= "2024-09-30")
].copy()

# Filtered dataset summary
print(f"Production Records: {len(df_prod):,} coils")
print(f"  Date Range: {df_prod['Production Date'].min().strftime('%Y-%m-%d')} to {df_prod['Production Date'].max().strftime('%Y-%m-%d')}")
print(f"  Unique Parent Coils (CID): {df_prod['CID'].nunique():,}")
print(f"  Records Excluded: {len(df_prod_raw) - len(df_prod):,} ({(len(df_prod_raw) - len(df_prod))/len(df_prod_raw)*100:.1f}%)")

print(f"\nMaintenance Events: {len(df_maint):,} logged incidents")
print(f"  Date Range: {df_maint['Start'].min().strftime('%Y-%m-%d')} to {df_maint['Start'].max().strftime('%Y-%m-%d')}")
print(f"  Equipment Tracked: {df_maint['Hierachy'].nunique()} units")
print(f"  Events Excluded: {len(df_maint_raw) - len(df_maint):,} ({(len(df_maint_raw) - len(df_maint))/len(df_maint_raw)*100:.1f}%)")