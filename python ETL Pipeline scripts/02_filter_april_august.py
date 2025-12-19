# ============================
# FILTER TO APRIL–AUGUST 2024
# ============================

# Parse the Production Date column
df_prod_raw['Production Date'] = pd.to_datetime(
    df_prod_raw['Production Date'],
    format='%m/%d/%y %H:%M',
    errors='coerce'
)

# Parse the Start date column in maintenance data
df_maint_raw['Start'] = pd.to_datetime(
    df_maint_raw['Start'],
    format='%m/%d/%y %H:%M',
    errors='coerce'
)

# Filter production data: April 1 to August 31, 2024
df_prod = df_prod_raw[
    (df_prod_raw['Production Date'] >= '2024-04-01') &
    (df_prod_raw['Production Date'] <= '2024-08-31')
].copy()

# Filter maintenance data: April 1 to August 31, 2024
df_maint = df_maint_raw[
    (df_maint_raw['Start'] >= '2024-04-01') &
    (df_maint_raw['Start'] <= '2024-08-31')
].copy()

print("="*60)
print("FILTERED DATA SUMMARY")
print("="*60)
print(f"\nProduction Data:")
print(f"  Original records: {len(df_prod_raw):,}")
print(f"  Filtered (Apr-Aug 2024): {len(df_prod):,}")
print(f"  Date range: {df_prod['Production Date'].min()} to {df_prod['Production Date'].max()}")
print(f"  Unique coils: {df_prod['CID'].nunique():,}")

print(f"\nMaintenance Data:")
print(f"  Original records: {len(df_maint_raw):,}")
print(f"  Filtered (Apr-Aug 2024): {len(df_maint):,}")
print(f"  Date range: {df_maint['Start'].min()} to {df_maint['Start'].max()}")
print(f"  Unique equipment: {df_maint['Hierachy'].nunique()}")

print("\n" + "="*60)