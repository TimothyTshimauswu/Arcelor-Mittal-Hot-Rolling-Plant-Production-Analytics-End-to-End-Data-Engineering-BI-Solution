 # ============================
# 3. CLEAN SUB AREA -> EQUIPMENT NAME
# ============================

def clean_subarea(x):
    if pd.isna(x):
        return None
    x = str(x).strip()
    # Remove trailing "(number)" pattern, e.g. "(1)", "(2)", "(8)"
    x = re.sub(r"\(\d+\)$", "", x).strip()
    return x

df_maint["SubArea_Clean"] = df_maint["Sub Area"].apply(clean_subarea)

# Display results
print("="*60)
print("SUB AREA CLEANING RESULTS")
print("="*60)
print(f"\nOriginal unique Sub Area values: {df_maint['Sub Area'].nunique()}")
print(f"Cleaned unique SubArea values: {df_maint['SubArea_Clean'].nunique()}")

print("\nTop 10 cleaned SubArea values:")
print(df_maint['SubArea_Clean'].value_counts().head(10))

print("\nSample mapping (Original -> Cleaned):")
sample_mapping = df_maint[['Sub Area', 'SubArea_Clean']].drop_duplicates().head(15)
for idx, row in sample_mapping.iterrows():
    print(f"  '{row['Sub Area']}' -> '{row['SubArea_Clean']}'")