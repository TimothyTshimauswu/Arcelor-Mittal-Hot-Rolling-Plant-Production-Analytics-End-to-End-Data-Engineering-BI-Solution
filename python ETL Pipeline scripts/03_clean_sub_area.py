# Clean equipment names by removing trailing numbering patterns
def clean_subarea(x):
    if pd.isna(x):
        return None
    x = str(x).strip()
    # Remove trailing "(number)" pattern
    x = re.sub(r"\(\d+\)$", "", x).strip()
    return x

df_maint["SubArea_Clean"] = df_maint["Sub Area"].apply(clean_subarea)

# Equipment name standardization results
print(f"Original unique Sub Area values: {df_maint['Sub Area'].nunique()}")
print(f"Cleaned unique SubArea values: {df_maint['SubArea_Clean'].nunique()}")
print(f"Equipment names consolidated: {df_maint['Sub Area'].nunique() - df_maint['SubArea_Clean'].nunique()}")

print("\nTop 10 equipment by maintenance frequency:")
print(df_maint['SubArea_Clean'].value_counts().head(10))

print("\nSample standardization mappings:")
sample_mapping = df_maint[['Sub Area', 'SubArea_Clean']].drop_duplicates().head(15)
for idx, row in sample_mapping.iterrows():
    print(f"  '{row['Sub Area']}' → '{row['SubArea_Clean']}'")