import pandas as pd
import numpy as np
import re
from datetime import datetime, time, timedelta

# ============================
# 1. LOAD RAW DATA
# ============================

df_prod_raw = pd.read_csv("coil_production_april_august_2024.csv", encoding='utf-8-sig')
df_maint_raw = pd.read_csv("maintenance_downtime_jna_Oct_2024.csv", encoding='utf-8-sig')

# Strip whitespace from column names
df_prod_raw.columns = df_prod_raw.columns.str.strip()
df_maint_raw.columns = df_maint_raw.columns.str.strip()

print("Production columns:", df_prod_raw.columns.tolist())
print("Maintenance columns:", df_maint_raw.columns.tolist())