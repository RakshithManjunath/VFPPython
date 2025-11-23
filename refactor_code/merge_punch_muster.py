import pandas as pd

# Load CSVs
muster_df = pd.read_csv("D:/TECHNO/muster.csv", parse_dates=["PDATE"])
punch_df = pd.read_csv("D:/TECHNO/punch.csv", parse_dates=["PDATE"])

# Select only the required columns from muster_df
shift_cols = ["TOKEN", "PDATE", "SHIFT_STATUS", "SHIFT_ST", "SHIFT_ED", "WORKHRS"]
muster_shift_info = muster_df[shift_cols].copy()

# Merge on TOKEN and PDATE
merged_df = pd.merge(punch_df, muster_shift_info, on=["TOKEN", "PDATE"], how="left")

# Save back to punch.csv or new file
# merged_df.to_csv("punch_updated.csv", index=False)
