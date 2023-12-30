import pandas as pd
from dbf import read_dbf

# Paths to the CSV and DBF files
csv_file_path = "C:/SALUSER/final.csv"
dbf_file_path = "D:/ZIONtest/punch.dbf"

# Load CSV data into a DataFrame
csv_data = pd.read_csv(csv_file_path)

# Load existing DBF data into a DataFrame using dbf
dbf_data = read_dbf(dbf_file_path)

# Concatenate the CSV data to the existing DBF data
merged_data = pd.concat([dbf_data, csv_data], ignore_index=True)

# Write the merged data back to the DBF file
merged_data.to_dbf(dbf_file_path, index=False)