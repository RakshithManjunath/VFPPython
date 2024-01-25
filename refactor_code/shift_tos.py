from punch import generate_punch
from muster import generate_muster
from test import test_db_len,delete_old_files
import pandas as pd

def create_final_csv(muster_df, punch_df):
    punch_df['PDATE'] = pd.to_datetime(punch_df['PDATE'])
    merged_df = pd.merge(muster_df, punch_df, on=['TOKEN', 'PDATE'], how='outer')

    mask = merged_df['MUSTER_STATUS'] == ""
    merged_df.loc[mask, 'MUSTER_STATUS'] = merged_df.loc[mask, 'PUNCH_STATUS']
    merged_df = merged_df.rename(columns={"MUSTER_STATUS": "STATUS"})

    merged_df = merged_df.drop(['DATE_JOIN', 'DATE_LEAVE', 'PUNCH_STATUS','INTIME1','OUTTIME1','INTIME2','OUTTIME2','INTIME3','OUTTIME3','INTIME4','OUTTIME4'], axis=1)

    status_counts_by_empcode = merged_df.groupby(['TOKEN', 'STATUS'])['STATUS'].count().unstack().reset_index()

    # Adjust the counts for 'A1'
    status_counts_by_empcode['A1'] = status_counts_by_empcode.get('A1', 0) / 2 if 'A1' in status_counts_by_empcode else 0

    # Fill NaN values with 0
    status_counts_by_empcode = status_counts_by_empcode.fillna(0)

    # Merge back to the original DataFrame
    merged_df = pd.merge(merged_df, status_counts_by_empcode, on='TOKEN')

    # Calculate totals with fractional counts
    merged_df['TOT_AB'] = merged_df.get('AB', 0)
    merged_df['TOT_WO'] = merged_df.get('WO', 0)
    merged_df['TOT_PR'] = (merged_df.get('PR', 0) + merged_df.get('A1', 0)).fillna(0)
    merged_df['TOT_PH'] = merged_df.get('PH', 0)

    # Drop duplicate rows
    merged_df = merged_df.drop_duplicates(subset=['TOKEN', 'PDATE'])

    # Sort the DataFrame by TOKEN and PDATE
    merged_df = merged_df.sort_values(by=['TOKEN', 'PDATE']).reset_index(drop=True)

    # Convert 'TOKEN' column back to integer dtype
    merged_df['TOKEN'] = merged_df['TOKEN'].astype('Int64')  # or 'int' if using pandas version 1.0.0 or later

    # Print the modified DataFrame
    print(merged_df)

    # Drop unnecessary columns
    columns_to_drop = ['A1','AB','PH','PR','WO']
    merged_df = merged_df.drop(columns=[col for col in columns_to_drop if col in merged_df], errors='ignore')

    # Save to CSV
    merged_df.to_csv('./final.csv', index=False)

delete_old_files('./muster.csv')
delete_old_files('./punch.csv')
delete_old_files('./final.csv')
delete_old_files('./empty_tables.txt')

db_check_flag = test_db_len()
if db_check_flag == 1:
    muster_df = generate_muster()
    punch_df = generate_punch()
    create_final_csv(muster_df, punch_df)
else:
    print("Check tables")
