from punch import generate_punch
from muster import generate_muster
from test import test_db_len,make_blank_files,delete_old_files,punch_mismatch,file_paths,check_ankura
import pandas as pd
import sys
import os

def create_final_csv(muster_df, punch_df):
    punch_df['PDATE'] = pd.to_datetime(punch_df['PDATE'])
    merged_df = pd.merge(muster_df, punch_df, on=['TOKEN', 'PDATE'], how='outer')

    mask = merged_df['MUSTER_STATUS'] == ""
    merged_df.loc[mask, 'MUSTER_STATUS'] = merged_df.loc[mask, 'PUNCH_STATUS']
    merged_df = merged_df.rename(columns={"MUSTER_STATUS": "STATUS"})

    table_paths = file_paths()
    with open(table_paths['gsel_date_path']) as file:
        gseldate = file.read()
        gseldate = pd.to_datetime(gseldate)

    condition = (
    (merged_df['PDATE'] < merged_df['DATE_JOIN']) | 
    (merged_df['PDATE'] > merged_df['DATE_LEAVE']) |
    (merged_df['DATE_LEAVE'] > 'gseldate')
    )

    merged_df.loc[condition, 'STATUS'] = ''

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
    merged_df['TOT_LV'] = merged_df.get('CL', 0) + merged_df.get('EL', 0) + merged_df.get('SL', 0)

    # Drop duplicate rows
    merged_df = merged_df.drop_duplicates(subset=['TOKEN', 'PDATE'])

    # Sort the DataFrame by TOKEN and PDATE
    merged_df = merged_df.sort_values(by=['TOKEN', 'PDATE']).reset_index(drop=True)

    # Convert 'TOKEN' column back to integer dtype
    merged_df['TOKEN'] = merged_df['TOKEN'].astype('Int64')  # or 'int' if using pandas version 1.0.0 or later

    # Print the modified DataFrame
    print(merged_df)

    # Drop unnecessary columns
    columns_to_drop = ['A1','AB','PH','PR','WO','CL','EL','SL']
    merged_df = merged_df.drop(columns=[col for col in columns_to_drop if col in merged_df], errors='ignore')

    # Save to CSV
    merged_df.to_csv(table_paths['final_csv_path'], index=False)

try:
    check_ankura()
    table_paths = file_paths()
    delete_old_files(table_paths['mismatch_csv_path'])
    make_blank_files(table_paths['muster_csv_path'],columns=['TOKEN','COMCODE','NAME','EMPCODE','EMP_DEPT','DEPT_NAME','EMP_DESI','DESI_NAME','DATE_JOIN','DATE_LEAVE','PDATE','MUSTER_STATUS'])
    make_blank_files(table_paths['punch_csv_path'],columns=['TOKEN','PDATE','INTIME1','OUTTIME1','INTIME2','OUTTIME2','INTIME3','OUTTIME3','INTIME4','OUTTIME4','INTIME','OUTTIME','TOTALTIME','REMARKS','PUNCH_STATUS'])
    make_blank_files(table_paths['final_csv_path'],columns=['TOKEN','COMCODE','NAME','EMPCODE','EMP_DEPT','DEPT_NAME','EMP_DESI','DESI_NAME','PDATE','STATUS','INTIME','OUTTIME','TOTALTIME','REMARKS','TOT_AB','TOT_WO','TOT_PR','TOT_PH','TOT_LV'])
    make_blank_files(table_paths['empty_tables_path'])
    db_check_flag = test_db_len()
    print("db check flag: ",db_check_flag)
    mismatch_flag = punch_mismatch()
    print("punch check flag: ",mismatch_flag)

    if db_check_flag == 1 and mismatch_flag == 1:
        muster_df = generate_muster()
        punch_df = generate_punch()
        create_final_csv(muster_df, punch_df)
    else:
        print("Either check empty_tables.txt or mismatch.csv")

except IOError:
    sys.exit()
