from punch import generate_punch
from muster import generate_muster
from test import test_db_len, make_blank_files, delete_old_files, create_new_csvs, punch_mismatch, file_paths, check_ankura, check_database, server_collect_db_data, client_collect_db_data, create_wdtest
from payroll_input import pay_input
import pandas as pd
import sys
import os

def create_final_csv(muster_df, punch_df,mismatch_df):
    punch_df['PDATE'] = pd.to_datetime(punch_df['PDATE'])
    merged_df = pd.merge(muster_df, punch_df, on=['TOKEN', 'PDATE'], how='outer')

    mask = merged_df['MUSTER_STATUS'] == ""
    merged_df.loc[mask, 'MUSTER_STATUS'] = merged_df.loc[mask, 'PUNCH_STATUS']
    merged_df = merged_df.rename(columns={"MUSTER_STATUS": "STATUS"})

    table_paths = file_paths()

    with open(table_paths['gsel_date_path']) as file:
        file_contents = file.readlines()
        file_contents = [string.strip('\n') for string in file_contents]
        gseldate = file_contents[0]
        gseldate = pd.to_datetime(gseldate)

    if 'STATUS' in merged_df.columns:

        combined_condition = (
            ((merged_df['PDATE'] < merged_df['DATE_JOIN']) | 
            (merged_df['PDATE'] > merged_df['DATE_LEAVE'])) |
            (merged_df['PDATE'] > gseldate)
        )

        # Update the 'STATUS' column based on the combined condition
        merged_df.loc[combined_condition, 'STATUS'] = "--"
    else:
        print("'STATUS' column does not exist in the DataFrame.")

    merged_df = merged_df.drop(['DATE_JOIN', 'DATE_LEAVE', 'PUNCH_STATUS','INTIME1','OUTTIME1','INTIME2','OUTTIME2','INTIME3','OUTTIME3','INTIME4','OUTTIME4'], axis=1)

    merged_df.loc[merged_df['STATUS'].isin(['WO', 'PH']), 'OT'] = merged_df['TOTALTIME']

    if mismatch_df is not None:
        print('Missing dates')
        mask = merged_df.apply(lambda row: (row['TOKEN'], row['PDATE']) in \
                      zip(mismatch_df['TOKEN'], mismatch_df['PDATE']), axis=1)

        # Update STATUS column to 'MM' for matching rows
        merged_df.loc[mask, 'STATUS'] = 'MM'

    merged_df.drop(columns=['MODE'], inplace=True)

    status_counts_by_empcode = merged_df.groupby(['TOKEN', 'STATUS'])['STATUS'].count().unstack().reset_index()

    # Adjust the counts for 'HD'
    status_counts_by_empcode['HD'] = status_counts_by_empcode.get('HD', 0) / 2 if 'HD' in status_counts_by_empcode else 0

    # Fill NaN values with 0
    status_counts_by_empcode = status_counts_by_empcode.fillna(0)

    # Merge back to the original DataFrame
    merged_df = pd.merge(merged_df, status_counts_by_empcode, on='TOKEN')

    # Calculate totals with fractional counts
    merged_df['TOT_AB'] = merged_df.get('AB', 0)
    merged_df['TOT_WO'] = merged_df.get('WO', 0)
    merged_df['TOT_PR'] = (merged_df.get('PR', 0) + merged_df.get('HD', 0)).fillna(0)
    merged_df['TOT_PH'] = merged_df.get('PH', 0)
    merged_df['TOT_LV'] = merged_df.get('CL', 0) + merged_df.get('EL', 0) + merged_df.get('SL', 0)
    merged_df['TOT_MM'] = merged_df.get('MM', 0)

    # Drop duplicate rows
    merged_df = merged_df.drop_duplicates(subset=['TOKEN', 'PDATE'])

    # Sort the DataFrame by TOKEN and PDATE
    merged_df = merged_df.sort_values(by=['TOKEN', 'PDATE']).reset_index(drop=True)

    for i in range(1, len(merged_df) - 1):
        if merged_df.at[i, 'STATUS'] == 'WO' and merged_df.at[i - 1, 'STATUS'] == 'AB' and merged_df.at[i + 1, 'STATUS'] == 'AB':
            merged_df.at[i, 'STATUS'] = 'AB'

    # Convert 'TOKEN' column back to integer dtype
    merged_df['TOKEN'] = merged_df['TOKEN'].astype('Int64')  # or 'int' if using pandas version 1.0.0 or later

    # Drop unnecessary columns
    columns_to_drop = ['HD','AB','PH','PR','WO','CL','EL','SL','--','MM']
    merged_df = merged_df.drop(columns=[col for col in columns_to_drop if col in merged_df], errors='ignore')

    # Save to CSV
    merged_df.to_csv(table_paths['final_csv_path'], index=False)

    pay_input(merged_df)

# try:
check_ankura()
pg_data_flag, process_mode_flag = check_database()
print(pg_data_flag, type(pg_data_flag))
print(process_mode_flag, type(process_mode_flag))
table_paths = file_paths()
create_new_csvs(table_paths['muster_csv_path'],['TOKEN','COMCODE','NAME','EMPCODE','EMP_DEPT','DEPT_NAME','EMP_DESI','DESI_NAME','DATE_JOIN','DATE_LEAVE','PDATE','MUSTER_STATUS'],
                table_paths['punch_csv_path'],['TOKEN','PDATE','INTIME1','OUTTIME1','INTIME2','OUTTIME2','INTIME3','OUTTIME3','INTIME4','OUTTIME4','INTIME','OUTTIME','TOTALTIME','REMARKS','PUNCH_STATUS'],
                table_paths['final_csv_path'],['TOKEN','COMCODE','NAME','EMPCODE','EMP_DEPT','DEPT_NAME','EMP_DESI','DESI_NAME','PDATE','STATUS','INTIME','OUTTIME','TOTALTIME','REMARKS','TOT_AB','TOT_WO','TOT_PR','TOT_PH','TOT_LV'])
delete_old_files(table_paths['mismatch_csv_path'])
make_blank_files(table_paths['muster_csv_path'],columns=['TOKEN','COMCODE','NAME','EMPCODE','EMP_DEPT','DEPT_NAME','EMP_DESI','DESI_NAME','DATE_JOIN','DATE_LEAVE','PDATE','MUSTER_STATUS'])
make_blank_files(table_paths['punch_csv_path'],columns=['TOKEN','PDATE','INTIME1','OUTTIME1','INTIME2','OUTTIME2','INTIME3','OUTTIME3','INTIME4','OUTTIME4','INTIME','OUTTIME','TOTALTIME','REMARKS','PUNCH_STATUS'])
make_blank_files(table_paths['final_csv_path'],columns=['TOKEN','COMCODE','NAME','EMPCODE','EMP_DEPT','DEPT_NAME','EMP_DESI','DESI_NAME','PDATE','STATUS','INTIME','OUTTIME','TOTALTIME','REMARKS','TOT_AB','TOT_WO','TOT_PR','TOT_PH','TOT_LV'])
make_blank_files(table_paths['empty_tables_path'])
delete_old_files(table_paths['mismatch_csv_path'])
delete_old_files(table_paths['payroll_input_path'])

delete_old_files(table_paths['passed_csv_path'])
delete_old_files(table_paths['punches_without_duplicates_path'])
delete_old_files(table_paths['day_one_out_excluded_path'])
delete_old_files(table_paths['orphaned_punches_path'])
if pg_data_flag == True:
    print("pg data is true!")
    server_df = server_collect_db_data()
    client_df = client_collect_db_data()
    if client_df is not None:
        create_wdtest(server_df,client_df)
if process_mode_flag == True:
    print("process data is true")        
    db_check_flag = test_db_len()
    print("db check flag: ",db_check_flag)
    mismatch_flag,mismatch_df = punch_mismatch()
    print("punch check flag: ",mismatch_flag)
    print("mismatch df: ",mismatch_df)

    # if db_check_flag == 1 and mismatch_flag == 1:
    if isinstance(db_check_flag, dict) and mismatch_flag == 1:
        muster_df= generate_muster(db_check_flag)
        punch_df = generate_punch()
        create_final_csv(muster_df, punch_df,mismatch_df)
        
    else:
        print("Either check empty_tables.txt or mismatch.csv")

# except Exception as e:
#     print(e)

# except IOError:
#     sys.exit()