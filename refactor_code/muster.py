import pandas as pd
from dbfread import DBF
from test import file_paths
from datetime import timedelta

def generate_muster(db_check_flag):
    table_paths = file_paths()
    
    holmast_flag = None
    lvform_flag = None
    if 'optional_tables' in db_check_flag:
        print(db_check_flag)
        holmast_flag = db_check_flag['optional_tables'][0]
        lvform_flag = db_check_flag['optional_tables'][1]
        print("holmast flag: ",holmast_flag, "lvform flag:", lvform_flag)

    dated_table = DBF(table_paths['dated_dbf_path'], load=True)
    start_date = dated_table.records[0]['MUFRDATE']
    end_date = dated_table.records[0]['MUTODATE']

    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    muster_table = DBF(table_paths['muster_dbf_path'], load=True)
    muster_df = pd.DataFrame(iter(muster_table))
    muster_df = muster_df[muster_df['SEC_STAFF']==True]

    if lvform_flag !=0:
        lvform_table = DBF(table_paths['lvform_dbf_path'], load=True)
        lvform_df = pd.DataFrame(iter(lvform_table))
        lvform_df = lvform_df[['EMPCODE','LV_ST','LV_TYPE']]
        lvform_df = lvform_df[(lvform_df['LV_ST'] >= start_date) & (lvform_df['LV_ST'] <= end_date)]

    if holmast_flag !=0:
        holidays_table = DBF(table_paths['holmast_dbf_path'], load=True)
        holidays_df = pd.DataFrame(holidays_table)
        filtered_holidays_df = holidays_df[(holidays_df['HOL_DT'] >= start_date) & (holidays_df['HOL_DT'] <= end_date)]

    muster_df = muster_df[(muster_df['DEL'] == False) | ((muster_df['DATE_LEAVE'] >= start_date) & (muster_df['DATE_LEAVE'] <= end_date))]
    muster_df = muster_df.sort_values(by=['TOKEN'])

    date_range = pd.date_range(start=start_date, end=end_date, freq='D')

    column_date_mapping = {f'WO_{i}': date.strftime('%Y-%m-%d') for i, date in enumerate(date_range, start=1)}

    selected_columns = muster_df.loc[:, column_date_mapping.keys()]

    selected_columns.columns = [column_date_mapping[col] for col in selected_columns.columns]

    selected_columns.index = muster_df['TOKEN']

    # Create an empty dictionary to store the results
    token_dates = {}

    # Iterate through each row of the DataFrame
    for index, row in selected_columns.iterrows():
        # Get the token value
        token = index
        
        # Get the dates where the value is True
        true_dates = [col for col, value in row.items() if value]
        
        # Store the result in the dictionary
        token_dates[token] = true_dates

    # Create a new DataFrame to store the expanded dates
    final_muster_df = pd.DataFrame()

    # Iterate through each row in muster_df
    for index, row in muster_df.iterrows():
        # Extract relevant information
        token = row['TOKEN']

        date_range = pd.date_range(start_date_str, end_date_str, closed=None)

        # Create a DataFrame with repeated information for each date in the range
        temp_df = pd.DataFrame({
            'TOKEN': [token] * len(date_range),
            'COMCODE': row['COMCODE'],
            'NAME': row['NAME'],
            'EMPCODE': row['EMPCODE'],
            'EMP_DEPT': row['EMP_DEPT'],
            'DEPT_NAME': row['DEPT_NAME'],
            'EMP_DESI': row['EMP_DESI'],
            'DESI_NAME': row['DESI_NAME'],
            'DATE_JOIN': row['DATE_JOIN'],
            'DATE_LEAVE': row['DATE_LEAVE'],
            'PDATE': date_range,
            'MUSTER_STATUS': "",
        })

        # Append the temporary DataFrame to the expanded_dates_df
        final_muster_df = final_muster_df.append(temp_df, ignore_index=True)

    # Update the 'ATT_STATUS' for corresponding holiday dates
    if holmast_flag !=0:
        for index, row in filtered_holidays_df.iterrows():
            holiday_date = row['HOL_DT']
            hol_type = row['HOL_TYPE']
            final_muster_df.loc[final_muster_df['PDATE'].dt.date == holiday_date, 'MUSTER_STATUS'] = hol_type

    # Set 'ATT_STATUS' to 'w/o' for the dates in token_dates, otherwise it will be 'AB'
    for token, dates in token_dates.items():
        final_muster_df.loc[(final_muster_df['TOKEN'] == token) & (final_muster_df['PDATE'].dt.strftime('%Y-%m-%d').isin(dates)), 'MUSTER_STATUS'] = 'WO'

    if lvform_flag !=0:
        for index, row in lvform_df.iterrows():
            lv_start = row['LV_ST']
            lv_type = row['LV_TYPE']
            empcode = row['EMPCODE']

            final_muster_df.loc[(final_muster_df['PDATE'].dt.date == lv_start) & (final_muster_df['EMPCODE']==empcode), 'MUSTER_STATUS'] = lv_type
    # Sort the DataFrame by TOKEN and PDATE
    final_muster_df = final_muster_df.sort_values(by=['TOKEN', 'PDATE'])

    # Save the result to a new CSV file
    final_muster_df.to_csv(table_paths['muster_csv_path'], index=False)

    return final_muster_df