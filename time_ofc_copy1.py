from dbfread import DBF
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Read muster data from DBF file
muster_table = DBF('D:/ZIONtest/muster.dbf', load=True)
must_token = [record['TOKEN'] for record in muster_table]
name = [record['NAME'] for record in muster_table]
employee_code = [record['EMPCODE'] for record in muster_table]
date_join = [record['DATE_JOIN'] for record in muster_table]
date_leave = [record['DATE_LEAVE'] for record in muster_table]
com_code = [record['COMCODE'] for record in muster_table]
dept_name = [record['DEPT_NAME'] for record in muster_table]
dept_code = [record['EMP_DEPT'] for record in muster_table]
desi_name = [record['DESI_NAME'] for record in muster_table]
desi_code = [record['EMP_DESI'] for record in muster_table]

muster_complete_df = pd.DataFrame(iter(muster_table))
print(muster_complete_df.head())

empcodes_with_true_wo_12 = muster_complete_df.loc[muster_complete_df['WO_3'], 'EMPCODE']
print(empcodes_with_true_wo_12)

start_column = 'WO_1'
end_column = 'WO_30'

# Filter columns from 'WO_1' to 'WO_30' where the values are True
true_wo_columns = muster_complete_df.loc[:, start_column:end_column].columns[muster_complete_df.loc[:, start_column:end_column].any()]

# Create a new column containing relevant 'WO' columns for each 'EMPCODE'
muster_complete_df['True_WO'] = muster_complete_df[true_wo_columns].apply(lambda x: ', '.join(x.index[x]), axis=1)

# Group by 'EMPCODE' and aggregate the 'True_WO' column
result = muster_complete_df.groupby('EMPCODE')['True_WO'].agg(', '.join).reset_index()

# Lambda function to replace WO_x with the corresponding date
replace_wo = lambda x: ', '.join([(datetime(2023, 9, 1) + timedelta(days=int(item.split("_")[1]) - 1)).strftime('%Y-%m-%d') for item in x.split(', ')])

# Apply the lambda function to the 'True_WO' column
result['True_WO'] = result['True_WO'].apply(replace_wo)

# Display the modified DataFrame
print(result)

# Display the result
result.to_csv("filtered_by_day.csv", index=False)

# Read dated data from DBF file
dated_table = DBF('D:/ZIONtest/dated.dbf', load=True)
start_date = dated_table.records[0]['MUFRDATE']
end_date = dated_table.records[0]['MUTODATE']

# Read holmast data from DBF file and filter based on start and end dates
holmast_table = DBF('D:/ZIONtest/holmast.dbf', load=True)
filtered_holmast = [record for record in holmast_table if start_date <= record['HOL_DT'] <= end_date]
filtered_hol_dt = [record['HOL_DT'] for record in filtered_holmast]
filtered_hol_desc = [record['HOL_DESC'] for record in filtered_holmast]
filtered_hol_type = [record['HOL_TYPE'] for record in filtered_holmast]
hol_dt1, hol_dt2 = filtered_hol_dt
hol_desc1, hol_desc2 = filtered_hol_desc
hol_type1, hol_type2 = filtered_hol_type

# Read lvform data from DBF file and filter based on start and end dates
lv_form_table = DBF('D:/ZIONtest/lvform.dbf', load=True)
filtered_lvform = [record for record in lv_form_table if start_date <= record['LV_ST'] <= end_date]
empcode = [record['EMPCODE'] for record in filtered_lvform]
lv_start = [record['LV_ST'] for record in filtered_lvform]
lv_type = [record['LV_TYPE'] for record in filtered_lvform]

# Read punches data from DBF file
punches_table = DBF('D:/ZIONtest/punches.dbf', load=True)
token = [record['TOKEN'] for record in punches_table]
pdtime = [record['PDTIME'] for record in punches_table]
mode = [record['MODE'] for record in punches_table]
punches_dict = {"token": token, "pdtime": pdtime, "mode": mode}
punches_df = pd.DataFrame(punches_dict)
punches_df['pdtime'] = pd.to_datetime(punches_df['pdtime'], format='%d-%b-%y %I:%M:%S %p')
punches_df.sort_values(by=['token', 'pdtime', 'mode'], inplace=True)

# Create a DataFrame for leave data
leave_dict = {"empcode": empcode, "lvstart": lv_start, "lvtype": lv_type}
leave_pd = pd.DataFrame(leave_dict)

# Create a DataFrame for muster data
muster_df = pd.DataFrame({'token': must_token, 'name': name, 'employee_code': employee_code, 'date_join': date_join,
                          'date_leave': date_leave, 'dept_name': dept_name, 'desi_name': desi_name,
                          'comcode': com_code, 'dept_code': dept_code, 'desi_code': desi_code})

# Merge muster and punches DataFrames based on 'token'
merged_df = pd.merge(muster_df, punches_df, on='token', how='left')
merged_df['pdtime'] = pd.to_datetime(merged_df['pdtime'], format='%d-%b-%y %H:%M:%S')
merged_df.sort_values(by=['token', 'pdtime', 'mode'], inplace=True)

# Create a dictionary for additional data
data = {
    "must_token": must_token,
    "name": name,
    "employee_code": employee_code,
    "date_join": date_join,
    "date_leave": date_leave,
    "new_start_date": start_date,
    "new_end_date": end_date,
    "holiday_ph1": hol_dt1,
    "holiday_ph2": hol_dt2,
    "leave1": "",
    "leave2": "",
    "date": ""
}

# Create a DataFrame for additional data
data = pd.DataFrame(data)
data['date_join'] = pd.to_datetime(data['date_join'])
data['new_start_date'] = pd.to_datetime(data['new_start_date'])
data['date_leave'] = pd.to_datetime(data['date_leave'])
data['new_end_date'] = pd.to_datetime(data['new_end_date'])

# Group leave data by 'empcode'
grouped_df = leave_pd.groupby('empcode')

# Fill leave information in the 'data' DataFrame
for empcode, group in grouped_df:
    leave_dates = group['lvstart'].tolist()
    data.loc[data['employee_code'] == empcode, 'leave1'] = leave_dates[0] if len(leave_dates) > 0 else ""
    data.loc[data['employee_code'] == empcode, 'leave2'] = leave_dates[1] if len(leave_dates) > 1 else ""

# Update 'new_start_date' and 'new_end_date' based on date_join and date_leave conditions
date_join_mask = data['date_join'] > data['new_start_date']
date_leave_mask = data['date_leave'] < data['new_end_date']
data.loc[date_join_mask, 'new_start_date'] = data.loc[date_join_mask, 'date_join']
data.loc[date_leave_mask, 'new_end_date'] = data.loc[date_leave_mask, 'date_leave']

# Create an empty DataFrame for expanded data
expanded_data = pd.DataFrame()

# Iterate over each row in the 'data' DataFrame
for index, row in data.iterrows():
    must_token = row['must_token']
    date_range = pd.date_range(start=row['new_start_date'], end=row['new_end_date'], freq='D')
    
    # Create a DataFrame for each row in 'data' with initial 'status' set to 'AB'
    expanded_df = pd.DataFrame({
        'must_token': must_token,
        'name': row['name'],
        'employee_code': row['employee_code'],
        'date_join': row['date_join'],
        'date_leave': row['date_leave'],
        'new_start_date': row['new_start_date'],
        'new_end_date': row['new_end_date'],
        'date': date_range,
        'status': 'AB'  # Initial status set to 'AB'
    })
    
    # Update 'status' based on leave and holiday conditions
    if not pd.isna(row['leave1']):
        expanded_df.loc[expanded_df['date'] == row['leave1'], 'status'] = 'LV'
    if not pd.isna(row['leave2']):
        expanded_df.loc[expanded_df['date'] == row['leave2'], 'status'] = 'LV'
    if not pd.isna(row['holiday_ph1']):
        expanded_df.loc[expanded_df['date'] == row['holiday_ph1'], 'status'] = 'PH'
    if not pd.isna(row['holiday_ph2']):
        expanded_df.loc[expanded_df['date'] == row['holiday_ph2'], 'status'] = 'PH'
    
    # Concatenate the expanded DataFrame to the overall expanded_data
    expanded_data = pd.concat([expanded_data, expanded_df], ignore_index=True)

# Drop unnecessary columns from expanded_data
columns_to_drop = ['holiday_ph1', 'holiday_ph2', 'leave1', 'leave2']
expanded_data = expanded_data.drop(columns_to_drop, axis=1, errors='ignore')

# Create an empty DataFrame for the final result
result_df = pd.DataFrame(columns=['date', 'token', 'name', 'employee_code', 'in_time', 'out_time', 'total_time', 'status'])

# Initialize variables for in and out punch times
in_punch_time = None
out_punch_time = None

# Iterate over each row in the 'merged_df' DataFrame
for index, row in merged_df.iterrows():
    if row['mode'] == 0:
        in_punch_time = row['pdtime']
    elif row['mode'] == 1:
        out_punch_time = row['pdtime']
        if in_punch_time is not None:
            time_difference = out_punch_time - in_punch_time
            if time_difference.total_seconds() > 0:
                hours, remainder = divmod(time_difference.seconds, 3600)
                minutes, seconds = divmod(remainder, 60)
                
                # Concatenate the punch data to the result DataFrame
                result_df = pd.concat([result_df, pd.DataFrame({
                    'date': [pd.to_datetime(in_punch_time, format='%d-%b-%y %H:%M:%S').strftime('%Y-%m-%d')],
                    'token': [row['token']],
                    'name': [row['name']],
                    'employee_code': [row['employee_code']],
                    'in_time': [in_punch_time.strftime('%Y-%m-%d %H:%M:%S')],
                    'out_time': [out_punch_time.strftime('%Y-%m-%d %H:%M:%S')],
                    'total_time': [f'{hours:02}:{minutes:02}:{seconds:02}'],
                    'status': ['PR']
                })], ignore_index=True)

# Create an empty DataFrame for all days
all_days = pd.DataFrame(columns=['token', 'in_time', 'out_time', 'total_time', 'date', 'status', 'name', 'employee_code'])

# Iterate over unique tokens in the result DataFrame
for token in result_df['token'].unique():
    existing_dates = set(result_df[(result_df['token'] == token) & (result_df['in_time'] != '')]['date'])
    
    # Iterate over days from 1 to 30
    for day in range(1, 31):
        date_str = f"{day:02}-Sep-23"
        if date_str not in existing_dates:
            # is_sunday = pd.to_datetime(date_str, format='%d-%b-%y').dayofweek == 6
            # status = 'wo' if is_sunday else 'AB'
            status = 'AB'
            
            # Additional condition to check date_join < new_start_date
            if pd.to_datetime(date_str, format='%d-%b-%y') < data[data['must_token'] == token]['new_start_date'].values[0]:
                status = ''
            
            # Concatenate the day data to the all_days DataFrame
            all_days = pd.concat([all_days, pd.DataFrame({
                'date': [pd.to_datetime(date_str, format='%d-%b-%y').strftime('%Y-%m-%d')],
                'token': [token],
                'name': [result_df[result_df['token'] == token]['name'].values[0]],
                'employee_code': [result_df[result_df['token'] == token]['employee_code'].values[0]],
                'in_time': [''],
                'out_time': [''],
                'total_time': [''],
                'status': [status]
            })], ignore_index=True)

# Concatenate the all_days DataFrame to the result DataFrame
result_df = pd.concat([result_df, all_days], ignore_index=True)

# Sort the result DataFrame by token and date
result_df = result_df.sort_values(by=['token', 'date'])

# Merge muster and result DataFrames based on 'token'
final_merged_df = pd.merge(muster_df, result_df, on='token', how='outer')

# Drop unnecessary columns from final_merged_df
final_merged_df.drop(['name_y', 'employee_code_y', 'date_join', 'date_leave'], axis=1, inplace=True)

# Rename columns in final_merged_df
final_merged_df.rename(columns={'name_x': 'NAME', 'employee_code_x': 'EMPCODE',
                                'token': 'TOKEN', 'date': 'PDATE',
                                'in_time': 'PUNCH1', 'out_time': 'PUNCH2',
                                'status': 'ATT_STATUS', 'total_time': 'WORKED',
                                'dept_name': 'DEPT_NAME', 'desi_name': 'DESI_NAME',
                                'comcode': 'COMCODE', 'dept_code': 'DEPT_CODE', 'desi_code': 'DESI_CODE'}, inplace=True)

final_merged_df = final_merged_df[~((final_merged_df['ATT_STATUS'] == 'AB') & (final_merged_df['PDATE'].isin(final_merged_df[final_merged_df['ATT_STATUS'] == 'PR']['PDATE'])))]

# Save the result to CSV
final_merged_df.to_csv('./final.csv', index=False)
