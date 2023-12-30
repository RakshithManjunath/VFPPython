from dbfread import DBF
import pandas as pd

# Read muster.dbf
muster_table = DBF('D:/ZIONtest/muster.dbf', load=True)

must_token = [record['TOKEN'] for record in muster_table]
name = [record['NAME'] for record in muster_table]
employee_code = [record['EMPCODE'] for record in muster_table]
date_join = [record['DATE_JOIN'] for record in muster_table]
date_leave = [record['DATE_LEAVE'] for record in muster_table]

# Read dated.dbf
dated_table = DBF('D:/ZIONtest/dated.dbf', load=True)
start_date = dated_table.records[0]['MUFRDATE']
end_date = dated_table.records[0]['MUTODATE']

# Load holmast_table
holmast_table = DBF('D:/ZIONtest/holmast.dbf', load=True)

# Filter rows based on date range
filtered_holmast = [
    record for record in holmast_table
    if start_date <= record['HOL_DT'] <= end_date
]

# Extract specific columns from filtered rows
filtered_hol_dt = [record['HOL_DT'] for record in filtered_holmast]
filtered_hol_desc = [record['HOL_DESC'] for record in filtered_holmast]
filtered_hol_type = [record['HOL_TYPE'] for record in filtered_holmast]

# hol_dt1, hol_dt2, hol_desc1, hol_desc2, hol_type1, hol_type2 = None
hol_dt1, hol_dt2 = filtered_hol_dt
hol_desc1, hol_desc2 = filtered_hol_desc
hol_type1, hol_type2 = filtered_hol_type

# Load lv_form_table
lv_form_table = DBF('D:/ZIONtest/lvform.dbf', load=True)

# Filter rows based on date range
filtered_lvform = [
    record for record in lv_form_table
    if start_date <= record['LV_ST'] <= end_date
]

empcode = [record['EMPCODE'] for record in filtered_lvform]
lv_start = [record['LV_ST'] for record in filtered_lvform]
lv_type = [record['LV_TYPE'] for record in filtered_lvform]

# Load punches_table
punches_table = DBF('D:/ZIONtest/punches.dbf', load=True)

# Extract columns from punches_table
token = [record['TOKEN'] for record in punches_table]
pdtime = [record['PDTIME'] for record in punches_table]
mode = [record['MODE'] for record in punches_table]

# Create a DataFrame for punches information
punches_dict = {"token": token, "pdtime": pdtime, "mode": mode}
punches_df = pd.DataFrame(punches_dict)

# Convert 'pdtime' to pandas datetime objects
punches_df['pdtime'] = pd.to_datetime(punches_df['pdtime'], format='%d-%b-%y %I:%M:%S %p')

# Sort the DataFrame by 'token', 'pdtime', and 'mode'
punches_df.sort_values(by=['token', 'pdtime', 'mode'], inplace=True)
print(punches_df)

# Create a DataFrame for leave information
leave_dict = {"empcode": empcode, "lvstart": lv_start, "lvtype": lv_type}
leave_pd = pd.DataFrame(leave_dict)

# Create DataFrames for muster_table and punches_table
muster_df = pd.DataFrame({'token': must_token, 'name': name, 'employee_code': employee_code,'date_join':date_join,'date_leave':date_leave})
punches_df = pd.DataFrame({'token': token, 'pdtime': pdtime, 'mode': mode})

# Merge punches_df with muster_df to get corresponding names and employee codes
merged_df = pd.merge(muster_df, punches_df, on='token', how='left')

# Convert 'pdtime' to datetime
merged_df['pdtime'] = pd.to_datetime(merged_df['pdtime'], format='%d-%b-%y %H:%M:%S')

# Sort DataFrame by 'token', 'pdtime', and 'mode'
merged_df.sort_values(by=['token', 'pdtime', 'mode'], inplace=True)

merged_df.to_csv('muster_left.csv',index=False)

# Create a DataFrame
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

data = pd.DataFrame(data)

# Convert date columns to pandas datetime objects for comparison
data['date_join'] = pd.to_datetime(data['date_join'])
data['new_start_date'] = pd.to_datetime(data['new_start_date'])
data['date_leave'] = pd.to_datetime(data['date_leave'])
data['new_end_date'] = pd.to_datetime(data['new_end_date'])

# Group by 'empcode'
grouped_df = leave_pd.groupby('empcode')

# Access records for each 'empcode'
for empcode, group in grouped_df:
    leave_dates = group['lvstart'].tolist()
    data.loc[data['employee_code'] == empcode, 'leave1'] = leave_dates[0] if len(leave_dates) > 0 else ""
    data.loc[data['employee_code'] == empcode, 'leave2'] = leave_dates[1] if len(leave_dates) > 1 else ""

# Identify records where date_join is greater than new_start_date
date_join_mask = data['date_join'] > data['new_start_date']
date_leave_mask = data['date_leave'] < data['new_end_date']

# Replace new_start_date with date_join where the condition is True
data.loc[date_join_mask, 'new_start_date'] = data.loc[date_join_mask, 'date_join']
data.loc[date_leave_mask, 'new_end_date'] = data.loc[date_leave_mask, 'date_leave']

# Create a new DataFrame to store the expanded date range
expanded_data = pd.DataFrame()

# Iterate over each row in the original data
for index, row in data.iterrows():
    must_token = row['must_token']
    name = row['name']
    employee_code = row['employee_code']
    date_join = row['date_join']
    date_leave = row['date_leave']
    new_start_date = row['new_start_date']
    new_end_date = row['new_end_date']
    holiday_ph1 = row['holiday_ph1']
    holiday_ph2 = row['holiday_ph2']
    leave1 = row['leave1']
    leave2 = row['leave2']

    # Generate a date range for the employee
    date_range = pd.date_range(start=new_start_date, end=new_end_date, freq='D')

    # Create a DataFrame for the expanded date range
    expanded_df = pd.DataFrame({
        'must_token': must_token,
        'name': name,
        'employee_code': employee_code,
        'date_join': date_join,
        'date_leave': date_leave,
        'new_start_date': new_start_date,
        'new_end_date': new_end_date,
        'holiday_ph1': holiday_ph1,
        'holiday_ph2': holiday_ph2,
        'leave1': leave1,
        'leave2': leave2,
        'date': date_range
    })

    # Add a new column 'status' to the expanded DataFrame
    expanded_df['status'] = 'AB'

    # Identify rows where the date is in holiday_ph1 or holiday_ph2
    holiday_mask = ((expanded_df['date'].dt.strftime('%Y-%m-%d') == hol_dt1.strftime('%Y-%m-%d')) |
                    (expanded_df['date'].dt.strftime('%Y-%m-%d') == hol_dt2.strftime('%Y-%m-%d')))

    # Convert leave columns in data to datetime format
    data['leave1'] = pd.to_datetime(data['leave1'], errors='coerce')
    data['leave2'] = pd.to_datetime(data['leave2'], errors='coerce')

    # Identify rows where the date is in leave1 or leave2 (when not NaT and not blank)
    leave1_mask = (~pd.isna(data.loc[index, 'leave1'])) & \
                (expanded_df['date'] >= data.loc[index, 'leave1']) & \
                (expanded_df['date'] <= data.loc[index, 'leave1'])
    leave2_mask = (~pd.isna(data.loc[index, 'leave2'])) & \
                (expanded_df['date'] >= data.loc[index, 'leave2']) & \
                (expanded_df['date'] <= data.loc[index, 'leave2'])

    # Update the 'status' column for rows in leave1 or leave2
    expanded_df.loc[leave1_mask | leave2_mask, 'status'] = 'LV'

    # Update the 'status' column for rows in holiday_mask
    expanded_df.loc[holiday_mask, 'status'] = 'PH'

    # Append the expanded DataFrame to the overall expanded_data DataFrame
    expanded_data = pd.concat([expanded_data, expanded_df], ignore_index=True)

# Drop unnecessary columns
expanded_data = expanded_data.drop(['holiday_ph1', 'holiday_ph2', 'leave1', 'leave2'], axis=1)

# Create a new dataframe to store the calculated times
result_df = pd.DataFrame(columns=['date', 'token', 'name','employee_code','in_time','out_time','total_time', 'status'])
result_df.to_csv('results.csv',index=False)

# Initialize variables to store in/out punch times
in_punch_time = None
out_punch_time = None

# Iterate over the rows and calculate time
for index, row in merged_df.iterrows():
    if row['mode'] == 0:
        in_punch_time = row['pdtime']
    elif row['mode'] == 1:
        out_punch_time = row['pdtime']
        if in_punch_time is not None:
            time_difference = out_punch_time - in_punch_time
            # Exclude rows where time difference is "0 days"
            if time_difference.total_seconds() > 0:
                hours, remainder = divmod(time_difference.seconds, 3600)
                minutes, seconds = divmod(remainder, 60)
                result_df = pd.concat([result_df, pd.DataFrame({
                    'date': [pd.to_datetime(in_punch_time, format='%d-%b-%y %H:%M:%S').strftime('%d-%b-%y')],
                    'token': [row['token']],
                    'name': [row['name']],
                    'employee_code': [row['employee_code']],
                    'in_time': [in_punch_time.strftime('%d-%b-%y %H:%M:%S')],
                    'out_time': [out_punch_time.strftime('%d-%b-%y %H:%M:%S')],
                    'total_time': [f'{hours:02}:{minutes:02}:{seconds:02}'],
                    'status': ['PR'] # Set status to 1 if 'in time' is present
                })], ignore_index=True)

# Create a dataframe for all absent days (1 to 30) for each token
all_days = pd.DataFrame(columns=['token', 'in_time', 'out_time', 'total_time', 'date', 'status', 'name', 'employee_code'])

for token in result_df['token'].unique():
    existing_dates = set(result_df[(result_df['token'] == token) & (result_df['in_time'] != '')]['date'])
    for day in range(1, 31):
        date_str = f"{day:02}-Sep-23"
        if date_str not in existing_dates:
            # Check if it's a Sunday
            is_sunday = pd.to_datetime(date_str, format='%d-%b-%y').dayofweek == 6
            status = 'wo' if is_sunday else 'AB'
            
            all_days = pd.concat([all_days, pd.DataFrame({
                'date': [pd.to_datetime(date_str, format='%d-%b-%y').strftime('%d-%b-%y')],
                'token': [token],
                'name': [result_df[result_df['token'] == token]['name'].values[0]],
                'employee_code': [result_df[result_df['token'] == token]['employee_code'].values[0]],
                'in_time': [''],
                'out_time': [''],
                'total_time': [''],
                'status': [status]  # Set status to 'wo' for Sundays, 'AB' for other absent dates
            })], ignore_index=True)

# Concatenate result_df and all_days
result_df = pd.concat([result_df, all_days], ignore_index=True)

# Sort the DataFrame by 'token', 'date'
result_df = result_df.sort_values(by=['token', 'date'])

# Save result_df to a CSV file
result_df.to_csv('./pyinput.csv', index=False)

# Save the updated DataFrame to CSV
expanded_data.to_csv('muster_with_status.csv', index=False)
