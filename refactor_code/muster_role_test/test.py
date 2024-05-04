# import pandas as pd

# data = pd.read_csv('final.csv', usecols=['TOKEN', 'PDATE', 'STATUS'])

# data['PDATE'] = pd.to_datetime(data['PDATE'])
# data['Day'] = data['PDATE'].dt.day

# pivoted_data = data.pivot(index='TOKEN', columns='Day', values='STATUS')
# pivoted_data.reset_index(inplace=True)

# min_day = data['Day'].min()
# max_day = data['Day'].max()

# day_columns = {i: f'day{i}' for i in range(min_day, max_day + 1)}
# pivoted_data.rename(columns=day_columns, inplace=True)

# pivoted_data.to_csv('pivoted_data.csv',index=False)

# other_data = pd.read_csv('payroll_input.csv')

# merged_data = pd.merge(other_data, pivoted_data, on='TOKEN', how='outer')

# merged_data.to_csv('merged.csv',index=False)

# employee_info_columns = ['TOKEN', 'NAME', 'EMPCODE', 'EMP_DEPT', 'DEPT_NAME', 'EMP_DESI', 'DESI_NAME']
# day_columns = [f'day{i}' for i in range(min_day, max_day + 1)]
# totals_columns = ['TOT_AB', 'TOT_WO', 'TOT_PR', 'TOT_PH', 'TOT_LV', 'OT', 'OT_ROUNDED']

# new_column_order = employee_info_columns + day_columns + totals_columns

# merged_data = merged_data[new_column_order]

# merged_data.to_csv('muster_role.csv', index=False)

import pandas as pd

data = pd.read_csv('final_30days.csv', usecols=['TOKEN', 'PDATE', 'STATUS'])

data['PDATE'] = pd.to_datetime(data['PDATE'])
data['Day'] = data['PDATE'].dt.day

pivoted_data = data.pivot(index='TOKEN', columns='Day', values='STATUS')
pivoted_data.reset_index(inplace=True)

min_day = data['Day'].min()
max_day = data['Day'].max()

# Modify column renaming based on the maximum day in the month
if max_day == 28:
    # If max_day is 28, create columns for days 29, 30, and 31 but leave them blank
    for day in range(29, 32):
        pivoted_data[f'day{day}'] = None
elif max_day == 29:
    # If max_day is 29, create columns for days 30 and 31 but leave them blank
    for day in range(30, 32):
        pivoted_data[f'day{day}'] = None
elif max_day == 30:
    # If max_day is 30, create column for day 31 but leave it blank
    pivoted_data['day31'] = None

# Rename columns with the prefix "day"
pivoted_data.columns = ['TOKEN'] + [f'day{col}' if isinstance(col, int) else col for col in pivoted_data.columns[1:]]
print(list(pivoted_data.columns[1:]))

other_data = pd.read_csv('payroll_input.csv')

merged_data = pd.merge(other_data, pivoted_data, on='TOKEN', how='outer')

# employee_info_columns = ['TOKEN', 'NAME', 'EMPCODE', 'EMP_DEPT', 'DEPT_NAME', 'EMP_DESI', 'DESI_NAME']
day_columns = list(pivoted_data.columns[1:])
totals_columns = ['TOT_AB', 'TOT_WO', 'TOT_PR', 'TOT_PH', 'TOT_LV', 'OT', 'OT_ROUNDED']

new_column_order = day_columns + totals_columns

merged_data = merged_data[new_column_order]

merged_data.to_csv('muster_role.csv', index=False)

