import pandas as pd

# Step 1: Load the punch data
data = pd.read_csv('punch.csv', usecols=['TOKEN', 'PDATE', 'PUNCH_STATUS'])

# Step 2: Convert the 'PDATE' to datetime and extract the day
data['PDATE'] = pd.to_datetime(data['PDATE'])
data['Day'] = data['PDATE'].dt.day

# Step 3: Pivot the table to create a new DataFrame where each token has one row and each day is a column with the punch status
pivoted_data = data.pivot(index='TOKEN', columns='Day', values='PUNCH_STATUS')
pivoted_data.reset_index(inplace=True)

# Determine the range of days from the data
min_day = data['Day'].min()
max_day = data['Day'].max()

# Rename columns based on the actual range of days
day_columns = {i: f'day{i}' for i in range(min_day, max_day + 1)}
pivoted_data.rename(columns=day_columns, inplace=True)

# Step 4: Load the other DataFrame
other_data = pd.read_csv('payroll_input.csv')

# Step 5: Merge the DataFrames
merged_data = pd.merge(other_data, pivoted_data, on='TOKEN', how='outer')

# Step 6: Construct the list of columns manually to ensure the correct order
employee_info_columns = ['TOKEN', 'NAME', 'EMPCODE']
day_columns = [f'day{i}' for i in range(min_day, max_day + 1)]  # Dynamically set based on the range found earlier
totals_columns = ['TOT_AB', 'TOT_WO', 'TOT_PR', 'TOT_PH', 'TOT_LV', 'OT', 'OT_ROUNDED']

# Combine the lists to form the new column order
new_column_order = employee_info_columns + day_columns + totals_columns

# Reorder the columns in the DataFrame according to new_column_order
merged_data = merged_data[new_column_order]

# Step 7: Save the result to a new CSV file
merged_data.to_csv('final_merged_data.csv', index=False)
