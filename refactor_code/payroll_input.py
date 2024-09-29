import pandas as pd
from test import file_paths
from py_paths import g_current_path

def ot_to_minutes(ot_str):
    if pd.isna(ot_str) or ot_str == '' or ot_str == '00:00':
        return 0
    else:
        return sum(int(x) * 60 ** i for i, x in enumerate(reversed(ot_str.split(':'))))

def minutes_to_hours_minutes(minutes):
    hours = minutes // 60
    minutes = minutes % 60
    return float('{:02}.{:02}'.format(int(hours), int(minutes)))

def minutes_to_hours_minutes_rounded(minutes):
    hours = minutes // 60
    minutes = minutes % 60
    total_hours = hours + minutes / 60
    rounded_hours = round(total_hours)
    return float('{:.1f}'.format(rounded_hours))

def pay_input(merged_df,g_current_path):
    table_paths = file_paths(g_current_path)
    columns_to_drop = ['PDATE', 'STATUS', 'INTIME', 'OUTTIME', 'TOTALTIME', 'REMARKS', 'TOT_MM']
    merged_df = merged_df.drop(columns=[col for col in columns_to_drop if col in merged_df], errors='ignore')

    merged_df['OT_MINUTES'] = merged_df['OT'].apply(ot_to_minutes)

    total_ot_minutes = merged_df.groupby('TOKEN')['OT_MINUTES'].sum().reset_index()
    total_ot_minutes.columns = ['TOKEN', 'TOTAL_OT_MINUTES']

    merged_df = pd.merge(merged_df, total_ot_minutes, on='TOKEN', how='left')

    merged_df['TOTAL_OT_HRS'] = merged_df['TOTAL_OT_MINUTES'].apply(minutes_to_hours_minutes)
    merged_df['OT_ROUNDED'] = merged_df['TOTAL_OT_MINUTES'].apply(minutes_to_hours_minutes_rounded)

    columns_to_drop = ['OT','OT_MINUTES','TOTAL_OT_MINUTES']
    merged_df = merged_df.drop(columns=[col for col in columns_to_drop if col in merged_df], errors='ignore')

    merged_df = merged_df.rename(columns={'TOTAL_OT_HRS': 'OT'})

    merged_df = merged_df.drop_duplicates(subset='TOKEN')

    # columns_to_drop = ['COMCODE','EMP_DEPT','DEPT_NAME','EMP_DESI','DESI_NAME']
    # merged_df = merged_df.drop(columns=[col for col in columns_to_drop if col in merged_df], errors='ignore')

    merged_df.to_csv(table_paths['payroll_input_path'], index=False)

    data = pd.read_csv(table_paths['final_csv_path'], usecols=['TOKEN', 'PDATE', 'STATUS'])

    data['PDATE'] = pd.to_datetime(data['PDATE'])
    data['Day'] = data['PDATE'].dt.day

    pivoted_data = data.pivot(index='TOKEN', columns='Day', values='STATUS')
    pivoted_data.reset_index(inplace=True)

    min_day = data['Day'].min()
    max_day = data['Day'].max()

    if max_day == 28:
        for day in range(29, 32):
            pivoted_data[f'day{day}'] = None
    elif max_day == 29:
        for day in range(30, 32):
            pivoted_data[f'day{day}'] = None
    elif max_day == 30:
        pivoted_data['day31'] = None

    pivoted_data.columns = ['TOKEN'] + [f'day{col}' if isinstance(col, int) else col for col in pivoted_data.columns[1:]]

    # other_data = pd.read_csv(table_paths['payroll_input_path'])
    # other_data.to_csv('other_data.csv',index=False)

    merged_data = pd.merge(merged_df, pivoted_data, on='TOKEN', how='outer')

    employee_info_columns = ['TOKEN', 'NAME', 'EMPCODE', 'EMP_DEPT', 'DEPT_NAME', 'EMP_DESI', 'DESI_NAME']
    day_columns = list(pivoted_data.columns[1:])
    totals_columns = ['TOT_AB', 'TOT_WO', 'TOT_PR', 'TOT_PH', 'TOT_LV', 'OT', 'OT_ROUNDED', 'COMCODE']

    new_column_order = employee_info_columns + day_columns + totals_columns

    merged_data = merged_data[new_column_order]

    merged_data.to_csv(table_paths['muster_role_path'], index=False)