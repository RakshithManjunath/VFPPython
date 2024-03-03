import pandas as pd

def ot_to_minutes(ot_str):
    if pd.isna(ot_str) or ot_str == '' or ot_str == '00:00':
        return 0
    else:
        return sum(int(x) * 60 ** i for i, x in enumerate(reversed(ot_str.split(':'))))

def minutes_to_hours_minutes(minutes):
    hours = minutes // 60
    minutes = minutes % 60
    return '{:02}:{:02}'.format(int(hours), int(minutes))

def pay_input(merged_df):
    columns_to_drop = ['PDATE', 'STATUS', 'INTIME', 'OUTTIME', 'TOTALTIME', 'REMARKS', 'TOT_MM']
    merged_df = merged_df.drop(columns=[col for col in columns_to_drop if col in merged_df], errors='ignore')

    merged_df['OT_MINUTES'] = merged_df['OT'].apply(ot_to_minutes)

    total_ot_minutes = merged_df.groupby('TOKEN')['OT_MINUTES'].sum().reset_index()
    total_ot_minutes.columns = ['TOKEN', 'TOTAL_OT_MINUTES']

    merged_df = pd.merge(merged_df, total_ot_minutes, on='TOKEN', how='left')

    merged_df['TOTAL_OT_HRS'] = merged_df['TOTAL_OT_MINUTES'].apply(minutes_to_hours_minutes)

    columns_to_drop = ['OT','OT_MINUTES','TOTAL_OT_MINUTES']
    merged_df = merged_df.drop(columns=[col for col in columns_to_drop if col in merged_df], errors='ignore')

    merged_df = merged_df.rename(columns={'TOTAL_OT_HRS': 'OT'})

    merged_df = merged_df.drop_duplicates(subset='TOKEN')

    merged_df.to_csv('./payroll_input.csv', index=False)