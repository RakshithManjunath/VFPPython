import pandas as pd

def pay_input(merged_df):
    columns_to_drop = ['PDATE', 'STATUS', 'INTIME', 'OUTTIME', 'TOTALTIME', 'REMARKS', 'TOT_MM']
    merged_df = merged_df.drop(columns=[col for col in columns_to_drop if col in merged_df], errors='ignore')

    merged_df.to_csv('./payroll_input.csv', index=False)