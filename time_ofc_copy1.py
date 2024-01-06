import pandas as pd
from dbfread import DBF
import numpy as np

muster_table = DBF('./muster.dbf', load=True)
muster_df = pd.DataFrame(iter(muster_table))

muster_df = muster_df[['TOKEN', 'COMCODE', 'NAME', 'EMPCODE', 
                         'EMP_DEPT','DEPT_NAME', 'EMP_DESI',
                         'DESI_NAME']]
muster_df = muster_df.sort_values(by=['EMPCODE'])

punches_table = DBF('./punches.dbf', load=True)
punches_df = pd.DataFrame(iter(punches_table))
punches_df = punches_df.sort_values(by=['TOKEN', 'PDATE', 'MODE'])

punches_df['PDTIME'] = pd.to_datetime(punches_df['PDTIME'], format='%d-%b-%y %H:%M:%S')

punches_df.sort_values(by=['TOKEN', 'PDTIME', 'MODE'], inplace=True)

result_df = pd.DataFrame(columns=['TOKEN', 'PDATE', 'INTIME', 'OUTTIME', 'INTIME1', 'OUTTIME1', 'TOTALTIME'])

in_punch_time = None
out_punch_time = None

for index, row in punches_df.iterrows():
    if row['MODE'] == 0:
        in_punch_time = row['PDTIME']
    elif row['MODE'] == 1:
        out_punch_time = row['PDTIME']
        if in_punch_time is not None:
            time_difference = out_punch_time - in_punch_time
            if time_difference.total_seconds() > 0:
                hours, remainder = divmod(time_difference.seconds, 3600)
                minutes, seconds = divmod(remainder, 60)
                
                # Check for duplicates based on 'PDATE' and 'TOKEN'
                duplicates = result_df[(result_df['PDATE'] == in_punch_time.strftime('%Y-%m-%d')) & (result_df['TOKEN'] == row['TOKEN'])]
                
                if duplicates.empty:
                    # No duplicates, add a new row
                    result_df = pd.concat([result_df, pd.DataFrame({
                        'TOKEN': [row['TOKEN']],
                        'INTIME': [in_punch_time.strftime('%Y-%m-%d %H:%M:%S')],
                        'OUTTIME': [out_punch_time.strftime('%Y-%m-%d %H:%M:%S')],
                        'PDATE': [in_punch_time.strftime('%Y-%m-%d')],
                        'INTIME1': [np.nan],
                        'OUTTIME1': [np.nan],
                        'TOTALTIME': [f'{hours:02}:{minutes:02}:{seconds:02}']
                    })], ignore_index=True)
                else:
                    # Duplicates found, update the existing row
                    result_df.loc[duplicates.index[-1], 'INTIME1'] = in_punch_time.strftime('%Y-%m-%d %H:%M:%S')
                    result_df.loc[duplicates.index[-1], 'OUTTIME1'] = out_punch_time.strftime('%Y-%m-%d %H:%M:%S')
                    
                    # Calculate TOTALTIME based on the sum of time differences for both pairs
                    total_time_difference_1 = pd.to_datetime(result_df.loc[duplicates.index[-1], 'OUTTIME']) - pd.to_datetime(result_df.loc[duplicates.index[-1], 'INTIME'])
                    total_time_difference_2 = pd.to_datetime(result_df.loc[duplicates.index[-1], 'OUTTIME1']) - pd.to_datetime(result_df.loc[duplicates.index[-1], 'INTIME1'])
                    
                    total_hours_1, total_remainder_1 = divmod(total_time_difference_1.seconds, 3600)
                    total_minutes_1, total_seconds_1 = divmod(total_remainder_1, 60)
                    
                    total_hours_2, total_remainder_2 = divmod(total_time_difference_2.seconds, 3600)
                    total_minutes_2, total_seconds_2 = divmod(total_remainder_2, 60)
                    
                    total_hours = total_hours_1 + total_hours_2
                    total_minutes = total_minutes_1 + total_minutes_2
                    total_seconds = total_seconds_1 + total_seconds_2
                    
                    result_df.loc[duplicates.index[-1], 'TOTALTIME'] = f'{total_hours:02}:{total_minutes:02}:{total_seconds:02}'

result_df = result_df.sort_values(by=['TOKEN', 'PDATE'])


final_merged_df = pd.merge(muster_df, result_df, on='TOKEN', how='left')
final_merged_df = final_merged_df.sort_values(by=['EMPCODE', 'TOKEN', 'PDATE'])
final_merged_df.to_csv('./final.csv', index=False)