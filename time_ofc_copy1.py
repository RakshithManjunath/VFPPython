import pandas as pd
from dbfread import DBF
import numpy as np

# Load and process muster data
muster_table = DBF('./muster.dbf', load=True)
muster_df = pd.DataFrame(iter(muster_table))
muster_df = muster_df[['TOKEN', 'COMCODE', 'NAME', 'EMPCODE', 'EMP_DEPT', 'DEPT_NAME', 'EMP_DESI', 'DESI_NAME']]
muster_df = muster_df.sort_values(by=['EMPCODE'])

# Load and process punches data
punches_table = DBF('./punches.dbf', load=True)
punches_df = pd.DataFrame(iter(punches_table))
punches_df = punches_df.sort_values(by=['TOKEN', 'PDATE', 'MODE'])

# Make seconds part '00' in PDTIME
punches_df['PDTIME'] = pd.to_datetime(punches_df['PDTIME'], format='%d-%b-%y %H:%M:%S').dt.strftime('%Y-%m-%d %H:%M:%S')

# Sort punches_df based on TOKEN, PDTIME, MODE
punches_df.sort_values(by=['TOKEN', 'PDTIME', 'MODE'], inplace=True)

# Initialize result DataFrame
result_df = pd.DataFrame(columns=['TOKEN', 'PDATE', 'INTIME', 'OUTTIME', 'INTIME1', 'OUTTIME1', 'INTIME2', 'OUTTIME2', 'INTIME3', 'OUTTIME3', 'TOTALTIME', 'INTIME4', 'OUTTIME4'])

in_punch_time = None
out_punch_time = None

# Iterate through punches_df to process punch data
for index, row in punches_df.iterrows():
    if row['MODE'] == 0:
        in_punch_time = pd.to_datetime(row['PDTIME']).replace(second=0)
    elif row['MODE'] == 1:
        out_punch_time = pd.to_datetime(row['PDTIME']).replace(second=0)
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
                        'INTIME': [in_punch_time.strftime('%Y-%m-%d %H:%M')],
                        'OUTTIME': [out_punch_time.strftime('%Y-%m-%d %H:%M')],
                        'PDATE': [in_punch_time.strftime('%Y-%m-%d')],
                        'INTIME1': [np.nan],
                        'OUTTIME1': [np.nan],
                        'INTIME2': [np.nan],
                        'OUTTIME2': [np.nan],
                        'INTIME3': [np.nan],
                        'OUTTIME3': [np.nan],
                        'TOTALTIME': [f'{hours:02}:{minutes:02}'],
                        'INTIME4': [in_punch_time.strftime('%Y-%m-%d %H:%M')],
                        'OUTTIME4': [out_punch_time.strftime('%Y-%m-%d %H:%M')]  # Initialize OUTTIME4 with NaN
                    })], ignore_index=True)
                else:
                    # Duplicates found, update the existing row
                    if pd.isna(result_df.loc[duplicates.index[-1], 'INTIME1']):
                        result_df.loc[duplicates.index[-1], 'INTIME1'] = in_punch_time.strftime('%Y-%m-%d %H:%M')
                        result_df.loc[duplicates.index[-1], 'OUTTIME1'] = out_punch_time.strftime('%Y-%m-%d %H:%M')
                    elif pd.isna(result_df.loc[duplicates.index[-1], 'INTIME2']):
                        result_df.loc[duplicates.index[-1], 'INTIME2'] = in_punch_time.strftime('%Y-%m-%d %H:%M')
                        result_df.loc[duplicates.index[-1], 'OUTTIME2'] = out_punch_time.strftime('%Y-%m-%d %H:%M')
                    elif pd.isna(result_df.loc[duplicates.index[-1], 'INTIME3']):
                        result_df.loc[duplicates.index[-1], 'INTIME3'] = in_punch_time.strftime('%Y-%m-%d %H:%M')
                        result_df.loc[duplicates.index[-1], 'OUTTIME3'] = out_punch_time.strftime('%Y-%m-%d %H:%M')

                    
                    # Update INTIME4 and OUTTIME4
                    result_df.loc[duplicates.index[-1], 'OUTTIME4'] = out_punch_time.strftime('%Y-%m-%d %H:%M') if not pd.isna(out_punch_time) else np.nan


                    # Calculate TOTALTIME based on the sum of time differences for all pairs
                    total_time_difference_1 = pd.to_datetime(result_df.loc[duplicates.index[-1], 'OUTTIME']) - pd.to_datetime(result_df.loc[duplicates.index[-1], 'INTIME'])
                    total_time_difference_2 = pd.to_datetime(result_df.loc[duplicates.index[-1], 'OUTTIME1']) - pd.to_datetime(result_df.loc[duplicates.index[-1], 'INTIME1'])
                    total_time_difference_3 = pd.to_datetime(result_df.loc[duplicates.index[-1], 'OUTTIME2']) - pd.to_datetime(result_df.loc[duplicates.index[-1], 'INTIME2'])
                    total_time_difference_4 = pd.to_datetime(result_df.loc[duplicates.index[-1], 'OUTTIME3']) - pd.to_datetime(result_df.loc[duplicates.index[-1], 'INTIME3'])
                    
                    # Sum only non-NaN values of type pd.Timedelta
                    total_time_difference = pd.to_timedelta(0)

                    if isinstance(total_time_difference_1, pd.Timedelta):
                        total_time_difference += total_time_difference_1

                    if isinstance(total_time_difference_2, pd.Timedelta):
                        total_time_difference += total_time_difference_2

                    if isinstance(total_time_difference_3, pd.Timedelta):
                        total_time_difference += total_time_difference_3

                    if isinstance(total_time_difference_4, pd.Timedelta):
                        total_time_difference += total_time_difference_4

                    # Calculate hours and minutes
                    total_hours, total_remainder = divmod(total_time_difference.seconds, 3600)
                    total_minutes, _ = divmod(total_remainder, 60)

                    result_df.loc[duplicates.index[-1], 'TOTALTIME'] = f'{total_hours:02}:{total_minutes:02}'

# Sort result_df based on TOKEN, PDATE
result_df = result_df.sort_values(by=['TOKEN', 'PDATE'])

merged_df = pd.merge(muster_df, result_df, on='TOKEN', how='left')

# Save the result to a CSV file
merged_df.to_csv('./6thJan_punches_modified.csv', index=False)