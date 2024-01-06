from dbfread import DBF
import pandas as pd
import numpy as np

muster_table = DBF('D:/ZIONtest/muster.dbf', load=True)
muster_df = pd.DataFrame(iter(muster_table))

dated_table = DBF('D:/ZIONtest/dated.dbf', load=True)
dated_df = pd.DataFrame(iter(dated_table))

start_date = dated_df['MUFRDATE']
end_date = dated_df['MUTODATE']

start_date = pd.to_datetime(start_date)
end_date = pd.to_datetime(end_date)

muster_df['DEL'] = muster_df['DEL'].astype(bool)
muster_only_false = muster_df[muster_df['DEL'] == False]

muster_df['DATE_LEAVE'] = pd.to_datetime(muster_df['DATE_LEAVE'])

mask = (muster_df['DATE_LEAVE'] >= start_date.min()) & (muster_df['DATE_LEAVE'] <= end_date.max()) 
filtered_muster_df = muster_df.loc[mask]

muster_df1 = pd.concat([filtered_muster_df,muster_only_false],ignore_index=True)
muster_df1 = muster_df1[['TOKEN', 'COMCODE', 'NAME', 'EMPCODE', 
                         'EMP_DEPT','DEPT_NAME', 'EMP_DESI',
                         'DESI_NAME']]
muster_df1 = muster_df1.sort_values(by=['EMPCODE'])

punches_table = DBF('D:/ZIONtest/punches.dbf', load=True)
punches_df = pd.DataFrame(iter(punches_table))
punches_df = punches_df.sort_values(by=['TOKEN', 'PDATE', 'MODE'])

in_punch_time = None
out_punch_time = None

# Filter the punches_df based on the date range
filtered_df = punches_df[
    punches_df['PDTIME'].between(start_date.iloc[0], end_date.iloc[0])
]

# Initialize the punches_df1
punches_df1 = pd.DataFrame(columns=['PDATE', 'TOKEN', 'IN_TIME', 'OUT_TIME', 'TOTAL_TIME', 'STATUS'])

# Iterate over each unique TOKEN
for token in filtered_df['TOKEN'].unique():
    # Filter data for the current TOKEN
    token_data = filtered_df[filtered_df['TOKEN'] == token]

    # Iterate over each row in the filtered DataFrame for the current TOKEN
    for index, row in token_data.iterrows():
        if row['MODE'] == 0:
            in_punch_time = row['PDTIME']
        elif row['MODE'] == 1:
            out_punch_time = row['PDTIME']
            if in_punch_time is not None:
                time_difference = out_punch_time - in_punch_time
                if time_difference.total_seconds() > 0:
                    hours, remainder = divmod(time_difference.seconds, 3600)
                    minutes, seconds = divmod(remainder, 60)

                    # Extract date from in_punch_time
                    punch_date = pd.to_datetime(in_punch_time, format='%d-%b-%y %H:%M:%S').strftime('%Y-%m-%d')

                    # Concatenate the punch data to the result DataFrame
                    punches_df1 = pd.concat([punches_df1, pd.DataFrame({
                        'PDATE': [punch_date],
                        'TOKEN': [token],
                        'IN_TIME': [in_punch_time.strftime('%Y-%m-%d %H:%M:%S')],
                        'OUT_TIME': [out_punch_time.strftime('%Y-%m-%d %H:%M:%S')],
                        'TOTAL_TIME': [f'{hours:02}:{minutes:02}:{seconds:02}']
                    })], ignore_index=True)

# Add missing dates within the range for each TOKEN
date_range = pd.date_range(start=start_date.iloc[0], end=end_date.iloc[0])

for token in filtered_df['TOKEN'].unique():
    # Filter data for the current TOKEN
    token_data = punches_df1[punches_df1['TOKEN'] == token]
    print(token_data)

    missing_dates = date_range[~date_range.isin(token_data['PDATE'])]

    for missing_date in missing_dates:
        punches_df1 = pd.concat([punches_df1, pd.DataFrame({
            'PDATE': [missing_date.strftime('%Y-%m-%d')],
            'TOKEN': [token],
            'IN_TIME': [np.nan],
            'OUT_TIME': [np.nan],
            'TOTAL_TIME': [np.nan]
        })], ignore_index=True)

# Sort the punches_df1 by 'TOKEN' and 'PDATE'
punches_df1 = punches_df1.sort_values(by=['TOKEN', 'PDATE'])

final_merged_df = pd.merge(muster_df1, punches_df1, on='TOKEN', how='left')
final_merged_df = final_merged_df.sort_values(by=['EMPCODE', 'TOKEN', 'PDATE'])
final_merged_df.to_csv('./final.csv', index=False)