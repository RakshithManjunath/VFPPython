import pandas as pd
import numpy as np
from dbfread import DBF

def generate_punch():
    dated_table = DBF('D:/ZIONtest/dated.dbf', load=True)
    start_date = dated_table.records[0]['MUFRDATE']
    end_date = dated_table.records[0]['MUTODATE']
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    muster_table = DBF('D:/ZIONtest/muster.dbf', load=True)
    muster_df = pd.DataFrame(iter(muster_table))
    muster_df = muster_df[['TOKEN', 'COMCODE', 'NAME', 'EMPCODE', 'EMP_DEPT', 'DEPT_NAME', 'EMP_DESI', 'DESI_NAME']]
    muster_df = muster_df.sort_values(by=['TOKEN'])

    punches_table = DBF('D:/ZIONtest/punches.dbf', load=True)
    punches_df = pd.DataFrame(iter(punches_table))
    punches_df = punches_df[(punches_df['PDATE'] >= start_date) & (punches_df['PDATE'] <= end_date)]
    punches_df['PDTIME'] = pd.to_datetime(punches_df['PDTIME'], format='%d-%b-%y %H:%M:%S').dt.strftime('%Y-%m-%d %H:%M:00')
    punches_df.sort_values(by=['TOKEN', 'PDTIME', 'MODE'], inplace=True)

    punch_df = pd.DataFrame(columns=['TOKEN', 'PDATE', 'INTIME1', 'OUTTIME1', 'INTIME2', 'OUTTIME2', 'INTIME3', 'OUTTIME3', 'INTIME4', 'OUTTIME4', 'INTIME', 'OUTTIME', 'TOTALTIME','REMARKS'])
    in_punch_time = None
    out_punch_time = None

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
                    attn_status = "PR" if hours >= 8 else "A1"
                    duplicates = punch_df[(punch_df['PDATE'] == in_punch_time.strftime('%Y-%m-%d')) & (punch_df['TOKEN'] == row['TOKEN'])]

                    if duplicates.empty:
                        punch_df = pd.concat([punch_df, pd.DataFrame({
                            'TOKEN': [row['TOKEN']],
                            'PDATE': [in_punch_time.strftime('%Y-%m-%d')],
                            'INTIME1': [in_punch_time.strftime('%Y-%m-%d %H:%M')],
                            'OUTTIME1': [out_punch_time.strftime('%Y-%m-%d %H:%M')],
                            'INTIME2': [np.nan],
                            'OUTTIME2': [np.nan],
                            'INTIME3': [np.nan],
                            'OUTTIME3': [np.nan],
                            'INTIME4': [np.nan],
                            'OUTTIME4': [np.nan],
                            'INTIME': [in_punch_time.strftime('%Y-%m-%d %H:%M')],
                            'OUTTIME': [out_punch_time.strftime('%Y-%m-%d %H:%M')],
                            'TOTALTIME': [f'{hours:02}:{minutes:02}'],
                            'ATTN_STATUS': attn_status,
                            'REMARKS': ""
                        })], ignore_index=True)
                    else:
                        if pd.isna(punch_df.loc[duplicates.index[-1], 'INTIME2']):
                            punch_df.loc[duplicates.index[-1], 'INTIME2'] = in_punch_time.strftime('%Y-%m-%d %H:%M')
                            punch_df.loc[duplicates.index[-1], 'OUTTIME2'] = out_punch_time.strftime('%Y-%m-%d %H:%M')
                        elif pd.isna(punch_df.loc[duplicates.index[-1], 'INTIME3']):
                            punch_df.loc[duplicates.index[-1], 'INTIME3'] = in_punch_time.strftime('%Y-%m-%d %H:%M')
                            punch_df.loc[duplicates.index[-1], 'OUTTIME3'] = out_punch_time.strftime('%Y-%m-%d %H:%M')
                        elif pd.isna(punch_df.loc[duplicates.index[-1], 'INTIME4']):
                            punch_df.loc[duplicates.index[-1], 'INTIME4'] = in_punch_time.strftime('%Y-%m-%d %H:%M')
                            punch_df.loc[duplicates.index[-1], 'OUTTIME4'] = out_punch_time.strftime('%Y-%m-%d %H:%M')

                        punch_df.loc[duplicates.index[-1], 'REMARKS'] = "*"
                        punch_df.loc[duplicates.index[-1], 'OUTTIME'] = out_punch_time.strftime('%Y-%m-%d %H:%M') if not pd.isna(out_punch_time) else np.nan

                        total_time_difference_1 = pd.to_datetime(punch_df.loc[duplicates.index[-1], 'OUTTIME1']) - pd.to_datetime(punch_df.loc[duplicates.index[-1], 'INTIME1'])
                        total_time_difference_2 = pd.to_datetime(punch_df.loc[duplicates.index[-1], 'OUTTIME2']) - pd.to_datetime(punch_df.loc[duplicates.index[-1], 'INTIME2'])
                        total_time_difference_3 = pd.to_datetime(punch_df.loc[duplicates.index[-1], 'OUTTIME3']) - pd.to_datetime(punch_df.loc[duplicates.index[-1], 'INTIME3'])
                        total_time_difference_4 = pd.to_datetime(punch_df.loc[duplicates.index[-1], 'OUTTIME4']) - pd.to_datetime(punch_df.loc[duplicates.index[-1], 'INTIME4'])

                        total_time_difference = pd.to_timedelta(0)

                        if isinstance(total_time_difference_1, pd.Timedelta):
                            total_time_difference += total_time_difference_1

                        if isinstance(total_time_difference_2, pd.Timedelta):
                            total_time_difference += total_time_difference_2

                        if isinstance(total_time_difference_3, pd.Timedelta):
                            total_time_difference += total_time_difference_3

                        if isinstance(total_time_difference_4, pd.Timedelta):
                            total_time_difference += total_time_difference_4

                        total_hours, total_remainder = divmod(total_time_difference.seconds, 3600)
                        total_minutes, _ = divmod(total_remainder, 60)

                        punch_df.loc[duplicates.index[-1], 'TOTALTIME'] = f'{total_hours:02}:{total_minutes:02}'
                        attn_status = "PR" if total_hours >= 8 else "A1"
                        punch_df.loc[duplicates.index[-1], 'ATTN_STATUS'] = attn_status  # Set ATTN_STATUS for duplicate rows

    date_range = pd.date_range(start=start_date_str, end=end_date_str, freq='D')

    for token in muster_df['TOKEN'].unique():
        token_punch_df = punch_df[punch_df['TOKEN'] == token]

        for date in date_range:
            date_str = date.strftime('%Y-%m-%d')

            if not ((token_punch_df['PDATE'] == date_str) & (token_punch_df['TOKEN'] == token)).any():
                new_row = pd.DataFrame({
                    'TOKEN': [token],
                    'PDATE': [date_str],
                    'INTIME1': [np.nan],
                    'OUTTIME1': [np.nan],
                    'INTIME2': [np.nan],
                    'OUTTIME2': [np.nan],
                    'INTIME3': [np.nan],
                    'OUTTIME3': [np.nan],
                    'INTIME4': [np.nan],
                    'OUTTIME4': [np.nan],
                    'INTIME': [np.nan],
                    'OUTTIME': [np.nan],
                    'TOTALTIME': [np.nan],
                    'ATTN_STATUS': "AB",  
                    'REMARKS': [np.nan],
                })

                punch_df = pd.concat([punch_df, new_row], ignore_index=True)

    punch_df = punch_df.sort_values(by=['TOKEN', 'PDATE'])
    punch_df.to_csv('punch.csv',index=False)
    return punch_df