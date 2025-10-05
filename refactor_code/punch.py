import pandas as pd
import numpy as np
from dbfread import DBF
from datetime import datetime
from datetime import timedelta
from test import file_paths
from py_paths import g_current_path

def mode_1_last_day(gseldate,start_date,end_date,start_date_str,end_date_str,table_paths):

    gseldate_date_format = datetime.strptime(gseldate, "%Y-%m-%d")
    next_day = gseldate_date_format + timedelta(days=1)
    is_last_day = gseldate_date_format.month != next_day.month

    # if is_last_day == True:
    #     end_date_dt = pd.to_datetime(end_date)
    #     end_date_plus1 = end_date_dt + pd.Timedelta(days=1)
    #     end_date_plus1_str = end_date_plus1.strftime('%Y-%m-%d')
    #     date_range = pd.date_range(start=start_date_str, end=end_date_plus1_str, freq='D')
    
    # else:
    #     date_range = pd.date_range(start=start_date_str, end=end_date_str, freq='D')

    if is_last_day == True:
        # Load the DBF file into a DataFrame
        punches_dbf_new_month = table_paths['punches_dbf_path']  
        punches_table_new_month = DBF(punches_dbf_new_month, load=True)
        print("punches dbf length: ", len(punches_table_new_month))
        punches_df_new_month = pd.DataFrame(iter(punches_table_new_month))
        punches_df_new_month['DEL'] = False
        print("punches df new month columns: ",punches_df_new_month.columns)

        # Convert 'end_date' to datetime and add one day
        end_date_dt = pd.to_datetime(end_date)
        end_date_plus1 = end_date_dt + pd.Timedelta(days=1)

        # Convert the dates to date format for filtering
        end_date_date = end_date_dt.date()
        end_date_plus1_date = end_date_plus1.date()

        end_date_plus1_str = end_date_plus1.strftime('%Y-%m-%d')
        # date_range = pd.date_range(start=start_date_str, end=end_date_plus1_str, freq='D')

        # Ensure 'PDATE' column is in date format
        punches_df_new_month['PDATE'] = pd.to_datetime(punches_df_new_month['PDATE']).dt.date

        # Filter the DataFrame by 'PDATE'
        filtered_df = punches_df_new_month[
            punches_df_new_month['PDATE'].between(end_date_date, end_date_plus1_date)
        ]

        # Sort by TOKEN, PDATE, and PDTIME to ensure the order of events is maintained
        filtered_df.sort_values(by=['TOKEN', 'PDATE', 'PDTIME'], inplace=True)

        # Initialize an empty list to store matching records
        matching_records = []

        # Iterate through each TOKEN group to find consecutive MODE=0 (end_date) and MODE=1 (end_date_plus1_date)
        for token, group in filtered_df.groupby('TOKEN'):
            group = group.reset_index(drop=True)  # Reset index for easier iteration
            for i in range(len(group) - 1):
                if (
                    group.loc[i, 'PDATE'] == end_date_date
                    and group.loc[i, 'MODE'] == 0
                    and group.loc[i + 1, 'PDATE'] == end_date_plus1_date
                    and group.loc[i + 1, 'MODE'] == 1
                ):
                    # Add the matching rows to the list
                    matching_records.append(group.loc[i])
                    matching_records.append(group.loc[i + 1])

        # Convert the matching records to a DataFrame
        consecutive_matching_df = pd.DataFrame(matching_records)

        print("Filtered consecutive matching records saved to 'consecutive_matching_records.csv'")

        # === Additional Step: Filter Out Only MODE=1 Records ===
        if not consecutive_matching_df.empty:  # Check if DataFrame has rows
            mode_1_only_df = consecutive_matching_df[consecutive_matching_df['MODE'] == 1]
            mode_1_only_df['DEL'] = "False"
        else:
            # Create an empty DataFrame with the desired columns
            mode_1_only_df = pd.DataFrame(columns=[
                'TOKEN', 'COMCODE', 'PDATE', 'HOURS', 'MINUTES', 
                'MODE', 'PDTIME', 'MCIP', 'DEL'
            ])

        print("Filtered MODE=1 records saved to 'mode_1_only_records.csv'")
    
        mode_1_only_df.to_csv('mode_1_only_df_check_punches.csv',index=False)
    else:
        mode_1_only_df = pd.DataFrame(columns=[
                'TOKEN', 'COMCODE', 'PDATE', 'HOURS', 'MINUTES', 
                'MODE', 'PDTIME', 'MCIP', 'DEL'
        ])
    date_range = pd.date_range(start=start_date_str, end=end_date_str, freq='D')

    return mode_1_only_df,date_range 

def hhmm_from_hours(hours: float) -> str:
    if hours is None or np.isnan(hours):
        return "0:00"
    total_seconds = int(max(hours, 0) * 3600)
    h, r = divmod(total_seconds, 3600)
    m, _ = divmod(r, 60)
    return f"{h}:{m:02d}"

def generate_punch(punches_df,muster_df,g_current_path):
    table_paths = file_paths(g_current_path)

    with open(table_paths['gsel_date_path']) as file:
        file_contents = file.readlines()
        file_contents = [string.strip('\n') for string in file_contents]
        gseldate = file_contents[0]
        gsel_datetime = pd.to_datetime(gseldate)
        ghalf_day,gfull_day = int(file_contents[1]),int(file_contents[2])

    dated_table = DBF(table_paths['dated_dbf_path'], load=True)
    start_date = dated_table.records[0]['MUFRDATE']
    end_date = dated_table.records[0]['MUTODATE']

    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    mode_1_only_df,date_range = mode_1_last_day(gseldate,start_date,end_date,start_date_str,end_date_str,table_paths)

    punches_df = pd.concat([punches_df,mode_1_only_df], ignore_index=True)
    punches_df.sort_values(by=['TOKEN', 'PDATE', 'PDTIME'], inplace=True)
    punches_df.to_csv('concated_punches_and_mode_1.csv',index=False)

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
                    days = time_difference.days
                    hours, remainder = divmod(time_difference.seconds, 3600)
                    minutes, seconds = divmod(remainder, 60)
                    minutes_status = int(time_difference.total_seconds() / 60)
                    attn_status = "PR" if minutes_status >= gfull_day else ("AB" if minutes_status <= ghalf_day else "HD")
                    overtime_hours, overtime_minutes = divmod(minutes_status - gfull_day, 60) if minutes_status > gfull_day else (0, 0)
                    overtime_formatted = "{:02d}:{:02d}".format(overtime_hours, overtime_minutes)
                    duplicates = punch_df[(punch_df['PDATE'] == in_punch_time.strftime('%Y-%m-%d')) & (punch_df['TOKEN'] == row['TOKEN'])]

                    if duplicates.empty:
                        punch_df = pd.concat([punch_df, pd.DataFrame({
                            'TOKEN': [row['TOKEN']],
                            'MODE': [row['MODE']],
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
                            'PUNCH_STATUS': attn_status,
                            'REMARKS': np.where(days > 0, '#', ''),
                            'OT': overtime_formatted
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

                        totaldays = total_time_difference.days

                        punch_df.loc[duplicates.index[-1], 'REMARKS'] = '*'  # Mark REMARKS as '*' by default

                        if totaldays > 0:
                            punch_df.loc[duplicates.index[-1], 'REMARKS'] = '#'  # Mark REMARKS as '#' if totaldays > 0

                        total_hours, total_remainder = divmod(total_time_difference.seconds, 3600)
                        total_minutes, _ = divmod(total_remainder, 60)

                        punch_df.loc[duplicates.index[-1], 'TOTALTIME'] = f'{total_hours:02}:{total_minutes:02}'
                        total_minutes_status = int(total_time_difference.total_seconds() / 60)
                        attn_status = "PR" if total_minutes_status >= gfull_day else ("AB" if total_minutes_status <= ghalf_day else "HD")
                        overtime_hours, overtime_minutes = divmod(total_minutes_status - gfull_day, 60) if total_minutes_status > gfull_day else (0, 0)
                        overtime_formatted = "{:02d}:{:02d}".format(overtime_hours, overtime_minutes)
                        punch_df.loc[duplicates.index[-1], 'OT'] = overtime_formatted
                        punch_df.loc[duplicates.index[-1], 'PUNCH_STATUS'] = attn_status  # Set PUNCH_STATUS for duplicate rows

    
    for token in muster_df['TOKEN'].unique():
        token_punch_df = punch_df[punch_df['TOKEN'] == token]

        for date in date_range:
            date_str = date.strftime('%Y-%m-%d')

            if not ((token_punch_df['PDATE'] == date_str) & (token_punch_df['TOKEN'] == token)).any():
                new_row = pd.DataFrame({
                    'TOKEN': [token],
                    'MODE': [np.nan],
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
                    'PUNCH_STATUS': "AB",  
                    'REMARKS': [np.nan],
                    'OT':""
                })

                punch_df = pd.concat([punch_df, new_row], ignore_index=True)

    punch_df = punch_df.sort_values(by=['TOKEN', 'PDATE'])
    punch_df.to_csv(table_paths['punch_csv_path'],index=False)

    # Select only the required columns from muster_df
    shift_cols = ["TOKEN", "PDATE", "SHIFT_STATUS", "SHIFT_ST", "SHIFT_ED", "WORKHRS"]
    muster_shift_info = muster_df[shift_cols].copy()

    # Merge on TOKEN and PDATE
    # Ensure both PDATE columns are datetime
    punch_df['PDATE'] = pd.to_datetime(punch_df['PDATE'], errors='coerce')
    muster_shift_info['PDATE'] = pd.to_datetime(muster_shift_info['PDATE'], errors='coerce')

    merged_df = pd.merge(punch_df, muster_shift_info, on=["TOKEN", "PDATE"], how="left")
    merged_df.to_csv("punch_updated.csv", index=False)

    out = merged_df.copy()

    # Parse times
    for col in ["INTIME", "OUTTIME"]:
        out[col] = pd.to_datetime(out[col], errors="coerce")

    out["WORKHRS"] = pd.to_numeric(out["WORKHRS"], errors="coerce")

    # Case 1: both INTIME and OUTTIME blank → mark PUNCH_STATUS as AB
    mask_absent = out["INTIME"].isna() & out["OUTTIME"].isna()
    out.loc[mask_absent, "PUNCH_STATUS"] = "AB"
    out.loc[mask_absent, ["TOTAL_HRS", "OT"]] = [0, "0:00"]

    # Case 2: rows with valid punches
    valid = ~mask_absent
    total_secs = (out.loc[valid, "OUTTIME"] - out.loc[valid, "INTIME"]).dt.total_seconds()
    total_secs = total_secs.clip(lower=0).fillna(0)
    out.loc[valid, "TOTAL_HRS"] = (total_secs / 3600.0) - float(0)
    out.loc[valid, "TOTAL_HRS"] = out.loc[valid, "TOTAL_HRS"].clip(lower=0)

    meets = (out.loc[valid, "TOTAL_HRS"] >= out.loc[valid, "WORKHRS"])
    out.loc[valid, "PUNCH_STATUS"] = np.where(meets, "PR", "HD")

    out.loc[valid, "OT_HRS"] = (out.loc[valid, "TOTAL_HRS"] - out.loc[valid, "WORKHRS"]).clip(lower=0)
    out.loc[valid, "OT"] = out.loc[valid, "OT_HRS"].apply(hhmm_from_hours)

    out = out.drop(['TOTAL_HRS', 'OT_HRS'], axis=1)

    out.to_csv('punch_newest_updated.csv',index=False)

    out = out.sort_values(by=['TOKEN', 'PDATE'])
    out.to_csv(table_paths['punch_csv_path'],index=False)

    return out

