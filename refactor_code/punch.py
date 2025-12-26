# import pandas as pd
# import numpy as np
# from dbfread import DBF
# from datetime import datetime
# from datetime import timedelta
# from test import file_paths
# from py_paths import g_current_path

# def mode_1_last_day(gseldate,start_date,end_date,start_date_str,end_date_str,table_paths):

#     gseldate_date_format = datetime.strptime(gseldate, "%Y-%m-%d")
#     next_day = gseldate_date_format + timedelta(days=1)
#     is_last_day = gseldate_date_format.month != next_day.month

#     # if is_last_day == True:
#     #     end_date_dt = pd.to_datetime(end_date)
#     #     end_date_plus1 = end_date_dt + pd.Timedelta(days=1)
#     #     end_date_plus1_str = end_date_plus1.strftime('%Y-%m-%d')
#     #     date_range = pd.date_range(start=start_date_str, end=end_date_plus1_str, freq='D')
    
#     # else:
#     #     date_range = pd.date_range(start=start_date_str, end=end_date_str, freq='D')

#     if is_last_day == True:
#         # Load the DBF file into a DataFrame
#         punches_dbf_new_month = table_paths['punches_dbf_path']  
#         punches_table_new_month = DBF(punches_dbf_new_month, load=True)
#         print("punches dbf length: ", len(punches_table_new_month))
#         punches_df_new_month = pd.DataFrame(iter(punches_table_new_month))
#         punches_df_new_month['DEL'] = False
#         print("punches df new month columns: ",punches_df_new_month.columns)

#         # Convert 'end_date' to datetime and add one day
#         end_date_dt = pd.to_datetime(end_date)
#         end_date_plus1 = end_date_dt + pd.Timedelta(days=1)

#         # Convert the dates to date format for filtering
#         end_date_date = end_date_dt.date()
#         end_date_plus1_date = end_date_plus1.date()

#         end_date_plus1_str = end_date_plus1.strftime('%Y-%m-%d')
#         # date_range = pd.date_range(start=start_date_str, end=end_date_plus1_str, freq='D')

#         # Ensure 'PDATE' column is in date format
#         punches_df_new_month['PDATE'] = pd.to_datetime(punches_df_new_month['PDATE']).dt.date

#         # Filter the DataFrame by 'PDATE'
#         filtered_df = punches_df_new_month[
#             punches_df_new_month['PDATE'].between(end_date_date, end_date_plus1_date)
#         ]

#         # Sort by TOKEN, PDATE, and PDTIME to ensure the order of events is maintained
#         filtered_df.sort_values(by=['TOKEN', 'PDATE', 'PDTIME'], inplace=True)

#         # Initialize an empty list to store matching records
#         matching_records = []

#         # Iterate through each TOKEN group to find consecutive MODE=0 (end_date) and MODE=1 (end_date_plus1_date)
#         for token, group in filtered_df.groupby('TOKEN'):
#             group = group.reset_index(drop=True)  # Reset index for easier iteration
#             for i in range(len(group) - 1):
#                 if (
#                     group.loc[i, 'PDATE'] == end_date_date
#                     and group.loc[i, 'MODE'] == 0
#                     and group.loc[i + 1, 'PDATE'] == end_date_plus1_date
#                     and group.loc[i + 1, 'MODE'] == 1
#                 ):
#                     # Add the matching rows to the list
#                     matching_records.append(group.loc[i])
#                     matching_records.append(group.loc[i + 1])

#         # Convert the matching records to a DataFrame
#         consecutive_matching_df = pd.DataFrame(matching_records)

#         print("Filtered consecutive matching records saved to 'consecutive_matching_records.csv'")

#         # === Additional Step: Filter Out Only MODE=1 Records ===
#         if not consecutive_matching_df.empty:  # Check if DataFrame has rows
#             mode_1_only_df = consecutive_matching_df[consecutive_matching_df['MODE'] == 1]
#             mode_1_only_df['DEL'] = "False"
#         else:
#             # Create an empty DataFrame with the desired columns
#             mode_1_only_df = pd.DataFrame(columns=[
#                 'TOKEN', 'COMCODE', 'PDATE', 'HOURS', 'MINUTES', 
#                 'MODE', 'PDTIME', 'MCIP', 'DEL'
#             ])

#         print("Filtered MODE=1 records saved to 'mode_1_only_records.csv'")
    
#         mode_1_only_df.to_csv('mode_1_only_df_check_punches.csv',index=False)
#     else:
#         mode_1_only_df = pd.DataFrame(columns=[
#                 'TOKEN', 'COMCODE', 'PDATE', 'HOURS', 'MINUTES', 
#                 'MODE', 'PDTIME', 'MCIP', 'DEL'
#         ])
#     date_range = pd.date_range(start=start_date_str, end=end_date_str, freq='D')

#     return mode_1_only_df,date_range 

# def hhmm_from_hours(hours: float) -> str:
#     if hours is None or np.isnan(hours):
#         return "0:00"
#     total_seconds = int(max(hours, 0) * 3600)
#     h, r = divmod(total_seconds, 3600)
#     m, _ = divmod(r, 60)
#     return f"{h}:{m:02d}"

# def generate_punch(punches_df,muster_df,g_current_path):
#     table_paths = file_paths(g_current_path)

#     with open(table_paths['gsel_date_path']) as file:
#         file_contents = file.readlines()
#         file_contents = [string.strip('\n') for string in file_contents]
#         gseldate = file_contents[0]
#         gsel_datetime = pd.to_datetime(gseldate)
#         ghalf_day,gfull_day = int(file_contents[1]),int(file_contents[2])


#     shinfo = pd.read_csv(table_paths['shiftmast_csv_path'])
#     shinfo = shinfo[['shcode','name','comcode','workhrs','gratime']]
#     shinfo['workhrs_minutes'] = (shinfo['workhrs'] * 60).astype(int)
#     shinfo['halfday_minutes'] = ((shinfo['workhrs']/2) * 60).astype(int)
#     subset = shinfo[['shcode','name','comcode','workhrs','gratime','workhrs_minutes','halfday_minutes']]
#     subset.to_csv("shift_info_extracted.csv", index=False)



#     dated_table = DBF(table_paths['dated_dbf_path'], load=True)
#     start_date = dated_table.records[0]['MUFRDATE']
#     end_date = dated_table.records[0]['MUTODATE']

#     start_date_str = start_date.strftime('%Y-%m-%d')
#     end_date_str = end_date.strftime('%Y-%m-%d')

#     mode_1_only_df,date_range = mode_1_last_day(gseldate,start_date,end_date,start_date_str,end_date_str,table_paths)

#     punches_df = pd.concat([punches_df,mode_1_only_df], ignore_index=True)
#     punches_df.sort_values(by=['TOKEN', 'PDATE', 'PDTIME'], inplace=True)
#     # punches_df.to_csv('concated_punches_and_mode_1.csv',index=False)

#     punch_df = pd.DataFrame(columns=['TOKEN', 'PDATE', 'INTIME1', 'OUTTIME1', 'INTIME2', 'OUTTIME2', 'INTIME3', 'OUTTIME3', 'INTIME4', 'OUTTIME4', 'INTIME', 'OUTTIME', 'TOTALTIME','REMARKS'])
#     in_punch_time = None
#     out_punch_time = None

#     for index, row in punches_df.iterrows():
#         if row['MODE'] == 0:
#             in_punch_time = pd.to_datetime(row['PDTIME']).replace(second=0)
#         elif row['MODE'] == 1:
#             out_punch_time = pd.to_datetime(row['PDTIME']).replace(second=0)
#             if in_punch_time is not None:
#                 time_difference = out_punch_time - in_punch_time
#                 if time_difference.total_seconds() > 0:
#                     days = time_difference.days
#                     hours, remainder = divmod(time_difference.seconds, 3600)
#                     minutes, seconds = divmod(remainder, 60)
#                     minutes_status = int(time_difference.total_seconds() / 60)
#                     attn_status = "PR" if minutes_status >= gfull_day else ("AB" if minutes_status <= ghalf_day else "HD")
#                     overtime_hours, overtime_minutes = divmod(minutes_status - gfull_day, 60) if minutes_status > gfull_day else (0, 0)
#                     overtime_formatted = "{:02d}:{:02d}".format(overtime_hours, overtime_minutes)
#                     duplicates = punch_df[(punch_df['PDATE'] == in_punch_time.strftime('%Y-%m-%d')) & (punch_df['TOKEN'] == row['TOKEN'])]

#                     if duplicates.empty:
#                         punch_df = pd.concat([punch_df, pd.DataFrame({
#                             'TOKEN': [row['TOKEN']],
#                             'MODE': [row['MODE']],
#                             'PDATE': [in_punch_time.strftime('%Y-%m-%d')],
#                             'INTIME1': [in_punch_time.strftime('%Y-%m-%d %H:%M')],
#                             'OUTTIME1': [out_punch_time.strftime('%Y-%m-%d %H:%M')],
#                             'INTIME2': [np.nan],
#                             'OUTTIME2': [np.nan],
#                             'INTIME3': [np.nan],
#                             'OUTTIME3': [np.nan],
#                             'INTIME4': [np.nan],
#                             'OUTTIME4': [np.nan],
#                             'INTIME': [in_punch_time.strftime('%Y-%m-%d %H:%M')],
#                             'OUTTIME': [out_punch_time.strftime('%Y-%m-%d %H:%M')],
#                             'TOTALTIME': [f'{hours:02}:{minutes:02}'],
#                             'PUNCH_STATUS': attn_status,
#                             'REMARKS': np.where(days > 0, '#', ''),
#                             'OT': overtime_formatted
#                         })], ignore_index=True)
#                     else:
#                         if pd.isna(punch_df.loc[duplicates.index[-1], 'INTIME2']):
#                             punch_df.loc[duplicates.index[-1], 'INTIME2'] = in_punch_time.strftime('%Y-%m-%d %H:%M')
#                             punch_df.loc[duplicates.index[-1], 'OUTTIME2'] = out_punch_time.strftime('%Y-%m-%d %H:%M')
#                         elif pd.isna(punch_df.loc[duplicates.index[-1], 'INTIME3']):
#                             punch_df.loc[duplicates.index[-1], 'INTIME3'] = in_punch_time.strftime('%Y-%m-%d %H:%M')
#                             punch_df.loc[duplicates.index[-1], 'OUTTIME3'] = out_punch_time.strftime('%Y-%m-%d %H:%M')
#                         elif pd.isna(punch_df.loc[duplicates.index[-1], 'INTIME4']):
#                             punch_df.loc[duplicates.index[-1], 'INTIME4'] = in_punch_time.strftime('%Y-%m-%d %H:%M')
#                             punch_df.loc[duplicates.index[-1], 'OUTTIME4'] = out_punch_time.strftime('%Y-%m-%d %H:%M')

#                         punch_df.loc[duplicates.index[-1], 'OUTTIME'] = out_punch_time.strftime('%Y-%m-%d %H:%M') if not pd.isna(out_punch_time) else np.nan

#                         total_time_difference_1 = pd.to_datetime(punch_df.loc[duplicates.index[-1], 'OUTTIME1']) - pd.to_datetime(punch_df.loc[duplicates.index[-1], 'INTIME1'])
#                         total_time_difference_2 = pd.to_datetime(punch_df.loc[duplicates.index[-1], 'OUTTIME2']) - pd.to_datetime(punch_df.loc[duplicates.index[-1], 'INTIME2'])
#                         total_time_difference_3 = pd.to_datetime(punch_df.loc[duplicates.index[-1], 'OUTTIME3']) - pd.to_datetime(punch_df.loc[duplicates.index[-1], 'INTIME3'])
#                         total_time_difference_4 = pd.to_datetime(punch_df.loc[duplicates.index[-1], 'OUTTIME4']) - pd.to_datetime(punch_df.loc[duplicates.index[-1], 'INTIME4'])

#                         total_time_difference = pd.to_timedelta(0)

#                         if isinstance(total_time_difference_1, pd.Timedelta):
#                             total_time_difference += total_time_difference_1

#                         if isinstance(total_time_difference_2, pd.Timedelta):
#                             total_time_difference += total_time_difference_2

#                         if isinstance(total_time_difference_3, pd.Timedelta):
#                             total_time_difference += total_time_difference_3

#                         if isinstance(total_time_difference_4, pd.Timedelta):
#                             total_time_difference += total_time_difference_4

#                         totaldays = total_time_difference.days

#                         punch_df.loc[duplicates.index[-1], 'REMARKS'] = '*'  # Mark REMARKS as '*' by default

#                         if totaldays > 0:
#                             punch_df.loc[duplicates.index[-1], 'REMARKS'] = '#'  # Mark REMARKS as '#' if totaldays > 0

#                         total_hours, total_remainder = divmod(total_time_difference.seconds, 3600)
#                         total_minutes, _ = divmod(total_remainder, 60)

#                         punch_df.loc[duplicates.index[-1], 'TOTALTIME'] = f'{total_hours:02}:{total_minutes:02}'
#                         total_minutes_status = int(total_time_difference.total_seconds() / 60)
#                         attn_status = "PR" if total_minutes_status >= gfull_day else ("AB" if total_minutes_status <= ghalf_day else "HD")
#                         overtime_hours, overtime_minutes = divmod(total_minutes_status - gfull_day, 60) if total_minutes_status > gfull_day else (0, 0)
#                         overtime_formatted = "{:02d}:{:02d}".format(overtime_hours, overtime_minutes)
#                         punch_df.loc[duplicates.index[-1], 'OT'] = overtime_formatted
#                         punch_df.loc[duplicates.index[-1], 'PUNCH_STATUS'] = attn_status  # Set PUNCH_STATUS for duplicate rows

    
#     for token in muster_df['TOKEN'].unique():
#         token_punch_df = punch_df[punch_df['TOKEN'] == token]

#         for date in date_range:
#             date_str = date.strftime('%Y-%m-%d')

#             if not ((token_punch_df['PDATE'] == date_str) & (token_punch_df['TOKEN'] == token)).any():
#                 new_row = pd.DataFrame({
#                     'TOKEN': [token],
#                     'MODE': [np.nan],
#                     'PDATE': [date_str],
#                     'INTIME1': [np.nan],
#                     'OUTTIME1': [np.nan],
#                     'INTIME2': [np.nan],
#                     'OUTTIME2': [np.nan],
#                     'INTIME3': [np.nan],
#                     'OUTTIME3': [np.nan],
#                     'INTIME4': [np.nan],
#                     'OUTTIME4': [np.nan],
#                     'INTIME': [np.nan],
#                     'OUTTIME': [np.nan],
#                     'TOTALTIME': [np.nan],
#                     'PUNCH_STATUS': "AB",  
#                     'REMARKS': [np.nan],
#                     'OT':""
#                 })

#                 punch_df = pd.concat([punch_df, new_row], ignore_index=True)

#     punch_df = punch_df.sort_values(by=['TOKEN', 'PDATE'])
#     punch_df.to_csv(table_paths['punch_csv_path'],index=False)

#     # Select only the required columns from muster_df
#     shift_cols = ["TOKEN", "PDATE", "SHIFT_STATUS", "SHIFT_ST", "SHIFT_ED", "WORKHRS"]
#     muster_shift_info = muster_df[shift_cols].copy()

#     # Merge on TOKEN and PDATE
#     # Ensure both PDATE columns are datetime
#     punch_df['PDATE'] = pd.to_datetime(punch_df['PDATE'], errors='coerce')
#     muster_shift_info['PDATE'] = pd.to_datetime(muster_shift_info['PDATE'], errors='coerce')

#     merged_df = pd.merge(punch_df, muster_shift_info, on=["TOKEN", "PDATE"], how="left")
#     # merged_df.to_csv("punch_updated.csv", index=False)

#     out = merged_df.copy()

#     # Parse times
#     for col in ["INTIME", "OUTTIME"]:
#         out[col] = pd.to_datetime(out[col], errors="coerce")

#     out["WORKHRS"] = pd.to_numeric(out["WORKHRS"], errors="coerce")

#     # Case 1: both INTIME and OUTTIME blank → mark PUNCH_STATUS as AB
#     mask_absent = out["INTIME"].isna() & out["OUTTIME"].isna()
#     out.loc[mask_absent, "PUNCH_STATUS"] = "AB"
#     out.loc[mask_absent, ["TOTAL_HRS", "OT"]] = [0, "0:00"]

#     # Case 2: rows with valid punches
#     valid = ~mask_absent
#     total_secs = (out.loc[valid, "OUTTIME"] - out.loc[valid, "INTIME"]).dt.total_seconds()
#     total_secs = total_secs.clip(lower=0).fillna(0)
#     out.loc[valid, "TOTAL_HRS"] = (total_secs / 3600.0) - float(0)
#     out.loc[valid, "TOTAL_HRS"] = out.loc[valid, "TOTAL_HRS"].clip(lower=0)

#     meets = (out.loc[valid, "TOTAL_HRS"] >= out.loc[valid, "WORKHRS"])
#     out.loc[valid, "PUNCH_STATUS"] = np.where(meets, "PR", "HD")

#     out.loc[valid, "OT_HRS"] = (out.loc[valid, "TOTAL_HRS"] - out.loc[valid, "WORKHRS"]).clip(lower=0)
#     out.loc[valid, "OT"] = out.loc[valid, "OT_HRS"].apply(hhmm_from_hours)

#     out = out.drop(['TOTAL_HRS', 'OT_HRS'], axis=1)

#     # out.to_csv('punch_newest_updated.csv',index=False)

#     out = out.sort_values(by=['TOKEN', 'PDATE'])
#     out.to_csv(table_paths['punch_csv_path'],index=False)

#     return out


import os
import sys
import pandas as pd
import numpy as np
from dbfread import DBF
from datetime import datetime, timedelta

from test import file_paths
from py_paths import g_current_path


# -------------------- Utils --------------------
def hhmm(minutes: int) -> str:
    h = minutes // 60
    m = minutes % 60
    return f"{h}:{m:02d}"


def hhmm_from_float(v):
    """Converts VFP-style time float like 21.30 -> '21:30'"""
    if pd.isna(v):
        return None
    v = float(v)
    h = int(v)
    m = int(round((v - h) * 100))
    return f"{h:02d}:{m:02d}"


def float_hhmm_to_minutes(x) -> int:
    """
    Converts HH.MM float (e.g., 0.15 meaning 15 minutes) to minutes.
    Works for values like 1.30 -> 90, 0.45 -> 45, 2.00 -> 120
    """
    if pd.isna(x):
        return 0
    x = pd.to_numeric(x, errors="coerce")
    if pd.isna(x):
        return 0
    hrs = int(x)
    mins = int(round((x - hrs) * 100))
    return hrs * 60 + mins


def safe_drop(df: pd.DataFrame, cols) -> pd.DataFrame:
    """Drop columns only if they exist (prevents KeyError)."""
    return df.drop(columns=[c for c in cols if c in df.columns], errors="ignore")


# -------------------- OUTPASS OVERRIDE --------------------
def apply_outpass_override(out: pd.DataFrame, outpass_csv_path: str) -> pd.DataFrame:
    """
    OUTPASS rule:
      - Read OUTPASS.csv columns: empcode, comcode, passdate, totpasshrs
      - Match out.TOKEN == empcode AND out.COMCODE == comcode AND out.PDATE == passdate (date match)
      - If matched: REMARKS='@'
      - If matched AND PUNCH_STATUS == 'HD': set to 'PR'
      - Keep TOTALTIME as-is
      - Add TOTPASSHRS column (from totpasshrs) at the end
    """
    out = out.copy()

    # Ensure the output column exists
    if "TOTPASSHRS" not in out.columns:
        out["TOTPASSHRS"] = np.nan

    # If file missing, no changes
    if not outpass_csv_path or (not os.path.exists(outpass_csv_path)):
        cols = [c for c in out.columns if c != "TOTPASSHRS"] + ["TOTPASSHRS"]
        return out[cols]

    # Need COMCODE in out to match
    if "COMCODE" not in out.columns:
        cols = [c for c in out.columns if c != "TOTPASSHRS"] + ["TOTPASSHRS"]
        return out[cols]

    # Robust CSV read for older pandas (no on_bad_lines)
    pass_df = pd.read_csv(
        outpass_csv_path,
        dtype=str,
        skipinitialspace=True,
        engine="python",
        error_bad_lines=False,   # old pandas
        warn_bad_lines=False     # old pandas
    )
    pass_df.columns = [c.strip().lower() for c in pass_df.columns]

    required = {"empcode", "comcode", "passdate", "totpasshrs"}
    if not required.issubset(set(pass_df.columns)):
        cols = [c for c in out.columns if c != "TOTPASSHRS"] + ["TOTPASSHRS"]
        return out[cols]

    # Clean
    pass_df["empcode"] = pass_df["empcode"].astype(str).str.strip().str.replace('"', "")
    pass_df["comcode"] = pass_df["comcode"].astype(str).str.strip().str.replace('"', "")
    pass_df["totpasshrs"] = pass_df["totpasshrs"].astype(str).str.strip().str.replace('"', "")

    # OUTPASS passdate is like 15/11/2025 (dayfirst)
    pass_df["passdate"] = pd.to_datetime(pass_df["passdate"], dayfirst=True, errors="coerce").dt.date
    pass_df = pass_df.dropna(subset=["passdate"])

    # If multiple passes for same emp+com+date, keep last
    pass_df = pass_df.drop_duplicates(subset=["empcode", "comcode", "passdate"], keep="last")

    # Build map (empcode, comcode, passdate) -> totpasshrs
    key_to_passhrs = {
        (r.empcode, r.comcode, r.passdate): r.totpasshrs
        for r in pass_df[["empcode", "comcode", "passdate", "totpasshrs"]].itertuples(index=False)
    }

    out_token = out["TOKEN"].astype(str).str.strip()
    out_com = out["COMCODE"].astype(str).str.strip()
    out_pdate = pd.to_datetime(out["PDATE"], errors="coerce").dt.date

    # IMPORTANT: keep keys as pure python tuples (do NOT convert to numpy arrays)
    keys = list(zip(out_token.tolist(), out_com.tolist(), out_pdate.tolist()))

    # matched_mask aligned to out.index
    matched_mask = pd.Series([k in key_to_passhrs for k in keys], index=out.index)

    # Fill TOTPASSHRS aligned, without numpy (avoids "unhashable numpy.ndarray")
    out.loc[matched_mask, "TOTPASSHRS"] = [
        key_to_passhrs[keys[i]]
        for i in range(len(keys))
        if matched_mask.iat[i]
    ]

    # Apply overrides
    if "REMARKS" not in out.columns:
        out["REMARKS"] = ""
    out["REMARKS"] = out["REMARKS"].fillna("").astype(str)
    out.loc[matched_mask, "REMARKS"] = "@"

    out.loc[
        matched_mask & out["PUNCH_STATUS"].astype(str).str.strip().str.upper().eq("HD"),
        "PUNCH_STATUS",
    ] = "PR"

    # Put TOTPASSHRS at the end
    cols = [c for c in out.columns if c != "TOTPASSHRS"] + ["TOTPASSHRS"]
    return out[cols]


# -------------------- Mode-1 last day helper --------------------
def mode_1_last_day(gseldate, start_date, end_date, start_date_str, end_date_str, table_paths):
    gseldate_date_format = datetime.strptime(gseldate, "%Y-%m-%d")
    next_day = gseldate_date_format + timedelta(days=1)
    is_last_day = gseldate_date_format.month != next_day.month

    if is_last_day:
        punches_table_new_month = DBF(table_paths["punches_dbf_path"], load=True)
        punches_df_new_month = pd.DataFrame(iter(punches_table_new_month))
        punches_df_new_month["DEL"] = False

        end_date_dt = pd.to_datetime(end_date)
        end_date_date = end_date_dt.date()
        end_date_plus1_date = (end_date_dt + timedelta(days=1)).date()

        punches_df_new_month["PDATE"] = pd.to_datetime(punches_df_new_month["PDATE"]).dt.date

        filtered_df = punches_df_new_month.loc[
            punches_df_new_month["PDATE"].between(end_date_date, end_date_plus1_date)
        ].copy()

        filtered_df = filtered_df.sort_values(by=["TOKEN", "PDATE", "PDTIME"])
        matching = []
        for token, group in filtered_df.groupby("TOKEN"):
            group = group.reset_index(drop=True)
            for i in range(len(group) - 1):
                if (
                    group.loc[i, "PDATE"] == end_date_date
                    and group.loc[i, "MODE"] == 0
                    and group.loc[i + 1, "PDATE"] == end_date_plus1_date
                    and group.loc[i + 1, "MODE"] == 1
                ):
                    matching.append(group.loc[i])
                    matching.append(group.loc[i + 1])

        df = pd.DataFrame(matching)
        if not df.empty:
            mode_1_only = df.loc[df["MODE"] == 1].copy()
            mode_1_only["DEL"] = False
        else:
            mode_1_only = pd.DataFrame(
                columns=["TOKEN", "COMCODE", "PDATE", "HOURS", "MINUTES", "MODE", "PDTIME", "MCIP", "DEL"]
            )

        mode_1_only.to_csv("mode_1_only_df_check_punches.csv", index=False)

    else:
        mode_1_only = pd.DataFrame(
            columns=["TOKEN", "COMCODE", "PDATE", "HOURS", "MINUTES", "MODE", "PDTIME", "MCIP", "DEL"]
        )

    date_range = pd.date_range(start=start_date_str, end=end_date_str, freq="D")
    return mode_1_only, date_range


# -------------------- Main punch generator --------------------
def generate_punch(punches_df, muster_df, g_current_path):
    table_paths = file_paths(g_current_path)

    with open(table_paths["gsel_date_path"]) as file:
        f = [x.strip() for x in file.readlines()]
    gseldate = f[0]
    ghalf_day = int(f[1])
    gfull_day = int(f[2])

    shinfo = pd.read_csv(table_paths["shiftmast_csv_path"])
    shinfo["shcode"] = shinfo["shcode"].astype(str).str.strip().str.upper()
    shinfo["shift_st_time"] = shinfo["shift_st"].apply(hhmm_from_float)
    shinfo["shift_ed_time"] = shinfo["shift_ed"].apply(hhmm_from_float)
    shinfo["workhrs_minutes"] = (shinfo["workhrs"] * 60).astype(int)
    shinfo["halfday_minutes"] = ((shinfo["workhrs"] / 2) * 60).astype(int)

    # inc_grt -> minutes
    shinfo["inc_grt"] = pd.to_numeric(shinfo["inc_grt"], errors="coerce").fillna(0)
    hrs = shinfo["inc_grt"].astype(int)
    mins = ((shinfo["inc_grt"] - hrs) * 100).round().astype(int)
    shinfo["inc_grt_minutes"] = hrs * 60 + mins

    # gratime -> minutes
    if "gratime" in shinfo.columns:
        shinfo["gratime_minutes"] = shinfo["gratime"].apply(float_hhmm_to_minutes)
    else:
        shinfo["gratime_minutes"] = 0

    shinfo_unique = shinfo.drop_duplicates(subset=["shcode"])
    shift_minutes = shinfo_unique.set_index("shcode")[["workhrs_minutes", "halfday_minutes"]].to_dict("index")

    shift_merge_info = shinfo_unique[
        ["shcode", "inc_grt_minutes", "gratime_minutes", "workhrs_minutes", "halfday_minutes", "workhrs",
         "shift_st_time", "shift_ed_time"]
    ].copy()

    muster_df = muster_df.copy()
    muster_df["PDATE"] = pd.to_datetime(muster_df["PDATE"]).dt.date
    muster_df["SHIFT_STATUS"] = muster_df["SHIFT_STATUS"].astype(str).str.strip().str.upper()
    if "STATUS" in muster_df.columns:
        muster_df["STATUS"] = muster_df["STATUS"].astype(str).str.upper()

    day_shift_map = muster_df.set_index(["TOKEN", "PDATE"])["SHIFT_STATUS"].to_dict()

    def thresholds(token, pdate):
        sc = day_shift_map.get((token, pdate))
        if sc in shift_minutes:
            return shift_minutes[sc]["workhrs_minutes"], shift_minutes[sc]["halfday_minutes"]
        return gfull_day, ghalf_day

    dated = DBF(table_paths["dated_dbf_path"], load=True)
    start_date = dated.records[0]["MUFRDATE"]
    end_date = dated.records[0]["MUTODATE"]

    mode1, date_range = mode_1_last_day(
        gseldate,
        start_date,
        end_date,
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
        table_paths,
    )

    punches_df = punches_df.copy()
    punches_df["PDATE"] = pd.to_datetime(punches_df["PDATE"]).dt.date

    if not mode1.empty:
        mode1 = mode1.copy()
        mode1["PDATE"] = pd.to_datetime(mode1["PDATE"]).dt.date
        punches_df = pd.concat([punches_df, mode1], ignore_index=True)

    punches_df = punches_df.sort_values(by=["TOKEN", "PDATE", "PDTIME"])

    punch_df = pd.DataFrame(
        columns=[
            "TOKEN", "COMCODE", "PDATE",
            "INTIME1", "OUTTIME1", "INTIME2", "OUTTIME2", "INTIME3", "OUTTIME3", "INTIME4", "OUTTIME4",
            "INTIME", "OUTTIME", "TOTALTIME", "PUNCH_STATUS", "REMARKS", "OT",
        ]
    )

    in_time = None
    out_time = None

    for _, row in punches_df.iterrows():
        if row["MODE"] == 0:
            in_time = pd.to_datetime(row["PDTIME"]).replace(second=0)
        elif row["MODE"] == 1:
            out_time = pd.to_datetime(row["PDTIME"]).replace(second=0)

            if in_time is not None:
                diff = out_time - in_time
                if diff.total_seconds() > 0:
                    minutes = int(diff.total_seconds() // 60)

                    full_m, half_m = thresholds(row["TOKEN"], in_time.date())
                    if minutes >= full_m:
                        st = "PR"
                    elif minutes <= half_m:
                        st = "AB"
                    else:
                        st = "HD"

                    otm = max(0, minutes - full_m)
                    ot_str = hhmm(otm)

                    pdate_str = in_time.strftime("%Y-%m-%d")

                    exists = punch_df[
                        (punch_df["TOKEN"] == row["TOKEN"]) &
                        (punch_df["PDATE"] == pdate_str)
                    ]

                    if exists.empty:
                        punch_df = pd.concat(
                            [
                                punch_df,
                                pd.DataFrame(
                                    {
                                        "TOKEN": [row["TOKEN"]],
                                        "COMCODE": [row.get("COMCODE", np.nan)],
                                        "PDATE": [pdate_str],
                                        "INTIME1": [in_time.strftime("%Y-%m-%d %H:%M")],
                                        "OUTTIME1": [out_time.strftime("%Y-%m-%d %H:%M")],
                                        "INTIME2": [np.nan],
                                        "OUTTIME2": [np.nan],
                                        "INTIME3": [np.nan],
                                        "OUTTIME3": [np.nan],
                                        "INTIME4": [np.nan],
                                        "OUTTIME4": [np.nan],
                                        "INTIME": [in_time.strftime("%Y-%m-%d %H:%M")],
                                        "OUTTIME": [out_time.strftime("%Y-%m-%d %H:%M")],
                                        "TOTALTIME": [hhmm(minutes)],
                                        "PUNCH_STATUS": [st],
                                        "REMARKS": ["" if diff.days == 0 else "#"],
                                        "OT": [ot_str],
                                    }
                                ),
                            ],
                            ignore_index=True,
                        )
                    else:
                        idx = exists.index[-1]
                        for col_in, col_out in [("INTIME2", "OUTTIME2"), ("INTIME3", "OUTTIME3"), ("INTIME4", "OUTTIME4")]:
                            if pd.isna(punch_df.loc[idx, col_in]):
                                punch_df.loc[idx, col_in] = in_time.strftime("%Y-%m-%d %H:%M")
                                punch_df.loc[idx, col_out] = out_time.strftime("%Y-%m-%d %H:%M")
                                break

                        punch_df.loc[idx, "OUTTIME"] = out_time.strftime("%Y-%m-%d %H:%M")

                        total = pd.to_timedelta(0)
                        for cin, cout in [("INTIME1", "OUTTIME1"), ("INTIME2", "OUTTIME2"),
                                          ("INTIME3", "OUTTIME3"), ("INTIME4", "OUTTIME4")]:
                            if not pd.isna(punch_df.loc[idx, cin]) and not pd.isna(punch_df.loc[idx, cout]):
                                total += pd.to_datetime(punch_df.loc[idx, cout]) - pd.to_datetime(punch_df.loc[idx, cin])

                        tm = int(total.total_seconds() // 60)
                        full_m2, half_m2 = thresholds(row["TOKEN"], in_time.date())
                        if tm >= full_m2:
                            st2 = "PR"
                        elif tm <= half_m2:
                            st2 = "AB"
                        else:
                            st2 = "HD"

                        ot2 = max(0, tm - full_m2)

                        punch_df.loc[idx, "TOTALTIME"] = hhmm(tm)
                        punch_df.loc[idx, "PUNCH_STATUS"] = st2
                        punch_df.loc[idx, "OT"] = hhmm(ot2)
                        punch_df.loc[idx, "REMARKS"] = "#" if total.days > 0 else "*"

    # Ensure AB for missing days for each token
    for token in muster_df["TOKEN"].unique():
        token_df = punch_df[punch_df["TOKEN"] == token]
        for d in date_range:
            ds = d.strftime("%Y-%m-%d")
            if not ((token_df["PDATE"] == ds) & (token_df["TOKEN"] == token)).any():
                punch_df = pd.concat(
                    [
                        punch_df,
                        pd.DataFrame(
                            {
                                "TOKEN": [token],
                                "COMCODE": [np.nan],
                                "PDATE": [ds],
                                "INTIME1": [np.nan],
                                "OUTTIME1": [np.nan],
                                "INTIME2": [np.nan],
                                "OUTTIME2": [np.nan],
                                "INTIME3": [np.nan],
                                "OUTTIME3": [np.nan],
                                "INTIME4": [np.nan],
                                "OUTTIME4": [np.nan],
                                "INTIME": [np.nan],
                                "OUTTIME": [np.nan],
                                "TOTALTIME": [np.nan],
                                "PUNCH_STATUS": ["AB"],
                                "REMARKS": [np.nan],
                                "OT": [""],
                            }
                        ),
                    ],
                    ignore_index=True,
                )

    punch_df.to_csv(table_paths["punch_csv_path"], index=False)

    # Merge muster shift info
    shift_cols = ["TOKEN", "PDATE", "SHIFT_STATUS", "STATUS"] if "STATUS" in muster_df.columns else ["TOKEN", "PDATE", "SHIFT_STATUS"]
    muster_merge = muster_df[shift_cols].copy()
    muster_merge["PDATE"] = pd.to_datetime(muster_merge["PDATE"])

    punch_df["PDATE"] = pd.to_datetime(punch_df["PDATE"])
    out = punch_df.merge(muster_merge, on=["TOKEN", "PDATE"], how="left")

    out = out.merge(shift_merge_info, left_on="SHIFT_STATUS", right_on="shcode", how="left")
    out = safe_drop(out, ["shcode"])

    if "COMCODE" not in out.columns:
        out["COMCODE"] = np.nan

    out["INTIME"] = pd.to_datetime(out["INTIME"], errors="coerce")
    out["OUTTIME"] = pd.to_datetime(out["OUTTIME"], errors="coerce")

    valid = ~(out["INTIME"].isna() & out["OUTTIME"].isna())
    secs = (out.loc[valid, "OUTTIME"] - out.loc[valid, "INTIME"]).dt.total_seconds().clip(lower=0)
    mins_actual = (secs // 60).astype(int)

    fullm = out.loc[valid, "workhrs_minutes"].fillna(gfull_day).astype(int)
    halfm = out.loc[valid, "halfday_minutes"].fillna(ghalf_day).astype(int)

    base_status = np.where(mins_actual >= fullm, "PR", np.where(mins_actual <= halfm, "AB", "HD"))
    out.loc[valid, "PUNCH_STATUS"] = base_status
    out.loc[valid, "TOTAL_HRS"] = [hhmm(m) for m in mins_actual]
    out.loc[valid, "TOTALTIME"] = out.loc[valid, "TOTAL_HRS"]

    otm = np.maximum(mins_actual - fullm, 0)
    incm = out.loc[valid, "inc_grt_minutes"].fillna(0).astype(int)
    otm = np.where(otm < incm, 0, otm)
    out.loc[valid, "OT"] = [hhmm(m) for m in otm]

    # GRATIME promotion
    base_status_s = pd.Series(base_status, index=out.loc[valid].index)
    grace_m = out.loc[valid, "gratime_minutes"].fillna(0).astype(int)
    mins_with_grace = (mins_actual + grace_m).astype(int)

    if "REMARKS" not in out.columns:
        out["REMARKS"] = ""

    out["REMARKS"] = out["REMARKS"].fillna("").astype(str)
    out.loc[out["REMARKS"].str.strip().eq("&"), "REMARKS"] = ""

    promote_mask = (base_status_s.eq("HD")) & (mins_actual < fullm) & (mins_with_grace >= fullm)
    promoted_idx = out.loc[valid].index[promote_mask]
    out.loc[promoted_idx, "PUNCH_STATUS"] = "PR"
    out.loc[promoted_idx, "REMARKS"] = "&"

    # OUTPASS override
    outpass_csv_path = os.path.join(g_current_path, "OUTPASS.csv")
    out = apply_outpass_override(out, outpass_csv_path)

    # MM override (final)
    mask_mm = (
        out.get("STATUS", pd.Series(index=out.index, dtype="object"))
        .astype(str)
        .str.strip()
        .str.upper()
        .eq("MM")
    )
    out.loc[mask_mm, "TOTALTIME"] = ""
    out.loc[mask_mm, "OT"] = ""
    out.loc[mask_mm, "REMARKS"] = ""
    if "TOTAL_HRS" in out.columns:
        out.loc[mask_mm, "TOTAL_HRS"] = ""
    if "TOTPASSHRS" in out.columns:
        out.loc[mask_mm, "TOTPASSHRS"] = ""

    out = out.sort_values(by=["TOKEN", "PDATE"])
    out.to_csv(table_paths["punch_csv_path"], index=False)
    return out



